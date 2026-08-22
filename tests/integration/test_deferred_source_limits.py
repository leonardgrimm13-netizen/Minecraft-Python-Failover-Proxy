from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import replace
from typing import Any, TypeVar

import pytest

import mc_failover.proxy as proxy_module
from mc_failover.config import AppConfig
from mc_failover.models import RejectionReason
from mc_failover.proxy_protocol import (
    build_proxy_unknown_header,
    build_proxy_v1_header,
    build_proxy_v2_local_header,
)
from tests.conftest import make_config
from tests.integration.test_proxy_and_relay import ProxyHarness

_T = TypeVar("_T")


def run(coroutine: Coroutine[Any, Any, _T]) -> _T:
    return asyncio.run(coroutine)


class FakeTransport:
    def __init__(self) -> None:
        self.abort_calls = 0

    def abort(self) -> None:
        self.abort_calls += 1


class FakeWriter:
    def __init__(self, peer_ip: str = "127.0.0.1", peer_port: int = 40_000) -> None:
        self.peer_ip = peer_ip
        self.peer_port = peer_port
        self.closed = False
        self.wait_closed_calls = 0
        self.transport = FakeTransport()

    def get_extra_info(self, name: str) -> object:
        if name == "peername":
            return (self.peer_ip, self.peer_port)
        if name == "sockname":
            return ("127.0.0.1", 25_565)
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.wait_closed_calls += 1


def reader_with(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


def limited_proxy_config(
    *,
    version: int = 1,
    per_ip: int = 1,
    rate: float = 0.0,
    burst: int = 0,
) -> AppConfig:
    config = make_config(25_565, 25_566)
    return replace(
        config,
        connection=replace(
            config.connection,
            max_connections=16,
            max_connections_per_ip=per_ip,
            new_connections_per_second=rate,
            new_connections_burst=burst,
        ),
        proxy_protocol=replace(
            config.proxy_protocol,
            accept=True,
            version=version,
            accept_version=version,
            trusted_proxy_ips=("127.0.0.0/8",),
        ),
    )


def install_blocking_upstream(
    monkeypatch: pytest.MonkeyPatch,
    harness: ProxyHarness,
) -> tuple[asyncio.Queue[None], asyncio.Event]:
    entered: asyncio.Queue[None] = asyncio.Queue()
    release = asyncio.Event()

    async def fake_connect(
        _target: object,
        _inbound: object,
        _client_writer: object,
    ) -> tuple[asyncio.StreamReader, FakeWriter]:
        return reader_with(b""), FakeWriter("127.0.0.1", 50_000)

    async def blocking_relay(*_args: object, **_kwargs: object) -> tuple[()]:
        entered.put_nowait(None)
        await release.wait()
        return ()

    monkeypatch.setattr(harness.proxy, "_connect", fake_connect)
    monkeypatch.setattr(proxy_module, "relay_streams", blocking_relay)
    return entered, release


async def handle(
    harness: ProxyHarness,
    header: bytes,
    *,
    peer_port: int,
) -> FakeWriter:
    writer = FakeWriter(peer_port=peer_port)
    await harness.proxy.handle_client(reader_with(header), writer)  # type: ignore[arg-type]
    return writer


def test_proxy_sources_behind_one_trusted_peer_use_separate_per_ip_buckets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        harness = ProxyHarness(limited_proxy_config(per_ip=1))
        entered, release = install_blocking_upstream(monkeypatch, harness)
        first_header = build_proxy_v1_header("203.0.113.10", "198.51.100.1", 40_001, 25_565)
        second_header = build_proxy_v1_header("203.0.113.11", "198.51.100.1", 40_002, 25_565)

        first = asyncio.create_task(handle(harness, first_header, peer_port=51_001))
        await asyncio.wait_for(entered.get(), timeout=0.5)
        second = asyncio.create_task(handle(harness, second_header, peer_port=51_002))
        await asyncio.wait_for(entered.get(), timeout=0.5)

        assert harness.limiter.active == 2
        assert harness.limiter.tracked_active_ips == 2
        assert harness.runtime.active_connections == 2
        assert harness.runtime.rejected_connections == 0

        release.set()
        first_writer, second_writer = await asyncio.gather(first, second)
        assert first_writer.closed and second_writer.closed
        assert harness.limiter.active == 0
        assert harness.limiter.tracked_active_ips == 0
        assert harness.runtime.active_connections == 0

    run(scenario())


def test_same_proxy_source_is_limited_even_when_socket_peer_ports_differ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        harness = ProxyHarness(limited_proxy_config(per_ip=1))
        entered, release = install_blocking_upstream(monkeypatch, harness)
        header = build_proxy_v1_header("203.0.113.20", "198.51.100.1", 40_001, 25_565)

        first = asyncio.create_task(handle(harness, header, peer_port=52_001))
        await asyncio.wait_for(entered.get(), timeout=0.5)
        rejected_writer = await asyncio.wait_for(
            handle(harness, header, peer_port=52_002), timeout=0.5
        )

        assert rejected_writer.closed
        assert harness.runtime.rejection_reasons[RejectionReason.PER_IP_LIMIT] == 1
        assert harness.runtime.rejected_connections == 1
        assert harness.runtime.backend_connections_established_total == 1
        assert harness.runtime.total_connections == 2
        assert harness.runtime.active_connections == 1
        assert harness.limiter.active == 1
        assert harness.limiter.tracked_active_ips == 1
        assert entered.empty()

        release.set()
        first_writer = await first
        assert first_writer.closed
        assert harness.runtime.active_connections == 0
        assert harness.limiter.active == 0
        assert harness.limiter.tracked_active_ips == 0

    run(scenario())


@pytest.mark.parametrize(
    ("version", "unknown_header"),
    [
        (1, build_proxy_unknown_header()),
        (2, build_proxy_v2_local_header()),
    ],
)
def test_unknown_and_local_headers_fall_back_to_the_socket_peer_bucket(
    monkeypatch: pytest.MonkeyPatch,
    version: int,
    unknown_header: bytes,
) -> None:
    async def scenario() -> None:
        harness = ProxyHarness(limited_proxy_config(version=version, per_ip=1))
        entered, release = install_blocking_upstream(monkeypatch, harness)

        first = asyncio.create_task(handle(harness, unknown_header, peer_port=53_001))
        await asyncio.wait_for(entered.get(), timeout=0.5)
        rejected_writer = await asyncio.wait_for(
            handle(harness, unknown_header, peer_port=53_002), timeout=0.5
        )

        assert rejected_writer.closed
        assert harness.runtime.rejection_reasons[RejectionReason.PER_IP_LIMIT] == 1
        assert harness.limiter.active == 1
        assert harness.limiter.tracked_active_ips == 1

        release.set()
        await first
        assert harness.limiter.active == 0
        assert harness.limiter.tracked_active_ips == 0

    run(scenario())


def test_invalid_proxy_header_is_charged_to_socket_peer_rate_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        harness = ProxyHarness(limited_proxy_config(per_ip=0, rate=1.0, burst=1))
        entered, release = install_blocking_upstream(monkeypatch, harness)

        invalid_writer = await handle(harness, b"BROKEN\r\n", peer_port=54_001)
        assert invalid_writer.closed
        assert harness.runtime.rejection_reasons[RejectionReason.INVALID_PROXY_HEADER] == 1
        assert harness.limiter.active == 0
        assert harness.limiter.tracked_active_ips == 0
        assert harness.limiter.tracked_rate_ips == 1

        unknown_writer = await handle(
            harness,
            build_proxy_unknown_header(),
            peer_port=54_002,
        )
        assert unknown_writer.closed
        assert harness.runtime.rejection_reasons[RejectionReason.RATE_LIMIT] == 1
        assert entered.empty()
        assert not release.is_set()
        assert harness.limiter.active == 0

    run(scenario())
