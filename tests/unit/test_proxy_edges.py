from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from typing import cast

import pytest

import mc_failover.proxy as proxy_module
from mc_failover.circuit_breaker import CircuitBreaker
from mc_failover.health import HealthState
from mc_failover.limits import ConnectionLimiter
from mc_failover.models import (
    HealthCheckResult,
    MaintenanceMode,
    RejectionReason,
    RoutingReason,
    Target,
    TargetName,
)
from mc_failover.proxy import ProxyServer
from mc_failover.proxy_protocol import ProxyProtocolInfo
from mc_failover.routing import MaintenanceWatcher, Router, RoutingDecision
from mc_failover.runtime import RuntimeState
from tests.conftest import make_config


class FakeTransport:
    def __init__(self) -> None:
        self.aborted = False

    def abort(self) -> None:
        self.aborted = True


class FakeWriter:
    def __init__(
        self,
        *,
        peer: object = ("127.0.0.1", 12345),
        local: object = ("127.0.0.1", 25565),
        drain_error: BaseException | None = None,
    ) -> None:
        self.peer = peer
        self.local = local
        self.drain_error = drain_error
        self.transport = FakeTransport()
        self.written = bytearray()
        self.closed = False

    def get_extra_info(self, name: str) -> object | None:
        if name == "peername":
            return self.peer
        if name == "sockname":
            return self.local
        return None

    def write(self, data: bytes) -> None:
        self.written.extend(data)

    async def drain(self) -> None:
        if self.drain_error is not None:
            raise self.drain_error

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


class FakeListener:
    def __init__(self, *, wait_forever: bool = False) -> None:
        self.closed = False
        self.wait_forever = wait_forever

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        if self.wait_forever:
            await asyncio.Event().wait()


class ProxyHarness:
    def __init__(self, *, main_ok: bool = True, fallback_ok: bool = True) -> None:
        self.config = make_config(25565, 25566)
        self.runtime = RuntimeState()
        self.main = HealthState(TargetName.MAIN, self.config.healthcheck)
        self.fallback = HealthState(TargetName.FALLBACK, self.config.fallback_healthcheck)
        self.main.report(HealthCheckResult(main_ok, "initial"), initial=True)
        self.fallback.report(HealthCheckResult(fallback_ok, "initial"), initial=True)
        self.circuit = CircuitBreaker(self.config.circuit_breaker)
        self.watcher = MaintenanceWatcher(self.config.maintenance)
        self.router = Router(self.config, self.main, self.fallback, self.circuit, self.watcher)
        self.limiter = ConnectionLimiter(
            self.config.connection.max_connections,
            self.config.connection.max_connections_per_ip,
            self.config.connection.new_connections_per_second,
            self.config.connection.new_connections_burst,
        )
        self.proxy = ProxyServer(
            self.config,
            self.runtime,
            self.router,
            self.main,
            self.fallback,
            self.circuit,
            self.limiter,
        )

    def update_config(self, config) -> None:
        self.config = config
        self.proxy.config = config
        self.router.config = config


def stream_writer(writer: FakeWriter) -> asyncio.StreamWriter:
    return cast(asyncio.StreamWriter, writer)


def decision(
    target: Target | None,
    *,
    mode: MaintenanceMode = MaintenanceMode.AUTO,
    permit=None,
) -> RoutingDecision:
    return RoutingDecision(
        target=target,
        requested_target=target.name if target is not None else TargetName.NONE,
        reason=(
            RoutingReason.MAIN_HEALTHY if target is not None else RoutingReason.NO_TARGET_AVAILABLE
        ),
        maintenance_mode=mode,
        maintenance_source="test",
        ready=target is not None,
        degraded=target is None,
        circuit_permit=permit,
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("host", None),
        (("host",), None),
        ((1, 2), None),
        (("host", True), None),
        (("host", "2"), None),
        (("host", 2), ("host", 2)),
        (("host", 2, 3), ("host", 2)),
    ],
)
def test_endpoint_validation(value: object, expected: tuple[str, int] | None) -> None:
    assert proxy_module._endpoint(value) == expected


def test_proxy_protocol_version_selection_and_outbound_header_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ProxyHarness()
    config = replace(
        harness.config,
        proxy_protocol=replace(
            harness.config.proxy_protocol,
            version=1,
            accept_version=2,
            send_version=2,
        ),
    )
    harness.update_config(config)
    assert proxy_module._accept_version(config) == 2
    assert proxy_module._send_version(config) == 2

    writer = FakeWriter()
    inbound = ProxyProtocolInfo("192.0.2.1", "198.51.100.2", 1234, 25565, "INET")
    header = harness.proxy._outbound_proxy_header(inbound, stream_writer(writer))
    assert header.startswith(b"\r\n\r\n\x00\r\nQUIT\n")

    unknown = ProxyProtocolInfo("", "", 0, 0, "UNKNOWN", command="LOCAL")
    assert harness.proxy._outbound_proxy_header(unknown, stream_writer(writer)).startswith(
        b"\r\n\r\n\x00\r\nQUIT\n"
    )

    no_endpoint = FakeWriter(peer=None)
    assert harness.proxy._outbound_proxy_header(None, stream_writer(no_endpoint)).startswith(
        b"\r\n\r\n\x00\r\nQUIT\n"
    )

    def invalid_header(*_args: object, **_kwargs: object) -> bytes:
        raise ValueError("mixed families")

    monkeypatch.setattr(proxy_module, "build_proxy_header_for_version", invalid_header)
    assert harness.proxy._outbound_proxy_header(None, stream_writer(writer)).startswith(
        b"\r\n\r\n\x00\r\nQUIT\n"
    )


@pytest.mark.asyncio
async def test_accept_shutdown_and_closed_tracker_races() -> None:
    harness = ProxyHarness()
    reader = asyncio.StreamReader()
    harness.runtime.shutting_down = True
    writer = FakeWriter()
    harness.proxy._accepted(reader, stream_writer(writer))
    assert writer.closed
    assert harness.runtime.incoming_connections_total == 1
    assert harness.runtime.rejection_reasons[RejectionReason.SHUTTING_DOWN] == 1

    harness.runtime.shutting_down = False
    await harness.proxy.tasks.cancel_all(0)
    raced = FakeWriter()
    harness.proxy._accepted(reader, stream_writer(raced))
    assert raced.closed
    assert stream_writer(raced) not in harness.proxy._writers
    assert harness.runtime.incoming_connections_total == 2
    assert harness.runtime.rejection_reasons[RejectionReason.SHUTTING_DOWN] == 2


@pytest.mark.asyncio
async def test_listener_stop_wait_timeout_and_shutdown_remaining_writers() -> None:
    harness = ProxyHarness()
    listener = FakeListener()
    harness.proxy.server = cast(asyncio.Server, listener)
    await harness.proxy.stop_accepting()
    assert listener.closed
    await harness.proxy.wait_listener_closed()
    await harness.proxy.stop_accepting()
    await harness.proxy.wait_listener_closed()

    timeout_listener = FakeListener(wait_forever=True)
    harness.proxy._closing_server = cast(asyncio.Server, timeout_listener)
    harness.update_config(
        replace(
            harness.config,
            connection=replace(harness.config.connection, shutdown_cancel_timeout_seconds=0.001),
        )
    )
    await harness.proxy.wait_listener_closed()

    harness = ProxyHarness()
    remaining = FakeWriter()
    harness.proxy._writers.add(stream_writer(remaining))
    lease, rejection = await harness.limiter.try_acquire("127.0.0.1")
    assert lease is not None and rejection is None
    await harness.proxy.shutdown_connections()
    assert remaining.closed
    assert harness.limiter.active == 1
    await harness.limiter.release(lease)


@pytest.mark.asyncio
async def test_inbound_proxy_disabled_untrusted_and_delegated_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ProxyHarness()
    reader = asyncio.StreamReader()
    assert await harness.proxy._read_inbound_proxy(reader, "127.0.0.1") is None

    harness.update_config(
        replace(
            harness.config,
            proxy_protocol=replace(
                harness.config.proxy_protocol,
                accept=True,
                trusted_proxy_ips=("192.0.2.0/24",),
            ),
        )
    )
    with pytest.raises(PermissionError):
        await harness.proxy._read_inbound_proxy(reader, "127.0.0.1")

    expected = ProxyProtocolInfo("203.0.113.1", "127.0.0.1", 4000, 25565, "INET")

    async def fake_read(
        version: int,
        actual_reader: asyncio.StreamReader,
        timeout: float,
        *,
        max_header_bytes: int,
    ) -> ProxyProtocolInfo:
        assert version == 1
        assert actual_reader is reader
        assert timeout > 0 and max_header_bytes > 0
        return expected

    harness.update_config(
        replace(
            harness.config,
            proxy_protocol=replace(
                harness.config.proxy_protocol,
                trusted_proxy_ips=("127.0.0.1",),
            ),
        )
    )
    monkeypatch.setattr(proxy_module, "read_proxy_header_for_version", fake_read)
    assert await harness.proxy._read_inbound_proxy(reader, "127.0.0.1") == expected


@pytest.mark.asyncio
async def test_connect_sends_proxy_header_and_cleans_up_write_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ProxyHarness()
    harness.update_config(
        replace(
            harness.config,
            proxy_protocol=replace(harness.config.proxy_protocol, send=True),
        )
    )
    upstream_reader = asyncio.StreamReader()
    upstream_writer = FakeWriter()

    async def open_success(*_args: object, **_kwargs: object):
        return upstream_reader, stream_writer(upstream_writer)

    monkeypatch.setattr(asyncio, "open_connection", open_success)
    target = Target(TargetName.MAIN, "example.invalid", 25565)
    returned_reader, returned_writer = await harness.proxy._connect(
        target, None, stream_writer(FakeWriter())
    )
    assert returned_reader is upstream_reader
    assert returned_writer is stream_writer(upstream_writer)
    assert upstream_writer.written.startswith(b"PROXY TCP4 ")
    assert returned_writer in harness.proxy._writers

    failing_writer = FakeWriter(drain_error=BrokenPipeError("gone"))

    async def open_failing(*_args: object, **_kwargs: object):
        return asyncio.StreamReader(), stream_writer(failing_writer)

    monkeypatch.setattr(asyncio, "open_connection", open_failing)
    with pytest.raises(BrokenPipeError):
        await harness.proxy._connect(target, None, stream_writer(FakeWriter()))
    assert failing_writer.closed
    assert stream_writer(failing_writer) not in harness.proxy._writers


@pytest.mark.asyncio
async def test_fallback_guards_connect_error_success_and_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ProxyHarness()
    main_target = harness.router.target(TargetName.MAIN)
    client = stream_writer(FakeWriter())
    forced = decision(main_target, mode=MaintenanceMode.FORCE_MAIN)
    assert await harness.proxy._fallback_after_main_failure(forced, None, client) is None

    harness.update_config(
        replace(
            harness.config,
            connection=replace(
                harness.config.connection,
                connect_fallback_on_main_connect_failure=False,
            ),
        )
    )
    assert (
        await harness.proxy._fallback_after_main_failure(decision(main_target), None, client)
        is None
    )

    harness.update_config(
        replace(
            harness.config,
            connection=replace(
                harness.config.connection,
                connect_fallback_on_main_connect_failure=True,
            ),
        )
    )

    async def fail_connect(*_args: object, **_kwargs: object):
        raise OSError("unreachable")

    monkeypatch.setattr(harness.proxy, "_connect", fail_connect)
    assert (
        await harness.proxy._fallback_after_main_failure(decision(main_target), None, client)
        is None
    )
    assert harness.runtime.fallback_connect_failures == 1

    async def cancel_connect(*_args: object, **_kwargs: object):
        raise asyncio.CancelledError

    monkeypatch.setattr(harness.proxy, "_connect", cancel_connect)
    with pytest.raises(asyncio.CancelledError):
        await harness.proxy._fallback_after_main_failure(decision(main_target), None, client)

    upstream_reader = asyncio.StreamReader()
    upstream_writer = stream_writer(FakeWriter())

    async def succeed_connect(*_args: object, **_kwargs: object):
        return upstream_reader, upstream_writer

    monkeypatch.setattr(harness.proxy, "_connect", succeed_connect)
    result = await harness.proxy._fallback_after_main_failure(decision(main_target), None, client)
    assert result == (harness.router.target(TargetName.FALLBACK), upstream_reader, upstream_writer)
    assert harness.runtime.fallback_connect_successes == 1


async def harmless_close(writer: asyncio.StreamWriter | None, *, timeout_seconds: float) -> None:
    assert timeout_seconds >= 0
    if writer is not None:
        cast(FakeWriter, writer).close()


@pytest.mark.asyncio
async def test_handle_client_early_rejects_untrusted_invalid_and_no_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(proxy_module, "close_stream_writer", harmless_close)

    harness = ProxyHarness()
    no_peer = FakeWriter(peer=None)
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(no_peer))
    assert harness.runtime.rejection_reasons[RejectionReason.NO_TARGET] == 1
    assert no_peer.closed

    harness = ProxyHarness()
    harness.limiter.max_connections = 0
    limited = FakeWriter()
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(limited))
    assert harness.runtime.rejection_reasons[RejectionReason.GLOBAL_LIMIT] == 1

    harness = ProxyHarness()
    harness.update_config(
        replace(
            harness.config,
            proxy_protocol=replace(
                harness.config.proxy_protocol,
                accept=True,
                trusted_proxy_ips=("192.0.2.0/24",),
            ),
        )
    )
    untrusted = FakeWriter()
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(untrusted))
    assert harness.runtime.rejection_reasons[RejectionReason.UNTRUSTED_PROXY] == 1
    assert harness.runtime.total_connections == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.runtime.active_connections == 0
    assert harness.limiter.active == 0

    harness = ProxyHarness()
    harness.update_config(
        replace(
            harness.config,
            proxy_protocol=replace(
                harness.config.proxy_protocol,
                accept=True,
                trust_all_proxies=True,
            ),
        )
    )

    async def invalid_header(*_args: object, **_kwargs: object):
        raise ValueError("invalid header")

    monkeypatch.setattr(harness.proxy, "_read_inbound_proxy", invalid_header)
    invalid = FakeWriter()
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(invalid))
    assert harness.runtime.rejection_reasons[RejectionReason.INVALID_PROXY_HEADER] == 1
    assert harness.runtime.total_connections == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.runtime.active_connections == 0
    assert harness.limiter.active == 0

    harness = ProxyHarness(main_ok=False, fallback_ok=False)
    no_target = FakeWriter()
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(no_target))
    assert harness.runtime.rejection_reasons[RejectionReason.NO_TARGET] == 1
    assert harness.runtime.total_connections == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.runtime.active_connections == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "read_error",
    [
        ConnectionResetError("client reset while sending PROXY header"),
        OSError("client socket failed while sending PROXY header"),
    ],
)
async def test_proxy_header_socket_read_failure_is_debug_rejection_with_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    read_error: OSError,
) -> None:
    monkeypatch.setattr(proxy_module, "close_stream_writer", harmless_close)
    harness = ProxyHarness()
    harness.update_config(
        replace(
            harness.config,
            proxy_protocol=replace(
                harness.config.proxy_protocol,
                accept=True,
                trust_all_proxies=True,
            ),
        )
    )

    async def fail_read(*_args: object, **_kwargs: object) -> ProxyProtocolInfo | None:
        raise read_error

    monkeypatch.setattr(harness.proxy, "_read_inbound_proxy", fail_read)
    client = FakeWriter()

    with caplog.at_level(logging.DEBUG, logger="mc-failover.proxy"):
        await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(client))

    proxy_records = [record for record in caplog.records if record.name == "mc-failover.proxy"]
    assert len(proxy_records) == 1
    assert proxy_records[0].levelno == logging.DEBUG
    assert proxy_records[0].getMessage().startswith("Rejected invalid or incomplete PROXY header")
    assert proxy_records[0].exc_info is None
    assert harness.runtime.rejection_reasons[RejectionReason.INVALID_PROXY_HEADER] == 1
    assert harness.runtime.connections_rejected_total == 1
    assert harness.runtime.total_connections == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.runtime.active_connections == 0
    assert harness.limiter.active == 0
    assert harness.limiter.tracked_active_ips == 0
    assert client.closed


@pytest.mark.asyncio
async def test_handle_client_connect_cancellation_releases_permit_and_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ProxyHarness()
    monkeypatch.setattr(proxy_module, "close_stream_writer", harmless_close)

    async def cancel_connect(*_args: object, **_kwargs: object):
        raise asyncio.CancelledError

    monkeypatch.setattr(harness.proxy, "_connect", cancel_connect)
    task = asyncio.create_task(
        harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(FakeWriter()))
    )
    with pytest.raises(asyncio.CancelledError):
        await task
    assert harness.limiter.active == 0
    assert harness.runtime.active_connections == 0


@pytest.mark.asyncio
async def test_handle_client_fallback_connect_failure_and_unexpected_error_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(proxy_module, "close_stream_writer", harmless_close)
    harness = ProxyHarness(main_ok=False, fallback_ok=True)

    async def fail_connect(*_args: object, **_kwargs: object):
        raise OSError("down")

    monkeypatch.setattr(harness.proxy, "_connect", fail_connect)
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(FakeWriter()))
    assert harness.runtime.fallback_connect_failures == 1
    assert harness.runtime.rejection_reasons[RejectionReason.BACKEND_CONNECT_FAILED] == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.limiter.active == 0

    harness = ProxyHarness()

    async def explode(*_args: object, **_kwargs: object):
        raise RuntimeError("internal test error")

    monkeypatch.setattr(harness.router, "select_for_connection", explode)
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(FakeWriter()))
    assert harness.limiter.active == 0
    assert harness.runtime.active_connections == 0


@pytest.mark.asyncio
async def test_handle_client_access_logs_main_and_main_failure_fallback_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(proxy_module, "close_stream_writer", harmless_close)

    async def relay_done(*_args: object, **_kwargs: object):
        return ()

    monkeypatch.setattr(proxy_module, "relay_streams", relay_done)
    harness = ProxyHarness()
    harness.update_config(
        replace(harness.config, logging=replace(harness.config.logging, access_log=True))
    )
    upstream_writer = stream_writer(FakeWriter())

    async def connect_main(*_args: object, **_kwargs: object):
        return asyncio.StreamReader(), upstream_writer

    monkeypatch.setattr(harness.proxy, "_connect", connect_main)
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(FakeWriter()))
    assert harness.runtime.main_connect_successes == 1
    assert harness.runtime.backend_connections_established_total == 1
    assert harness.runtime.active_connections == 0

    harness = ProxyHarness()
    harness.update_config(
        replace(harness.config, logging=replace(harness.config.logging, access_log=True))
    )
    attempts = 0

    async def fail_then_fallback(*_args: object, **_kwargs: object):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("main down")
        return asyncio.StreamReader(), stream_writer(FakeWriter())

    monkeypatch.setattr(harness.proxy, "_connect", fail_then_fallback)
    await harness.proxy.handle_client(asyncio.StreamReader(), stream_writer(FakeWriter()))
    assert attempts == 2
    assert harness.runtime.main_connect_failures == 1
    assert harness.runtime.fallback_connect_successes == 1
    assert harness.runtime.backend_connections_established_total == 1
    assert harness.runtime.connections_rejected_total == 0


@pytest.mark.asyncio
async def test_handle_client_cleanup_error_cannot_leak_active_backend_gauge(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(proxy_module, "close_stream_writer", harmless_close)

    async def relay_done(*_args: object, **_kwargs: object) -> tuple[()]:
        return ()

    harness = ProxyHarness()
    upstream_writer = stream_writer(FakeWriter())

    async def connect_main(*_args: object, **_kwargs: object):
        return asyncio.StreamReader(), upstream_writer

    original_release = harness.limiter.release

    async def release_then_fail(lease) -> None:
        await original_release(lease)
        raise RuntimeError("injected cleanup failure")

    monkeypatch.setattr(proxy_module, "relay_streams", relay_done)
    monkeypatch.setattr(harness.proxy, "_connect", connect_main)
    monkeypatch.setattr(harness.limiter, "release", release_then_fail)

    with caplog.at_level(logging.ERROR, logger="mc-failover.proxy"):
        await harness.proxy.handle_client(
            asyncio.StreamReader(),
            stream_writer(FakeWriter()),
        )

    assert harness.runtime.backend_connections_established_total == 1
    assert harness.runtime.active_connections == 0
    assert harness.limiter.active == 0
    cleanup_records = [
        record
        for record in caplog.records
        if record.getMessage() == "Unexpected connection-limiter cleanup failure"
    ]
    assert len(cleanup_records) == 1
    assert cleanup_records[0].levelno == logging.ERROR
    assert cleanup_records[0].exc_info is not None


@pytest.mark.asyncio
async def test_handle_client_cancellation_during_writer_cleanup_releases_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_started = asyncio.Event()

    async def blocking_close(
        _writer: asyncio.StreamWriter | None,
        *,
        timeout_seconds: float,
    ) -> None:
        assert timeout_seconds >= 0
        cleanup_started.set()
        await asyncio.Event().wait()

    async def relay_done(*_args: object, **_kwargs: object) -> tuple[()]:
        return ()

    harness = ProxyHarness()
    upstream_writer = stream_writer(FakeWriter())
    client_writer = stream_writer(FakeWriter())
    harness.proxy._writers.update((upstream_writer, client_writer))

    async def connect_main(*_args: object, **_kwargs: object):
        return asyncio.StreamReader(), upstream_writer

    monkeypatch.setattr(proxy_module, "close_stream_writer", blocking_close)
    monkeypatch.setattr(proxy_module, "relay_streams", relay_done)
    monkeypatch.setattr(harness.proxy, "_connect", connect_main)

    task = asyncio.create_task(harness.proxy.handle_client(asyncio.StreamReader(), client_writer))
    await asyncio.wait_for(cleanup_started.wait(), timeout=1.0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert harness.runtime.backend_connections_established_total == 1
    assert harness.runtime.active_connections == 0
    assert harness.limiter.active == 0
    assert harness.proxy._writers == set()
