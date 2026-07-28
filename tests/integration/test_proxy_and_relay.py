from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from mc_failover.circuit_breaker import CircuitBreaker
from mc_failover.health import HealthState
from mc_failover.limits import ConnectionLimiter
from mc_failover.models import CircuitState, HealthCheckResult, RejectionReason, TargetName
from mc_failover.proxy import ProxyServer
from mc_failover.routing import MaintenanceWatcher, Router
from mc_failover.runtime import RuntimeState
from tests.conftest import (
    close_writer,
    closed_local_port,
    make_config,
    running_server,
    server_port,
)


class ProxyHarness:
    def __init__(self, config, *, main_ok: bool = True, fallback_ok: bool = True) -> None:
        self.config = config
        self.runtime = RuntimeState()
        self.main_health = HealthState(TargetName.MAIN, config.healthcheck)
        self.fallback_health = HealthState(TargetName.FALLBACK, config.fallback_healthcheck)
        self.main_health.report(HealthCheckResult(main_ok, "initial"), initial=True)
        self.fallback_health.report(HealthCheckResult(fallback_ok, "initial"), initial=True)
        self.circuit = CircuitBreaker(config.circuit_breaker)
        self.maintenance = MaintenanceWatcher(config.maintenance)
        self.router = Router(
            config,
            self.main_health,
            self.fallback_health,
            self.circuit,
            self.maintenance,
        )
        self.limiter = ConnectionLimiter(
            config.connection.max_connections,
            config.connection.max_connections_per_ip,
            config.connection.new_connections_per_second,
            config.connection.new_connections_burst,
        )
        self.proxy = ProxyServer(
            config,
            self.runtime,
            self.router,
            self.main_health,
            self.fallback_health,
            self.circuit,
            self.limiter,
        )

    async def start(self) -> int:
        await self.maintenance.refresh()
        server = await self.proxy.start()
        return server_port(server)

    async def stop(self) -> None:
        self.runtime.shutting_down = True
        await self.proxy.stop_accepting()
        await self.proxy.shutdown_connections()


@pytest.mark.asyncio
async def test_normal_bidirectional_forwarding_and_large_payload() -> None:
    backend_done = asyncio.Event()

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(16_384):
                writer.write(data)
                await writer.drain()
        finally:
            backend_done.set()
            await close_writer(writer)

    async with running_server(echo) as backend:
        port = server_port(backend)
        harness = ProxyHarness(make_config(port, port))
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        payload = bytes(range(256)) * 4096
        writer.write(payload)
        await writer.drain()
        assert await asyncio.wait_for(reader.readexactly(len(payload)), 5.0) == payload
        await close_writer(writer)
        await asyncio.wait_for(backend_done.wait(), 2.0)
        await harness.stop()
        assert harness.runtime.incoming_connections_total == 1
        assert harness.runtime.backend_connections_established_total == 1
        assert harness.runtime.active_connections == 0
        assert harness.proxy.tasks.active_count == 0


@pytest.mark.asyncio
async def test_client_half_close_still_receives_backend_response() -> None:
    backend_done = asyncio.Event()

    async def answer_after_eof(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request = await reader.read()
        writer.write(b"response:" + request)
        await writer.drain()
        if writer.can_write_eof():
            writer.write_eof()
        backend_done.set()
        await close_writer(writer)

    async with running_server(answer_after_eof) as backend:
        port = server_port(backend)
        harness = ProxyHarness(make_config(port, port))
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(b"request")
        await writer.drain()
        assert writer.can_write_eof()
        writer.write_eof()
        assert await asyncio.wait_for(reader.read(), 2.0) == b"response:request"
        await asyncio.wait_for(backend_done.wait(), 2.0)
        await close_writer(writer)
        await harness.stop()


@pytest.mark.asyncio
async def test_server_half_close_keeps_client_to_server_direction_open() -> None:
    received = bytearray()
    backend_done = asyncio.Event()

    async def half_close_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b"server-finished")
        await writer.drain()
        assert writer.can_write_eof()
        writer.write_eof()
        received.extend(await reader.read())
        backend_done.set()
        await close_writer(writer)

    async with running_server(half_close_server) as backend:
        port = server_port(backend)
        config = make_config(port, port)
        config = replace(
            config,
            connection=replace(config.connection, relay_drain_timeout_seconds=1.0),
        )
        harness = ProxyHarness(config)
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        assert await asyncio.wait_for(reader.read(), 2.0) == b"server-finished"
        writer.write(b"late-client-data")
        await writer.drain()
        writer.write_eof()
        await asyncio.wait_for(backend_done.wait(), 2.0)
        assert bytes(received) == b"late-client-data"
        await close_writer(writer)
        await harness.stop()


@pytest.mark.asyncio
async def test_global_idle_timeout_closes_a_silent_connection() -> None:
    backend_closed = asyncio.Event()

    async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()
        backend_closed.set()
        await close_writer(writer)

    async with running_server(silent) as backend:
        port = server_port(backend)
        config = make_config(port, port)
        config = replace(
            config,
            connection=replace(config.connection, idle_timeout_seconds=0.05),
        )
        harness = ProxyHarness(config)
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        assert await asyncio.wait_for(reader.read(), 1.0) == b""
        await asyncio.wait_for(backend_closed.wait(), 1.0)
        await close_writer(writer)
        await harness.stop()


@pytest.mark.asyncio
async def test_half_close_drain_timeout_prevents_an_endless_peer_task() -> None:
    backend_closed = asyncio.Event()

    async def never_answers(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()
        await reader.read()
        backend_closed.set()
        await close_writer(writer)

    async with running_server(never_answers) as backend:
        port = server_port(backend)
        config = make_config(port, port)
        config = replace(
            config,
            connection=replace(
                config.connection,
                idle_timeout_seconds=0.0,
                relay_drain_timeout_seconds=0.05,
            ),
        )
        harness = ProxyHarness(config)
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(b"done")
        await writer.drain()
        writer.write_eof()
        assert await asyncio.wait_for(reader.read(), 1.0) == b""
        await close_writer(writer)
        await harness.stop()


@pytest.mark.asyncio
async def test_backend_reset_is_expected_and_releases_resources() -> None:
    reset_done = asyncio.Event()

    async def reset(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(1)
        writer.transport.abort()
        reset_done.set()

    async with running_server(reset) as backend:
        port = server_port(backend)
        harness = ProxyHarness(make_config(port, port))
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(b"x")
        await writer.drain()
        await asyncio.wait_for(reset_done.wait(), 1.0)
        try:
            assert await asyncio.wait_for(reader.read(), 1.0) == b""
        except ConnectionResetError:
            pass
        await close_writer(writer)
        await harness.stop()
        assert harness.limiter.active == 0


@pytest.mark.asyncio
async def test_real_main_failures_open_circuit_and_new_clients_skip_main() -> None:
    main_port = await closed_local_port()

    async def fallback(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b"F")
        await writer.drain()
        await close_writer(writer)

    async with running_server(fallback) as backend:
        fallback_port = server_port(backend)
        config = make_config(main_port, fallback_port)
        harness = ProxyHarness(config)
        proxy_port = await harness.start()

        for _ in range(2):
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            assert await asyncio.wait_for(reader.readexactly(1), 1.0) == b"F"
            await close_writer(writer)
        assert (await harness.circuit.snapshot()).state is CircuitState.OPEN
        assert harness.runtime.main_connect_failures == 2

        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        assert await asyncio.wait_for(reader.readexactly(1), 1.0) == b"F"
        await close_writer(writer)
        assert harness.runtime.main_connect_failures == 2
        assert harness.runtime.fallback_connect_successes == 3
        await harness.stop()
        assert harness.runtime.incoming_connections_total == 3
        assert harness.runtime.backend_connections_established_total == 3
        assert harness.runtime.connections_rejected_total == 0


@pytest.mark.asyncio
async def test_both_unhealthy_rejects_without_upstream_attempt() -> None:
    port = await closed_local_port()
    harness = ProxyHarness(make_config(port, port), main_ok=False, fallback_ok=False)
    proxy_port = await harness.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    assert await asyncio.wait_for(reader.read(), 1.0) == b""
    await close_writer(writer)
    await harness.stop()
    assert harness.runtime.incoming_connections_total == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.runtime.rejected_connections == 1
    assert harness.runtime.main_connect_failures == 0
    assert harness.runtime.fallback_connect_failures == 0


@pytest.mark.asyncio
async def test_all_backend_connect_attempts_fail_as_one_client_rejection() -> None:
    unavailable = await closed_local_port()
    harness = ProxyHarness(make_config(unavailable, unavailable))
    proxy_port = await harness.start()

    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    assert await asyncio.wait_for(reader.read(), 1.0) == b""
    await close_writer(writer)
    await harness.stop()

    assert harness.runtime.incoming_connections_total == 1
    assert harness.runtime.total_connections == 1
    assert harness.runtime.main_connect_failures == 1
    assert harness.runtime.fallback_connect_failures == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.runtime.rejection_reasons[RejectionReason.BACKEND_CONNECT_FAILED] == 1
    assert harness.runtime.active_connections == 0


@pytest.mark.asyncio
async def test_connection_limit_and_cancel_during_active_transfer() -> None:
    backend_connected = asyncio.Event()

    async def hold(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        backend_connected.set()
        await reader.read()
        await close_writer(writer)

    async with running_server(hold) as backend:
        port = server_port(backend)
        config = make_config(port, port)
        config = replace(
            config,
            connection=replace(
                config.connection,
                max_connections=1,
                max_connections_per_ip=1,
                shutdown_grace_seconds=0.0,
                shutdown_cancel_timeout_seconds=0.2,
            ),
        )
        harness = ProxyHarness(config)
        proxy_port = await harness.start()
        first_reader, first_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        await asyncio.wait_for(backend_connected.wait(), 1.0)
        second_reader, second_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        assert await asyncio.wait_for(second_reader.read(), 1.0) == b""
        await close_writer(second_writer)

        await harness.stop()
        assert await asyncio.wait_for(first_reader.read(), 1.0) == b""
        await close_writer(first_writer)
        assert harness.runtime.incoming_connections_total == 2
        assert harness.runtime.backend_connections_established_total == 1
        assert harness.runtime.connections_rejected_total == 1
        assert harness.runtime.active_connections == 0
        assert harness.limiter.active == 0
        assert harness.proxy.tasks.active_count == 0
