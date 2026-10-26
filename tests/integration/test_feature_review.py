"""Independent regressions found while reviewing the integrated feature paths."""

from __future__ import annotations

import asyncio
import json
import socket
import struct
from dataclasses import replace

import pytest

import mc_failover.app as application_module
from mc_failover.app import FailoverApplication
from mc_failover.config import BackendConfig
from mc_failover.local_status import LocalStatusService
from mc_failover.minecraft_status import decode_varint, read_status_packet
from mc_failover.models import CircuitState, HealthCheckResult
from mc_failover.proxy_protocol import build_proxy_header_for_version
from tests.conftest import (
    FakeClock,
    close_writer,
    closed_local_port,
    make_config,
    running_server,
    server_port,
)
from tests.integration.test_deferred_source_limits import FakeWriter, reader_with
from tests.unit.test_local_status import handshake


async def wait_for_idle(app: FailoverApplication) -> None:
    async def wait() -> None:
        while app.proxy.tasks.active_count:  # noqa: ASYNC110 -- bounded external state
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), 1)


@pytest.mark.asyncio
async def test_real_sequential_backend_resets_open_circuit_despite_successful_connects() -> None:
    async def reset_backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if await reader.read(1):
            raw_socket = writer.get_extra_info("socket")
            assert raw_socket is not None
            raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            writer.transport.abort()
        else:
            await close_writer(writer)

    async def fallback(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if await reader.read(1):
            writer.write(b"F")
            await writer.drain()
        await close_writer(writer)

    async with running_server(reset_backend) as main, running_server(fallback) as secondary:
        config = make_config(server_port(main), server_port(secondary))
        config = replace(
            config,
            healthcheck=replace(config.healthcheck, interval_seconds=60),
            fallback_healthcheck=replace(config.fallback_healthcheck, interval_seconds=60),
            passive_health=replace(
                config.passive_health, failure_threshold=100, quarantine_seconds=10
            ),
        )
        app = FailoverApplication(config)
        try:
            await app.start()
            assert app.proxy.server is not None
            port = server_port(app.proxy.server)
            for _ in range(2):
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                try:
                    writer.write(b"x")
                    await writer.drain()
                    assert await asyncio.wait_for(reader.read(), 1) == b""
                finally:
                    await close_writer(writer)
                await wait_for_idle(app)
            entry = app.registry.entries["MAIN"]
            assert entry.passive_failures == 2
            assert (await entry.circuit.snapshot()).state is CircuitState.OPEN
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            try:
                writer.write(b"x")
                await writer.drain()
                assert await asyncio.wait_for(reader.read(), 1) == b"F"
            finally:
                await close_writer(writer)
        finally:
            await app.shutdown()
        assert app.proxy.tasks.active_count == 0
        assert app.limiter.active == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2])
async def test_integrated_offline_status_authenticates_proxy_header_before_handshake(
    version: int,
) -> None:
    unavailable = await closed_local_port()
    config = make_config(unavailable, unavailable)
    config = replace(
        config,
        status=replace(config.status, enabled=True, motd_offline="§cOffline\nTry later"),
        proxy_protocol=replace(
            config.proxy_protocol,
            accept=True,
            version=version,
            trusted_proxy_ips=("127.0.0.1",),
        ),
    )
    app = FailoverApplication(config)
    try:
        await app.start()
        assert app.proxy.server is not None
        reader, writer = await asyncio.open_connection("127.0.0.1", server_port(app.proxy.server))
        try:
            header = build_proxy_header_for_version(
                version, "203.0.113.7", "198.51.100.10", 42000, 25565
            )
            writer.write(header + handshake(protocol=-1) + b"\x01\x00")
            await writer.drain()
            packet = await asyncio.wait_for(read_status_packet(reader), 1)
            length, offset = decode_varint(packet, 1)
            status = json.loads(packet[offset : offset + length])
            assert status["description"] == {"text": "§cOffline\nTry later"}
            assert "127.0.0.1" not in packet.decode("utf-8")
            assert "203.0.113.7" not in packet.decode("utf-8")
            ping = b"\x09\x01" + bytes(8)
            writer.write(ping)
            await writer.drain()
            assert await asyncio.wait_for(reader.readexactly(10), 1) == ping
            assert await asyncio.wait_for(reader.read(), 1) == b""
        finally:
            await close_writer(writer)
    finally:
        await app.shutdown()
    assert app.limiter.active == 0
    assert app.snapshot.status_service.active == 0


@pytest.mark.asyncio
async def test_failed_reload_preparation_does_not_mutate_live_passive_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def healthy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()
        await close_writer(writer)

    async with running_server(healthy) as backend:
        config = make_config(
            server_port(backend), server_port(backend), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=18080),
            passive_health=replace(
                config.passive_health, failure_threshold=1, quarantine_seconds=1
            ),
        )
        clock = FakeClock()
        app = FailoverApplication(config, clock=clock)
        try:
            await app.start()
            # Isolate candidate preparation from legitimate concurrent checks.
            app._generation_stop.set()
            await asyncio.gather(*app._generation_tasks)
            assert app.registry.record_passive_failure("MAIN", "backend_reset")
            old = app.registry.entries["MAIN"]
            old_state = (
                old.statistics.quarantine_until,
                tuple(old.statistics.failures),
                old.statistics.recovery_started,
                old.statistics.recovery_successes,
            )
            clock.advance(2)
            candidate = replace(
                config, healthcheck=replace(config.healthcheck, interval_seconds=0.1)
            )
            prepared = False

            def fail_resource(_config: object) -> LocalStatusService:
                nonlocal prepared
                prepared = True
                raise OSError("injected resource preparation failure")

            monkeypatch.setattr(application_module, "LocalStatusService", fail_resource)
            assert not await app.reload_config(candidate)
            assert prepared
            assert app.config is config
            assert app.registry.entries["MAIN"] is old
            assert (
                old.statistics.quarantine_until,
                tuple(old.statistics.failures),
                old.statistics.recovery_started,
                old.statistics.recovery_successes,
            ) == old_state
        finally:
            await app.shutdown()


@pytest.mark.asyncio
async def test_failed_nonmain_connect_does_not_release_another_backends_half_open_probe() -> None:
    unavailable = await closed_local_port()
    config = make_config(unavailable, unavailable)
    config = replace(
        config,
        backends=tuple(
            BackendConfig(
                backend_id,
                "127.0.0.1",
                unavailable,
                config.healthcheck,
                config.circuit_breaker,
                priority=priority,
            )
            for priority, backend_id in enumerate(("MAIN", "FALLBACK"))
        ),
    )
    clock = FakeClock()
    app = FailoverApplication(config, clock=clock)
    for entry in app.registry.ordered:
        entry.health.report(HealthCheckResult(True, "initial"), initial=True)
        for _ in range(2):
            await entry.circuit.record_failure(await entry.circuit.acquire())
    clock.advance(1)
    main = app.registry.entries["MAIN"]
    other_session_probe = await main.circuit.acquire()
    assert other_session_probe.probe
    try:
        await app.proxy.handle_client(reader_with(b""), FakeWriter())  # type: ignore[arg-type]
        assert (await main.circuit.snapshot()).half_open_in_flight == 1
    finally:
        await main.circuit.release(other_session_probe)
        await app.shutdown()


@pytest.mark.asyncio
async def test_cancellation_after_backend_connect_releases_untransferred_socket() -> None:
    backend_done = asyncio.Event()

    async def hold(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()
        await close_writer(writer)
        backend_done.set()

    async with running_server(hold) as backend:
        config = make_config(server_port(backend), server_port(backend))
        app = FailoverApplication(config)
        main = app.registry.entries["MAIN"]
        main.health.report(HealthCheckResult(True, "initial"), initial=True)
        decision = await app.router.select_for_connection()
        try:
            async with main.circuit._lock:
                connecting = asyncio.create_task(
                    app.proxy._connect_candidates(
                        decision,
                        app.router,
                        None,
                        FakeWriter(),  # type: ignore[arg-type]
                    )
                )

                async def await_socket() -> None:
                    while not app.proxy._writers:  # noqa: ASYNC110 -- bounded external state
                        await asyncio.sleep(0)

                await asyncio.wait_for(await_socket(), 1)
                connecting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await connecting
            assert not app.proxy._writers
            await asyncio.wait_for(backend_done.wait(), 1)
        finally:
            await app.shutdown()
