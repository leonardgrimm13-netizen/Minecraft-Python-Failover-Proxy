"""Status-only clients must recover expired circuits without login traffic."""

from __future__ import annotations

import asyncio
import errno
from dataclasses import replace

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.models import CircuitState
from tests.conftest import (
    FakeClock,
    close_writer,
    closed_local_port,
    make_config,
    running_server,
    server_port,
)
from tests.integration.test_local_status_service import query


async def open_circuit(app: FailoverApplication, backend_id: str = "MAIN") -> None:
    entry = app.registry.entries[backend_id]
    for _ in range(entry.circuit.failure_threshold):
        await entry.circuit.record_failure(await entry.circuit.acquire())


@pytest.mark.asyncio
async def test_status_only_recovers_expired_open_circuit_without_active_checks() -> None:
    accepted = 0

    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal accepted
        accepted += 1
        await close_writer(writer)

    async with running_server(backend) as server:
        config = make_config(
            server_port(server), server_port(server), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=18080),
            backends=(
                replace(
                    config.effective_backends[0],
                    healthcheck=replace(config.healthcheck, enabled=False),
                ),
            ),
            status=replace(config.status, enabled=True, always=True),
        )
        clock = FakeClock()
        app = FailoverApplication(config, clock=clock)
        await app.start()
        try:
            await open_circuit(app)
            for _ in range(3):
                assert (await query(config.proxy.listen_port))["description"] == {
                    "text": config.status.motd_offline
                }
            assert accepted == 0
            clock.advance(1)
            result = await query(config.proxy.listen_port)
            assert result["description"] == {"text": config.status.motd_online}
            assert accepted == 1
            assert app.circuit_breaker.snapshot_now().state is CircuitState.CLOSED
        finally:
            await app.shutdown()
        assert app.proxy.tasks.active_count == app.limiter.active == 0


@pytest.mark.asyncio
async def test_status_probe_failure_is_cooled_down_and_single_attempt_per_query() -> None:
    config = make_config(
        await closed_local_port(), await closed_local_port(), proxy_port=await closed_local_port()
    )
    config = replace(
        config,
        monitoring=replace(config.monitoring, listen_port=18080),
        backends=tuple(
            replace(
                b,
                healthcheck=replace(b.healthcheck, enabled=False),
                circuit_breaker=config.circuit_breaker,
            )
            for b in config.effective_backends
        ),
        status=replace(config.status, enabled=True, always=True),
        passive_health=replace(config.passive_health, enabled=False),
    )
    clock = FakeClock()
    app = FailoverApplication(config, clock=clock)
    await app.start()
    try:
        await open_circuit(app)
        await open_circuit(app, "FALLBACK")
        clock.advance(1)
        await query(config.proxy.listen_port)
        assert app.registry.entries["MAIN"].circuit.snapshot_now().open_total == 2
        assert app.registry.entries["FALLBACK"].circuit.snapshot_now().open_total == 1
        await query(config.proxy.listen_port)
        assert app.registry.entries["FALLBACK"].circuit.snapshot_now().open_total == 2
        for _ in range(5):
            await query(config.proxy.listen_port)
        assert all(e.circuit.snapshot_now().open_total == 2 for e in app.registry.ordered)
        assert all(e.circuit.snapshot_now().half_open_in_flight == 0 for e in app.registry.ordered)
    finally:
        await app.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown", [False, True])
async def test_status_probe_capacity_shared_over_reloads_and_cancelled_safely(
    shutdown: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await close_writer(writer)

    async with running_server(backend) as server:
        config = make_config(
            server_port(server), server_port(server), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=18080),
            backends=(
                replace(
                    config.effective_backends[0],
                    healthcheck=replace(config.healthcheck, enabled=False),
                    circuit_breaker=replace(config.circuit_breaker, half_open_max_attempts=2),
                ),
            ),
            status=replace(config.status, enabled=True, always=True),
            connection=replace(config.connection, shutdown_grace_seconds=0),
        )
        clock = FakeClock()
        app = FailoverApplication(config, clock=clock)
        await app.start()
        started = asyncio.Event()
        release = asyncio.Event()
        attempts = 0
        current = 0
        maximum = 0
        original = app.endpoint_guard.open_connection

        async def blocked(host: str, port: int, *, timeout_seconds: float):
            nonlocal attempts, current, maximum
            attempts += 1
            current += 1
            maximum = max(maximum, current)
            if attempts == 2:
                started.set()
            try:
                await release.wait()
                return await original(host, port, timeout_seconds=timeout_seconds)
            finally:
                current -= 1

        monkeypatch.setattr(app.endpoint_guard, "open_connection", blocked)
        clients: list[asyncio.Task[dict[str, object]]] = []
        try:
            await open_circuit(app)
            clock.advance(1)
            clients = [asyncio.create_task(query(config.proxy.listen_port)) for _ in range(2)]
            await asyncio.wait_for(started.wait(), 1)
            circuit = app.circuit_breaker
            assert circuit.snapshot_now().half_open_in_flight == 2
            for _ in range(3):
                assert await app.reload_config(config)
                assert app.circuit_breaker is circuit
            # No waiting behind the two slow probes and no further network I/O.
            results = await asyncio.wait_for(
                asyncio.gather(*(query(config.proxy.listen_port) for _ in range(30))), 1
            )
            assert all(
                result["description"] == {"text": config.status.motd_offline} for result in results
            )
            assert attempts == maximum == 2
            if shutdown:
                await app.shutdown()
                await asyncio.gather(*clients, return_exceptions=True)
            else:
                release.set()
                assert all(
                    result["description"] == {"text": config.status.motd_online}
                    for result in await asyncio.gather(*clients)
                )
                assert circuit.snapshot_now().state is CircuitState.CLOSED
            assert circuit.snapshot_now().half_open_in_flight == 0
        finally:
            release.set()
            await app.shutdown()
            await asyncio.gather(*clients, return_exceptions=True)
        assert (
            current
            == app.proxy.tasks.active_count
            == app.background.active_count
            == app.limiter.active
            == 0
        )


@pytest.mark.asyncio
async def test_invalid_status_request_cannot_trigger_recovery() -> None:
    from tests.unit.test_local_status import handshake

    config = make_config(await closed_local_port(), await closed_local_port())
    config = replace(
        config,
        healthcheck=replace(config.healthcheck, enabled=False),
        fallback_healthcheck=replace(config.fallback_healthcheck, enabled=False),
        status=replace(config.status, enabled=True, always=True),
    )
    clock = FakeClock()
    app = FailoverApplication(config, clock=clock)
    await app.start()
    try:
        await open_circuit(app)
        clock.advance(1)
        assert app.proxy.server is not None
        reader, writer = await asyncio.open_connection("127.0.0.1", server_port(app.proxy.server))
        try:
            writer.write(handshake() + b"\x01\x01")
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 1) == b""
            assert app.circuit_breaker.snapshot_now().state is CircuitState.OPEN
            assert app.circuit_breaker.snapshot_now().open_total == 1
        finally:
            await close_writer(writer)
    finally:
        await app.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_number", [None, errno.ECONNREFUSED, errno.EMFILE])
async def test_recovery_probe_io_verdict_distinguishes_local_resource_errors(
    error_number: int | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(25561, 25562)
    config = replace(config, healthcheck=replace(config.healthcheck, enabled=False))
    clock = FakeClock()
    app = FailoverApplication(config, clock=clock)
    await open_circuit(app)
    clock.advance(1)

    async def fail(*_args, **_kwargs):
        if error_number is None:
            raise asyncio.TimeoutError
        raise OSError(error_number, "controlled probe failure")

    monkeypatch.setattr(app.endpoint_guard, "open_connection", fail)
    try:
        assert not await app.proxy._status_recovery_probe(asyncio.get_running_loop().time() + 1)
        state = app.circuit_breaker.snapshot_now()
        assert state.half_open_in_flight == 0
        if error_number == errno.EMFILE:
            assert state.open_total == 1
            assert state.state is CircuitState.HALF_OPEN
        else:
            assert state.open_total == 2
            assert state.state is CircuitState.OPEN
    finally:
        await app.shutdown()


@pytest.mark.asyncio
async def test_probe_deadline_exhaustion_and_shutdown_release_without_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(25561, 25562)
    config = replace(config, healthcheck=replace(config.healthcheck, enabled=False))
    clock = FakeClock()
    app = FailoverApplication(config, clock=clock)
    await open_circuit(app)
    clock.advance(1)
    try:
        assert not await app.proxy._status_recovery_probe(asyncio.get_running_loop().time() - 1)
        assert app.circuit_breaker.snapshot_now().half_open_in_flight == 0
        app.request_shutdown()
        assert not await app.proxy._status_recovery_probe(asyncio.get_running_loop().time() + 1)
    finally:
        await app.shutdown()


@pytest.mark.asyncio
async def test_fallback_status_can_recover_preferred_backend_but_forced_policy_cannot() -> None:
    from mc_failover.models import MaintenanceMode

    accepted = 0

    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal accepted
        accepted += 1
        await close_writer(writer)

    async with running_server(backend) as server:
        config = make_config(
            server_port(server), server_port(server), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=18080),
            healthcheck=replace(config.healthcheck, enabled=False),
            fallback_healthcheck=replace(config.fallback_healthcheck, enabled=False),
            status=replace(config.status, enabled=True, always=True),
        )
        clock = FakeClock()
        app = FailoverApplication(config, clock=clock)
        await app.start()
        try:
            await open_circuit(app)
            assert (await query(config.proxy.listen_port))["description"] == {
                "text": config.status.motd_fallback
            }
            clock.advance(1)
            forced = replace(
                config, maintenance=replace(config.maintenance, mode=MaintenanceMode.FORCE_FALLBACK)
            )
            assert await app.reload_config(forced)
            assert (await query(config.proxy.listen_port))["description"] == {
                "text": config.status.motd_maintenance
            }
            assert accepted == 0
            assert app.circuit_breaker.snapshot_now().state is CircuitState.OPEN
            assert await app.reload_config(config)
            assert (await query(config.proxy.listen_port))["description"] == {
                "text": config.status.motd_online
            }
            assert accepted == 1
        finally:
            await app.shutdown()


@pytest.mark.asyncio
async def test_short_status_deadline_keeps_response_and_failed_probe_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(25561, 25562)
    config = replace(
        config,
        backends=(
            replace(
                config.effective_backends[0], healthcheck=replace(config.healthcheck, enabled=False)
            ),
        ),
        status=replace(config.status, enabled=True, always=True, session_timeout_seconds=0.05),
    )
    clock = FakeClock()
    app = FailoverApplication(config, clock=clock)
    attempts = 0

    async def stalled(*_args, timeout_seconds: float):
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(timeout_seconds)
        raise asyncio.TimeoutError

    monkeypatch.setattr(app.endpoint_guard, "open_connection", stalled)
    await app.start()
    try:
        await open_circuit(app)
        clock.advance(1)
        assert app.proxy.server is not None
        for _ in range(3):
            result = await query(server_port(app.proxy.server))
            assert result["description"] == {"text": config.status.motd_offline}
        assert attempts == 1
        assert app.circuit_breaker.snapshot_now().open_total == 2
        assert app.circuit_breaker.snapshot_now().half_open_in_flight == 0
    finally:
        await app.shutdown()
