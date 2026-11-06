"""Review regressions using held TCP sessions and real reload generations."""

from __future__ import annotations

import asyncio
import socket
import struct
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
from tests.integration.test_hot_reload import echo


@pytest.mark.asyncio
async def test_atomic_health_interval_reload_keeps_open_circuits() -> None:
    clock = FakeClock()
    async with running_server(echo) as backend:
        config = make_config(
            server_port(backend), server_port(backend), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            backends=tuple(
                replace(
                    b,
                    healthcheck=replace(b.healthcheck, interval_seconds=60),
                    circuit_breaker=config.circuit_breaker,
                )
                for b in config.effective_backends
            ),
        )
        config = replace(config, monitoring=replace(config.monitoring, listen_port=18080))
        app = FailoverApplication(config, clock=clock)
        await app.start()
        try:
            circuits = [entry.circuit for entry in app.registry.ordered]
            for circuit in circuits:
                for _ in range(circuit.failure_threshold):
                    await circuit.record_failure(await circuit.acquire())
            candidate = replace(
                config,
                backends=tuple(
                    replace(b, healthcheck=replace(b.healthcheck, interval_seconds=30))
                    for b in config.backends
                ),
            )
            assert await app.reload_config(candidate)
            for old, entry in zip(circuits, app.registry.ordered, strict=True):
                assert entry.circuit is old
                assert entry.circuit.snapshot_now().state is CircuitState.OPEN
                assert entry.circuit.snapshot_now().retry_after_seconds == pytest.approx(0.2)
                assert entry.circuit.snapshot_now().failures_in_window == 2
            clock.advance(1)
            for entry in app.registry.ordered:
                probe = await entry.circuit.acquire()
                assert probe.probe
                await entry.circuit.record_success(probe)
                assert entry.circuit.snapshot_now().state is CircuitState.CLOSED
        finally:
            await app.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["same", "removed", "endpoint", "readded"])
@pytest.mark.parametrize("failure", ["reset", "early_eof"])
async def test_old_sessions_notify_only_current_incarnation_and_policy(
    change: str, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = asyncio.Event()
    established: asyncio.Queue[None] = asyncio.Queue()

    async def controlled(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            if not await reader.read(1):
                return  # A healthcheck, not a held relay.
            established.put_nowait(None)
            await release.wait()
            if failure == "reset":
                writer.get_extra_info("socket").setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                )
                writer.transport.abort()
        finally:
            await close_writer(writer)

    async with running_server(controlled) as backend, running_server(echo) as replacement:
        config = make_config(
            server_port(backend), server_port(replacement), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            backends=tuple(
                replace(
                    b,
                    healthcheck=replace(b.healthcheck, interval_seconds=60),
                    circuit_breaker=config.circuit_breaker,
                )
                for b in config.effective_backends
            ),
            passive_health=replace(config.passive_health, failure_threshold=1),
        )
        config = replace(config, monitoring=replace(config.monitoring, listen_port=18080))
        app = FailoverApplication(config)
        await app.start()
        clients = []
        try:
            assert app.proxy.server is not None
            for _ in range(8):
                reader, writer = await asyncio.open_connection(
                    "127.0.0.1", server_port(app.proxy.server)
                )
                clients.append((reader, writer))
                writer.write(b"x")
                await writer.drain()
                await asyncio.wait_for(established.get(), 1)
            old_entry = app.registry.entries["MAIN"]
            old_tasks = app._generation_tasks
            for generation in range(3):
                candidate = replace(
                    config,
                    passive_health=replace(
                        config.passive_health,
                        failure_threshold=20,
                        quarantine_seconds=60,
                        early_session_seconds=10,
                    ),
                )
                if change == "removed" or (change == "readded" and generation == 0):
                    candidate = replace(candidate, backends=(config.backends[1],))
                elif change == "endpoint":
                    candidate = replace(
                        candidate,
                        backends=(
                            replace(config.backends[0], port=server_port(replacement)),
                            config.backends[1],
                        ),
                    )
                assert await app.reload_config(candidate)
            assert all(task.done() for task in old_tasks)
            checks = 0
            checked = asyncio.Event()
            for entry in app.registry.ordered:
                original = entry.monitor.check_once

                async def counted(*, initial: bool = False, original=original):
                    nonlocal checks
                    checks += 1
                    checked.set()
                    return await original(initial=initial)

                monkeypatch.setattr(entry.monitor, "check_once", counted)
            release.set()
            await asyncio.wait_for(app.proxy.tasks.wait_remaining(2), 3)
            if change == "same":
                await asyncio.wait_for(checked.wait(), 1)
                assert checks == 1  # Coalesced across all old sessions, one live worker.
                assert not old_entry.check_requested.is_set()
                current = app.registry.entries["MAIN"]
                assert current.statistics is old_entry.statistics
                assert current.statistics.quarantine_until is None
                assert current.passive_failures == (8 if failure == "reset" else 0)
            else:
                await asyncio.sleep(0.03)
                assert checks == 0
                if change == "endpoint":
                    assert app.registry.entries["MAIN"].passive_failures == 0
        finally:
            release.set()
            await asyncio.gather(*(close_writer(w) for _, w in clients))
            await app.shutdown()
        assert (
            app.background.active_count == app.proxy.tasks.active_count == app.limiter.active == 0
        )
