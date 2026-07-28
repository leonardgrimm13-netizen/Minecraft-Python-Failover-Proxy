from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.health import HealthState
from mc_failover.models import HealthStatus, TargetName
from tests.conftest import close_writer, closed_local_port, make_config, server_port


async def wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    tick = asyncio.Event()
    while not predicate():
        delay = min(0.005, deadline - loop.time())
        if delay <= 0:
            raise TimeoutError("condition was not met before the deadline")
        handle = loop.call_later(delay, tick.set)
        try:
            await tick.wait()
        finally:
            handle.cancel()
        tick.clear()


def has_status(state: HealthState, expected: HealthStatus) -> bool:
    return state.status is expected


def target_handler(
    marker: bytes, completed: asyncio.Event
) -> Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read(1)
        if data:
            writer.write(marker + data)
            await writer.drain()
            completed.set()
        await close_writer(writer)

    return handler


@pytest.mark.asyncio
async def test_fallback_start_main_recovery_and_both_targets_down() -> None:
    main_port = await closed_local_port()
    fallback_player = asyncio.Event()
    fallback_server = await asyncio.start_server(
        target_handler(b"F", fallback_player), "127.0.0.1", 0
    )
    fallback_port = server_port(fallback_server)
    config = make_config(main_port, fallback_port)
    config = replace(
        config,
        healthcheck=replace(config.healthcheck, interval_seconds=0.02, timeout_seconds=0.05),
        fallback_healthcheck=replace(
            config.fallback_healthcheck, interval_seconds=0.02, timeout_seconds=0.05
        ),
    )
    app = FailoverApplication(config)
    await app.start()
    assert has_status(app.main_health, HealthStatus.UNHEALTHY)
    assert has_status(app.fallback_health, HealthStatus.HEALTHY)
    assert app.router.snapshot().active_target is TargetName.FALLBACK

    assert app.proxy.server is not None
    proxy_port = server_port(app.proxy.server)
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    writer.write(b"1")
    await writer.drain()
    assert await asyncio.wait_for(reader.readexactly(2), 1.0) == b"F1"
    await asyncio.wait_for(fallback_player.wait(), 1.0)
    await close_writer(writer)

    main_player = asyncio.Event()
    main_server = await asyncio.start_server(
        target_handler(b"M", main_player), "127.0.0.1", main_port
    )
    await wait_until(lambda: has_status(app.main_health, HealthStatus.HEALTHY))
    assert app.router.snapshot().active_target is TargetName.MAIN
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    writer.write(b"2")
    await writer.drain()
    assert await asyncio.wait_for(reader.readexactly(2), 1.0) == b"M2"
    await asyncio.wait_for(main_player.wait(), 1.0)
    await close_writer(writer)

    main_server.close()
    await main_server.wait_closed()
    fallback_server.close()
    await fallback_server.wait_closed()
    await wait_until(
        lambda: (
            has_status(app.main_health, HealthStatus.UNHEALTHY)
            and has_status(app.fallback_health, HealthStatus.UNHEALTHY)
        )
    )
    assert not app.router.snapshot().ready
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    assert await asyncio.wait_for(reader.read(), 1.0) == b""
    await close_writer(writer)
    await app.shutdown()
    assert app.background.active_count == 0
    assert app.proxy.tasks.active_count == 0


@pytest.mark.asyncio
async def test_graceful_shutdown_stops_accepting_then_drains_existing_connection() -> None:
    connected = asyncio.Event()
    reply_allowed = asyncio.Event()
    backend_done = asyncio.Event()

    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read(4)
        if not data:
            await close_writer(writer)
            return
        connected.set()
        await reply_allowed.wait()
        writer.write(data.upper())
        await writer.drain()
        await close_writer(writer)
        backend_done.set()

    server = await asyncio.start_server(backend, "127.0.0.1", 0)
    port = server_port(server)
    config = make_config(port, port, monitoring=True)
    config = replace(
        config,
        connection=replace(
            config.connection,
            shutdown_grace_seconds=1.0,
            shutdown_cancel_timeout_seconds=0.2,
        ),
        fallback_healthcheck=replace(config.fallback_healthcheck, enabled=False),
    )
    app = FailoverApplication(config)
    await app.start()
    assert app.proxy.server is not None
    proxy_port = server_port(app.proxy.server)
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    writer.write(b"ping")
    await writer.drain()
    await asyncio.wait_for(connected.wait(), 1.0)

    shutdown = asyncio.create_task(app.shutdown())
    await wait_until(lambda: app.runtime.shutting_down)
    await wait_until(lambda: app.proxy.server is None)
    reply_allowed.set()
    assert await asyncio.wait_for(reader.readexactly(4), 1.0) == b"PING"
    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", proxy_port)
    await close_writer(writer)
    await asyncio.wait_for(backend_done.wait(), 1.0)
    await asyncio.wait_for(shutdown, 2.0)
    server.close()
    await server.wait_closed()
    assert app.runtime.active_connections == 0
    assert app.limiter.active == 0
    assert app.proxy.tasks.active_count == 0
    assert app.monitoring.tasks.active_count == 0
