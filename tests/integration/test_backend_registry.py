from __future__ import annotations

import asyncio
import socket
from contextlib import AsyncExitStack
from dataclasses import replace
from typing import Any, cast

import pytest

from mc_failover.config import TargetConfig
from mc_failover.endpoint_safety import EndpointLoopGuard
from mc_failover.registry import BackendRegistry
from mc_failover.routing import MaintenanceWatcher, Router
from tests.conftest import close_writer, closed_local_port, running_server, server_port
from tests.unit.test_backend_registry import explicit_config


@pytest.mark.asyncio
async def test_ten_real_tcp_backends_independent_failure_recovery_and_removed_sessions() -> None:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(1024):
                writer.write(data)
                await writer.drain()
        finally:
            await close_writer(writer)

    async with AsyncExitStack() as stack:
        servers = [await stack.enter_async_context(running_server(echo)) for _ in range(10)]
        original = explicit_config(10)
        config = replace(
            original,
            backends=tuple(
                replace(backend, port=server_port(server))
                for backend, server in zip(original.backends, servers, strict=True)
            ),
        )
        registry = BackendRegistry(config, endpoint_guard=EndpointLoopGuard(config))
        await registry.check_all(initial=True)
        assert all(entry.health.healthy for entry in registry.ordered)
        first, second = registry.ordered[:2]
        router = Router(
            config,
            first.health,
            second.health,
            first.circuit,
            MaintenanceWatcher(config.maintenance),
            registry=registry,
        )
        for server in servers[:4]:
            server.close()
            await server.wait_closed()
        await registry.check_all(include_reused=True)
        assert router.snapshot().backend_id == "FALLBACK_4"
        target = router.snapshot().target
        assert target is not None
        reader, writer = await asyncio.open_connection(target.host, target.port)
        try:
            removed = replace(config, backends=(config.backends[-1],))
            replacement = BackendRegistry(removed, previous=registry)
            await replacement.check_all(initial=True)
            assert tuple(replacement.entries) == ("FALLBACK_9",)
            writer.write(b"existing session retained after removal")
            await writer.drain()
            assert (
                await reader.readexactly(len(b"existing session retained after removal"))
                == b"existing session retained after removal"
            )
        finally:
            await close_writer(writer)


@pytest.mark.asyncio
async def test_slow_minecraft_check_does_not_block_other_backend_health() -> None:
    accepted = asyncio.Event()
    release = asyncio.Event()

    async def slow(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        accepted.set()
        try:
            await reader.read(1024)
            await release.wait()
        finally:
            await close_writer(writer)

    async def fast(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await close_writer(writer)

    async with running_server(slow) as slow_server, running_server(fast) as fast_server:
        config = explicit_config(3)
        closed = await closed_local_port()
        config = replace(
            config,
            backends=(
                replace(
                    config.backends[0],
                    port=server_port(slow_server),
                    healthcheck=replace(
                        config.backends[0].healthcheck, mode="minecraft_status", timeout_seconds=0.1
                    ),
                ),
                replace(config.backends[1], port=server_port(fast_server)),
                replace(config.backends[2], port=closed),
            ),
        )
        registry = BackendRegistry(config)
        task = asyncio.create_task(registry.check_all(initial=True))
        await accepted.wait()
        for _ in range(20):
            if registry.entries["FALLBACK_1"].health.healthy is True:
                break
            await asyncio.sleep(0.001)
        assert registry.entries["FALLBACK_1"].health.healthy is True
        assert registry.entries["MAIN"].health.snapshot().healthy is None
        await task
        assert registry.entries["MAIN"].health.snapshot().healthy is False
        assert registry.entries["FALLBACK_2"].health.healthy is False
        release.set()


@pytest.mark.asyncio
async def test_all_generic_health_targets_receive_endpoint_loop_validation() -> None:
    async def close(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await close_writer(writer)

    async with running_server(close) as proxy:
        config = explicit_config(3)
        config = replace(
            config,
            proxy=replace(config.proxy, listen_port=server_port(proxy)),
            backends=(
                *config.backends[:2],
                replace(config.backends[2], host="localhost", port=server_port(proxy)),
            ),
        )
        guard = EndpointLoopGuard(config)
        with pytest.raises(OSError, match=r"backends\.FALLBACK_2"):
            await guard.validate_all()
        registry = BackendRegistry(config, endpoint_guard=guard)
        await registry.entries["FALLBACK_2"].monitor.check_once(initial=True)
        assert registry.entries["FALLBACK_2"].health.last_result is not None
        assert registry.entries["FALLBACK_2"].health.last_result.reason == "unsafe_listener_loop"
        assert (
            registry.entries["FALLBACK_2"].config.port
            == TargetConfig("localhost", server_port(proxy)).port
        )


@pytest.mark.asyncio
async def test_cancel_at_real_connect_completion_closes_owned_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.read()
        finally:
            await close_writer(writer)

    async with running_server(backend) as server:
        config = explicit_config()
        guard = EndpointLoopGuard(config)
        loop = asyncio.get_running_loop()
        connect_sock = cast(Any, loop)._connect_sock
        observed: list[socket.socket] = []
        task: asyncio.Task[tuple[asyncio.StreamReader, asyncio.StreamWriter]]

        async def cancel_after_socket_connect(*args: Any, **kwargs: Any) -> socket.socket:
            connected = await connect_sock(*args, **kwargs)
            observed.append(connected)
            task.cancel()
            return connected

        monkeypatch.setattr(loop, "_connect_sock", cancel_after_socket_connect)
        task = asyncio.create_task(
            guard.open_connection("127.0.0.1", server_port(server), timeout_seconds=1)
        )
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        unclosed = [sock.fileno() for sock in observed if sock.fileno() != -1]
        # Close the failing implementation's leaked raw descriptors so the
        # real backend can exit and the assertion reports promptly.
        for sock in observed:
            sock.close()
        assert observed and not unclosed
