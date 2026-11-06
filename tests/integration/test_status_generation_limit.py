"""A single inspection budget follows active sessions across runtime generations."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import replace
from typing import Any

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.config import LocalStatusConfig
from mc_failover.local_status import LocalStatusService
from mc_failover.minecraft_status import read_status_packet
from tests.conftest import close_writer, closed_local_port, make_config, running_server, server_port
from tests.integration.test_hot_reload import echo
from tests.unit.test_local_status import handshake


async def hold_status(port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(handshake() + b"\x01\x00")
        await writer.drain()
        assert (await asyncio.wait_for(read_status_packet(reader), 1))[0] == 0
        # Omit the ping until the caller finishes the held status exchange.
        return reader, writer
    except BaseException:
        await close_writer(writer)
        raise


async def finish_status(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    ping = b"\x09\x01" + (1).to_bytes(8, "big")
    writer.write(ping)
    await writer.drain()
    assert await asyncio.wait_for(reader.readexactly(10), 1) == ping
    assert await asyncio.wait_for(reader.read(), 1) == b""
    await close_writer(writer)


@pytest.mark.asyncio
async def test_status_inspection_cap_is_global_across_three_reloads() -> None:
    async with running_server(echo) as backend:
        config = make_config(
            server_port(backend), server_port(backend), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=18080),
            status=replace(
                config.status,
                enabled=True,
                always=True,
                max_concurrent=2,
                session_timeout_seconds=30,
            ),
        )
        app = FailoverApplication(config)
        await app.start()
        held: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []
        services = [app.snapshot.status_service]
        try:
            held.append(await hold_status(config.proxy.listen_port))
            assert await app.reload_config(config)
            services.append(app.snapshot.status_service)
            held.append(await hold_status(config.proxy.listen_port))
            for _ in range(2):
                assert await app.reload_config(config)
                services.append(app.snapshot.status_service)
            reader, writer = await asyncio.open_connection("127.0.0.1", config.proxy.listen_port)
            held.append((reader, writer))
            writer.write(handshake() + b"\x01\x00")
            await writer.drain()
            # The third request cannot obtain a fresh budget from generation 4.
            await assert_rejected(reader)
            assert all(service.active == 2 for service in services)
            await finish_status(*held[0])
            held.pop(0)
            reader, writer = await hold_status(config.proxy.listen_port)
            held.append((reader, writer))
            await finish_status(reader, writer)
            held.pop()
        finally:
            await asyncio.gather(*(close_writer(writer) for _, writer in held))
            await app.shutdown()
        assert all(service.active == 0 for service in services)
        assert app.proxy.tasks.active_count == app.limiter.active == 0


async def assert_rejected(reader: asyncio.StreamReader) -> None:
    try:
        data = await asyncio.wait_for(reader.read(1), 1)
    except ConnectionResetError:
        # Rejecting before consuming a sent handshake may reset TCP.
        return
    assert data == b""


@pytest.mark.asyncio
async def test_rejected_reload_does_not_change_shared_status_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with running_server(echo) as backend:
        config = make_config(
            server_port(backend), server_port(backend), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=18080),
            status=replace(config.status, enabled=True, always=True, max_concurrent=1),
        )
        app = FailoverApplication(config)
        await app.start()
        held = await hold_status(config.proxy.listen_port)
        old = app.snapshot.status_service
        try:
            candidate = replace(config, status=replace(config.status, max_concurrent=9))

            def fail_create(coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
                coroutine.close()
                raise RuntimeError("cannot prepare new worker")

            monkeypatch.setattr(app.background, "create", fail_create)
            assert not await app.reload_config(candidate)
            assert app.snapshot.status_service is old
            reader, writer = await asyncio.open_connection("127.0.0.1", config.proxy.listen_port)
            try:
                writer.write(handshake() + b"\x01\x00")
                await writer.drain()
                await assert_rejected(reader)
                assert old.active == 1
            finally:
                await close_writer(writer)
        finally:
            await close_writer(held[1])
            await app.shutdown()
        assert old.active == app.limiter.active == 0


@pytest.mark.asyncio
async def test_lower_status_limit_preserves_held_exchanges_and_blocks_new_admissions() -> None:
    async with running_server(echo) as backend:
        config = make_config(
            server_port(backend), server_port(backend), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=18080),
            status=replace(config.status, enabled=True, always=True, max_concurrent=2),
        )
        app = FailoverApplication(config)
        await app.start()
        old = app.snapshot.status_service
        held: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []
        try:
            for _ in range(2):
                held.append(await hold_status(config.proxy.listen_port))
            assert await app.reload_config(
                replace(config, status=replace(config.status, max_concurrent=1))
            )
            # Both admitted exchanges survive the lower cap; neither old nor new
            # generation may admit work while the global count is at/above it.
            assert app.snapshot.status_service.active == old.active == 2
            assert not old.try_acquire()
            assert not app.snapshot.status_service.try_acquire()
            await finish_status(*held[0])
            held.pop(0)
            assert old.active == 1
            assert not old.try_acquire()
            await finish_status(*held[0])
            held.clear()
            reader, writer = await hold_status(config.proxy.listen_port)
            held.append((reader, writer))
            await finish_status(reader, writer)
            held.clear()
        finally:
            await asyncio.gather(*(close_writer(writer) for _, writer in held))
            await app.shutdown()
        assert app.snapshot.status_service.active == old.active == app.limiter.active == 0


def test_shared_status_budget_retains_per_service_release_ownership() -> None:
    old = LocalStatusService(replace(LocalStatusConfig(), max_concurrent=2))
    new = LocalStatusService(old.config, previous=old)
    new.activate()
    assert old.try_acquire()
    assert new.try_acquire()
    old.release()
    with pytest.raises(RuntimeError, match="released more than once"):
        old.release()
    assert new.active == 1
    new.release()
    assert old.active == new.active == 0


@pytest.mark.asyncio
async def test_slow_handshake_keeps_global_slot_through_reloads_and_shutdown() -> None:
    async with running_server(echo) as backend:
        config = make_config(
            server_port(backend), server_port(backend), proxy_port=await closed_local_port()
        )
        config = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=18080),
            status=replace(
                config.status,
                enabled=True,
                always=True,
                max_concurrent=1,
                handshake_timeout_seconds=30,
                session_timeout_seconds=30,
            ),
        )
        app = FailoverApplication(config)
        await app.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", config.proxy.listen_port)
        old = app.snapshot.status_service
        writer.write(handshake()[:1])
        await writer.drain()

        async def wait_for_slot() -> None:
            while old.active != 1:  # noqa: ASYNC110 -- externally observed handler admission
                await asyncio.sleep(0.001)

        try:
            await asyncio.wait_for(wait_for_slot(), 1)
            for _ in range(3):
                assert await app.reload_config(config)
            new_reader, new_writer = await asyncio.open_connection(
                "127.0.0.1", config.proxy.listen_port
            )
            try:
                new_writer.write(handshake() + b"\x01\x00")
                await new_writer.drain()
                await assert_rejected(new_reader)
                assert old.active == app.snapshot.status_service.active == 1
            finally:
                await close_writer(new_writer)
            app.request_shutdown()
            await app.shutdown()
            assert await asyncio.wait_for(reader.read(), 1) == b""
        finally:
            await close_writer(writer)
            await app.shutdown()
        assert old.active == app.snapshot.status_service.active == app.limiter.active == 0
        assert app.proxy.tasks.active_count == app.background.active_count == 0
