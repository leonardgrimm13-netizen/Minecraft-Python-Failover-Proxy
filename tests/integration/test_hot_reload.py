from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.config import load_config
from tests.conftest import (
    close_writer,
    closed_local_port,
    make_config,
    running_server,
    server_port,
)


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(4096):
            writer.write(data)
            await writer.drain()
    finally:
        await close_writer(writer)


async def exchange(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, data: bytes) -> None:
    writer.write(data)
    await writer.drain()
    assert await asyncio.wait_for(reader.readexactly(len(data)), 1) == data


@pytest.mark.asyncio
async def test_reload_preserves_live_session_and_rejects_listener_change() -> None:
    async with running_server(echo) as main, running_server(echo) as fallback:
        config = make_config(
            server_port(main), server_port(fallback), proxy_port=await closed_local_port()
        )
        config = replace(config, monitoring=replace(config.monitoring, listen_port=18080))
        app = FailoverApplication(config)
        await app.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", config.proxy.listen_port)
        try:
            await exchange(reader, writer, b"before reload")
            candidate = replace(
                config, healthcheck=replace(config.healthcheck, interval_seconds=0.1)
            )
            assert await app.reload_config(candidate)
            assert app.config == candidate
            await exchange(reader, writer, b"after reload")
            invalid = replace(candidate, proxy=replace(candidate.proxy, listen_port=25566))
            assert not await app.reload_config(invalid)
            assert app.config == candidate
            await exchange(reader, writer, b"after rejected reload")
            assert app.runtime.config_generation == 2
            assert app.runtime.reload_successes == 1
            assert app.runtime.reload_failures == 1
        finally:
            await close_writer(writer)
            await app.shutdown()
        assert app.proxy.tasks.active_count == 0
        assert app.background.active_count == 0
        assert app.limiter.active == 0


@pytest.mark.asyncio
async def test_reload_bad_file_and_shutdown_leave_current_generation(tmp_path: Path) -> None:
    config = make_config(25561, 25562, proxy_port=await closed_local_port())
    app = FailoverApplication(config)
    bad = tmp_path / "broken.toml"
    bad.write_text("[unterminated", encoding="utf-8")
    assert not await app.reload_config(bad)
    assert app.config is config
    assert app.runtime.reload_failures == 1
    app.request_shutdown()
    assert not await app.reload_config(config)
    await app.shutdown()


def test_reload_api_available_for_file_based_operator_control() -> None:
    config = load_config(Path("config.example.toml"))
    app = FailoverApplication(config)
    assert callable(getattr(app, "request_reload", None))


@pytest.mark.asyncio
async def test_reload_file_requests_coalesce_and_update_status(tmp_path: Path) -> None:
    async with running_server(echo) as main, running_server(echo) as fallback:
        proxy_port = await closed_local_port()
        source = await asyncio.to_thread(Path("config.example.toml").read_text, encoding="utf-8")
        source = source.replace('listen_host = "0.0.0.0"', 'listen_host = "127.0.0.1"')
        source = source.replace("listen_port = 25565", f"listen_port = {proxy_port}")
        source = source.replace("port = 25564", f"port = {server_port(main)}")
        source = source.replace("port = 25566", f"port = {server_port(fallback)}")
        source = source.replace("enabled = true\nlisten_host", "enabled = false\nlisten_host")
        path = tmp_path / "proxy.toml"
        path.write_text(source, encoding="utf-8")
        app = FailoverApplication(load_config(path), config_path=path)
        await app.start()
        try:
            path.write_text(
                source.replace(
                    'motd_online = "Minecraft network online"', 'motd_online = "Reloaded online"'
                ),
                encoding="utf-8",
            )
            for _ in range(100):
                app.request_reload()

            async def wait_for_reload() -> None:
                while app.runtime.reload_successes == 0:  # noqa: ASYNC110 -- externally observed reload, bounded by wait_for
                    await asyncio.sleep(0.005)

            await asyncio.wait_for(wait_for_reload(), 2)
            assert app.runtime.reload_successes == 1
            assert app.runtime.config_generation == 2
            assert app.config.status.motd_online == "Reloaded online"
        finally:
            await app.shutdown()


@pytest.mark.asyncio
async def test_prepare_failure_and_new_task_failure_leave_old_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with running_server(echo) as main, running_server(echo) as fallback:
        config = make_config(
            server_port(main), server_port(fallback), proxy_port=await closed_local_port()
        )
        config = replace(config, monitoring=replace(config.monitoring, listen_port=18080))
        app = FailoverApplication(config)
        await app.start()
        try:
            old = app.snapshot
            from mc_failover import app as app_module
            from mc_failover.endpoint_safety import EndpointLoopGuard

            original_guard = EndpointLoopGuard

            def invalid_guard(_config):
                raise OSError("resource preparation failed")

            monkeypatch.setattr(app_module, "EndpointLoopGuard", invalid_guard)
            assert not await app.reload_config(config)
            assert app.snapshot is old
            monkeypatch.setattr(app_module, "EndpointLoopGuard", original_guard)
            original_create = app.background.create
            count = 0

            def fail_second_create(coroutine):
                nonlocal count
                count += 1
                if count == 2:
                    coroutine.close()
                    raise RuntimeError("injected task preparation failure")
                return original_create(coroutine)

            monkeypatch.setattr(app.background, "create", fail_second_create)
            assert not await app.reload_config(config)
            assert app.snapshot is old
            assert app.config is config
            reader, writer = await asyncio.open_connection("127.0.0.1", config.proxy.listen_port)
            try:
                await exchange(reader, writer, b"rollback remains usable")
            finally:
                await close_writer(writer)
        finally:
            await app.shutdown()


@pytest.mark.asyncio
async def test_reload_preparation_is_cancelled_by_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    config = make_config(25561, 25562, proxy_port=await closed_local_port())
    app = FailoverApplication(config)
    entered = asyncio.Event()

    async def blocked_prepare(_source):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app, "_prepare_reload", blocked_prepare)
    task = asyncio.create_task(app.reload_config(config))
    await entered.wait()
    await app.shutdown()
    assert not await task
    assert app.runtime.config_generation == 1
    assert app.runtime.last_reload_result == "cancelled"


@pytest.mark.asyncio
async def test_caller_cancelling_reload_retains_old_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25561, 25562))
    entered = asyncio.Event()

    async def blocked_prepare(_source):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app, "_prepare_reload", blocked_prepare)
    task = asyncio.create_task(app.reload_config())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert app.runtime.config_generation == 1
    assert app.runtime.reload_failures == 1
    await app.shutdown()


@pytest.mark.asyncio
async def test_removed_main_monitor_does_not_retain_retired_backend() -> None:
    import gc
    import weakref

    async with running_server(echo) as main, running_server(echo) as fallback:
        config = make_config(
            server_port(main), server_port(fallback), proxy_port=await closed_local_port()
        )
        config = replace(config, monitoring=replace(config.monitoring, listen_port=18080))
        app = FailoverApplication(config)
        await app.start()
        try:
            removed = weakref.ref(app.registry.entries["MAIN"])
            candidate = replace(config, backends=(config.effective_backends[1],))
            assert await app.reload_config(candidate)
            await asyncio.sleep(0)
            gc.collect()
            assert removed() is None
            assert app.monitoring.main_health.healthy is None
        finally:
            await app.shutdown()
