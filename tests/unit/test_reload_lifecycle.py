from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from mc_failover.app import FailoverApplication
from tests.conftest import closed_local_port, make_config


async def application() -> FailoverApplication:
    config = make_config(25561, 25562, proxy_port=await closed_local_port())
    return FailoverApplication(
        replace(
            config,
            healthcheck=replace(config.healthcheck, enabled=False),
            fallback_healthcheck=replace(config.fallback_healthcheck, enabled=False),
            monitoring=replace(config.monitoring, listen_port=18080),
        )
    )


@pytest.mark.asyncio
async def test_prestart_reload_is_rejected_without_generations_or_workers() -> None:
    app = await application()
    original = app.snapshot
    try:
        for count in range(1, 11):
            assert not await app.reload_config(app.config)
            assert app.snapshot is original
            assert app.runtime.config_generation == 1
            assert app.runtime.reload_successes == 0
            assert app.runtime.reload_failures == count
            assert app.runtime.last_reload_result == "not_started"
            assert app._reload_preparation is None
            assert app._generation_tasks == ()
            assert app.background.active_count == 0
        await app.start()
        assert app.background.active_count == 3
        assert await app.reload_config(app.config)
        assert app.runtime.config_generation == 2
        assert app.runtime.reload_successes == 1
        assert app.runtime.reload_failures == 10
        assert app.runtime.last_reload_result == "success"
        assert len(app._generation_tasks) == 2
        assert app.background.active_count == 3
    finally:
        await app.shutdown()
    assert app.background.active_count == 0
    assert app.proxy.tasks.active_count == 0


@pytest.mark.asyncio
async def test_reload_during_startup_cannot_activate_a_second_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = await application()
    original = app.snapshot
    entered, proceed = asyncio.Event(), asyncio.Event()
    original_check_all = app.registry.check_all

    async def gated_checks(*, initial: bool = False) -> None:
        entered.set()
        await proceed.wait()
        await original_check_all(initial=initial)

    monkeypatch.setattr(app.registry, "check_all", gated_checks)
    starting = asyncio.create_task(app.start())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert not await app.reload_config(app.config)
        assert app.runtime.last_reload_result == "not_started"
        assert app.snapshot is original
        assert app.runtime.config_generation == 1
        assert app.background.active_count == 0
        proceed.set()
        await starting
        assert app.background.active_count == 3
        assert await app.reload_config(app.config)
        assert app.runtime.config_generation == 2
        assert app.background.active_count == 3
    finally:
        proceed.set()
        await starting
        await app.shutdown()
    assert app.background.active_count == 0


@pytest.mark.asyncio
async def test_shutdown_before_start_does_not_count_rejected_reload_as_failure() -> None:
    app = await application()
    app.request_shutdown()
    assert not await app.reload_config(app.config)
    assert app.runtime.reload_failures == 0
    assert app.runtime.config_generation == 1
    assert app.background.active_count == 0
    await app.shutdown()
