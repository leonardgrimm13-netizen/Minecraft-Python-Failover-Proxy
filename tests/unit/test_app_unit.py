from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.models import HealthCheckResult
from tests.conftest import make_config


class _Socket:
    def __init__(self, address: tuple[str, int]) -> None:
        self.address = address

    def getsockname(self) -> tuple[str, int]:
        return self.address


class _Server:
    def __init__(self, *addresses: tuple[str, int]) -> None:
        self.sockets = [_Socket(address) for address in addresses]


class _ClosingTracker:
    def __init__(self) -> None:
        self.created = 0

    def create(self, coroutine: Coroutine[Any, Any, object]) -> MagicMock:
        self.created += 1
        coroutine.close()
        return MagicMock()


@pytest.mark.asyncio
async def test_main_health_callback_only_releases_circuit_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    record = AsyncMock()
    monkeypatch.setattr(app.circuit_breaker, "record_health_success", record)
    callback = app.main_monitor.on_result
    assert callback is not None

    await callback(HealthCheckResult(False, "down"))
    record.assert_not_awaited()
    await callback(HealthCheckResult(True, "up"))
    record.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_start_initializes_checks_listeners_tasks_and_logs_monitoring(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566, monitoring=True))
    refresh = AsyncMock()
    main_check = AsyncMock()
    fallback_check = AsyncMock()
    monkeypatch.setattr(app.maintenance, "refresh", refresh)
    monkeypatch.setattr(app.endpoint_guard, "validate_all", AsyncMock())
    monkeypatch.setattr(app.main_monitor, "check_once", main_check)
    monkeypatch.setattr(app.fallback_monitor, "check_once", fallback_check)
    proxy_server = _Server(("127.0.0.1", 19132), ("::1", 19132))
    monitoring_server = _Server(("127.0.0.1", 8080))
    proxy_start = AsyncMock(return_value=proxy_server)
    monitoring_start = AsyncMock(return_value=monitoring_server)
    monkeypatch.setattr(app.proxy, "start", proxy_start)
    monkeypatch.setattr(app.monitoring, "start", monitoring_start)
    tracker = _ClosingTracker()
    app.background = tracker  # type: ignore[assignment]

    with caplog.at_level("INFO", logger="mc-failover.app"):
        await app.start()

    refresh.assert_awaited_once_with()
    main_check.assert_awaited_once_with(initial=True)
    fallback_check.assert_awaited_once_with(initial=True)
    proxy_start.assert_awaited_once_with()
    monitoring_start.assert_awaited_once_with()
    assert tracker.created == 3
    assert app._started
    assert "Proxy listening sockets=" in caplog.text
    assert "Monitoring listening sockets=" in caplog.text

    with pytest.raises(RuntimeError, match="already started"):
        await app.start()


@pytest.mark.asyncio
async def test_start_without_monitoring_omits_monitoring_log(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    monkeypatch.setattr(app.maintenance, "refresh", AsyncMock())
    monkeypatch.setattr(app.main_monitor, "check_once", AsyncMock())
    monkeypatch.setattr(app.fallback_monitor, "check_once", AsyncMock())
    monkeypatch.setattr(app.proxy, "start", AsyncMock(return_value=_Server()))
    monkeypatch.setattr(app.monitoring, "start", AsyncMock(return_value=None))
    tracker = _ClosingTracker()
    app.background = tracker  # type: ignore[assignment]

    with caplog.at_level("INFO", logger="mc-failover.app"):
        await app.start()

    assert "Monitoring listening sockets=" not in caplog.text
    assert tracker.created == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [OSError("bind"), asyncio.CancelledError()])
async def test_start_rolls_back_partial_listener_start(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    proxy_stop = AsyncMock()
    monitoring_stop = AsyncMock()
    wait_closed = AsyncMock()
    monkeypatch.setattr(app.maintenance, "refresh", AsyncMock())
    monkeypatch.setattr(app.main_monitor, "check_once", AsyncMock())
    monkeypatch.setattr(app.fallback_monitor, "check_once", AsyncMock())
    monkeypatch.setattr(
        app.proxy,
        "start",
        AsyncMock(return_value=_Server(("127.0.0.1", 19132))),
    )
    monkeypatch.setattr(app.monitoring, "start", AsyncMock(side_effect=failure))
    monkeypatch.setattr(app.proxy, "stop_accepting", proxy_stop)
    monkeypatch.setattr(app.monitoring, "stop", monitoring_stop)
    monkeypatch.setattr(app.proxy, "wait_listener_closed", wait_closed)

    with pytest.raises(type(failure)):
        await app.start()

    proxy_stop.assert_awaited_once_with()
    monitoring_stop.assert_awaited_once_with()
    wait_closed.assert_awaited_once_with()
    assert not app._started


def test_request_shutdown_sets_stop_event() -> None:
    app = FailoverApplication(make_config(25565, 25566))
    assert not app.stop_event.is_set()
    app.request_shutdown()
    assert app.stop_event.is_set()
    assert app.runtime.shutting_down


@pytest.mark.asyncio
async def test_shutdown_request_cancels_hanging_initial_checks_before_listeners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    main_entered = asyncio.Event()
    fallback_entered = asyncio.Event()
    cancelled = 0

    async def blocking_check(entered: asyncio.Event, *, initial: bool = False) -> None:
        nonlocal cancelled
        assert initial
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled += 1

    monkeypatch.setattr(app.maintenance, "refresh", AsyncMock())
    monkeypatch.setattr(
        app.main_monitor,
        "check_once",
        lambda *, initial=False: blocking_check(main_entered, initial=initial),
    )
    monkeypatch.setattr(
        app.fallback_monitor,
        "check_once",
        lambda *, initial=False: blocking_check(fallback_entered, initial=initial),
    )
    proxy_start = AsyncMock()
    monitoring_start = AsyncMock()
    monkeypatch.setattr(app.proxy, "start", proxy_start)
    monkeypatch.setattr(app.monitoring, "start", monitoring_start)

    startup = asyncio.create_task(app.start())
    await asyncio.gather(main_entered.wait(), fallback_entered.wait())
    app.request_shutdown()
    await asyncio.wait_for(startup, timeout=0.2)

    assert cancelled == 2
    proxy_start.assert_not_awaited()
    monitoring_start.assert_not_awaited()
    assert app._started
    assert app.runtime.shutting_down
    await app.shutdown()


@pytest.mark.asyncio
async def test_start_after_preexisting_shutdown_request_opens_no_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    app.request_shutdown()
    refresh = AsyncMock()
    proxy_start = AsyncMock()
    monkeypatch.setattr(app.maintenance, "refresh", refresh)
    monkeypatch.setattr(app.proxy, "start", proxy_start)

    await app.start()

    refresh.assert_not_awaited()
    proxy_start.assert_not_awaited()
    assert app._started
    await app.shutdown()


@pytest.mark.asyncio
async def test_run_until_stopped_starts_waits_and_shuts_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))

    async def start_and_request_stop() -> None:
        app.request_shutdown()

    start = AsyncMock(side_effect=start_and_request_stop)
    shutdown = AsyncMock()
    monkeypatch.setattr(app, "start", start)
    monkeypatch.setattr(app, "shutdown", shutdown)

    await app.run_until_stopped()

    start.assert_awaited_once_with()
    shutdown.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_until_stopped_shuts_down_when_owner_task_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    started = asyncio.Event()

    async def fake_start() -> None:
        started.set()

    shutdown = AsyncMock()
    monkeypatch.setattr(app, "start", fake_start)
    monkeypatch.setattr(app, "shutdown", shutdown)

    owner = asyncio.create_task(app.run_until_stopped())
    await started.wait()
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    shutdown.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_concurrent_start_calls_cannot_open_duplicate_listeners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_refresh() -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(app.maintenance, "refresh", slow_refresh)
    monkeypatch.setattr(app.main_monitor, "check_once", AsyncMock())
    monkeypatch.setattr(app.fallback_monitor, "check_once", AsyncMock())
    proxy_start = AsyncMock(return_value=_Server())
    monkeypatch.setattr(app.proxy, "start", proxy_start)
    monkeypatch.setattr(app.monitoring, "start", AsyncMock(return_value=None))
    app.background = _ClosingTracker()  # type: ignore[assignment]

    first = asyncio.create_task(app.start())
    await entered.wait()
    second = asyncio.create_task(app.start())
    await asyncio.sleep(0)
    release.set()
    await first
    with pytest.raises(RuntimeError, match="already started"):
        await second
    proxy_start.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_shutdown_interrupts_startup_and_waits_for_start_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    entered = asyncio.Event()

    async def hanging_refresh() -> None:
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app.maintenance, "refresh", hanging_refresh)
    proxy_start = AsyncMock()
    monkeypatch.setattr(app.proxy, "start", proxy_start)
    monkeypatch.setattr(app.proxy, "stop_accepting", AsyncMock())
    monkeypatch.setattr(app.monitoring, "stop", AsyncMock())
    monkeypatch.setattr(app.background, "cancel_all", AsyncMock())
    monkeypatch.setattr(app.proxy, "shutdown_connections", AsyncMock())

    startup = asyncio.create_task(app.start())
    await entered.wait()
    await asyncio.wait_for(app.shutdown(), timeout=0.2)
    await asyncio.wait_for(startup, timeout=0.2)

    proxy_start.assert_not_awaited()
    assert app._shutdown_complete


@pytest.mark.asyncio
async def test_shutdown_cancels_hanging_listener_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def hanging_proxy_start() -> _Server:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        raise AssertionError("unreachable")

    monkeypatch.setattr(app.maintenance, "refresh", AsyncMock())
    monkeypatch.setattr(app.endpoint_guard, "validate_all", AsyncMock())
    monkeypatch.setattr(app.main_monitor, "check_once", AsyncMock())
    monkeypatch.setattr(app.fallback_monitor, "check_once", AsyncMock())
    monkeypatch.setattr(app.proxy, "start", hanging_proxy_start)
    monkeypatch.setattr(app.proxy, "stop_accepting", AsyncMock())
    monkeypatch.setattr(app.proxy, "wait_listener_closed", AsyncMock())
    monkeypatch.setattr(app.monitoring, "stop", AsyncMock())
    monkeypatch.setattr(app.background, "cancel_all", AsyncMock())
    monkeypatch.setattr(app.proxy, "shutdown_connections", AsyncMock())

    startup = asyncio.create_task(app.start())
    await entered.wait()
    await asyncio.wait_for(app.shutdown(), timeout=0.2)
    await asyncio.wait_for(startup, timeout=0.2)

    assert cancelled.is_set()
    assert app._shutdown_complete


@pytest.mark.asyncio
async def test_shutdown_stops_all_components_and_is_idempotent(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    app.runtime.active_connections = 3
    app.runtime.incoming_connections_total = 12
    app.runtime.backend_connections_established_total = 8
    app.runtime.connections_rejected_total = 4
    proxy_stop = AsyncMock()
    monitoring_stop = AsyncMock()
    background_cancel = AsyncMock()
    connections_shutdown = AsyncMock()
    monkeypatch.setattr(app.proxy, "stop_accepting", proxy_stop)
    monkeypatch.setattr(app.monitoring, "stop", monitoring_stop)
    monkeypatch.setattr(app.background, "cancel_all", background_cancel)
    monkeypatch.setattr(app.proxy, "shutdown_connections", connections_shutdown)

    with caplog.at_level("INFO", logger="mc-failover.app"):
        await app.shutdown()
        await app.shutdown()

    assert app.runtime.shutting_down
    assert app.stop_event.is_set()
    assert app._shutdown_complete
    proxy_stop.assert_awaited_once_with()
    monitoring_stop.assert_awaited_once_with()
    background_cancel.assert_awaited_once_with(
        app.config.connection.shutdown_cancel_timeout_seconds
    )
    connections_shutdown.assert_awaited_once_with()
    assert "active_connections=3" in caplog.text
    assert "incoming_connections_total=12" in caplog.text
    assert "backend_connections_established_total=8" in caplog.text
    assert "connections_rejected_total=4" in caplog.text


@pytest.mark.asyncio
async def test_shutdown_lock_serializes_concurrent_callers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_stop() -> None:
        entered.set()
        await release.wait()

    proxy_stop = AsyncMock(side_effect=slow_stop)
    connections_shutdown = AsyncMock()
    monkeypatch.setattr(app.proxy, "stop_accepting", proxy_stop)
    monkeypatch.setattr(app.monitoring, "stop", AsyncMock())
    monkeypatch.setattr(app.background, "cancel_all", AsyncMock())
    monkeypatch.setattr(app.proxy, "shutdown_connections", connections_shutdown)

    first = asyncio.create_task(app.shutdown())
    await entered.wait()
    second = asyncio.create_task(app.shutdown())
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    proxy_stop.assert_awaited_once_with()
    connections_shutdown.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_shutdown_stops_independent_components_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    entered = {name: asyncio.Event() for name in ("monitoring", "background", "connections")}
    release = asyncio.Event()

    async def block(name: str, *_args: object) -> None:
        entered[name].set()
        await release.wait()

    monkeypatch.setattr(app.proxy, "stop_accepting", AsyncMock())
    monkeypatch.setattr(app.monitoring, "stop", lambda: block("monitoring"))
    monkeypatch.setattr(app.background, "cancel_all", lambda *_args: block("background"))
    monkeypatch.setattr(app.proxy, "shutdown_connections", lambda: block("connections"))

    shutdown = asyncio.create_task(app.shutdown())
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in entered.values())),
        timeout=0.2,
    )
    release.set()
    await shutdown

    assert app._shutdown_complete


@pytest.mark.asyncio
async def test_shutdown_collects_all_components_before_reporting_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FailoverApplication(make_config(25565, 25566))
    completed = asyncio.Event()

    async def fail_monitoring() -> None:
        raise OSError("monitoring cleanup failed")

    async def complete_connections() -> None:
        await asyncio.sleep(0)
        completed.set()

    monkeypatch.setattr(app.proxy, "stop_accepting", AsyncMock())
    monkeypatch.setattr(app.monitoring, "stop", fail_monitoring)
    monkeypatch.setattr(app.background, "cancel_all", AsyncMock())
    monkeypatch.setattr(app.proxy, "shutdown_connections", complete_connections)

    with pytest.raises(RuntimeError, match="shutdown component failed"):
        await app.shutdown()

    assert completed.is_set()
    assert not app._shutdown_complete
