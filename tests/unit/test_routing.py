from __future__ import annotations

import asyncio
import itertools
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mc_failover.circuit_breaker import CircuitBreaker
from mc_failover.config import AppConfig, MaintenanceConfig, load_config
from mc_failover.health import HealthState
from mc_failover.models import (
    HealthCheckResult,
    HealthStatus,
    MaintenanceMode,
    RoutingReason,
    TargetName,
)
from mc_failover.routing import MaintenanceWatcher, Router

BASE_TOML = """\
[proxy]
listen_host = "127.0.0.1"
listen_port = 25565

[main]
host = "127.0.0.1"
port = 25564

[fallback]
host = "127.0.0.1"
port = 25566

[healthcheck]
mode = "tcp"
interval_seconds = 3.0
timeout_seconds = 2.0
fail_after = 2
recover_after = 2

[connection]
timeout_seconds = 5.0
buffer_size = 65536

[logging]
level = "INFO"
"""


class FakeClock:
    def __init__(self) -> None:
        self.monotonic_value = 100.0
        self.utc_value = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)

    def monotonic(self) -> float:
        return self.monotonic_value

    def utc_now(self) -> datetime:
        return self.utc_value

    def advance(self, seconds: float) -> None:
        self.monotonic_value += seconds
        self.utc_value += timedelta(seconds=seconds)


def load_test_config(tmp_path: Path) -> AppConfig:
    path = tmp_path / "routing.toml"
    path.write_text(BASE_TOML, encoding="utf-8")
    return load_config(path)


def health_state(
    config: AppConfig,
    target: TargetName,
    status: HealthStatus,
    clock: FakeClock,
) -> HealthState:
    check = config.healthcheck if target is TargetName.MAIN else config.fallback_healthcheck
    if status is HealthStatus.DISABLED:
        check = replace(check, enabled=False)
    state = HealthState(target, check, clock=clock)
    if status is HealthStatus.HEALTHY:
        state.report(HealthCheckResult(True, "test_healthy"), initial=True)
    elif status is HealthStatus.UNHEALTHY:
        state.report(HealthCheckResult(False, "test_unhealthy"), initial=True)
    return state


def make_router(
    config: AppConfig,
    main_status: HealthStatus,
    fallback_status: HealthStatus,
    *,
    clock: FakeClock,
    maintenance_mode: MaintenanceMode = MaintenanceMode.AUTO,
    failure_threshold: int = 1,
    open_seconds: float = 5.0,
) -> tuple[Router, CircuitBreaker, MaintenanceWatcher]:
    maintenance_config = replace(config.maintenance, mode=maintenance_mode)
    config = replace(config, maintenance=maintenance_config)
    main = health_state(config, TargetName.MAIN, main_status, clock)
    fallback = health_state(config, TargetName.FALLBACK, fallback_status, clock)
    breaker = CircuitBreaker(
        enabled=True,
        failure_threshold=failure_threshold,
        failure_window_seconds=10.0,
        open_seconds=open_seconds,
        half_open_max_attempts=1,
        clock=clock,
    )
    watcher = MaintenanceWatcher(maintenance_config)
    return Router(config, main, fallback, breaker, watcher), breaker, watcher


ALL_HEALTH_STATES = tuple(HealthStatus)


@pytest.mark.parametrize(
    ("main_status", "fallback_status"),
    list(itertools.product(ALL_HEALTH_STATES, repeat=2)),
)
def test_auto_routing_complete_health_matrix(
    tmp_path: Path,
    main_status: HealthStatus,
    fallback_status: HealthStatus,
) -> None:
    config = load_test_config(tmp_path)
    clock = FakeClock()
    router, _breaker, _watcher = make_router(config, main_status, fallback_status, clock=clock)

    decision = router.snapshot()
    main_routable = main_status in {HealthStatus.HEALTHY, HealthStatus.DISABLED}
    fallback_routable = fallback_status in {HealthStatus.HEALTHY, HealthStatus.DISABLED}
    if main_routable:
        assert decision.active_target is TargetName.MAIN
        assert decision.reason is RoutingReason.MAIN_HEALTHY
        assert decision.ready is True
        assert decision.degraded is (
            main_status is not HealthStatus.HEALTHY or fallback_status is not HealthStatus.HEALTHY
        )
    elif fallback_routable:
        assert decision.active_target is TargetName.FALLBACK
        assert decision.reason is RoutingReason.MAIN_UNAVAILABLE_FALLBACK_HEALTHY
        assert decision.ready is True
        assert decision.degraded is True
    else:
        assert decision.target is None
        assert decision.active_target is TargetName.NONE
        assert decision.reason is RoutingReason.NO_TARGET_AVAILABLE
        assert decision.ready is False
        assert decision.degraded is True


@pytest.mark.asyncio
async def test_open_circuit_routes_directly_to_healthy_fallback(tmp_path: Path) -> None:
    config = load_test_config(tmp_path)
    clock = FakeClock()
    router, breaker, _watcher = make_router(
        config, HealthStatus.HEALTHY, HealthStatus.HEALTHY, clock=clock
    )
    permit = await breaker.acquire()
    await breaker.record_failure(permit)

    snapshot = router.snapshot()
    assert snapshot.active_target is TargetName.FALLBACK
    assert snapshot.reason is RoutingReason.MAIN_CIRCUIT_OPEN_FALLBACK_HEALTHY
    selected = await router.select_for_connection()
    assert selected.active_target is TargetName.FALLBACK
    assert selected.reason is RoutingReason.MAIN_CIRCUIT_OPEN_FALLBACK_HEALTHY
    assert selected.circuit_permit is None


@pytest.mark.asyncio
async def test_expired_open_circuit_allows_one_controlled_half_open_main_probe(
    tmp_path: Path,
) -> None:
    config = load_test_config(tmp_path)
    clock = FakeClock()
    router, breaker, _watcher = make_router(
        config,
        HealthStatus.HEALTHY,
        HealthStatus.HEALTHY,
        clock=clock,
        open_seconds=4.0,
    )
    permit = await breaker.acquire()
    await breaker.record_failure(permit)
    clock.advance(4.0)

    first = await router.select_for_connection()
    second = await router.select_for_connection()
    assert first.active_target is TargetName.MAIN
    assert first.reason is RoutingReason.MAIN_CIRCUIT_HALF_OPEN
    assert first.circuit_permit is not None and first.circuit_permit.probe
    assert second.active_target is TargetName.FALLBACK
    assert second.reason is RoutingReason.MAIN_CIRCUIT_OPEN_FALLBACK_HEALTHY

    await breaker.release(first.circuit_permit)


@pytest.mark.asyncio
async def test_open_circuit_and_unavailable_fallback_yields_no_target(tmp_path: Path) -> None:
    config = load_test_config(tmp_path)
    clock = FakeClock()
    router, breaker, _watcher = make_router(
        config, HealthStatus.HEALTHY, HealthStatus.UNHEALTHY, clock=clock
    )
    permit = await breaker.acquire()
    await breaker.record_failure(permit)

    decision = await router.select_for_connection()
    assert decision.target is None
    assert decision.active_target is TargetName.NONE
    assert decision.reason is RoutingReason.NO_TARGET_AVAILABLE
    assert decision.ready is False


@pytest.mark.parametrize(
    ("mode", "forced_status", "other_status", "target", "reason"),
    [
        (
            MaintenanceMode.FORCE_MAIN,
            HealthStatus.HEALTHY,
            HealthStatus.HEALTHY,
            TargetName.MAIN,
            RoutingReason.FORCE_MAIN,
        ),
        (
            MaintenanceMode.FORCE_MAIN,
            HealthStatus.DISABLED,
            HealthStatus.HEALTHY,
            TargetName.MAIN,
            RoutingReason.FORCE_MAIN,
        ),
        (
            MaintenanceMode.FORCE_FALLBACK,
            HealthStatus.HEALTHY,
            HealthStatus.HEALTHY,
            TargetName.FALLBACK,
            RoutingReason.FORCE_FALLBACK,
        ),
        (
            MaintenanceMode.FORCE_FALLBACK,
            HealthStatus.DISABLED,
            HealthStatus.HEALTHY,
            TargetName.FALLBACK,
            RoutingReason.FORCE_FALLBACK,
        ),
    ],
)
def test_force_modes_route_only_to_the_requested_available_target(
    tmp_path: Path,
    mode: MaintenanceMode,
    forced_status: HealthStatus,
    other_status: HealthStatus,
    target: TargetName,
    reason: RoutingReason,
) -> None:
    config = load_test_config(tmp_path)
    clock = FakeClock()
    main_status = forced_status if mode is MaintenanceMode.FORCE_MAIN else other_status
    fallback_status = forced_status if mode is MaintenanceMode.FORCE_FALLBACK else other_status
    router, _breaker, _watcher = make_router(
        config,
        main_status,
        fallback_status,
        clock=clock,
        maintenance_mode=mode,
    )

    decision = router.snapshot()
    assert decision.active_target is target
    assert decision.requested_target is target
    assert decision.reason is reason
    assert decision.ready is True
    assert decision.degraded is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "main_status", "fallback_status", "requested", "reason"),
    [
        (
            MaintenanceMode.FORCE_MAIN,
            HealthStatus.UNHEALTHY,
            HealthStatus.HEALTHY,
            TargetName.MAIN,
            RoutingReason.FORCED_MAIN_UNAVAILABLE,
        ),
        (
            MaintenanceMode.FORCE_FALLBACK,
            HealthStatus.HEALTHY,
            HealthStatus.UNHEALTHY,
            TargetName.FALLBACK,
            RoutingReason.FORCED_FALLBACK_UNAVAILABLE,
        ),
        (
            MaintenanceMode.FORCE_MAIN,
            HealthStatus.UNKNOWN,
            HealthStatus.DISABLED,
            TargetName.MAIN,
            RoutingReason.FORCED_MAIN_UNAVAILABLE,
        ),
        (
            MaintenanceMode.FORCE_FALLBACK,
            HealthStatus.DISABLED,
            HealthStatus.UNKNOWN,
            TargetName.FALLBACK,
            RoutingReason.FORCED_FALLBACK_UNAVAILABLE,
        ),
    ],
)
async def test_unavailable_forced_target_never_silently_uses_other_target(
    tmp_path: Path,
    mode: MaintenanceMode,
    main_status: HealthStatus,
    fallback_status: HealthStatus,
    requested: TargetName,
    reason: RoutingReason,
) -> None:
    config = load_test_config(tmp_path)
    router, _breaker, _watcher = make_router(
        config,
        main_status,
        fallback_status,
        clock=FakeClock(),
        maintenance_mode=mode,
    )

    snapshot = router.snapshot()
    selected = await router.select_for_connection()
    for decision in (snapshot, selected):
        assert decision.target is None
        assert decision.active_target is TargetName.NONE
        assert decision.requested_target is requested
        assert decision.reason is reason
        assert decision.ready is False


@pytest.mark.asyncio
async def test_force_main_does_not_bypass_an_open_circuit_or_fall_back(tmp_path: Path) -> None:
    config = load_test_config(tmp_path)
    clock = FakeClock()
    router, breaker, _watcher = make_router(
        config,
        HealthStatus.HEALTHY,
        HealthStatus.HEALTHY,
        clock=clock,
        maintenance_mode=MaintenanceMode.FORCE_MAIN,
    )
    permit = await breaker.acquire()
    await breaker.record_failure(permit)

    decision = await router.select_for_connection()
    assert decision.target is None
    assert decision.requested_target is TargetName.MAIN
    assert decision.reason is RoutingReason.FORCED_MAIN_UNAVAILABLE


def test_shutdown_never_returns_a_routing_target(tmp_path: Path) -> None:
    config = load_test_config(tmp_path)
    router, _breaker, _watcher = make_router(
        config, HealthStatus.HEALTHY, HealthStatus.HEALTHY, clock=FakeClock()
    )
    decision = router.snapshot(shutting_down=True)
    assert decision.target is None
    assert decision.active_target is TargetName.NONE
    assert decision.reason is RoutingReason.SHUTTING_DOWN
    assert decision.ready is False


@pytest.mark.asyncio
async def test_file_maintenance_overrides_are_cached_and_fallback_has_priority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fallback_file = tmp_path / "force-fallback"
    main_file = tmp_path / "force-main"
    config = MaintenanceConfig(
        mode=MaintenanceMode.AUTO,
        force_fallback_file=fallback_file,
        force_main_file=main_file,
        file_check_interval_seconds=1.0,
    )
    watcher = MaintenanceWatcher(config)

    async def immediate_to_thread(function, /, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", immediate_to_thread)
    assert (await watcher.refresh()).mode is MaintenanceMode.AUTO
    main_file.touch()
    assert (await watcher.refresh()).mode is MaintenanceMode.FORCE_MAIN
    fallback_file.touch()
    snapshot = await watcher.refresh()
    assert snapshot.mode is MaintenanceMode.FORCE_FALLBACK
    assert snapshot.source == "force_fallback_file"


@pytest.mark.asyncio
async def test_static_maintenance_mode_ignores_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = MaintenanceConfig(
        mode=MaintenanceMode.FORCE_MAIN,
        force_fallback_file=tmp_path / "fallback",
        force_main_file=tmp_path / "main",
        file_check_interval_seconds=1.0,
    )
    watcher = MaintenanceWatcher(config)

    async def forbidden_to_thread(*_args, **_kwargs):
        raise AssertionError("static maintenance mode must not touch the filesystem")

    monkeypatch.setattr(asyncio, "to_thread", forbidden_to_thread)
    snapshot = await watcher.refresh()
    assert snapshot.mode is MaintenanceMode.FORCE_MAIN
    assert snapshot.source == "config"
