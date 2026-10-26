from __future__ import annotations

import asyncio
import gc
import os
import weakref
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from mc_failover.circuit_breaker import CircuitBreaker
from mc_failover.config import (
    AppConfig,
    ConfigError,
    LocalStatusConfig,
    PassiveHealthConfig,
    ReloadConfig,
    RoutingConfig,
    load_config,
    validate_config,
)
from mc_failover.health import HealthState
from mc_failover.models import HealthCheckResult, MaintenanceMode, TargetName
from mc_failover.registry import BackendRegistry
from mc_failover.routing import MaintenanceWatcher, Router
from tests.conftest import FakeClock, make_config


def explicit_config(count: int = 3) -> AppConfig:
    config = make_config(25564, 25566, proxy_port=25565)
    base = config.effective_backends[0]
    return replace(
        config,
        backends=tuple(
            replace(
                base,
                id="MAIN" if index == 0 else f"FALLBACK_{index}",
                port=26000 + index,
                priority=index * 10,
            )
            for index in range(count)
        ),
    )


def registry_router(
    config: AppConfig, clock: FakeClock | None = None
) -> tuple[BackendRegistry, Router]:
    registry = BackendRegistry(config, clock=clock or FakeClock())
    first, second = registry.ordered[:2]
    router = Router(
        config,
        first.health,
        second.health,
        first.circuit,
        MaintenanceWatcher(config.maintenance),
        registry=registry,
    )
    for entry in registry.ordered:
        entry.health.report(HealthCheckResult(True, "test_ready"), initial=True)
    return registry, router


def new_toml(count: int = 3) -> str:
    prefix = """[proxy]
listen_host = "127.0.0.1"
listen_port = 25565
[connection]
timeout_seconds = 1.0
buffer_size = 4096
[logging]
level = "INFO"
"""
    return prefix + "".join(
        f'\n[[backends]]\nid = "{name}"\nhost = "127.0.0.1"\nport = {26000 + index}\npriority = {index * 10}\n'
        for index, name in enumerate(["MAIN", *(f"FALLBACK_{i}" for i in range(1, count))])
    )


@pytest.mark.parametrize("count", [1, 3, 5, 10])
def test_explicit_backend_parser_and_legacy_aliases(tmp_path: Path, count: int) -> None:
    path = tmp_path / "new.toml"
    path.write_text(new_toml(count), encoding="utf-8")
    config = load_config(path)
    assert len(config.effective_backends) == count
    assert config.main.port == 26000
    assert config.fallback.port == (26001 if count > 1 else 26000)
    assert config.backends[0].healthcheck.mode == "tcp"
    assert config.status.enabled is False


def test_complete_new_settings_and_nested_overrides(tmp_path: Path) -> None:
    path = tmp_path / "new.toml"
    path.write_text(
        new_toml(1)
        + """
[backends.healthcheck]
mode = "minecraft_status"
recover_after = 4
motd_must_contain = "READY"
[backends.circuit_breaker]
failure_threshold = 7
[routing]
max_connect_attempts = 5
connect_budget_seconds = 3
max_parallel_healthchecks = 2
[passive_health]
failure_threshold = 4
failure_window_seconds = 20
quarantine_seconds = 5
early_session_seconds = 1
[reload]
enabled = false
[maintenance]
mode = "force_target"
force_target = "MAIN"
[status]
enabled = true
always = true
maintenance = false
motd_offline = "offline\\nTry later"
players_online = 0
players_max = 50
protocol_version = 767
dynamic_hint = true
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.backends[0].healthcheck.recover_after == 4
    assert config.backends[0].circuit_breaker.failure_threshold == 7
    assert config.routing == RoutingConfig(5, 3, 2)
    assert config.passive_health == PassiveHealthConfig(True, 4, 20, 5, 1)
    assert config.reload == ReloadConfig(False)
    assert config.status.motd_offline == "offline\nTry later"
    assert config.status.players_max == 50


@pytest.mark.parametrize(
    "suffix",
    [
        '\n[main]\nhost="localhost"\nport=25564\n',
        '\n[healthcheck]\nmode="tcp"\n',
        '\n[[backends]]\nid="MAIN"\nhost="localhost"\nport=25566\n',
        '\n[[backends]]\nid="bad id"\nhost="localhost"\nport=25566\n',
        '\n[[backends]]\nid="NONE"\nhost="localhost"\nport=25566\n',
        "\n[backends.healthcheck]\nunrecognized=true\n",
        "\n[backends.circuit_breaker]\nunrecognized=true\n",
        "\n[routing]\nmax_connect_attempts=0\n",
        "\n[passive_health]\nfailure_threshold=true\n",
        '\n[maintenance]\nmode="force_target"\nforce_target="absent"\n',
        '\n[maintenance]\nforce_target="MAIN"\n',
        "\n[status]\nplayers_online=1\n",
        "\n[status]\nplayers_online=2\nplayers_max=1\n",
        '\n[status]\nfavicon="invalid"\n',
    ],
)
def test_new_config_rejects_malformed_or_ambiguous_settings(tmp_path: Path, suffix: str) -> None:
    path = tmp_path / "invalid.toml"
    path.write_text(new_toml(1) + suffix, encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(path)


@pytest.mark.parametrize("contents", ["backends=[]\n", "backends=42\n"])
def test_backend_table_must_be_nonempty_array(tmp_path: Path, contents: str) -> None:
    path = tmp_path / "invalid.toml"
    path.write_text(contents + new_toml(1).split("\n[[backends]]")[0], encoding="utf-8")
    with pytest.raises(ConfigError, match="backends"):
        load_config(path)


def test_configuration_file_is_bounded_and_utf8(tmp_path: Path) -> None:
    path = tmp_path / "large.toml"
    path.write_bytes(b"#" * (2 * 1024 * 1024 + 1))
    with pytest.raises(ConfigError, match="2 MiB"):
        load_config(path)
    path.write_bytes(b"\xff")
    with pytest.raises(ConfigError, match="UTF-8"):
        load_config(path)


def test_configuration_symlink_targets_regular_file(tmp_path: Path) -> None:
    target = tmp_path / "regular.toml"
    target.write_text(new_toml(), encoding="utf-8")
    link = tmp_path / "config.toml"
    link.symlink_to(target)
    assert len(load_config(link).backends) == 3


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO filesystem objects require POSIX")
def test_configuration_fifo_is_rejected_without_waiting_for_writer(tmp_path: Path) -> None:
    fifo = tmp_path / "config.pipe"
    os.mkfifo(fifo)
    with pytest.raises(ConfigError, match="reguläre Datei"):
        load_config(fifo)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [3, 5, 10])
async def test_priority_routing_skips_multiple_failures_and_all_unavailable(count: int) -> None:
    registry, router = registry_router(explicit_config(count))
    for index, entry in enumerate(registry.ordered):
        assert (await router.select_for_connection()).backend_id == entry.config.id
        entry.health.report(HealthCheckResult(False, "offline"), initial=True)
        assert entry.active_connections == 0
        assert entry.config.priority == index * 10
    assert router.snapshot().target is None
    registry.ordered[1].health.report(HealthCheckResult(True, "recovered"))
    assert router.snapshot().backend_id == "FALLBACK_1"


def test_equal_priority_declaration_order_disabled_and_excluded() -> None:
    config = explicit_config()
    config = replace(
        config, backends=tuple(replace(entry, priority=10) for entry in config.backends)
    )
    _registry, router = registry_router(config)
    assert router.snapshot().backend_id == "MAIN"
    config = replace(
        config, backends=(replace(config.backends[0], enabled=False), *config.backends[1:])
    )
    _registry, router = registry_router(config)
    assert router.snapshot().backend_id == "FALLBACK_1"


@pytest.mark.asyncio
async def test_excluded_backend_is_never_retried_and_circuit_recovery_is_bounded() -> None:
    clock = FakeClock()
    config = explicit_config()
    config = replace(
        config,
        backends=tuple(
            replace(entry, circuit_breaker=replace(entry.circuit_breaker, failure_threshold=1))
            for entry in config.backends
        ),
    )
    registry, router = registry_router(config, clock)
    first = await router.select_for_connection()
    assert first.circuit_permit is not None
    await registry.entries["MAIN"].circuit.record_failure(first.circuit_permit)
    assert router.snapshot().backend_id == "FALLBACK_1"
    assert (
        await router.select_for_connection(excluded=frozenset({"FALLBACK_1"}))
    ).backend_id == "FALLBACK_2"
    clock.advance(0.2)
    probe = await router.select_for_connection()
    assert (
        probe.backend_id == "MAIN"
        and probe.circuit_permit is not None
        and probe.circuit_permit.probe
    )
    assert (await router.select_for_connection()).backend_id == "FALLBACK_1"
    await registry.entries["MAIN"].circuit.record_success(probe.circuit_permit)
    assert router.snapshot().backend_id == "MAIN"


@pytest.mark.parametrize(
    "mode",
    [MaintenanceMode.FORCE_MAIN, MaintenanceMode.FORCE_FALLBACK, MaintenanceMode.FORCE_TARGET],
)
@pytest.mark.asyncio
async def test_forced_modes_fail_closed(mode: MaintenanceMode) -> None:
    config = explicit_config()
    forced = (
        "MAIN"
        if mode is MaintenanceMode.FORCE_MAIN
        else "FALLBACK"
        if mode is MaintenanceMode.FORCE_FALLBACK
        else "FALLBACK_2"
    )
    config = replace(
        config,
        maintenance=replace(
            config.maintenance,
            mode=mode,
            force_target="FALLBACK_2" if mode is MaintenanceMode.FORCE_TARGET else None,
        ),
    )
    registry, router = registry_router(config)
    if forced in registry.entries:
        assert router.snapshot().backend_id == forced
        registry.entries[forced].health.report(HealthCheckResult(False, "offline"), initial=True)
    assert (await router.select_for_connection()).target is None


@pytest.mark.asyncio
async def test_passive_window_quarantine_and_confirmed_recovery() -> None:
    clock = FakeClock()
    config = explicit_config()
    registry, router = registry_router(config, clock)
    entry = registry.entries["MAIN"]
    assert not registry.record_passive_failure("MAIN", "backend_reset")
    clock.advance(31)
    assert not registry.record_passive_failure("MAIN", "backend_reset")
    assert entry.passive_failures_in_window == 1
    assert not registry.record_passive_failure("MAIN", "backend_write_error")
    assert registry.record_passive_failure("MAIN", "backend_read_error")
    assert router.snapshot().backend_id == "FALLBACK_1"
    assert entry.quarantine_remaining_seconds == 10
    registry._report_active(entry, HealthCheckResult(True, "still_in_cooldown"))
    assert not bool(entry.routable)
    clock.advance(10)
    registry._report_active(entry, HealthCheckResult(False, "unavailable"))
    assert not bool(entry.routable)
    registry._report_active(entry, HealthCheckResult(True, "confirmed_recovery"))
    assert bool(entry.routable)
    assert entry.passive_failures == 4
    assert registry.record_passive_failure("missing", "backend_reset") is False


def test_disabled_passive_and_active_checks_have_explicit_recovery() -> None:
    clock = FakeClock()
    config = explicit_config()
    config = replace(
        config,
        backends=tuple(
            replace(entry, healthcheck=replace(entry.healthcheck, enabled=False))
            for entry in config.backends
        ),
    )
    registry, _router = registry_router(config, clock)
    for _ in range(3):
        registry.record_passive_failure("MAIN", "backend_reset")
    assert not bool(registry.entries["MAIN"].routable)
    clock.advance(10)
    assert bool(registry.entries["MAIN"].routable)
    disabled = BackendRegistry(
        replace(config, passive_health=replace(config.passive_health, enabled=False))
    )
    assert disabled.record_passive_failure("MAIN", "backend_reset") is False


def test_request_check_is_coalesced_and_throttled() -> None:
    clock = FakeClock()
    registry, _router = registry_router(explicit_config(), clock)
    entry = registry.entries["MAIN"]
    for _ in range(100):
        registry.request_check("MAIN")
    assert entry.check_requested.is_set()
    entry.check_requested.clear()
    registry.request_check("MAIN")
    assert not entry.check_requested.is_set()
    clock.advance(1)
    registry.request_check("MAIN")
    assert entry.check_requested.is_set()
    registry.request_check("absent")


@pytest.mark.asyncio
async def test_reload_reuses_unchanged_state_and_priority_only_statistics() -> None:
    config = explicit_config()
    previous, _router = registry_router(config)
    old = previous.entries["MAIN"]
    old.active_connections = 5
    old.connect_failures += 1
    old.connect_successes += 2
    same = BackendRegistry(config, previous=previous)
    assert same.entries["MAIN"] is not old
    assert same.entries["MAIN"].health is old.health
    assert same.entries["MAIN"].statistics is old.statistics
    changed = replace(
        config, backends=(replace(config.backends[0], priority=100), *config.backends[1:])
    )
    updated = BackendRegistry(changed, previous=previous)
    current = updated.entries["MAIN"]
    assert current is not old and current.health is old.health and current.circuit is old.circuit
    assert current.active_connections == 5
    old.active_connections -= 1
    assert current.active_connections == 4
    assert current.connect_failures == 1 and current.connect_successes == 2
    await same.check_all(initial=True)
    moved = BackendRegistry(
        replace(config, backends=(replace(config.backends[0], port=27000), *config.backends[1:])),
        previous=previous,
    )
    assert moved.entries["MAIN"].health is not old.health
    assert moved.entries["MAIN"].health.healthy is None


@pytest.mark.asyncio
async def test_healthchecks_parallelism_worker_cleanup_and_wakeup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = explicit_config(10)
    config = replace(config, routing=replace(config.routing, max_parallel_healthchecks=2))
    registry = BackendRegistry(config)
    active = 0
    maximum = 0
    checks = 0

    async def check_once(*, initial: bool = False) -> HealthCheckResult:
        nonlocal active, maximum, checks
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.002)
        checks += 1
        active -= 1
        return HealthCheckResult(True, "bounded_check")

    for entry in registry.ordered:
        monkeypatch.setattr(entry.monitor, "check_once", check_once)
    await registry.check_all(initial=True)
    assert checks == 10 and maximum == 2
    stop = asyncio.Event()
    task = asyncio.create_task(registry.run(stop))
    await asyncio.sleep(0)
    registry.request_check("MAIN")
    await asyncio.sleep(0.02)
    assert checks >= 11
    stop.set()
    await asyncio.wait_for(task, timeout=0.2)
    assert not any(task.get_name().startswith("health-") for task in asyncio.all_tasks())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("routing", cast(Any, False)),
        ("passive_health", cast(Any, False)),
        ("reload", cast(Any, False)),
        ("status", cast(Any, False)),
        ("backends", cast(Any, [])),
        ("backends", cast(Any, (False,))),
        ("routing", RoutingConfig(0)),
        ("routing", RoutingConfig(max_parallel_healthchecks=0)),
        ("routing", RoutingConfig(connect_budget_seconds=0)),
        ("passive_health", PassiveHealthConfig(enabled=cast(Any, 1))),
        ("passive_health", PassiveHealthConfig(quarantine_seconds=0)),
        ("status", LocalStatusConfig(max_handshake_bytes=0)),
        ("status", LocalStatusConfig(enabled=cast(Any, 1))),
        ("status", LocalStatusConfig(motd_online="bad\x00input")),
        ("status", LocalStatusConfig(version_name=" bad")),
        ("status", LocalStatusConfig(players_online=1)),
        ("status", LocalStatusConfig(players_online=2, players_max=1)),
    ],
)
def test_direct_dataclass_validation_keeps_new_security_boundaries(field: str, value: Any) -> None:
    with pytest.raises(ConfigError):
        validate_config(replace(explicit_config(), **{field: value}))


def test_legacy_adapter_is_dynamic_and_target_roles_remain_compatible() -> None:
    config = make_config(25564, 25566, proxy_port=25565)
    assert config.effective_backends[1].circuit_breaker.enabled is False
    changed = replace(config, main=replace(config.main, port=25567))
    assert changed.effective_backends[0].port == 25567
    registry = BackendRegistry(changed)
    assert registry.entries["MAIN"].target.name is TargetName.MAIN
    assert registry.entries["FALLBACK"].target.name is TargetName.FALLBACK


def test_retired_registry_is_collected_even_with_old_session_entry_alive() -> None:
    config = explicit_config()
    previous, _router = registry_router(config)
    old_session_entry = previous.entries["MAIN"]
    reference = weakref.ref(previous)
    updated = BackendRegistry(config, previous=previous)
    del previous, _router
    gc.collect()
    assert reference() is None
    old_session_entry.active_connections += 1
    assert updated.entries["MAIN"].active_connections == 1


def test_changed_check_preserves_session_counters_without_reusing_health() -> None:
    config = explicit_config()
    previous, _router = registry_router(config)
    old = previous.entries["MAIN"]
    old.active_connections = 7
    old.connect_failures = 2
    changed = replace(
        config,
        backends=(
            replace(
                config.backends[0],
                healthcheck=replace(config.backends[0].healthcheck, interval_seconds=4),
            ),
            *config.backends[1:],
        ),
    )
    current = BackendRegistry(changed, previous=previous).entries["MAIN"]
    assert current.health is not old.health and current.health.healthy is None
    assert current.statistics is old.statistics
    old.active_connections -= 1
    assert current.active_connections == 6 and current.connect_failures == 2


def test_passive_window_keeps_latest_samples_and_disabled_policy_routes_immediately() -> None:
    clock = FakeClock()
    config = explicit_config()
    previous, _router = registry_router(config, clock)
    for _ in range(10):
        previous.record_passive_failure("MAIN", "backend_reset")
        clock.advance(1)
    samples = previous.entries["MAIN"].statistics.failures
    assert tuple(samples) == (107, 108, 109)
    assert previous.entries["MAIN"].passive_failures == 10
    updated = BackendRegistry(
        replace(config, passive_health=replace(config.passive_health, enabled=False)),
        previous=previous,
        clock=clock,
    )
    assert updated.entries["MAIN"].routable
    assert not previous.entries["MAIN"].routable
    clock.advance(31)
    assert updated.entries["MAIN"].passive_failures_in_window == 0


def test_enabling_backend_requires_fresh_prepared_check() -> None:
    config = explicit_config()
    disabled_config = replace(
        config, backends=(replace(config.backends[0], enabled=False), *config.backends[1:])
    )
    old = BackendRegistry(disabled_config)
    enabled = BackendRegistry(config, previous=old)
    assert "MAIN" not in enabled._reused
    assert enabled.entries["MAIN"].health is not old.entries["MAIN"].health


def test_explicit_backend_options_override_injected_legacy_aliases() -> None:
    config = explicit_config()
    config = replace(
        config,
        backends=(
            replace(
                config.backends[0],
                healthcheck=replace(config.backends[0].healthcheck, enabled=False),
                circuit_breaker=replace(config.backends[0].circuit_breaker, failure_threshold=9),
            ),
            *config.backends[1:],
        ),
    )
    legacy_health = HealthState(TargetName.MAIN, config.healthcheck)
    legacy_circuit = CircuitBreaker(config.circuit_breaker)
    registry = BackendRegistry(config, main_health=legacy_health, circuit_breaker=legacy_circuit)
    assert registry.entries["MAIN"].health is not legacy_health
    assert registry.entries["MAIN"].health.routable
    assert registry.entries["MAIN"].circuit is not legacy_circuit
    assert registry.entries["MAIN"].circuit.failure_threshold == 9


@pytest.mark.asyncio
async def test_prepared_checks_cannot_mutate_shared_passive_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    config = explicit_config()
    previous, _router = registry_router(config, clock)
    for _ in range(3):
        previous.record_passive_failure("MAIN", "backend_reset")
    clock.advance(10)
    changed = replace(
        config,
        backends=(
            replace(
                config.backends[0],
                healthcheck=replace(config.backends[0].healthcheck, interval_seconds=4),
            ),
            *config.backends[1:],
        ),
    )
    candidate = BackendRegistry(changed, previous=previous, clock=clock)

    async def healthy(*_args: Any, **_kwargs: Any) -> HealthCheckResult:
        return HealthCheckResult(True, "candidate_healthy")

    monkeypatch.setattr("mc_failover.health.perform_health_check", healthy)
    shared = previous.entries["MAIN"].statistics
    await candidate.check_all(initial=True, preparing=True)
    assert shared.quarantine_until is not None
    assert shared.recovery_successes == 0
    assert not previous.entries["MAIN"].routable
    assert candidate.entries["MAIN"].monitor.on_result is not None
    await candidate.entries["MAIN"].monitor.check_once()
    assert shared.quarantine_until is None
