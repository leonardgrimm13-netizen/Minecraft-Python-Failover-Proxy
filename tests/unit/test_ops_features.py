from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone

from mc_failover.circuit_breaker import CircuitBreaker
from mc_failover.config import BackendConfig
from mc_failover.endpoint_safety import EndpointLoopGuard
from mc_failover.health import HealthState
from mc_failover.metrics import render_metrics
from mc_failover.models import HealthCheckResult, TargetName
from mc_failover.monitoring import HttpRequest, MonitoringServer, build_monitoring_payload
from mc_failover.routing import MaintenanceWatcher, Router
from mc_failover.runtime import RuntimeState
from tests.conftest import make_config


def context():
    config = make_config(25564, 25566)
    main = HealthState(TargetName.MAIN, config.healthcheck)
    fallback = HealthState(TargetName.FALLBACK, config.fallback_healthcheck)
    main.report(HealthCheckResult(True, "ok", 1.25), initial=True)
    fallback.report(HealthCheckResult(True, "ok", 2.5), initial=True)
    breaker = CircuitBreaker(config.circuit_breaker)
    router = Router(config, main, fallback, breaker, MaintenanceWatcher(config.maintenance))
    return config, RuntimeState(), main, fallback, breaker, router


def test_runtime_reload_defaults_and_metric_families() -> None:
    _config, runtime, main, fallback, breaker, router = context()
    assert runtime.config_generation == 1
    assert runtime.reload_successes == 0
    assert runtime.reload_failures == 0
    assert runtime.last_reload_at is None
    assert runtime.last_reload_result == "never"
    runtime.reload_successes = 2
    runtime.reload_failures = 3
    runtime.config_generation = 3
    runtime.last_reload_at = datetime(2026, 10, 25, 12, tzinfo=timezone.utc)
    rendered = render_metrics(runtime, main, fallback, breaker, router)
    assert "mc_failover_config_generation 3\n" in rendered
    assert "mc_failover_config_reload_successes_total 2\n" in rendered
    assert "mc_failover_config_reload_failures_total 3\n" in rendered
    assert "mc_failover_config_last_reload_timestamp_seconds 1792929600\n" in rendered


def test_monitoring_reload_diagnostics_are_public_and_secret_free() -> None:
    config, runtime, main, fallback, breaker, router = context()
    runtime.last_reload_result = "rejected"
    runtime.reload_failures = 1
    payload = build_monitoring_payload(
        config, runtime, main, fallback, breaker, router, include_sensitive=False
    )
    assert payload["configuration"] == {
        "generation": 1,
        "reload_successes": 0,
        "reload_failures": 1,
        "last_reload_at": None,
        "last_reload_result": "rejected",
    }
    serialized = json.dumps(payload)
    assert '"host"' not in serialized
    assert '"port"' not in serialized


def test_dynamic_backends_are_reported_without_losing_legacy_families() -> None:
    base, runtime, main, fallback, breaker, _router = context()
    backend_configs = tuple(
        BackendConfig(
            id=f"BACKEND_{index}",
            host="192.0.2.1",
            port=25560 + index,
            healthcheck=base.healthcheck,
            circuit_breaker=base.circuit_breaker,
            priority=index * 10,
        )
        for index in range(5)
    )
    config = replace(base, backends=backend_configs)
    router = Router(config, main, fallback, breaker, MaintenanceWatcher(config.maintenance))
    for entry in router.registry.ordered:
        entry.health.report(HealthCheckResult(True, "tcp_connect_ok", 2.5), initial=True)
    entry = router.registry.entries["BACKEND_0"]
    entry.active_connections = 7
    entry.connect_failures = 2
    entry.connect_successes = 9
    router.registry.record_passive_failure("BACKEND_0", "backend_reset")
    payload = build_monitoring_payload(
        config, runtime, main, fallback, breaker, router, include_sensitive=False
    )
    assert payload["selected_backend"] == "BACKEND_0"
    assert len(payload["backends"]) == 5
    selected = payload["backends"]["BACKEND_0"]
    assert selected["selected"] is True
    assert selected["priority"] == 0
    assert selected["active_connections"] == 7
    assert selected["connect_failures_total"] == 2
    assert selected["passive_failures_total"] == 1
    assert selected["last_result"]["latency_ms"] == 2.5
    assert "192.0.2.1" not in json.dumps(payload)
    sensitive = build_monitoring_payload(
        config, runtime, main, fallback, breaker, router, include_sensitive=True
    )
    assert sensitive["targets"]["BACKEND_4"] == {"host": "192.0.2.1", "port": 25564}
    rendered = render_metrics(runtime, main, fallback, breaker, router)
    assert 'mc_failover_backend_active_connections{backend="BACKEND_0"} 7\n' in rendered
    assert 'mc_failover_backend_selected{backend="BACKEND_0"} 1\n' in rendered
    assert 'mc_failover_backend_connect_failures_total{backend="BACKEND_0"} 2\n' in rendered
    assert 'mc_failover_backend_passive_failures_total{backend="BACKEND_0"} 1\n' in rendered
    assert "mc_failover_main_connect_failures_total" in rendered
    assert "192.0.2.1" not in rendered


def test_monitoring_reconfigure_swaps_policy_and_ready_in_one_event_loop_step() -> None:
    config, runtime, main, fallback, breaker, router = context()
    server = MonitoringServer(config, runtime, main, fallback, breaker, router)
    updated = replace(
        config,
        monitoring=replace(config.monitoring, bearer_token="replacement-secret"),
    )
    replacement = Router(updated, main, fallback, breaker, MaintenanceWatcher(updated.maintenance))
    guard = EndpointLoopGuard(updated)
    runtime.config_generation = 2
    runtime.last_reload_result = "success"
    server.reconfigure(updated, main, fallback, breaker, replacement, guard)
    assert server.router is replacement
    assert server.endpoint_guard is guard
    unauthorized = server._dispatch(HttpRequest("GET", "/ready", "HTTP/1.1", {}))
    assert unauthorized.startswith(b"HTTP/1.1 401")
    response = server._dispatch(
        HttpRequest("GET", "/ready", "HTTP/1.1", {"authorization": ("Bearer replacement-secret",)})
    )
    payload = json.loads(response.split(b"\r\n\r\n", 1)[1])
    assert payload["selected_backend"] == "MAIN"
    assert set(payload["backends"]) == {"MAIN", "FALLBACK"}
    assert payload["configuration"]["generation"] == 2
    assert "replacement-secret" not in response.decode()
