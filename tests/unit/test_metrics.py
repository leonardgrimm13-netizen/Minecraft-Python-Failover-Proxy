from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mc_failover.circuit_breaker import CircuitBreaker
from mc_failover.config import AppConfig, load_config
from mc_failover.health import HealthState
from mc_failover.metrics import escape_label, render_metrics
from mc_failover.models import (
    CircuitState,
    HealthCheckResult,
    HealthStatus,
    RejectionReason,
    TargetName,
)
from mc_failover.routing import MaintenanceWatcher, Router
from mc_failover.runtime import RuntimeState

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
        self.monotonic_value = 1_000.0
        self.utc_value = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)

    def monotonic(self) -> float:
        return self.monotonic_value

    def utc_now(self) -> datetime:
        return self.utc_value

    def advance(self, seconds: float) -> None:
        self.monotonic_value += seconds
        self.utc_value += timedelta(seconds=seconds)

    def jump_wall_clock(self, seconds: float) -> None:
        self.utc_value += timedelta(seconds=seconds)


def load_test_config(tmp_path: Path) -> AppConfig:
    path = tmp_path / "metrics.toml"
    path.write_text(BASE_TOML, encoding="utf-8")
    return load_config(path)


def metric_context(
    tmp_path: Path,
) -> tuple[
    FakeClock,
    RuntimeState,
    HealthState,
    HealthState,
    CircuitBreaker,
    Router,
]:
    config = load_test_config(tmp_path)
    clock = FakeClock()
    runtime = RuntimeState(clock=clock)
    main = HealthState(TargetName.MAIN, config.healthcheck, clock=clock)
    fallback = HealthState(TargetName.FALLBACK, config.fallback_healthcheck, clock=clock)
    breaker = CircuitBreaker(config.circuit_breaker, clock=clock)
    router = Router(config, main, fallback, breaker, MaintenanceWatcher(config.maintenance))
    return clock, runtime, main, fallback, breaker, router


def _sample_value(body: str, sample: str) -> str:
    match = re.search(rf"^{re.escape(sample)} ([^\n]+)$", body, flags=re.MULTILINE)
    assert match is not None, f"missing sample: {sample}"
    return match.group(1)


def test_label_escaping_handles_backslashes_quotes_and_newlines() -> None:
    assert escape_label('route\\name"line\nnext') == 'route\\\\name\\"line\\nnext'


def test_every_rendered_metric_family_has_help_and_type(tmp_path: Path) -> None:
    _clock, runtime, main, fallback, breaker, router = metric_context(tmp_path)
    body = render_metrics(runtime, main, fallback, breaker, router)
    metric_names = {
        line.split("{", 1)[0].split(" ", 1)[0]
        for line in body.splitlines()
        if line and not line.startswith("#")
    }
    assert metric_names
    for name in metric_names:
        assert f"# HELP {name} " in body
        assert f"# TYPE {name} " in body


def test_required_metric_names_and_types_are_exact(tmp_path: Path) -> None:
    _clock, runtime, main, fallback, breaker, router = metric_context(tmp_path)
    body = render_metrics(runtime, main, fallback, breaker, router)
    required_types = {
        "mc_failover_active_connections": "gauge",
        "mc_failover_incoming_connections_total": "counter",
        "mc_failover_backend_connections_established_total": "counter",
        "mc_failover_connections_rejected_total": "counter",
        "mc_failover_connections_total": "counter",
        "mc_failover_rejected_connections_total": "counter",
        "mc_failover_main_connect_failures_total": "counter",
        "mc_failover_fallback_connect_failures_total": "counter",
        "mc_failover_main_connect_successes_total": "counter",
        "mc_failover_fallback_connect_successes_total": "counter",
        "mc_failover_circuit_breaker_state": "gauge",
        "mc_failover_circuit_breaker_open_total": "counter",
    }
    for name, metric_type in required_types.items():
        assert f"# HELP {name} " in body
        assert f"# TYPE {name} {metric_type}\n" in body
        assert re.search(rf"^{name}(?:\{{| )", body, flags=re.MULTILINE)
    assert (
        "# HELP mc_failover_active_connections "
        "Current Minecraft client sessions with an established backend relay."
    ) in body
    assert (
        "# HELP mc_failover_connections_total "
        "Deprecated: Minecraft connections granted a global limiter lease."
    ) in body
    assert (
        "# HELP mc_failover_rejected_connections_total "
        "Deprecated compatibility family for Minecraft client rejections."
    ) in body


def test_runtime_health_and_utc_time_values_are_rendered_without_clock_mixups(
    tmp_path: Path,
) -> None:
    clock, runtime, main, fallback, breaker, router = metric_context(tmp_path)
    main.report(HealthCheckResult(True, "ok", latency_ms=12.5), initial=True)
    fallback.report(HealthCheckResult(False, "down", latency_ms=20.0), initial=True)
    for _ in range(3):
        runtime.incoming_connection_received()
    for _ in range(2):
        runtime.connection_admitted()
        runtime.backend_connection_started()
    runtime.backend_connection_finished()
    runtime.connect_succeeded(TargetName.MAIN)
    runtime.connect_failed(TargetName.MAIN)
    runtime.connect_succeeded(TargetName.FALLBACK)
    runtime.connect_failed(TargetName.FALLBACK)
    runtime.reject(RejectionReason.RATE_LIMIT)
    checked_at = clock.utc_now().timestamp()

    clock.jump_wall_clock(86_400.0)
    clock.monotonic_value += 2.5
    body = render_metrics(runtime, main, fallback, breaker, router)

    assert _sample_value(body, "mc_failover_uptime_seconds") == "2.5"
    assert _sample_value(body, "mc_failover_active_connections") == "1"
    assert _sample_value(body, "mc_failover_incoming_connections_total") == "3"
    assert _sample_value(body, "mc_failover_backend_connections_established_total") == "2"
    assert _sample_value(body, "mc_failover_connections_total") == "2"
    assert _sample_value(body, "mc_failover_main_connect_successes_total") == "1"
    assert _sample_value(body, "mc_failover_main_connect_failures_total") == "1"
    assert _sample_value(body, "mc_failover_fallback_connect_successes_total") == "1"
    assert _sample_value(body, "mc_failover_fallback_connect_failures_total") == "1"
    assert (
        _sample_value(body, 'mc_failover_healthcheck_latency_milliseconds{target="MAIN"}') == "12.5"
    )
    assert _sample_value(body, 'mc_failover_healthcheck_age_seconds{target="MAIN"}') == "2.5"
    assert float(
        _sample_value(body, 'mc_failover_healthcheck_timestamp_seconds{target="MAIN"}')
    ) == pytest.approx(checked_at)
    assert _sample_value(body, 'mc_failover_rejected_connections_total{reason="rate_limit"}') == "1"
    assert _sample_value(body, 'mc_failover_connections_rejected_total{reason="rate_limit"}') == "1"


@pytest.mark.asyncio
async def test_circuit_breaker_metrics_show_open_state_and_open_counter(tmp_path: Path) -> None:
    clock, runtime, main, fallback, _configured_breaker, _router = metric_context(tmp_path)
    config = load_test_config(tmp_path)
    main.report(HealthCheckResult(True, "ok"), initial=True)
    fallback.report(HealthCheckResult(True, "ok"), initial=True)
    breaker = CircuitBreaker(
        enabled=True,
        failure_threshold=1,
        failure_window_seconds=10.0,
        open_seconds=7.0,
        half_open_max_attempts=1,
        clock=clock,
    )
    router = Router(config, main, fallback, breaker, MaintenanceWatcher(config.maintenance))
    permit = await breaker.acquire()
    await breaker.record_failure(permit)

    body = render_metrics(runtime, main, fallback, breaker, router)
    assert _sample_value(body, 'mc_failover_circuit_breaker_state{state="open"}') == "1"
    assert _sample_value(body, 'mc_failover_circuit_breaker_state{state="closed"}') == "0"
    assert _sample_value(body, "mc_failover_circuit_breaker_open_total") == "1"
    assert _sample_value(body, "mc_failover_circuit_breaker_retry_after_seconds") == "7"
    assert _sample_value(body, 'mc_failover_active_target{target="FALLBACK"}') == "1"


def test_metric_labels_are_drawn_only_from_bounded_enums(tmp_path: Path) -> None:
    _clock, runtime, main, fallback, breaker, router = metric_context(tmp_path)
    body = render_metrics(runtime, main, fallback, breaker, router)

    canonical_rejection_labels = set(
        re.findall(r'^mc_failover_connections_rejected_total\{reason="([^"]+)"\}', body, re.M)
    )
    legacy_rejection_labels = set(
        re.findall(r'^mc_failover_rejected_connections_total\{reason="([^"]+)"\}', body, re.M)
    )
    state_labels = set(
        re.findall(r'^mc_failover_circuit_breaker_state\{state="([^"]+)"\}', body, re.M)
    )
    target_labels = set(re.findall(r'^mc_failover_active_target\{target="([^"]+)"\}', body, re.M))
    health_pairs = set(
        re.findall(
            r'^mc_failover_target_health_status\{status="([^"]+)",target="([^"]+)"\}',
            body,
            re.M,
        )
    )

    assert canonical_rejection_labels == {
        reason.value for reason in RejectionReason if reason is not RejectionReason.MONITORING_LIMIT
    }
    assert legacy_rejection_labels == {reason.value for reason in RejectionReason}
    assert 'mc_failover_connections_rejected_total{reason="monitoring_limit"}' not in body
    assert (
        _sample_value(
            body,
            'mc_failover_rejected_connections_total{reason="monitoring_limit"}',
        )
        == "0"
    )
    assert state_labels == {state.value for state in CircuitState}
    assert target_labels == {target.value for target in TargetName}
    assert health_pairs == {
        (status.value, target.value)
        for status in HealthStatus
        for target in (TargetName.MAIN, TargetName.FALLBACK)
    }
    reason_lines = [
        line for line in body.splitlines() if line.startswith("mc_failover_routing_reason_info{")
    ]
    assert len(reason_lines) == 1
