"""Prometheus text exposition with fixed metric families and bounded labels."""

from __future__ import annotations

import math
from collections.abc import Iterable

from .circuit_breaker import CircuitBreaker
from .health import HealthState
from .models import CircuitState, HealthStatus, RejectionReason, TargetName
from .routing import Router
from .runtime import RuntimeState


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    if not math.isfinite(value):
        raise ValueError("Prometheus samples must be finite")
    return format(value, ".15g")


def _family(
    name: str,
    help_text: str,
    metric_type: str,
    samples: Iterable[tuple[dict[str, str], int | float]],
) -> list[str]:
    lines = [f"# HELP {name} {help_text}", f"# TYPE {name} {metric_type}"]
    for labels, value in samples:
        suffix = ""
        if labels:
            rendered = ",".join(
                f'{key}="{escape_label(label)}"' for key, label in sorted(labels.items())
            )
            suffix = f"{{{rendered}}}"
        lines.append(f"{name}{suffix} {_number(value)}")
    return lines


def render_metrics(
    runtime: RuntimeState,
    main_health: HealthState,
    fallback_health: HealthState,
    circuit_breaker: CircuitBreaker,
    router: Router,
) -> str:
    """Render a complete Prometheus 0.0.4 response."""

    lines: list[str] = []
    add = lines.extend
    decision = router.snapshot(shutting_down=runtime.shutting_down)
    main = main_health.snapshot()
    fallback = fallback_health.snapshot()
    circuit = circuit_breaker.snapshot_now()

    add(_family("mc_failover_up", "Whether the monitoring handler is running.", "gauge", [({}, 1)]))
    add(
        _family(
            "mc_failover_uptime_seconds",
            "Process uptime measured with a monotonic clock.",
            "gauge",
            [({}, runtime.uptime_seconds)],
        )
    )
    add(
        _family(
            "mc_failover_shutting_down",
            "Whether graceful shutdown has started.",
            "gauge",
            [({}, int(runtime.shutting_down))],
        )
    )
    add(
        _family(
            "mc_failover_active_connections",
            "Current Minecraft client sessions with an established backend relay.",
            "gauge",
            [({}, runtime.active_connections)],
        )
    )
    add(
        _family(
            "mc_failover_incoming_connections_total",
            "TCP connections accepted by the Minecraft listener since process start.",
            "counter",
            [({}, runtime.incoming_connections_total)],
        )
    )
    add(
        _family(
            "mc_failover_backend_connections_established_total",
            "Client connections with a backend successfully prepared for relay since process start.",
            "counter",
            [({}, runtime.backend_connections_established_total)],
        )
    )
    rejection_samples = [
        (
            {"reason": reason.value},
            runtime.rejection_reasons.get(reason, 0),
        )
        for reason in RejectionReason
        if reason is not RejectionReason.MONITORING_LIMIT
    ]
    legacy_rejection_samples = [
        (
            {"reason": reason.value},
            runtime.rejection_reasons.get(reason, 0),
        )
        for reason in RejectionReason
    ]
    add(
        _family(
            "mc_failover_connections_rejected_total",
            "Explicitly rejected Minecraft client connections, by bounded reason.",
            "counter",
            rejection_samples,
        )
    )
    add(
        _family(
            "mc_failover_connections_total",
            "Deprecated: Minecraft connections granted a global limiter lease.",
            "counter",
            [({}, runtime.total_connections)],
        )
    )
    add(
        _family(
            "mc_failover_rejected_connections_total",
            "Deprecated compatibility family for Minecraft client rejections.",
            "counter",
            legacy_rejection_samples,
        )
    )
    add(
        _family(
            "mc_failover_monitoring_rejected_connections_total",
            "Monitoring connections rejected by the independent HTTP limit.",
            "counter",
            [({}, runtime.monitoring_rejected_connections)],
        )
    )
    add(
        _family(
            "mc_failover_main_connect_failures_total",
            "Failed real connection attempts to MAIN.",
            "counter",
            [({}, runtime.main_connect_failures)],
        )
    )
    add(
        _family(
            "mc_failover_fallback_connect_failures_total",
            "Failed real connection attempts to FALLBACK.",
            "counter",
            [({}, runtime.fallback_connect_failures)],
        )
    )
    add(
        _family(
            "mc_failover_main_connect_successes_total",
            "Successful real MAIN connections prepared for relay.",
            "counter",
            [({}, runtime.main_connect_successes)],
        )
    )
    add(
        _family(
            "mc_failover_fallback_connect_successes_total",
            "Successful real FALLBACK connections prepared for relay.",
            "counter",
            [({}, runtime.fallback_connect_successes)],
        )
    )

    health_snapshots = (main, fallback)
    add(
        _family(
            "mc_failover_target_health_status",
            "One-hot active health state for each configured target.",
            "gauge",
            [
                (
                    {"target": snapshot.target.value, "status": status.value},
                    int(snapshot.status is status),
                )
                for snapshot in health_snapshots
                for status in HealthStatus
            ],
        )
    )
    add(
        _family(
            "mc_failover_healthcheck_successes_total",
            "Successful active checks by target.",
            "counter",
            [
                ({"target": snapshot.target.value}, snapshot.total_successes)
                for snapshot in health_snapshots
            ],
        )
    )
    add(
        _family(
            "mc_failover_healthcheck_failures_total",
            "Failed active checks by target.",
            "counter",
            [
                ({"target": snapshot.target.value}, snapshot.total_failures)
                for snapshot in health_snapshots
            ],
        )
    )
    latency_samples = [
        ({"target": snapshot.target.value}, snapshot.last_result.latency_ms)
        for snapshot in health_snapshots
        if snapshot.last_result is not None and snapshot.last_result.latency_ms is not None
    ]
    add(
        _family(
            "mc_failover_healthcheck_latency_milliseconds",
            "Latency of the latest completed check by target.",
            "gauge",
            latency_samples,
        )
    )
    age_samples = [
        ({"target": snapshot.target.value}, snapshot.seconds_since_last_check)
        for snapshot in health_snapshots
        if snapshot.seconds_since_last_check is not None
    ]
    add(
        _family(
            "mc_failover_healthcheck_age_seconds",
            "Monotonic age of the latest completed check by target.",
            "gauge",
            age_samples,
        )
    )
    timestamp_samples = [
        ({"target": snapshot.target.value}, snapshot.last_check_at.timestamp())
        for snapshot in health_snapshots
        if snapshot.last_check_at is not None
    ]
    add(
        _family(
            "mc_failover_healthcheck_timestamp_seconds",
            "UTC Unix timestamp of the latest completed check by target.",
            "gauge",
            timestamp_samples,
        )
    )

    add(
        _family(
            "mc_failover_circuit_breaker_state",
            "One-hot MAIN circuit-breaker state.",
            "gauge",
            [({"state": state.value}, int(circuit.state is state)) for state in CircuitState],
        )
    )
    add(
        _family(
            "mc_failover_circuit_breaker_open_total",
            "Number of transitions to the open state.",
            "counter",
            [({}, circuit.open_total)],
        )
    )
    add(
        _family(
            "mc_failover_circuit_breaker_retry_after_seconds",
            "Seconds until MAIN may receive a half-open probe.",
            "gauge",
            [({}, circuit.retry_after_seconds)],
        )
    )
    add(
        _family(
            "mc_failover_active_target",
            "One-hot target selected for new connections.",
            "gauge",
            [
                ({"target": target.value}, int(decision.active_target is target))
                for target in TargetName
            ],
        )
    )
    add(
        _family(
            "mc_failover_routing_reason_info",
            "Current bounded routing reason.",
            "gauge",
            [({"reason": decision.reason.value}, 1)],
        )
    )
    return "\n".join(lines) + "\n"
