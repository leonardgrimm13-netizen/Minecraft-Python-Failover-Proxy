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

    health_snapshots = ((TargetName.MAIN, main), (TargetName.FALLBACK, fallback))
    add(
        _family(
            "mc_failover_target_health_status",
            "One-hot active health state for each configured target.",
            "gauge",
            [
                (
                    {"target": target.value, "status": status.value},
                    int(snapshot.status is status),
                )
                for target, snapshot in health_snapshots
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
                ({"target": target.value}, snapshot.total_successes)
                for target, snapshot in health_snapshots
            ],
        )
    )
    add(
        _family(
            "mc_failover_healthcheck_failures_total",
            "Failed active checks by target.",
            "counter",
            [
                ({"target": target.value}, snapshot.total_failures)
                for target, snapshot in health_snapshots
            ],
        )
    )
    latency_samples = [
        ({"target": target.value}, snapshot.last_result.latency_ms)
        for target, snapshot in health_snapshots
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
        ({"target": target.value}, snapshot.seconds_since_last_check)
        for target, snapshot in health_snapshots
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
        ({"target": target.value}, snapshot.last_check_at.timestamp())
        for target, snapshot in health_snapshots
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
    for name, help_text, metric_type, value in (
        (
            "config_generation",
            "Active validated configuration generation.",
            "gauge",
            runtime.config_generation,
        ),
        (
            "config_reload_successes_total",
            "Successfully activated configurations.",
            "counter",
            runtime.reload_successes,
        ),
        (
            "config_reload_failures_total",
            "Configuration reload attempts rejected before activation.",
            "counter",
            runtime.reload_failures,
        ),
        (
            "config_last_reload_timestamp_seconds",
            "UTC Unix timestamp of the last successful configuration reload; zero before any reload.",
            "gauge",
            runtime.last_reload_at.timestamp() if runtime.last_reload_at is not None else 0,
        ),
    ):
        add(_family(f"mc_failover_{name}", help_text, metric_type, [({}, value)]))
    entries = router.registry.ordered
    snapshots = {entry.config.id: entry.health.snapshot() for entry in entries}
    circuits = {entry.config.id: entry.circuit.snapshot_now() for entry in entries}
    for name, help_text, metric_type, samples in (
        (
            "enabled",
            "Whether the configured backend is enabled.",
            "gauge",
            [({"backend": entry.config.id}, int(entry.config.enabled)) for entry in entries],
        ),
        (
            "priority",
            "Configured routing priority; lower values are preferred.",
            "gauge",
            [({"backend": entry.config.id}, entry.config.priority) for entry in entries],
        ),
        (
            "selected",
            "Whether the backend is selected for new connections.",
            "gauge",
            [
                ({"backend": entry.config.id}, int(decision.backend_id == entry.config.id))
                for entry in entries
            ],
        ),
        (
            "routable",
            "Whether active/passive health and the circuit allow ordinary routing.",
            "gauge",
            [
                (
                    {"backend": entry.config.id},
                    int(entry.routable and circuits[entry.config.id].state is CircuitState.CLOSED),
                )
                for entry in entries
            ],
        ),
        (
            "active_connections",
            "Current established sessions for the configured backend.",
            "gauge",
            [({"backend": entry.config.id}, entry.active_connections) for entry in entries],
        ),
        (
            "connect_failures_total",
            "Failed real connection attempts for the backend.",
            "counter",
            [({"backend": entry.config.id}, entry.connect_failures) for entry in entries],
        ),
        (
            "connect_successes_total",
            "Successful prepared real connections for the backend.",
            "counter",
            [({"backend": entry.config.id}, entry.connect_successes) for entry in entries],
        ),
        (
            "passive_failures_total",
            "Backend session failures supported by I/O or health evidence.",
            "counter",
            [({"backend": entry.config.id}, entry.passive_failures) for entry in entries],
        ),
        (
            "passive_failures_in_window",
            "Bounded count of passive failures in the current window.",
            "gauge",
            [({"backend": entry.config.id}, entry.passive_failures_in_window) for entry in entries],
        ),
        (
            "quarantine_remaining_seconds",
            "Minimum remaining passive quarantine duration.",
            "gauge",
            [
                ({"backend": entry.config.id}, entry.quarantine_remaining_seconds)
                for entry in entries
            ],
        ),
        (
            "healthcheck_successes_total",
            "Successful active checks for the backend.",
            "counter",
            [
                ({"backend": entry.config.id}, snapshots[entry.config.id].total_successes)
                for entry in entries
            ],
        ),
        (
            "healthcheck_failures_total",
            "Failed active checks for the backend.",
            "counter",
            [
                ({"backend": entry.config.id}, snapshots[entry.config.id].total_failures)
                for entry in entries
            ],
        ),
        (
            "health_status",
            "One-hot health status for configured backend IDs.",
            "gauge",
            [
                (
                    {"backend": entry.config.id, "status": status.value},
                    int(snapshots[entry.config.id].status is status),
                )
                for entry in entries
                for status in HealthStatus
            ],
        ),
        (
            "healthcheck_latency_milliseconds",
            "Latest completed backend check latency.",
            "gauge",
            [
                ({"backend": backend_id}, snapshot.last_result.latency_ms)
                for backend_id, snapshot in snapshots.items()
                if snapshot.last_result is not None and snapshot.last_result.latency_ms is not None
            ],
        ),
        (
            "healthcheck_age_seconds",
            "Monotonic age of the latest completed backend check.",
            "gauge",
            [
                ({"backend": backend_id}, snapshot.seconds_since_last_check)
                for backend_id, snapshot in snapshots.items()
                if snapshot.seconds_since_last_check is not None
            ],
        ),
        (
            "circuit_breaker_state",
            "One-hot circuit state for configured backend IDs.",
            "gauge",
            [
                ({"backend": backend_id, "state": state.value}, int(snapshot.state is state))
                for backend_id, snapshot in circuits.items()
                for state in CircuitState
            ],
        ),
        (
            "circuit_breaker_open_total",
            "Backend circuit transitions to open.",
            "counter",
            [
                ({"backend": backend_id}, snapshot.open_total)
                for backend_id, snapshot in circuits.items()
            ],
        ),
        (
            "circuit_breaker_retry_after_seconds",
            "Seconds until the backend permits a recovery probe.",
            "gauge",
            [
                ({"backend": backend_id}, snapshot.retry_after_seconds)
                for backend_id, snapshot in circuits.items()
            ],
        ),
    ):
        add(_family(f"mc_failover_backend_{name}", help_text, metric_type, samples))
    return "\n".join(lines) + "\n"
