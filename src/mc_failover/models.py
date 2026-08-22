"""Shared bounded state and result types."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TargetName(str, Enum):
    MAIN = "MAIN"
    FALLBACK = "FALLBACK"
    NONE = "NONE"


class HealthStatus(str, Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DISABLED = "disabled"


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class MaintenanceMode(str, Enum):
    AUTO = "auto"
    FORCE_MAIN = "force_main"
    FORCE_FALLBACK = "force_fallback"


class RoutingReason(str, Enum):
    MAIN_HEALTHY = "main_healthy"
    MAIN_CIRCUIT_HALF_OPEN = "main_circuit_half_open"
    MAIN_UNAVAILABLE_FALLBACK_HEALTHY = "main_unavailable_fallback_healthy"
    MAIN_CIRCUIT_OPEN_FALLBACK_HEALTHY = "main_circuit_open_fallback_healthy"
    MAIN_CONNECT_FAILED_FALLBACK = "main_connect_failed_fallback"
    FORCE_MAIN = "force_main"
    FORCE_FALLBACK = "force_fallback"
    FORCED_MAIN_UNAVAILABLE = "forced_main_unavailable"
    FORCED_FALLBACK_UNAVAILABLE = "forced_fallback_unavailable"
    NO_TARGET_AVAILABLE = "no_target_available"
    SHUTTING_DOWN = "shutting_down"


class RejectionReason(str, Enum):
    GLOBAL_LIMIT = "global_limit"
    PER_IP_LIMIT = "per_ip_limit"
    RATE_LIMIT = "rate_limit"
    UNTRUSTED_PROXY = "untrusted_proxy"
    INVALID_PROXY_HEADER = "invalid_proxy_header"
    NO_TARGET = "no_target"
    BACKEND_CONNECT_FAILED = "backend_connect_failed"
    SHUTTING_DOWN = "shutting_down"
    MONITORING_LIMIT = "monitoring_limit"


@dataclass(frozen=True, slots=True)
class Target:
    name: TargetName
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class HealthCheckResult:
    ok: bool
    reason: str
    latency_ms: float | None = None
    version_name: str | None = None
    players_online: int | None = None
    players_max: int | None = None
    motd_text: str | None = None
