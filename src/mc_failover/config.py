"""Strict TOML configuration loading and validation.

The parser intentionally performs type checks before converting values.  In
particular, TOML booleans are never accepted where an integer is expected and
security-sensitive options fail closed.
"""

from __future__ import annotations

import ipaddress
import math
import os
import re
import socket
import stat
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Final, cast

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - exercised on Python 3.10
    import tomli as tomllib

from . import models
from .minecraft_status import MAX_STATUS_HOST_BYTES, MAX_STATUS_HOST_CHARS


class ConfigError(ValueError):
    """Raised when a configuration file is missing, malformed, or unsafe."""


@dataclass(frozen=True, slots=True)
class ProxyConfig:
    listen_host: str
    listen_port: int
    backlog: int


@dataclass(frozen=True, slots=True)
class TargetConfig:
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class HealthCheckConfig:
    enabled: bool
    mode: str
    interval_seconds: float
    timeout_seconds: float
    fail_after: int
    recover_after: int
    min_recovery_seconds: float
    target_host: str | None
    target_port: int | None
    protocol_version: int
    status_hostname: str | None
    require_valid_json: bool
    log_status_details: bool
    jitter_seconds: float
    max_latency_ms: float
    expected_version_contains: str
    motd_must_contain: str
    motd_must_not_contain: str
    min_players_max: int
    reject_uninitialized_protocol: bool = True


@dataclass(frozen=True, slots=True)
class ConnectionConfig:
    timeout_seconds: float
    buffer_size: int
    idle_timeout_seconds: float
    write_timeout_seconds: float
    relay_drain_timeout_seconds: float
    shutdown_grace_seconds: float
    shutdown_cancel_timeout_seconds: float
    connect_fallback_on_main_connect_failure: bool
    tcp_keepalive: bool
    max_connections: int
    max_connections_per_ip: int
    new_connections_per_second: float
    new_connections_burst: int


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    level: str
    access_log: bool


@dataclass(frozen=True, slots=True)
class MaintenanceConfig:
    mode: models.MaintenanceMode
    force_fallback_file: Path | None
    force_main_file: Path | None
    file_check_interval_seconds: float
    force_target: str | None = None


@dataclass(frozen=True, slots=True)
class ProxyProtocolConfig:
    accept: bool
    send: bool
    version: int
    accept_version: int | None
    send_version: int | None
    trust_all_proxies: bool
    trusted_proxy_ips: tuple[str, ...]
    header_timeout_seconds: float
    max_header_bytes: int


@dataclass(frozen=True, slots=True)
class MonitoringConfig:
    enabled: bool
    listen_host: str
    listen_port: int
    allow_remote: bool
    bearer_token: str | None
    allow_unauthenticated_remote: bool
    expose_sensitive_state: bool
    max_connections: int
    request_timeout_seconds: float
    write_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class CircuitBreakerConfig:
    enabled: bool
    failure_threshold: int
    failure_window_seconds: float
    open_seconds: float
    half_open_max_attempts: int


@dataclass(frozen=True, slots=True)
class BackendConfig:
    id: str
    host: str
    port: int
    healthcheck: HealthCheckConfig
    circuit_breaker: CircuitBreakerConfig
    priority: int = 100
    enabled: bool = True
    connect_timeout_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class RoutingConfig:
    max_connect_attempts: int = 16
    connect_budget_seconds: float = 10.0
    max_parallel_healthchecks: int = 16


@dataclass(frozen=True, slots=True)
class PassiveHealthConfig:
    enabled: bool = True
    failure_threshold: int = 3
    failure_window_seconds: float = 30.0
    quarantine_seconds: float = 10.0
    early_session_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class ReloadConfig:
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class LocalStatusConfig:
    enabled: bool = False
    always: bool = False
    maintenance: bool = True
    handshake_timeout_seconds: float = 2.0
    session_timeout_seconds: float = 5.0
    max_handshake_bytes: int = 2048
    max_concurrent: int = 64
    motd_online: str = "Minecraft network online"
    motd_maintenance: str = "Minecraft network under maintenance"
    motd_fallback: str = "Minecraft network running on a fallback server"
    motd_offline: str = "Server temporarily offline\nPlease try again later."
    version_name: str = "Failover Proxy"
    protocol_version: int = -1
    players_online: int | None = None
    players_max: int | None = None
    favicon: str | None = None
    dynamic_hint: bool = False


@dataclass(frozen=True, slots=True)
class AppConfig:
    proxy: ProxyConfig
    main: TargetConfig
    fallback: TargetConfig
    healthcheck: HealthCheckConfig
    fallback_healthcheck: HealthCheckConfig
    connection: ConnectionConfig
    logging: LoggingConfig
    maintenance: MaintenanceConfig
    proxy_protocol: ProxyProtocolConfig
    monitoring: MonitoringConfig
    circuit_breaker: CircuitBreakerConfig
    strict_unknown_keys: bool = True
    backends: tuple[BackendConfig, ...] = ()
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    passive_health: PassiveHealthConfig = field(default_factory=PassiveHealthConfig)
    reload: ReloadConfig = field(default_factory=ReloadConfig)
    status: LocalStatusConfig = field(default_factory=LocalStatusConfig)

    @property
    def effective_backends(self) -> tuple[BackendConfig, ...]:
        """Normalize legacy targets without freezing programmatic legacy overrides."""
        if self.backends:
            return self.backends
        return (
            BackendConfig(
                "MAIN",
                self.main.host,
                self.main.port,
                self.healthcheck,
                self.circuit_breaker,
                priority=0,
            ),
            BackendConfig(
                "FALLBACK",
                self.fallback.host,
                self.fallback.port,
                self.fallback_healthcheck,
                replace(self.circuit_breaker, enabled=False),
                priority=100,
            ),
        )


_MISSING: Final = object()
_BEARER_TOKEN_MAX_CHARS: Final = 2048
_LOG_LEVELS: Final = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_HEALTH_MODES: Final = {"tcp", "minecraft_status"}

_ROOT_KEYS: Final = {
    "config",
    "proxy",
    "main",
    "fallback",
    "healthcheck",
    "fallback_healthcheck",
    "connection",
    "logging",
    "maintenance",
    "proxy_protocol",
    "monitoring",
    "circuit_breaker",
    "backends",
    "routing",
    "passive_health",
    "reload",
    "status",
}
_SECTION_KEYS: Final[dict[str, frozenset[str]]] = {
    "config": frozenset({"strict_unknown_keys"}),
    "proxy": frozenset({"listen_host", "listen_port", "backlog"}),
    "main": frozenset({"host", "port"}),
    "fallback": frozenset({"host", "port"}),
    "healthcheck": frozenset(
        {
            "enabled",
            "mode",
            "interval_seconds",
            "timeout_seconds",
            "fail_after",
            "recover_after",
            "min_recovery_seconds",
            "target_host",
            "target_port",
            "protocol_version",
            "status_hostname",
            "require_valid_json",
            "reject_uninitialized_protocol",
            "log_status_details",
            "jitter_seconds",
            "max_latency_ms",
            "expected_version_contains",
            "motd_must_contain",
            "motd_must_not_contain",
            "min_players_max",
        }
    ),
    "fallback_healthcheck": frozenset(
        {
            "enabled",
            "mode",
            "interval_seconds",
            "timeout_seconds",
            "fail_after",
            "recover_after",
            "min_recovery_seconds",
            "target_host",
            "target_port",
            "protocol_version",
            "status_hostname",
            "require_valid_json",
            "reject_uninitialized_protocol",
            "log_status_details",
            "jitter_seconds",
            "max_latency_ms",
            "expected_version_contains",
            "motd_must_contain",
            "motd_must_not_contain",
            "min_players_max",
        }
    ),
    "connection": frozenset(
        {
            "timeout_seconds",
            "buffer_size",
            "idle_timeout_seconds",
            "write_timeout_seconds",
            "relay_drain_timeout_seconds",
            "shutdown_grace_seconds",
            "shutdown_cancel_timeout_seconds",
            "connect_fallback_on_main_connect_failure",
            "tcp_keepalive",
            "max_connections",
            "max_connections_per_ip",
            "new_connections_per_second",
            "new_connections_burst",
        }
    ),
    "logging": frozenset({"level", "access_log"}),
    "maintenance": frozenset(
        {
            "mode",
            "force_fallback_file",
            "force_main_file",
            "file_check_interval_seconds",
            "force_target",
        }
    ),
    "proxy_protocol": frozenset(
        {
            "accept",
            "send",
            "version",
            "accept_version",
            "send_version",
            "trust_all_proxies",
            "trusted_proxy_ips",
            "header_timeout_seconds",
            "max_header_bytes",
        }
    ),
    "monitoring": frozenset(
        {
            "enabled",
            "listen_host",
            "listen_port",
            "allow_remote",
            "bearer_token",
            "allow_unauthenticated_remote",
            "expose_sensitive_state",
            "max_connections",
            "request_timeout_seconds",
            "write_timeout_seconds",
        }
    ),
    "circuit_breaker": frozenset(
        {
            "enabled",
            "failure_threshold",
            "failure_window_seconds",
            "open_seconds",
            "half_open_max_attempts",
        }
    ),
}
_SECTION_KEYS.update(
    {
        "routing": frozenset(
            {"max_connect_attempts", "connect_budget_seconds", "max_parallel_healthchecks"}
        ),
        "passive_health": frozenset(
            {
                "enabled",
                "failure_threshold",
                "failure_window_seconds",
                "quarantine_seconds",
                "early_session_seconds",
            }
        ),
        "reload": frozenset({"enabled"}),
        "status": frozenset(
            {
                "enabled",
                "always",
                "maintenance",
                "handshake_timeout_seconds",
                "session_timeout_seconds",
                "max_handshake_bytes",
                "max_concurrent",
                "motd_online",
                "motd_maintenance",
                "motd_fallback",
                "motd_offline",
                "version_name",
                "protocol_version",
                "players_online",
                "players_max",
                "favicon",
                "dynamic_hint",
            }
        ),
    }
)


def _path(section: str, key: str) -> str:
    return f"[{section}].{key}"


def _render(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return repr(value)


def _invalid(path: str, expected: str, value: Any) -> ConfigError:
    return ConfigError(
        f"Ungültiger Konfigurationswert {path}: erwartet {expected}, erhalten {_render(value)}"
    )


def _invalid_secret(path: str, expected: str) -> ConfigError:
    return ConfigError(
        f"Ungültiger Konfigurationswert {path}: erwartet {expected}, erhalten <redacted>"
    )


def _section(data: Mapping[str, Any], name: str, *, required: bool) -> dict[str, Any]:
    value = data.get(name, _MISSING)
    if value is _MISSING:
        if required:
            raise ConfigError(f"Fehlende Konfigurationssektion [{name}]")
        return {}
    if not isinstance(value, dict):
        raise _invalid(f"[{name}]", "TOML-Table", value)
    return cast(dict[str, Any], value)


def _value(table: Mapping[str, Any], section: str, key: str, default: Any = _MISSING) -> Any:
    if key in table:
        return table[key]
    if default is not _MISSING:
        return default
    raise ConfigError(f"Fehlender Konfigurationswert {_path(section, key)}")


def _bool(table: Mapping[str, Any], section: str, key: str, default: Any = _MISSING) -> bool:
    value = _value(table, section, key, default)
    if not isinstance(value, bool):
        raise _invalid(_path(section, key), "Boolean", value)
    return value


def _int(
    table: Mapping[str, Any],
    section: str,
    key: str,
    *,
    minimum: int,
    maximum: int,
    default: Any = _MISSING,
) -> int:
    value = _value(table, section, key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise _invalid(_path(section, key), f"Integer zwischen {minimum} und {maximum}", value)
    return cast(int, value)


def _optional_int(
    table: Mapping[str, Any],
    section: str,
    key: str,
    *,
    minimum: int,
    maximum: int,
    default: int | None,
) -> int | None:
    value = _value(table, section, key, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise _invalid(
            _path(section, key),
            f"Integer zwischen {minimum} und {maximum} oder nicht gesetzt",
            value,
        )
    return cast(int, value)


def _number(
    table: Mapping[str, Any],
    section: str,
    key: str,
    *,
    minimum: float,
    maximum: float,
    default: Any = _MISSING,
    minimum_inclusive: bool = True,
) -> float:
    value = _value(table, section, key, default)
    valid_type = not isinstance(value, bool) and isinstance(value, (int, float))
    try:
        numeric = float(value) if valid_type else math.nan
    except (OverflowError, ValueError):
        numeric = math.nan
    lower_ok = numeric >= minimum if minimum_inclusive else numeric > minimum
    if not valid_type or not math.isfinite(numeric) or not lower_ok or numeric > maximum:
        comparator = "zwischen" if minimum_inclusive else "größer als"
        expected = (
            f"endliche Zahl {comparator} {minimum} und {maximum}"
            if minimum_inclusive
            else f"endliche Zahl größer als {minimum} und höchstens {maximum}"
        )
        raise _invalid(_path(section, key), expected, value)
    return numeric


def _string(
    table: Mapping[str, Any],
    section: str,
    key: str,
    *,
    default: Any = _MISSING,
    allow_empty: bool = False,
    maximum: int = 4096,
) -> str:
    value = _value(table, section, key, default)
    if not isinstance(value, str):
        raise _invalid(_path(section, key), "String", value)
    cleaned = value.strip()
    if value != cleaned or (not allow_empty and not cleaned) or len(cleaned) > maximum:
        qualifier = f"nicht-leerer String mit höchstens {maximum} Zeichen"
        raise _invalid(_path(section, key), qualifier, value)
    return cleaned


def _optional_string(
    table: Mapping[str, Any],
    section: str,
    key: str,
    *,
    default: str | None = None,
    maximum: int = 4096,
) -> str | None:
    value = _value(table, section, key, default)
    if value is None:
        return None
    if not isinstance(value, str):
        raise _invalid(_path(section, key), "String oder nicht gesetzt", value)
    cleaned = value.strip()
    if value == "":
        return None
    if value != cleaned or len(cleaned) > maximum:
        raise _invalid(_path(section, key), f"String mit höchstens {maximum} Zeichen", value)
    return cleaned


def _string_list(
    table: Mapping[str, Any], section: str, key: str, default: tuple[str, ...]
) -> tuple[str, ...]:
    value = _value(table, section, key, list(default))
    if not isinstance(value, list):
        raise _invalid(_path(section, key), "Liste von Strings", value)
    result: list[str] = []
    for index, entry in enumerate(value):
        entry_path = f"{_path(section, key)}[{index}]"
        if not isinstance(entry, str) or not entry or entry != entry.strip():
            raise _invalid(entry_path, "nicht-leerer IP/CIDR-String", entry)
        cleaned = entry.strip()
        try:
            ipaddress.ip_network(cleaned, strict=False)
        except ValueError as exc:
            raise _invalid(entry_path, "gültige IPv4-/IPv6-Adresse oder CIDR", entry) from exc
        result.append(cleaned)
    return tuple(result)


def _path_value(
    table: Mapping[str, Any], section: str, key: str, config_directory: Path
) -> Path | None:
    value = _optional_string(table, section, key, maximum=4096)
    if value is None:
        return None
    if _contains_control(value):
        raise _invalid(_path(section, key), "gültiger Dateipfad ohne Steuerzeichen", value)
    try:
        candidate = Path(value).expanduser()
    except (OSError, RuntimeError) as exc:
        raise _invalid(_path(section, key), "auflösbarer Dateipfad", value) from exc
    if not candidate.is_absolute():
        candidate = config_directory / candidate
    try:
        return candidate.resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise _invalid(_path(section, key), "auflösbarer Dateipfad", value) from exc


def _bearer_token(table: Mapping[str, Any]) -> str | None:
    value = _value(table, "monitoring", "bearer_token", None)
    if value is None or value == "":
        return None
    if (
        not isinstance(value, str)
        or value != value.strip()
        or any(not 0x21 <= ord(character) <= 0x7E for character in value)
        or len(value) > _BEARER_TOKEN_MAX_CHARS
    ):
        raise _invalid_secret(
            "[monitoring].bearer_token",
            "nicht-leeres druckbares ASCII-Bearer-Token ohne Leerzeichen",
        )
    return value


def _reject_unknown(table: Mapping[str, Any], allowed: frozenset[str], section: str) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise ConfigError(f"Unbekannter Konfigurationsschlüssel {_path(section, unknown[0])}")


def _parse_healthcheck(
    table: Mapping[str, Any],
    section: str,
    *,
    defaults: HealthCheckConfig | None,
    required: bool | None = None,
) -> HealthCheckConfig:
    required = defaults is None if required is None else required
    base = defaults or HealthCheckConfig(
        enabled=True,
        mode="tcp",
        interval_seconds=3.0,
        timeout_seconds=2.0,
        fail_after=2,
        recover_after=2,
        min_recovery_seconds=0.0,
        target_host=None,
        target_port=None,
        protocol_version=767,
        status_hostname=None,
        require_valid_json=True,
        log_status_details=False,
        jitter_seconds=0.0,
        max_latency_ms=0.0,
        expected_version_contains="",
        motd_must_contain="",
        motd_must_not_contain="",
        min_players_max=0,
        reject_uninitialized_protocol=True,
    )
    require_valid_json = _bool(
        table,
        section,
        "require_valid_json",
        base.require_valid_json,
    )
    # Preserve configurations that deliberately disabled JSON validation before
    # reject_uninitialized_protocol existed.  Explicitly enabling the new filter
    # with JSON validation disabled is still rejected by _validate_healthcheck.
    reject_uninitialized_protocol_default = (
        base.reject_uninitialized_protocol if require_valid_json else False
    )
    return HealthCheckConfig(
        enabled=_bool(table, section, "enabled", base.enabled),
        mode=_string(
            table,
            section,
            "mode",
            default=_MISSING if required else base.mode,
            maximum=32,
        ),
        interval_seconds=_number(
            table,
            section,
            "interval_seconds",
            minimum=0.0,
            maximum=3600.0,
            minimum_inclusive=False,
            default=_MISSING if required else base.interval_seconds,
        ),
        timeout_seconds=_number(
            table,
            section,
            "timeout_seconds",
            minimum=0.0,
            maximum=600.0,
            minimum_inclusive=False,
            default=_MISSING if required else base.timeout_seconds,
        ),
        fail_after=_int(
            table,
            section,
            "fail_after",
            minimum=1,
            maximum=1_000_000,
            default=_MISSING if required else base.fail_after,
        ),
        recover_after=_int(
            table,
            section,
            "recover_after",
            minimum=1,
            maximum=1_000_000,
            default=_MISSING if required else base.recover_after,
        ),
        min_recovery_seconds=_number(
            table,
            section,
            "min_recovery_seconds",
            minimum=0.0,
            maximum=86_400.0,
            default=base.min_recovery_seconds,
        ),
        target_host=_optional_string(
            table,
            section,
            "target_host",
            default=base.target_host,
            maximum=253,
        ),
        target_port=_optional_int(
            table,
            section,
            "target_port",
            minimum=1,
            maximum=65_535,
            default=base.target_port,
        ),
        protocol_version=_int(
            table,
            section,
            "protocol_version",
            minimum=1,
            maximum=2_147_483_647,
            default=base.protocol_version,
        ),
        status_hostname=_optional_string(
            table,
            section,
            "status_hostname",
            default=base.status_hostname,
            maximum=MAX_STATUS_HOST_CHARS,
        ),
        require_valid_json=require_valid_json,
        reject_uninitialized_protocol=_bool(
            table,
            section,
            "reject_uninitialized_protocol",
            reject_uninitialized_protocol_default,
        ),
        log_status_details=_bool(
            table,
            section,
            "log_status_details",
            base.log_status_details,
        ),
        jitter_seconds=_number(
            table,
            section,
            "jitter_seconds",
            minimum=0.0,
            maximum=3600.0,
            default=base.jitter_seconds,
        ),
        max_latency_ms=_number(
            table,
            section,
            "max_latency_ms",
            minimum=0.0,
            maximum=3_600_000.0,
            default=base.max_latency_ms,
        ),
        expected_version_contains=_string(
            table,
            section,
            "expected_version_contains",
            default=base.expected_version_contains,
            allow_empty=True,
            maximum=1024,
        ),
        motd_must_contain=_string(
            table,
            section,
            "motd_must_contain",
            default=base.motd_must_contain,
            allow_empty=True,
            maximum=4096,
        ),
        motd_must_not_contain=_string(
            table,
            section,
            "motd_must_not_contain",
            default=base.motd_must_not_contain,
            allow_empty=True,
            maximum=4096,
        ),
        min_players_max=_int(
            table,
            section,
            "min_players_max",
            minimum=0,
            maximum=2_147_483_647,
            default=base.min_players_max,
        ),
    )


def _parse_circuit(
    table: Mapping[str, Any], section: str, defaults: CircuitBreakerConfig
) -> CircuitBreakerConfig:
    return CircuitBreakerConfig(
        enabled=_bool(table, section, "enabled", defaults.enabled),
        failure_threshold=_int(
            table,
            section,
            "failure_threshold",
            minimum=1,
            maximum=1_000_000,
            default=defaults.failure_threshold,
        ),
        failure_window_seconds=_number(
            table,
            section,
            "failure_window_seconds",
            minimum=0,
            maximum=86400,
            minimum_inclusive=False,
            default=defaults.failure_window_seconds,
        ),
        open_seconds=_number(
            table,
            section,
            "open_seconds",
            minimum=0,
            maximum=86400,
            minimum_inclusive=False,
            default=defaults.open_seconds,
        ),
        half_open_max_attempts=_int(
            table,
            section,
            "half_open_max_attempts",
            minimum=1,
            maximum=1_000_000,
            default=defaults.half_open_max_attempts,
        ),
    )


def _motd(table: Mapping[str, Any], key: str, default: str) -> str:
    value = _value(table, "status", key, default)
    try:
        valid = isinstance(value, str) and len(value) <= 8192 and len(value.encode("utf-8")) <= 8192
    except UnicodeError:
        valid = False
    if not valid:
        raise ConfigError(f"[status].{key} muss ein String mit höchstens 8192 UTF-8-Bytes sein")
    value = cast(str, value)
    if any(ord(character) < 32 and character not in "\n\t" for character in value):
        raise ConfigError(f"[status].{key} enthält unzulässige Steuerzeichen")
    return value


def load_config(path: Path) -> AppConfig:
    """Load *path* as TOML and return a fully validated immutable config."""

    if not isinstance(path, Path):
        raise ConfigError(f"Konfigurationspfad muss pathlib.Path sein, erhalten {_render(path)}")
    try:
        # Opening a FIFO normally blocks before the bounded read can run. A
        # nonblocking open plus fstat rejects special files without a check/open
        # race, while retaining symlinks to ordinary configuration files.
        descriptor = os.open(
            path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0)
        )
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ConfigError(
                    f"Konfigurationsdatei konnte nicht gelesen werden: {path} (keine reguläre Datei)"
                )
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                contents = handle.read(2 * 1024 * 1024 + 1)
        finally:
            os.close(descriptor)
        if len(contents) > 2 * 1024 * 1024:
            raise ConfigError("Konfigurationsdatei überschreitet 2 MiB")
        raw = tomllib.loads(contents.decode("utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"Konfigurationsdatei nicht gefunden: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Ungültiges TOML in {path}: {exc}") from exc
    except UnicodeError as exc:
        raise ConfigError(f"Konfigurationsdatei muss gültiges UTF-8 enthalten: {path}") from exc
    except OSError as exc:
        raise ConfigError(
            f"Konfigurationsdatei konnte nicht gelesen werden: {path} ({exc})"
        ) from exc

    config_table = _section(raw, "config", required=False)
    strict_unknown_keys = _bool(config_table, "config", "strict_unknown_keys", True)
    multi_backend = "backends" in raw
    if multi_backend and set(raw) & {"main", "fallback", "healthcheck", "fallback_healthcheck"}:
        raise ConfigError(
            "[[backends]] darf nicht mit Legacy-Ziel-/Healthcheck-Sektionen kombiniert werden"
        )
    sections = {
        "proxy": _section(raw, "proxy", required=True),
        "main": _section(raw, "main", required=not multi_backend),
        "fallback": _section(raw, "fallback", required=not multi_backend),
        "healthcheck": _section(raw, "healthcheck", required=not multi_backend),
        "fallback_healthcheck": _section(raw, "fallback_healthcheck", required=False),
        "connection": _section(raw, "connection", required=True),
        "logging": _section(raw, "logging", required=True),
        "maintenance": _section(raw, "maintenance", required=False),
        "proxy_protocol": _section(raw, "proxy_protocol", required=False),
        "monitoring": _section(raw, "monitoring", required=False),
        "circuit_breaker": _section(raw, "circuit_breaker", required=False),
        "routing": _section(raw, "routing", required=False),
        "passive_health": _section(raw, "passive_health", required=False),
        "reload": _section(raw, "reload", required=False),
        "status": _section(raw, "status", required=False),
    }
    if strict_unknown_keys:
        unknown_root = sorted(set(raw) - _ROOT_KEYS)
        if unknown_root:
            unknown_name = unknown_root[0]
            unknown_path = (
                f"[{unknown_name}]" if isinstance(raw[unknown_name], dict) else unknown_name
            )
            raise ConfigError(f"Unbekannter Konfigurationsschlüssel {unknown_path}")
        _reject_unknown(config_table, _SECTION_KEYS["config"], "config")
        for section_name, table in sections.items():
            _reject_unknown(table, _SECTION_KEYS[section_name], section_name)

    proxy_table = sections["proxy"]
    main_table = sections["main"]
    fallback_table = sections["fallback"]
    connection_table = sections["connection"]
    logging_table = sections["logging"]
    maintenance_table = sections["maintenance"]
    protocol_table = sections["proxy_protocol"]
    monitoring_table = sections["monitoring"]
    circuit_table = sections["circuit_breaker"]

    proxy = ProxyConfig(
        listen_host=_string(proxy_table, "proxy", "listen_host", maximum=253),
        listen_port=_int(proxy_table, "proxy", "listen_port", minimum=1, maximum=65_535),
        backlog=_int(proxy_table, "proxy", "backlog", minimum=1, maximum=65_535, default=256),
    )
    main = TargetConfig(
        host=_string(
            main_table,
            "main",
            "host",
            maximum=253,
            default="127.0.0.1" if multi_backend else _MISSING,
        ),
        port=_int(
            main_table,
            "main",
            "port",
            minimum=1,
            maximum=65_535,
            default=25564 if multi_backend else _MISSING,
        ),
    )
    fallback = TargetConfig(
        host=_string(
            fallback_table,
            "fallback",
            "host",
            maximum=253,
            default="127.0.0.1" if multi_backend else _MISSING,
        ),
        port=_int(
            fallback_table,
            "fallback",
            "port",
            minimum=1,
            maximum=65_535,
            default=25566 if multi_backend else _MISSING,
        ),
    )
    healthcheck = _parse_healthcheck(
        sections["healthcheck"], "healthcheck", defaults=None, required=not multi_backend
    )
    fallback_defaults = replace(
        healthcheck,
        enabled=True,
        mode="tcp",
        target_host=None,
        target_port=None,
        status_hostname=None,
        reject_uninitialized_protocol=True,
        log_status_details=False,
        max_latency_ms=0.0,
        expected_version_contains="",
        motd_must_contain="",
        motd_must_not_contain="",
        min_players_max=0,
    )
    fallback_healthcheck = _parse_healthcheck(
        sections["fallback_healthcheck"],
        "fallback_healthcheck",
        defaults=fallback_defaults,
    )
    connection = ConnectionConfig(
        timeout_seconds=_number(
            connection_table,
            "connection",
            "timeout_seconds",
            minimum=0.0,
            maximum=600.0,
            minimum_inclusive=False,
        ),
        buffer_size=_int(
            connection_table,
            "connection",
            "buffer_size",
            minimum=1024,
            maximum=16 * 1024 * 1024,
        ),
        idle_timeout_seconds=_number(
            connection_table,
            "connection",
            "idle_timeout_seconds",
            minimum=0.0,
            maximum=604_800.0,
            default=300.0,
        ),
        write_timeout_seconds=_number(
            connection_table,
            "connection",
            "write_timeout_seconds",
            minimum=0.0,
            maximum=600.0,
            minimum_inclusive=False,
            default=30.0,
        ),
        relay_drain_timeout_seconds=_number(
            connection_table,
            "connection",
            "relay_drain_timeout_seconds",
            minimum=0.0,
            maximum=600.0,
            minimum_inclusive=False,
            default=10.0,
        ),
        shutdown_grace_seconds=_number(
            connection_table,
            "connection",
            "shutdown_grace_seconds",
            minimum=0.0,
            maximum=3600.0,
            default=30.0,
        ),
        shutdown_cancel_timeout_seconds=_number(
            connection_table,
            "connection",
            "shutdown_cancel_timeout_seconds",
            minimum=0.0,
            maximum=600.0,
            minimum_inclusive=False,
            default=5.0,
        ),
        connect_fallback_on_main_connect_failure=_bool(
            connection_table,
            "connection",
            "connect_fallback_on_main_connect_failure",
            False,
        ),
        tcp_keepalive=_bool(connection_table, "connection", "tcp_keepalive", False),
        max_connections=_int(
            connection_table,
            "connection",
            "max_connections",
            minimum=1,
            maximum=1_000_000,
            default=4096,
        ),
        max_connections_per_ip=_int(
            connection_table,
            "connection",
            "max_connections_per_ip",
            minimum=0,
            maximum=1_000_000,
            default=0,
        ),
        new_connections_per_second=_number(
            connection_table,
            "connection",
            "new_connections_per_second",
            minimum=0.0,
            maximum=1_000_000.0,
            default=0.0,
        ),
        new_connections_burst=_int(
            connection_table,
            "connection",
            "new_connections_burst",
            minimum=0,
            maximum=1_000_000,
            default=0,
        ),
    )
    logging_config = LoggingConfig(
        level=_string(logging_table, "logging", "level", maximum=16).upper(),
        access_log=_bool(logging_table, "logging", "access_log", False),
    )
    config_directory = path.resolve(strict=False).parent
    mode_raw = _string(maintenance_table, "maintenance", "mode", default="auto", maximum=32)
    try:
        maintenance_mode = models.MaintenanceMode(mode_raw)
    except ValueError as exc:
        raise _invalid(
            _path("maintenance", "mode"),
            "auto, force_main, force_fallback oder force_target",
            mode_raw,
        ) from exc
    maintenance = MaintenanceConfig(
        mode=maintenance_mode,
        force_fallback_file=_path_value(
            maintenance_table, "maintenance", "force_fallback_file", config_directory
        ),
        force_main_file=_path_value(
            maintenance_table, "maintenance", "force_main_file", config_directory
        ),
        file_check_interval_seconds=_number(
            maintenance_table,
            "maintenance",
            "file_check_interval_seconds",
            minimum=0.0,
            maximum=3600.0,
            minimum_inclusive=False,
            default=1.0,
        ),
        force_target=_optional_string(maintenance_table, "maintenance", "force_target", maximum=64),
    )
    proxy_protocol = ProxyProtocolConfig(
        accept=_bool(protocol_table, "proxy_protocol", "accept", False),
        send=_bool(protocol_table, "proxy_protocol", "send", False),
        version=_int(protocol_table, "proxy_protocol", "version", minimum=1, maximum=2, default=1),
        accept_version=_optional_int(
            protocol_table,
            "proxy_protocol",
            "accept_version",
            minimum=1,
            maximum=2,
            default=None,
        ),
        send_version=_optional_int(
            protocol_table,
            "proxy_protocol",
            "send_version",
            minimum=1,
            maximum=2,
            default=None,
        ),
        trust_all_proxies=_bool(protocol_table, "proxy_protocol", "trust_all_proxies", False),
        trusted_proxy_ips=_string_list(protocol_table, "proxy_protocol", "trusted_proxy_ips", ()),
        header_timeout_seconds=_number(
            protocol_table,
            "proxy_protocol",
            "header_timeout_seconds",
            minimum=0.0,
            maximum=60.0,
            minimum_inclusive=False,
            default=2.0,
        ),
        max_header_bytes=_int(
            protocol_table,
            "proxy_protocol",
            "max_header_bytes",
            minimum=15,
            maximum=65_551,
            default=4096,
        ),
    )
    monitoring = MonitoringConfig(
        enabled=_bool(monitoring_table, "monitoring", "enabled", False),
        listen_host=_string(
            monitoring_table, "monitoring", "listen_host", default="127.0.0.1", maximum=253
        ),
        listen_port=_int(
            monitoring_table,
            "monitoring",
            "listen_port",
            minimum=1,
            maximum=65_535,
            default=8080,
        ),
        allow_remote=_bool(monitoring_table, "monitoring", "allow_remote", False),
        bearer_token=_bearer_token(monitoring_table),
        allow_unauthenticated_remote=_bool(
            monitoring_table, "monitoring", "allow_unauthenticated_remote", False
        ),
        expose_sensitive_state=_bool(
            monitoring_table, "monitoring", "expose_sensitive_state", False
        ),
        max_connections=_int(
            monitoring_table,
            "monitoring",
            "max_connections",
            minimum=1,
            maximum=100_000,
            default=128,
        ),
        request_timeout_seconds=_number(
            monitoring_table,
            "monitoring",
            "request_timeout_seconds",
            minimum=0.0,
            maximum=60.0,
            minimum_inclusive=False,
            default=2.0,
        ),
        write_timeout_seconds=_number(
            monitoring_table,
            "monitoring",
            "write_timeout_seconds",
            minimum=0.0,
            maximum=60.0,
            minimum_inclusive=False,
            default=2.0,
        ),
    )
    circuit_breaker = CircuitBreakerConfig(
        enabled=_bool(circuit_table, "circuit_breaker", "enabled", True),
        failure_threshold=_int(
            circuit_table,
            "circuit_breaker",
            "failure_threshold",
            minimum=1,
            maximum=1_000_000,
            default=3,
        ),
        failure_window_seconds=_number(
            circuit_table,
            "circuit_breaker",
            "failure_window_seconds",
            minimum=0.0,
            maximum=86_400.0,
            minimum_inclusive=False,
            default=10.0,
        ),
        open_seconds=_number(
            circuit_table,
            "circuit_breaker",
            "open_seconds",
            minimum=0.0,
            maximum=86_400.0,
            minimum_inclusive=False,
            default=15.0,
        ),
        half_open_max_attempts=_int(
            circuit_table,
            "circuit_breaker",
            "half_open_max_attempts",
            minimum=1,
            maximum=1_000_000,
            default=1,
        ),
    )

    backends: tuple[BackendConfig, ...] = ()
    if multi_backend:
        backend_tables = raw["backends"]
        if not isinstance(backend_tables, list) or not backend_tables:
            raise ConfigError("[[backends]] muss mindestens einen Backend-Eintrag enthalten")
        parsed_backends: list[BackendConfig] = []
        for index, table in enumerate(backend_tables):
            section = f"backends.{index}"
            if not isinstance(table, dict):
                raise ConfigError(f"[{section}] muss ein TOML-Table sein")
            if strict_unknown_keys:
                _reject_unknown(
                    table,
                    frozenset(
                        {
                            "id",
                            "host",
                            "port",
                            "priority",
                            "enabled",
                            "connect_timeout_seconds",
                            "healthcheck",
                            "circuit_breaker",
                        }
                    ),
                    section,
                )
            check_table = _section(table, "healthcheck", required=False)
            breaker_table = _section(table, "circuit_breaker", required=False)
            if strict_unknown_keys:
                _reject_unknown(check_table, _SECTION_KEYS["healthcheck"], f"{section}.healthcheck")
                _reject_unknown(
                    breaker_table, _SECTION_KEYS["circuit_breaker"], f"{section}.circuit_breaker"
                )
            parsed_backends.append(
                BackendConfig(
                    id=_string(table, section, "id", maximum=64),
                    host=_string(table, section, "host", maximum=253),
                    port=_int(table, section, "port", minimum=1, maximum=65535),
                    priority=_int(
                        table, section, "priority", minimum=0, maximum=2_147_483_647, default=100
                    ),
                    enabled=_bool(table, section, "enabled", True),
                    connect_timeout_seconds=(
                        _number(
                            table,
                            section,
                            "connect_timeout_seconds",
                            minimum=0,
                            maximum=600,
                            minimum_inclusive=False,
                        )
                        if "connect_timeout_seconds" in table
                        else None
                    ),
                    healthcheck=_parse_healthcheck(
                        check_table, f"{section}.healthcheck", defaults=None, required=False
                    ),
                    circuit_breaker=_parse_circuit(
                        breaker_table, f"{section}.circuit_breaker", circuit_breaker
                    ),
                )
            )
        backends = tuple(parsed_backends)
        aliases = {backend.id: backend for backend in backends}
        main_alias = aliases.get("MAIN", backends[0])
        fallback_alias = aliases.get("FALLBACK", backends[min(1, len(backends) - 1)])
        main = TargetConfig(main_alias.host, main_alias.port)
        fallback = TargetConfig(fallback_alias.host, fallback_alias.port)
        healthcheck = main_alias.healthcheck
        fallback_healthcheck = fallback_alias.healthcheck

    routing_table = sections["routing"]
    routing_config = RoutingConfig(
        max_connect_attempts=_int(
            routing_table,
            "routing",
            "max_connect_attempts",
            minimum=1,
            maximum=1_000_000,
            default=16,
        ),
        connect_budget_seconds=_number(
            routing_table,
            "routing",
            "connect_budget_seconds",
            minimum=0,
            maximum=600,
            minimum_inclusive=False,
            default=10,
        ),
        max_parallel_healthchecks=_int(
            routing_table,
            "routing",
            "max_parallel_healthchecks",
            minimum=1,
            maximum=10_000,
            default=16,
        ),
    )
    passive_table = sections["passive_health"]
    passive_config = PassiveHealthConfig(
        enabled=_bool(passive_table, "passive_health", "enabled", True),
        failure_threshold=_int(
            passive_table,
            "passive_health",
            "failure_threshold",
            minimum=1,
            maximum=1_000_000,
            default=3,
        ),
        failure_window_seconds=_number(
            passive_table,
            "passive_health",
            "failure_window_seconds",
            minimum=0,
            maximum=86_400,
            minimum_inclusive=False,
            default=30,
        ),
        quarantine_seconds=_number(
            passive_table,
            "passive_health",
            "quarantine_seconds",
            minimum=0,
            maximum=86_400,
            minimum_inclusive=False,
            default=10,
        ),
        early_session_seconds=_number(
            passive_table,
            "passive_health",
            "early_session_seconds",
            minimum=0,
            maximum=600,
            default=0,
        ),
    )
    status_table = sections["status"]
    status_defaults = LocalStatusConfig()
    status_config = LocalStatusConfig(
        enabled=_bool(status_table, "status", "enabled", False),
        always=_bool(status_table, "status", "always", False),
        maintenance=_bool(status_table, "status", "maintenance", True),
        handshake_timeout_seconds=_number(
            status_table,
            "status",
            "handshake_timeout_seconds",
            minimum=0,
            maximum=60,
            minimum_inclusive=False,
            default=2,
        ),
        session_timeout_seconds=_number(
            status_table,
            "status",
            "session_timeout_seconds",
            minimum=0,
            maximum=60,
            minimum_inclusive=False,
            default=5,
        ),
        max_handshake_bytes=_int(
            status_table, "status", "max_handshake_bytes", minimum=16, maximum=65536, default=2048
        ),
        max_concurrent=_int(
            status_table, "status", "max_concurrent", minimum=1, maximum=100_000, default=64
        ),
        motd_online=_motd(status_table, "motd_online", status_defaults.motd_online),
        motd_maintenance=_motd(status_table, "motd_maintenance", status_defaults.motd_maintenance),
        motd_fallback=_motd(status_table, "motd_fallback", status_defaults.motd_fallback),
        motd_offline=_motd(status_table, "motd_offline", status_defaults.motd_offline),
        version_name=_string(
            status_table,
            "status",
            "version_name",
            default=status_defaults.version_name,
            maximum=128,
        ),
        protocol_version=_int(
            status_table,
            "status",
            "protocol_version",
            minimum=-1,
            maximum=2_147_483_647,
            default=-1,
        ),
        players_online=_optional_int(
            status_table, "status", "players_online", minimum=0, maximum=2_147_483_647, default=None
        ),
        players_max=_optional_int(
            status_table, "status", "players_max", minimum=0, maximum=2_147_483_647, default=None
        ),
        favicon=_optional_string(status_table, "status", "favicon", maximum=24000),
        dynamic_hint=_bool(status_table, "status", "dynamic_hint", False),
    )
    result = AppConfig(
        proxy=proxy,
        main=main,
        fallback=fallback,
        healthcheck=healthcheck,
        fallback_healthcheck=fallback_healthcheck,
        connection=connection,
        logging=logging_config,
        maintenance=maintenance,
        proxy_protocol=proxy_protocol,
        monitoring=monitoring,
        circuit_breaker=circuit_breaker,
        strict_unknown_keys=strict_unknown_keys,
        backends=backends,
        routing=routing_config,
        passive_health=passive_config,
        reload=ReloadConfig(enabled=_bool(sections["reload"], "reload", "enabled", True)),
        status=status_config,
    )
    validate_config(result)
    return result


def _contains_control(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _validate_plain_string(path: str, value: Any, *, allow_empty: bool, maximum: int) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise _invalid(path, "String ohne äußere Leerzeichen", value)
    if (not allow_empty and not value) or len(value) > maximum or _contains_control(value):
        qualifier = "String" if allow_empty else "nicht-leerer String"
        raise _invalid(path, f"{qualifier} ohne Steuerzeichen, max. {maximum} Zeichen", value)
    return value


def _validate_host(path: str, value: Any, *, target: bool = False) -> None:
    host = _validate_plain_string(path, value, allow_empty=False, maximum=253)
    if any(character.isspace() for character in host) or any(c in host for c in "/\\[]"):
        raise _invalid(path, "gültige IP-Adresse oder Hostname", value)
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if ":" in host:
            raise _invalid(path, "gültige IP-Adresse oder Hostname", value) from None
        candidate = host[:-1] if host.endswith(".") else host
        try:
            socket.inet_aton(candidate)
        except (OSError, UnicodeError):
            pass
        else:
            raise _invalid(
                path,
                "kanonische IPv4-Adresse oder Hostname (keine Legacy-IPv4-Schreibweise)",
                value,
            ) from None
        if not candidate or ".." in candidate:
            raise _invalid(path, "gültige IP-Adresse oder Hostname", value) from None
        try:
            encoded = candidate.encode("idna")
        except UnicodeError as exc:
            raise _invalid(path, "gültige IP-Adresse oder Hostname", value) from exc
        labels = encoded.split(b".")
        invalid_label = any(
            not label
            or len(label) > 63
            or label.startswith(b"-")
            or label.endswith(b"-")
            or any(
                not (byte == 45 or byte == 95 or 48 <= byte <= 57 or 97 <= byte <= 122)
                for byte in label.lower()
            )
            for label in labels
        )
        if len(encoded) > 253 or invalid_label:
            raise _invalid(path, "gültige IP-Adresse oder Hostname", value) from None
        return
    if target and address.is_unspecified:
        raise _invalid(path, "konkrete Zieladresse (keine Wildcard)", value)


def _validate_port(path: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65_535:
        raise _invalid(path, "Integer zwischen 1 und 65535", value)


def _validate_int(path: str, value: Any, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise _invalid(path, f"Integer zwischen {minimum} und {maximum}", value)


def _validate_number(
    path: str, value: Any, minimum: float, maximum: float, *, exclusive_minimum: bool = False
) -> None:
    valid_type = not isinstance(value, bool) and isinstance(value, (int, float))
    try:
        numeric = float(value) if valid_type else math.nan
    except (OverflowError, ValueError):
        numeric = math.nan
    lower_ok = numeric > minimum if exclusive_minimum else numeric >= minimum
    if not valid_type or not math.isfinite(numeric) or not lower_ok or numeric > maximum:
        raise _invalid(path, f"endliche Zahl im gültigen Bereich {minimum}..{maximum}", value)


def _validate_healthcheck(section: str, value: Any) -> None:
    if not isinstance(value, HealthCheckConfig):
        raise _invalid(f"[{section}]", "HealthCheckConfig", value)
    if not isinstance(value.enabled, bool):
        raise _invalid(_path(section, "enabled"), "Boolean", value.enabled)
    if not isinstance(value.mode, str) or value.mode not in _HEALTH_MODES:
        raise _invalid(_path(section, "mode"), "tcp oder minecraft_status", value.mode)
    _validate_number(
        _path(section, "interval_seconds"),
        value.interval_seconds,
        0.0,
        3600.0,
        exclusive_minimum=True,
    )
    _validate_number(
        _path(section, "timeout_seconds"), value.timeout_seconds, 0.0, 600.0, exclusive_minimum=True
    )
    _validate_int(_path(section, "fail_after"), value.fail_after, 1, 1_000_000)
    _validate_int(_path(section, "recover_after"), value.recover_after, 1, 1_000_000)
    _validate_number(
        _path(section, "min_recovery_seconds"), value.min_recovery_seconds, 0.0, 86_400.0
    )
    if value.target_host is not None:
        _validate_host(_path(section, "target_host"), value.target_host, target=True)
    if value.target_port is not None:
        _validate_port(_path(section, "target_port"), value.target_port)
    _validate_int(_path(section, "protocol_version"), value.protocol_version, 1, 2_147_483_647)
    if value.status_hostname is not None:
        _validate_host(_path(section, "status_hostname"), value.status_hostname)
        if len(value.status_hostname.encode("utf-8")) > MAX_STATUS_HOST_BYTES:
            raise _invalid(
                _path(section, "status_hostname"),
                f"Hostname mit höchstens {MAX_STATUS_HOST_BYTES} UTF-8-Bytes",
                value.status_hostname,
            )
    for field_name, field_value in (
        ("require_valid_json", value.require_valid_json),
        ("reject_uninitialized_protocol", value.reject_uninitialized_protocol),
        ("log_status_details", value.log_status_details),
    ):
        if not isinstance(field_value, bool):
            raise _invalid(_path(section, field_name), "Boolean", field_value)
    _validate_number(_path(section, "jitter_seconds"), value.jitter_seconds, 0.0, 3600.0)
    _validate_number(_path(section, "max_latency_ms"), value.max_latency_ms, 0.0, 3_600_000.0)
    for filter_name, filter_value, maximum in (
        ("expected_version_contains", value.expected_version_contains, 1024),
        ("motd_must_contain", value.motd_must_contain, 4096),
        ("motd_must_not_contain", value.motd_must_not_contain, 4096),
    ):
        _validate_plain_string(
            _path(section, filter_name), filter_value, allow_empty=True, maximum=maximum
        )
    _validate_int(_path(section, "min_players_max"), value.min_players_max, 0, 2_147_483_647)
    json_filters = (
        value.expected_version_contains
        or value.motd_must_contain
        or value.motd_must_not_contain
        or value.min_players_max > 0
    )
    if json_filters and not value.require_valid_json:
        raise ConfigError(
            f"Ungültige Konfiguration [{section}]: JSON-Filter erfordern "
            f"{_path(section, 'require_valid_json')} = true"
        )
    if json_filters and value.mode != "minecraft_status":
        raise ConfigError(
            f"Ungültige Konfiguration [{section}]: JSON-Filter erfordern mode = 'minecraft_status'"
        )
    if (
        value.mode == "minecraft_status"
        and value.reject_uninitialized_protocol
        and not value.require_valid_json
    ):
        raise ConfigError(
            f"Ungültige Konfiguration [{section}]: "
            f"{_path(section, 'reject_uninitialized_protocol')} = true erfordert "
            f"{_path(section, 'require_valid_json')} = true"
        )


def _host_identity(host: str) -> tuple[str, int | None]:
    normalized = host.lower().rstrip(".")
    if normalized == "localhost":
        return "localhost", None
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return normalized, None
    return str(address), address.version


def _target_hits_listener(target_host: str, listen_host: str) -> bool:
    target_identity, target_version = _host_identity(target_host)
    listen_identity, listen_version = _host_identity(listen_host)
    if target_identity == listen_identity:
        return True
    target_loopback = target_identity == "localhost"
    if target_version is not None:
        target_loopback = ipaddress.ip_address(target_identity).is_loopback
    listen_loopback = listen_identity == "localhost"
    if listen_version is not None:
        listen_address = ipaddress.ip_address(listen_identity)
        listen_loopback = listen_address.is_loopback
        if listen_address.is_unspecified and target_loopback:
            return target_version in {listen_address.version, None}
    return target_loopback and listen_loopback


def _listeners_overlap(first: str, second: str) -> bool:
    first_identity, first_version = _host_identity(first)
    second_identity, second_version = _host_identity(second)
    if first_identity == second_identity:
        return True
    if first_identity == "localhost" or second_identity == "localhost":
        other = second_identity if first_identity == "localhost" else first_identity
        try:
            other_address = ipaddress.ip_address(other)
            return other_address.is_loopback or other_address.is_unspecified
        except ValueError:
            return False
    if first_version is None or second_version is None:
        return False
    first_address = ipaddress.ip_address(first_identity)
    second_address = ipaddress.ip_address(second_identity)
    return first_address.version == second_address.version and (
        first_address.is_unspecified or second_address.is_unspecified
    )


def _is_local_monitor_host(host: str) -> bool:
    identity, version = _host_identity(host)
    if identity == "localhost":
        return True
    if version is None:
        return False
    return ipaddress.ip_address(identity).is_loopback


def configured_targets(
    config: AppConfig,
) -> tuple[tuple[str, str, TargetConfig, HealthCheckConfig], ...]:
    """Safety-validation names retain legacy diagnostics for existing installations."""
    if not config.backends:
        return (
            ("main", "healthcheck", config.main, config.healthcheck),
            ("fallback", "fallback_healthcheck", config.fallback, config.fallback_healthcheck),
        )
    return tuple(
        (
            f"backends.{backend.id}",
            f"backends.{backend.id}.healthcheck",
            TargetConfig(backend.host, backend.port),
            backend.healthcheck,
        )
        for backend in config.backends
    )


def _validate_extensions(config: AppConfig) -> None:
    if not isinstance(config.backends, tuple):
        raise ConfigError("backends muss ein Tuple von BackendConfig sein")
    seen: set[str] = set()
    for backend in config.backends:
        if not isinstance(backend, BackendConfig):
            raise ConfigError("backends enthält ungültigen BackendConfig-Eintrag")
        if (
            not isinstance(backend.id, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", backend.id)
            or backend.id == "NONE"
        ):
            raise ConfigError(
                "Backend-ID muss 1..64 ASCII-Zeichen (Buchstaben/Ziffern/_.-) enthalten und darf nicht NONE sein"
            )
        if backend.id in seen:
            raise ConfigError("Doppelte Backend-ID")
        seen.add(backend.id)
        _validate_host(f"[backends.{backend.id}].host", backend.host, target=True)
        _validate_port(f"[backends.{backend.id}].port", backend.port)
        _validate_int(f"[backends.{backend.id}].priority", backend.priority, 0, 2_147_483_647)
        if not isinstance(backend.enabled, bool):
            raise ConfigError("Backend enabled muss Boolean sein")
        if backend.connect_timeout_seconds is not None:
            _validate_number(
                "backend.connect_timeout_seconds",
                backend.connect_timeout_seconds,
                0,
                600,
                exclusive_minimum=True,
            )
        _validate_healthcheck(f"backends.{backend.id}.healthcheck", backend.healthcheck)
        _validate_circuit(backend.circuit_breaker, f"backends.{backend.id}.circuit_breaker")
    if not isinstance(config.routing, RoutingConfig):
        raise ConfigError("routing muss RoutingConfig sein")
    _validate_int(
        "[routing].max_connect_attempts", config.routing.max_connect_attempts, 1, 1_000_000
    )
    _validate_int(
        "[routing].max_parallel_healthchecks", config.routing.max_parallel_healthchecks, 1, 10_000
    )
    _validate_number(
        "[routing].connect_budget_seconds",
        config.routing.connect_budget_seconds,
        0,
        600,
        exclusive_minimum=True,
    )
    if not isinstance(config.passive_health, PassiveHealthConfig):
        raise ConfigError("passive_health muss PassiveHealthConfig sein")
    passive = config.passive_health
    if not isinstance(passive.enabled, bool):
        raise ConfigError("[passive_health].enabled muss Boolean sein")
    _validate_int("[passive_health].failure_threshold", passive.failure_threshold, 1, 1_000_000)
    for name, value, maximum, exclusive in (
        ("failure_window_seconds", passive.failure_window_seconds, 86400, True),
        ("quarantine_seconds", passive.quarantine_seconds, 86400, True),
        ("early_session_seconds", passive.early_session_seconds, 600, False),
    ):
        _validate_number(f"[passive_health].{name}", value, 0, maximum, exclusive_minimum=exclusive)
    if not isinstance(config.reload, ReloadConfig) or not isinstance(config.reload.enabled, bool):
        raise ConfigError("[reload].enabled muss Boolean sein")
    if config.maintenance.force_target is not None:
        _validate_plain_string(
            "[maintenance].force_target",
            config.maintenance.force_target,
            allow_empty=False,
            maximum=64,
        )
    if config.maintenance.mode is models.MaintenanceMode.FORCE_TARGET:
        selected = next(
            (
                entry
                for entry in config.effective_backends
                if entry.id == config.maintenance.force_target and entry.enabled
            ),
            None,
        )
        if selected is None:
            raise ConfigError(
                "[maintenance].force_target muss ein vorhandenes aktiviertes Backend bezeichnen"
            )
    elif config.maintenance.force_target is not None:
        raise ConfigError("[maintenance].force_target erfordert mode='force_target'")
    if not isinstance(config.status, LocalStatusConfig):
        raise ConfigError("status muss LocalStatusConfig sein")
    status = config.status
    for name in ("enabled", "always", "maintenance", "dynamic_hint"):
        if not isinstance(getattr(status, name), bool):
            raise ConfigError(f"[status].{name} muss Boolean sein")
    for name in ("handshake_timeout_seconds", "session_timeout_seconds"):
        _validate_number(f"[status].{name}", getattr(status, name), 0, 60, exclusive_minimum=True)
    _validate_int("[status].max_handshake_bytes", status.max_handshake_bytes, 16, 65536)
    _validate_int("[status].max_concurrent", status.max_concurrent, 1, 100_000)
    _validate_int("[status].protocol_version", status.protocol_version, -1, 2_147_483_647)
    _validate_plain_string(
        "[status].version_name", status.version_name, allow_empty=False, maximum=128
    )
    for name in ("motd_online", "motd_maintenance", "motd_fallback", "motd_offline"):
        _motd({name: getattr(status, name)}, name, "")
    for name in ("players_online", "players_max"):
        value = getattr(status, name)
        if value is not None:
            _validate_int(f"[status].{name}", value, 0, 2_147_483_647)
    if (status.players_online is None) != (status.players_max is None):
        raise ConfigError("[status] players_online und players_max müssen gemeinsam gesetzt werden")
    if (
        status.players_online is not None
        and status.players_max is not None
        and status.players_online > status.players_max
    ):
        raise ConfigError("[status].players_online darf players_max nicht überschreiten")
    if status.favicon is not None:
        from .local_status import validate_favicon

        try:
            if not isinstance(status.favicon, str):
                raise ValueError
            validate_favicon(status.favicon)
        except ValueError as exc:
            raise ConfigError(
                "[status].favicon muss ein gültiges begrenztes 64x64 PNG-Data-URI sein"
            ) from exc


def _validate_circuit(circuit: Any, section: str) -> None:
    if not isinstance(circuit, CircuitBreakerConfig):
        raise ConfigError(f"[{section}] muss CircuitBreakerConfig sein")
    if not isinstance(circuit.enabled, bool):
        raise ConfigError(f"[{section}].enabled muss Boolean sein")
    _validate_int(f"[{section}].failure_threshold", circuit.failure_threshold, 1, 1_000_000)
    _validate_number(
        f"[{section}].failure_window_seconds",
        circuit.failure_window_seconds,
        0,
        86_400,
        exclusive_minimum=True,
    )
    _validate_number(
        f"[{section}].open_seconds", circuit.open_seconds, 0, 86_400, exclusive_minimum=True
    )
    _validate_int(
        f"[{section}].half_open_max_attempts", circuit.half_open_max_attempts, 1, 1_000_000
    )


def validate_config(config: AppConfig) -> None:
    """Validate an :class:`AppConfig`, including cross-section invariants."""

    if not isinstance(config, AppConfig):
        raise _invalid("config", "AppConfig", config)
    if not isinstance(config.strict_unknown_keys, bool):
        raise _invalid("[config].strict_unknown_keys", "Boolean", config.strict_unknown_keys)
    if not isinstance(config.proxy, ProxyConfig):
        raise _invalid("[proxy]", "ProxyConfig", config.proxy)
    _validate_host("[proxy].listen_host", config.proxy.listen_host)
    _validate_port("[proxy].listen_port", config.proxy.listen_port)
    _validate_int("[proxy].backlog", config.proxy.backlog, 1, 65_535)
    for section, target in (("main", config.main), ("fallback", config.fallback)):
        if not isinstance(target, TargetConfig):
            raise _invalid(f"[{section}]", "TargetConfig", target)
        _validate_host(_path(section, "host"), target.host, target=True)
        _validate_port(_path(section, "port"), target.port)
    _validate_healthcheck("healthcheck", config.healthcheck)
    _validate_healthcheck("fallback_healthcheck", config.fallback_healthcheck)

    if not isinstance(config.connection, ConnectionConfig):
        raise _invalid("[connection]", "ConnectionConfig", config.connection)
    connection = config.connection
    for field_name, field_value, maximum, exclusive in (
        ("timeout_seconds", connection.timeout_seconds, 600.0, True),
        ("idle_timeout_seconds", connection.idle_timeout_seconds, 604_800.0, False),
        ("write_timeout_seconds", connection.write_timeout_seconds, 600.0, True),
        ("relay_drain_timeout_seconds", connection.relay_drain_timeout_seconds, 600.0, True),
        ("shutdown_grace_seconds", connection.shutdown_grace_seconds, 3600.0, False),
        (
            "shutdown_cancel_timeout_seconds",
            connection.shutdown_cancel_timeout_seconds,
            600.0,
            True,
        ),
        ("new_connections_per_second", connection.new_connections_per_second, 1_000_000.0, False),
    ):
        _validate_number(
            _path("connection", field_name), field_value, 0.0, maximum, exclusive_minimum=exclusive
        )
    _validate_int("[connection].buffer_size", connection.buffer_size, 1024, 16 * 1024 * 1024)
    _validate_int("[connection].max_connections", connection.max_connections, 1, 1_000_000)
    _validate_int(
        "[connection].max_connections_per_ip", connection.max_connections_per_ip, 0, 1_000_000
    )
    _validate_int(
        "[connection].new_connections_burst", connection.new_connections_burst, 0, 1_000_000
    )
    for field_name, field_value in (
        (
            "connect_fallback_on_main_connect_failure",
            connection.connect_fallback_on_main_connect_failure,
        ),
        ("tcp_keepalive", connection.tcp_keepalive),
    ):
        if not isinstance(field_value, bool):
            raise _invalid(_path("connection", field_name), "Boolean", field_value)
    if connection.max_connections_per_ip > connection.max_connections:
        raise ConfigError(
            "Ungültige Konfiguration [connection]: max_connections_per_ip darf "
            "max_connections nicht überschreiten"
        )
    rate_enabled = connection.new_connections_per_second > 0
    if rate_enabled != (connection.new_connections_burst > 0):
        raise ConfigError(
            "Ungültige Konfiguration [connection]: new_connections_per_second und "
            "new_connections_burst müssen gemeinsam aktiviert oder deaktiviert werden"
        )

    if not isinstance(config.logging, LoggingConfig):
        raise _invalid("[logging]", "LoggingConfig", config.logging)
    if not isinstance(config.logging.level, str) or config.logging.level not in _LOG_LEVELS:
        raise _invalid("[logging].level", "gültiges Log-Level", config.logging.level)
    if not isinstance(config.logging.access_log, bool):
        raise _invalid("[logging].access_log", "Boolean", config.logging.access_log)

    if not isinstance(config.maintenance, MaintenanceConfig):
        raise _invalid("[maintenance]", "MaintenanceConfig", config.maintenance)
    if not isinstance(config.maintenance.mode, models.MaintenanceMode):
        raise _invalid("[maintenance].mode", "MaintenanceMode", config.maintenance.mode)
    for path_name, path_value in (
        ("force_fallback_file", config.maintenance.force_fallback_file),
        ("force_main_file", config.maintenance.force_main_file),
    ):
        if path_value is not None and not isinstance(path_value, Path):
            raise _invalid(_path("maintenance", path_name), "Path oder nicht gesetzt", path_value)
    if (
        config.maintenance.force_fallback_file is not None
        and config.maintenance.force_fallback_file == config.maintenance.force_main_file
    ):
        raise ConfigError(
            "Ungültige Konfiguration [maintenance]: force_fallback_file und "
            "force_main_file müssen unterschiedliche Pfade sein"
        )
    _validate_number(
        "[maintenance].file_check_interval_seconds",
        config.maintenance.file_check_interval_seconds,
        0.0,
        3600.0,
        exclusive_minimum=True,
    )

    if not isinstance(config.proxy_protocol, ProxyProtocolConfig):
        raise _invalid("[proxy_protocol]", "ProxyProtocolConfig", config.proxy_protocol)
    protocol = config.proxy_protocol
    for field_name, field_value in (
        ("accept", protocol.accept),
        ("send", protocol.send),
        ("trust_all_proxies", protocol.trust_all_proxies),
    ):
        if not isinstance(field_value, bool):
            raise _invalid(_path("proxy_protocol", field_name), "Boolean", field_value)
    _validate_int("[proxy_protocol].version", protocol.version, 1, 2)
    for version_name, version_value in (
        ("accept_version", protocol.accept_version),
        ("send_version", protocol.send_version),
    ):
        if version_value is not None:
            _validate_int(_path("proxy_protocol", version_name), version_value, 1, 2)
    if not isinstance(protocol.trusted_proxy_ips, tuple):
        raise _invalid(
            "[proxy_protocol].trusted_proxy_ips",
            "Tuple von IP/CIDR-Strings",
            protocol.trusted_proxy_ips,
        )
    for index, entry in enumerate(protocol.trusted_proxy_ips):
        if not isinstance(entry, str) or not entry or entry != entry.strip():
            raise _invalid(
                f"[proxy_protocol].trusted_proxy_ips[{index}]", "nicht-leerer IP/CIDR-String", entry
            )
        try:
            ipaddress.ip_network(entry, strict=False)
        except ValueError as exc:
            raise _invalid(
                f"[proxy_protocol].trusted_proxy_ips[{index}]",
                "gültige IPv4-/IPv6-Adresse oder CIDR",
                entry,
            ) from exc
    _validate_number(
        "[proxy_protocol].header_timeout_seconds",
        protocol.header_timeout_seconds,
        0.0,
        60.0,
        exclusive_minimum=True,
    )
    _validate_int("[proxy_protocol].max_header_bytes", protocol.max_header_bytes, 15, 65_551)
    effective_accept_version = protocol.accept_version or protocol.version
    if protocol.accept and effective_accept_version == 2 and protocol.max_header_bytes < 16:
        raise ConfigError(
            "Ungültige Konfiguration [proxy_protocol].max_header_bytes: "
            "PROXY Protocol v2 benötigt mindestens 16 Bytes"
        )
    if protocol.trust_all_proxies and protocol.trusted_proxy_ips:
        raise ConfigError(
            "Unsichere/widersprüchliche Konfiguration [proxy_protocol]: "
            "trust_all_proxies=true darf nicht mit trusted_proxy_ips kombiniert werden"
        )
    if protocol.accept and not protocol.trust_all_proxies and not protocol.trusted_proxy_ips:
        raise ConfigError(
            "Unsichere Konfiguration [proxy_protocol]: accept=true erfordert mindestens "
            "einen gültigen Eintrag in trusted_proxy_ips oder das explizite gefährliche "
            "Opt-in trust_all_proxies=true"
        )

    if not isinstance(config.monitoring, MonitoringConfig):
        raise _invalid("[monitoring]", "MonitoringConfig", config.monitoring)
    monitoring = config.monitoring
    _validate_host("[monitoring].listen_host", monitoring.listen_host)
    _validate_port("[monitoring].listen_port", monitoring.listen_port)
    for field_name, field_value in (
        ("enabled", monitoring.enabled),
        ("allow_remote", monitoring.allow_remote),
        ("allow_unauthenticated_remote", monitoring.allow_unauthenticated_remote),
        ("expose_sensitive_state", monitoring.expose_sensitive_state),
    ):
        if not isinstance(field_value, bool):
            raise _invalid(_path("monitoring", field_name), "Boolean", field_value)
    if monitoring.bearer_token is not None:
        token = monitoring.bearer_token
        if (
            not isinstance(token, str)
            or not token
            or token != token.strip()
            or any(not 0x21 <= ord(character) <= 0x7E for character in token)
            or len(token) > _BEARER_TOKEN_MAX_CHARS
        ):
            raise _invalid_secret(
                "[monitoring].bearer_token",
                "nicht-leeres druckbares ASCII-Bearer-Token ohne Leerzeichen",
            )
    _validate_int("[monitoring].max_connections", monitoring.max_connections, 1, 100_000)
    _validate_number(
        "[monitoring].request_timeout_seconds",
        monitoring.request_timeout_seconds,
        0.0,
        60.0,
        exclusive_minimum=True,
    )
    _validate_number(
        "[monitoring].write_timeout_seconds",
        monitoring.write_timeout_seconds,
        0.0,
        60.0,
        exclusive_minimum=True,
    )
    if monitoring.enabled and not _is_local_monitor_host(monitoring.listen_host):
        if not monitoring.allow_remote:
            raise ConfigError(
                "Unsichere Konfiguration [monitoring]: ein nichtlokaler listen_host "
                "erfordert allow_remote=true"
            )
        if monitoring.bearer_token is None and not monitoring.allow_unauthenticated_remote:
            raise ConfigError(
                "Unsichere Konfiguration [monitoring]: Remote-Monitoring erfordert "
                "bearer_token oder das explizite gefährliche Opt-in "
                "allow_unauthenticated_remote=true"
            )

    if not isinstance(config.circuit_breaker, CircuitBreakerConfig):
        raise _invalid("[circuit_breaker]", "CircuitBreakerConfig", config.circuit_breaker)
    circuit = config.circuit_breaker
    if not isinstance(circuit.enabled, bool):
        raise _invalid("[circuit_breaker].enabled", "Boolean", circuit.enabled)
    _validate_int("[circuit_breaker].failure_threshold", circuit.failure_threshold, 1, 1_000_000)
    _validate_number(
        "[circuit_breaker].failure_window_seconds",
        circuit.failure_window_seconds,
        0.0,
        86_400.0,
        exclusive_minimum=True,
    )
    _validate_number(
        "[circuit_breaker].open_seconds",
        circuit.open_seconds,
        0.0,
        86_400.0,
        exclusive_minimum=True,
    )
    _validate_int(
        "[circuit_breaker].half_open_max_attempts", circuit.half_open_max_attempts, 1, 1_000_000
    )

    _validate_extensions(config)

    listener = config.proxy
    for section, health_section, target, healthcheck in configured_targets(config):
        if target.port == listener.listen_port and _target_hits_listener(
            target.host, listener.listen_host
        ):
            raise ConfigError(
                f"Ungültige Konfiguration [{section}]: Ziel erzeugt eine Schleife zum Proxy-Listener"
            )
        health_host = healthcheck.target_host or target.host
        health_port = healthcheck.target_port or target.port
        if health_port == listener.listen_port and _target_hits_listener(
            health_host, listener.listen_host
        ):
            raise ConfigError(
                f"Ungültige Konfiguration [{health_section}]: Ziel erzeugt eine Schleife zum Proxy-Listener"
            )
    if (
        monitoring.enabled
        and monitoring.listen_port == listener.listen_port
        and _listeners_overlap(monitoring.listen_host, listener.listen_host)
    ):
        raise ConfigError("Ungültige Konfiguration: [monitoring]- und [proxy]-Listener überlappen")
    if monitoring.enabled:
        for target_section, health_section, target, healthcheck in configured_targets(config):
            if target.port == monitoring.listen_port and _target_hits_listener(
                target.host, monitoring.listen_host
            ):
                raise ConfigError(
                    f"Ungültige Konfiguration [{target_section}]: Ziel zeigt auf den Monitoring-Listener"
                )
            health_host = healthcheck.target_host or target.host
            health_port = healthcheck.target_port or target.port
            if health_port == monitoring.listen_port and _target_hits_listener(
                health_host, monitoring.listen_host
            ):
                raise ConfigError(
                    f"Ungültige Konfiguration [{health_section}]: Ziel zeigt auf den Monitoring-Listener"
                )
