from __future__ import annotations

import copy
import json
import math
from pathlib import Path
from typing import Any

import pytest

from mc_failover.config import ConfigError, load_config
from mc_failover.models import MaintenanceMode

BASE_SECTIONS: dict[str, dict[str, Any]] = {
    "proxy": {"listen_host": "127.0.0.1", "listen_port": 25_565},
    "main": {"host": "127.0.0.1", "port": 25_564},
    "fallback": {"host": "127.0.0.1", "port": 25_566},
    "healthcheck": {
        "mode": "tcp",
        "interval_seconds": 3.0,
        "timeout_seconds": 2.0,
        "fail_after": 2,
        "recover_after": 3,
        "min_recovery_seconds": 4.0,
    },
    "connection": {"timeout_seconds": 5.0, "buffer_size": 65_536},
    "logging": {"level": "INFO"},
}


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
    return str(value)


def write_config(
    tmp_path: Path,
    overrides: dict[str, dict[str, Any]] | None = None,
    *,
    filename: str = "config.toml",
) -> Path:
    sections = copy.deepcopy(BASE_SECTIONS)
    for section, values in (overrides or {}).items():
        sections.setdefault(section, {}).update(values)
    lines: list[str] = []
    for section, values in sections.items():
        lines.append(f"[{section}]")
        lines.extend(f"{key} = {_toml_value(value)}" for key, value in values.items())
        lines.append("")
    path = tmp_path / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def test_old_configuration_gets_secure_complete_defaults(tmp_path: Path) -> None:
    config = load_config(write_config(tmp_path))

    assert config.strict_unknown_keys is True
    assert config.proxy.backlog == 256
    assert config.healthcheck.enabled is True
    assert config.healthcheck.reject_uninitialized_protocol is True
    assert config.fallback_healthcheck.enabled is True
    assert config.fallback_healthcheck.reject_uninitialized_protocol is True
    assert config.fallback_healthcheck.mode == "tcp"
    assert config.fallback_healthcheck.interval_seconds == config.healthcheck.interval_seconds
    assert config.fallback_healthcheck.timeout_seconds == config.healthcheck.timeout_seconds
    assert config.fallback_healthcheck.fail_after == config.healthcheck.fail_after
    assert config.fallback_healthcheck.recover_after == config.healthcheck.recover_after
    assert (
        config.fallback_healthcheck.min_recovery_seconds == config.healthcheck.min_recovery_seconds
    )
    assert config.connection.idle_timeout_seconds == 300.0
    assert config.connection.write_timeout_seconds == 30.0
    assert config.connection.relay_drain_timeout_seconds == 10.0
    assert config.connection.shutdown_grace_seconds == 30.0
    assert config.connection.shutdown_cancel_timeout_seconds == 5.0
    assert config.connection.max_connections == 4096
    assert config.connection.max_connections_per_ip == 0
    assert config.connection.new_connections_per_second == 0
    assert config.connection.new_connections_burst == 0
    assert config.logging.access_log is False
    assert config.maintenance.mode is MaintenanceMode.AUTO
    assert config.maintenance.file_check_interval_seconds == 1.0
    assert config.proxy_protocol.accept is False
    assert config.proxy_protocol.trust_all_proxies is False
    assert config.monitoring.enabled is False
    assert config.monitoring.expose_sensitive_state is False
    assert config.circuit_breaker.enabled is True


@pytest.mark.parametrize(
    ("overrides", "expected_path"),
    [
        ({"connection": {"max_conections": 12}}, "[connection].max_conections"),
        ({"config": {"strict_unknown_key": True}}, "[config].strict_unknown_key"),
        ({"conection": {"timeout_seconds": 1.0}}, "[conection]"),
    ],
)
def test_strict_unknown_keys_report_exact_toml_path(
    tmp_path: Path, overrides: dict[str, dict[str, Any]], expected_path: str
) -> None:
    with pytest.raises(ConfigError, match=expected_path.replace("[", r"\[").replace("]", r"\]")):
        load_config(write_config(tmp_path, overrides))


def test_unknown_keys_can_be_explicitly_permitted(tmp_path: Path) -> None:
    config = load_config(
        write_config(
            tmp_path,
            {
                "config": {"strict_unknown_keys": False},
                "connection": {"future_option": 123},
                "future_section": {"value": True},
            },
        )
    )
    assert config.strict_unknown_keys is False


@pytest.mark.parametrize(
    ("section", "key"),
    [
        ("proxy", "listen_port"),
        ("proxy", "backlog"),
        ("healthcheck", "fail_after"),
        ("healthcheck", "protocol_version"),
        ("connection", "buffer_size"),
        ("connection", "max_connections"),
        ("monitoring", "listen_port"),
        ("circuit_breaker", "failure_threshold"),
    ],
)
def test_boolean_is_never_accepted_as_integer(tmp_path: Path, section: str, key: str) -> None:
    with pytest.raises(ConfigError, match=rf"\[{section}\]\.{key}"):
        load_config(write_config(tmp_path, {section: {key: True}}))


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("proxy", "listen_port", 0),
        ("proxy", "backlog", 65_536),
        ("healthcheck", "interval_seconds", 0.0),
        ("healthcheck", "timeout_seconds", math.inf),
        ("healthcheck", "jitter_seconds", math.nan),
        ("connection", "buffer_size", 1023),
        ("connection", "write_timeout_seconds", -1.0),
        ("connection", "shutdown_grace_seconds", 3601.0),
        ("proxy_protocol", "header_timeout_seconds", 0.0),
        ("monitoring", "request_timeout_seconds", math.nan),
        ("circuit_breaker", "open_seconds", math.inf),
    ],
)
def test_numbers_are_finite_and_bounded(tmp_path: Path, section: str, key: str, value: Any) -> None:
    with pytest.raises(ConfigError, match=rf"\[{section}\]\.{key}"):
        load_config(write_config(tmp_path, {section: {key: value}}))


def test_explicit_fallback_healthcheck_overrides_inherited_defaults(tmp_path: Path) -> None:
    config = load_config(
        write_config(
            tmp_path,
            {
                "fallback_healthcheck": {
                    "enabled": False,
                    "interval_seconds": 9.0,
                    "fail_after": 7,
                }
            },
        )
    )
    assert config.fallback_healthcheck.enabled is False
    assert config.fallback_healthcheck.mode == "tcp"
    assert config.fallback_healthcheck.interval_seconds == 9.0
    assert config.fallback_healthcheck.timeout_seconds == 2.0
    assert config.fallback_healthcheck.fail_after == 7
    assert config.fallback_healthcheck.recover_after == 3


def test_uninitialized_protocol_filter_is_independently_configurable_per_target(
    tmp_path: Path,
) -> None:
    config = load_config(
        write_config(
            tmp_path,
            {
                "healthcheck": {"reject_uninitialized_protocol": False},
                "fallback_healthcheck": {
                    "mode": "minecraft_status",
                    "reject_uninitialized_protocol": False,
                },
            },
        )
    )

    assert config.healthcheck.reject_uninitialized_protocol is False
    assert config.fallback_healthcheck.reject_uninitialized_protocol is False

    secure_fallback = load_config(
        write_config(
            tmp_path,
            {"healthcheck": {"reject_uninitialized_protocol": False}},
            filename="secure-fallback.toml",
        )
    )
    assert secure_fallback.healthcheck.reject_uninitialized_protocol is False
    assert secure_fallback.fallback_healthcheck.reject_uninitialized_protocol is True


def test_legacy_status_config_without_json_keeps_new_filter_disabled(
    tmp_path: Path,
) -> None:
    config = load_config(
        write_config(
            tmp_path,
            {
                "healthcheck": {
                    "mode": "minecraft_status",
                    "require_valid_json": False,
                }
            },
        )
    )

    assert config.healthcheck.require_valid_json is False
    assert config.healthcheck.reject_uninitialized_protocol is False

    with pytest.raises(
        ConfigError,
        match=r"\[healthcheck\]\.reject_uninitialized_protocol.*"
        r"\[healthcheck\]\.require_valid_json",
    ):
        load_config(
            write_config(
                tmp_path,
                {
                    "healthcheck": {
                        "mode": "minecraft_status",
                        "require_valid_json": False,
                        "reject_uninitialized_protocol": True,
                    }
                },
                filename="explicit-invalid-filter.toml",
            )
        )


def test_proxy_protocol_accept_fails_closed_without_trust(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"\[proxy_protocol\].*trusted_proxy_ips"):
        load_config(write_config(tmp_path, {"proxy_protocol": {"accept": True}}))


def test_proxy_protocol_explicit_trust_all_is_allowed_but_not_with_a_list(
    tmp_path: Path,
) -> None:
    trusted_all = load_config(
        write_config(
            tmp_path,
            {"proxy_protocol": {"accept": True, "trust_all_proxies": True}},
        )
    )
    assert trusted_all.proxy_protocol.trust_all_proxies is True

    with pytest.raises(ConfigError, match="widersprüchliche"):
        load_config(
            write_config(
                tmp_path,
                {
                    "proxy_protocol": {
                        "accept": True,
                        "trust_all_proxies": True,
                        "trusted_proxy_ips": ["127.0.0.1"],
                    }
                },
                filename="contradiction.toml",
            )
        )


def test_proxy_protocol_accepts_ipv4_ipv6_and_cidr_entries(tmp_path: Path) -> None:
    entries = ["127.0.0.1", "10.0.0.0/8", "::1", "2001:db8::/32"]
    config = load_config(
        write_config(
            tmp_path,
            {"proxy_protocol": {"accept": True, "trusted_proxy_ips": entries}},
        )
    )
    assert config.proxy_protocol.trusted_proxy_ips == tuple(entries)


@pytest.mark.parametrize(
    "trusted_proxy_ips",
    ["127.0.0.1", [123], [""], ["not-a-network"], ["10.0.0.0/999"]],
)
def test_proxy_protocol_rejects_wrong_list_types_and_invalid_entries(
    tmp_path: Path, trusted_proxy_ips: Any
) -> None:
    with pytest.raises(ConfigError, match=r"\[proxy_protocol\]\.trusted_proxy_ips"):
        load_config(
            write_config(
                tmp_path,
                {
                    "proxy_protocol": {
                        "accept": True,
                        "trusted_proxy_ips": trusted_proxy_ips,
                    }
                },
            )
        )


@pytest.mark.parametrize(
    ("monitoring", "valid"),
    [
        ({"enabled": True, "listen_host": "127.0.0.2"}, True),
        ({"enabled": True, "listen_host": "::1"}, True),
        ({"enabled": True, "listen_host": "0.0.0.0"}, False),
        (
            {"enabled": True, "listen_host": "0.0.0.0", "allow_remote": True},
            False,
        ),
        (
            {
                "enabled": True,
                "listen_host": "0.0.0.0",
                "allow_remote": True,
                "bearer_token": "fixed-test-token",
            },
            True,
        ),
        (
            {
                "enabled": True,
                "listen_host": "::",
                "allow_remote": True,
                "allow_unauthenticated_remote": True,
            },
            True,
        ),
    ],
)
def test_remote_monitoring_requires_permission_and_authentication(
    tmp_path: Path, monitoring: dict[str, Any], valid: bool
) -> None:
    path = write_config(tmp_path, {"monitoring": monitoring})
    if valid:
        assert load_config(path).monitoring.enabled is True
    else:
        with pytest.raises(ConfigError, match=r"\[monitoring\]"):
            load_config(path)


def test_remote_monitoring_rejects_whitespace_in_bearer_token(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"\[monitoring\]\.bearer_token"):
        load_config(
            write_config(
                tmp_path,
                {
                    "monitoring": {
                        "enabled": True,
                        "listen_host": "0.0.0.0",
                        "allow_remote": True,
                        "bearer_token": "bad token",
                    }
                },
            )
        )


def test_invalid_bearer_token_is_redacted_from_config_error(tmp_path: Path) -> None:
    secret = "must-not-appear in-errors"
    with pytest.raises(ConfigError) as captured:
        load_config(
            write_config(
                tmp_path,
                {
                    "monitoring": {
                        "enabled": True,
                        "listen_host": "127.0.0.1",
                        "bearer_token": secret,
                    }
                },
            )
        )
    assert secret not in str(captured.value)
    assert "<redacted>" in str(captured.value)


def test_relative_maintenance_paths_are_resolved_from_config_directory(tmp_path: Path) -> None:
    directory = tmp_path / "nested" / "configuration"
    config = load_config(
        write_config(
            directory,
            {
                "maintenance": {
                    "force_fallback_file": "state/fallback",
                    "force_main_file": "../main",
                    "file_check_interval_seconds": 0.25,
                }
            },
        )
    )
    assert config.maintenance.force_fallback_file == (directory / "state/fallback").resolve()
    assert config.maintenance.force_main_file == (directory / "../main").resolve()
    assert config.maintenance.file_check_interval_seconds == 0.25


def test_maintenance_override_files_must_be_distinct(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unterschiedliche Pfade"):
        load_config(
            write_config(
                tmp_path,
                {
                    "maintenance": {
                        "force_fallback_file": "same",
                        "force_main_file": "same",
                    }
                },
            )
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"main": {"host": "127.0.0.1", "port": 25_565}},
        {"fallback": {"host": "localhost", "port": 25_565}},
        {"healthcheck": {"target_host": "127.0.0.1", "target_port": 25_565}},
        {
            "fallback_healthcheck": {
                "target_host": "localhost",
                "target_port": 25_565,
            }
        },
    ],
)
def test_targets_and_healthchecks_cannot_loop_to_proxy_listener(
    tmp_path: Path, overrides: dict[str, dict[str, Any]]
) -> None:
    with pytest.raises(ConfigError, match="Schleife zum Proxy-Listener"):
        load_config(write_config(tmp_path, overrides))


def test_proxy_and_monitoring_listeners_cannot_overlap(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="Listener überlappen"):
        load_config(
            write_config(
                tmp_path,
                {
                    "proxy": {"listen_host": "0.0.0.0"},
                    "monitoring": {
                        "enabled": True,
                        "listen_host": "127.0.0.1",
                        "listen_port": 25_565,
                    },
                },
            )
        )


def test_targets_cannot_point_at_monitoring_listener(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="Monitoring-Listener"):
        load_config(
            write_config(
                tmp_path,
                {
                    "main": {"host": "127.0.0.1", "port": 8080},
                    "monitoring": {"enabled": True, "listen_port": 8080},
                },
            )
        )


def test_ipv6_hosts_status_hostname_and_networks_are_validated(tmp_path: Path) -> None:
    config = load_config(
        write_config(
            tmp_path,
            {
                "proxy": {"listen_host": "::1"},
                "main": {"host": "2001:db8::10"},
                "fallback": {"host": "2001:db8::20"},
                "healthcheck": {
                    "mode": "minecraft_status",
                    "status_hostname": "minecraft.example.test",
                },
                "proxy_protocol": {
                    "accept": True,
                    "trusted_proxy_ips": ["::1/128", "2001:db8::/32"],
                },
            },
        )
    )
    assert config.proxy.listen_host == "::1"
    assert config.healthcheck.status_hostname == "minecraft.example.test"


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("main", "host", "bad host"),
        ("fallback", "host", "0.0.0.0"),
        ("healthcheck", "status_hostname", "bad\nhost"),
        ("healthcheck", "status_hostname", "x" * 256),
    ],
)
def test_invalid_hosts_and_status_hostnames_are_rejected(
    tmp_path: Path, section: str, key: str, value: str
) -> None:
    with pytest.raises(ConfigError, match=rf"\[{section}\]\.{key}"):
        load_config(write_config(tmp_path, {section: {key: value}}))


def test_rate_limit_rate_and_burst_must_be_enabled_together(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="gemeinsam aktiviert"):
        load_config(
            write_config(
                tmp_path,
                {"connection": {"new_connections_per_second": 5.0}},
            )
        )

    config = load_config(
        write_config(
            tmp_path,
            {
                "connection": {
                    "new_connections_per_second": 5.0,
                    "new_connections_burst": 10,
                }
            },
            filename="rate-enabled.toml",
        )
    )
    assert config.connection.new_connections_per_second == 5.0
    assert config.connection.new_connections_burst == 10
