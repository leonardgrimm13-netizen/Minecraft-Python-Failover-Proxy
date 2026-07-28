from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

import mc_failover.config as config_module
from mc_failover.config import AppConfig, ConfigError, load_config, validate_config
from tests.unit.test_config import write_config


@pytest.fixture
def valid_config(tmp_path: Path) -> AppConfig:
    return load_config(write_config(tmp_path))


def _assert_invalid(config: Any, expected_path: str) -> None:
    with pytest.raises(ConfigError, match=re.escape(expected_path)):
        validate_config(config)


def _replace_section(config: AppConfig, section: str, **changes: Any) -> AppConfig:
    current = getattr(config, section)
    return replace(config, **{section: replace(current, **changes)})


@pytest.mark.parametrize(
    ("bad_path", "rendered"),
    [
        ("config.toml", "'config.toml'"),
        (False, "false"),
    ],
)
def test_load_config_requires_pathlib_path_and_renders_value_safely(
    bad_path: object, rendered: str
) -> None:
    with pytest.raises(ConfigError, match=re.escape(rendered)):
        load_config(cast(Path, bad_path))


def test_load_config_reports_missing_malformed_and_unreadable_files(tmp_path: Path) -> None:
    missing = tmp_path / "missing.toml"
    with pytest.raises(ConfigError, match="nicht gefunden"):
        load_config(missing)

    malformed = tmp_path / "malformed.toml"
    malformed.write_text("[proxy\nlisten_port = 25565", encoding="utf-8")
    with pytest.raises(ConfigError, match="Ungültiges TOML"):
        load_config(malformed)

    with pytest.raises(ConfigError, match="konnte nicht gelesen"):
        load_config(tmp_path)


def test_required_sections_tables_and_values_report_exact_paths(tmp_path: Path) -> None:
    empty = tmp_path / "empty.toml"
    empty.write_text("", encoding="utf-8")
    with pytest.raises(ConfigError, match=r"Fehlende Konfigurationssektion \[proxy\]"):
        load_config(empty)

    wrong_table = tmp_path / "wrong-table.toml"
    wrong_table.write_text("proxy = 42\n", encoding="utf-8")
    with pytest.raises(ConfigError, match=r"\[proxy\].*TOML-Table"):
        load_config(wrong_table)

    missing_value = write_config(tmp_path, filename="missing-value.toml")
    contents = missing_value.read_text(encoding="utf-8")
    missing_value.write_text(contents.replace("listen_port = 25565\n", ""), encoding="utf-8")
    with pytest.raises(ConfigError, match=r"Fehlender Konfigurationswert \[proxy\]\.listen_port"):
        load_config(missing_value)


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("config", "strict_unknown_keys", 1),
        ("proxy_protocol", "accept", "yes"),
        ("healthcheck", "reject_uninitialized_protocol", 1),
        ("main", "host", 123),
        ("main", "host", ""),
        ("main", "host", " padded.example"),
        ("healthcheck", "target_port", True),
        ("healthcheck", "target_host", 123),
        ("healthcheck", "target_host", "x" * 254),
    ],
)
def test_parser_rejects_wrong_boolean_string_and_optional_types(
    tmp_path: Path, section: str, key: str, value: Any
) -> None:
    with pytest.raises(ConfigError, match=rf"\[{section}\]\.{key}"):
        load_config(write_config(tmp_path, {section: {key: value}}))


def test_optional_blank_strings_normalize_to_none(tmp_path: Path) -> None:
    config = load_config(
        write_config(
            tmp_path,
            {
                "healthcheck": {"target_host": "", "status_hostname": ""},
                "monitoring": {"bearer_token": ""},
            },
        )
    )
    assert config.healthcheck.target_host is None
    assert config.healthcheck.status_hostname is None
    assert config.monitoring.bearer_token is None


def test_extreme_toml_integer_is_rejected_as_non_finite_number(tmp_path: Path) -> None:
    huge_integer = 10**1000
    with pytest.raises(ConfigError, match=r"\[healthcheck\]\.interval_seconds"):
        load_config(write_config(tmp_path, {"healthcheck": {"interval_seconds": huge_integer}}))


def test_scalar_unknown_root_key_reports_scalar_name(tmp_path: Path) -> None:
    path = write_config(tmp_path)
    path.write_text("future_value = 1\n\n" + path.read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(ConfigError, match=r"Unbekannter Konfigurationsschlüssel future_value"):
        load_config(path)


def test_parser_rejects_whitespace_in_proxy_allowlist_and_redacts_non_ascii_token(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError, match=r"trusted_proxy_ips\[0\]"):
        load_config(
            write_config(
                tmp_path,
                {"proxy_protocol": {"trusted_proxy_ips": [" 127.0.0.1"]}},
            )
        )

    secret = "non-ascii-ä"
    with pytest.raises(ConfigError) as captured:
        load_config(
            write_config(
                tmp_path,
                {"monitoring": {"bearer_token": secret}},
                filename="non-ascii-token.toml",
            )
        )
    assert secret not in str(captured.value)
    assert "<redacted>" in str(captured.value)


def test_invalid_maintenance_mode_and_control_character_path_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"\[maintenance\]\.mode"):
        load_config(write_config(tmp_path, {"maintenance": {"mode": "sometimes"}}))

    with pytest.raises(ConfigError, match=r"\[maintenance\]\.force_main_file"):
        load_config(
            write_config(
                tmp_path,
                {"maintenance": {"force_main_file": "unsafe\u007fpath"}},
                filename="control-path.toml",
            )
        )


def test_maintenance_absolute_path_is_preserved(tmp_path: Path) -> None:
    flag = tmp_path / "absolute" / "force-main"
    config = load_config(write_config(tmp_path, {"maintenance": {"force_main_file": str(flag)}}))
    assert config.maintenance.force_main_file == flag.resolve()


def test_maintenance_path_expansion_and_resolution_errors_are_wrapped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expand_path = write_config(
        tmp_path,
        {"maintenance": {"force_main_file": "explode-expand"}},
        filename="expand.toml",
    )
    original_expanduser = Path.expanduser

    def explode_expanduser(path: Path) -> Path:
        if path.name == "explode-expand":
            raise RuntimeError("private expansion detail")
        return original_expanduser(path)

    monkeypatch.setattr(Path, "expanduser", explode_expanduser)
    with pytest.raises(ConfigError, match=r"\[maintenance\]\.force_main_file"):
        load_config(expand_path)

    monkeypatch.setattr(Path, "expanduser", original_expanduser)
    resolve_path = write_config(
        tmp_path,
        {"maintenance": {"force_main_file": "explode-resolve"}},
        filename="resolve.toml",
    )
    original_resolve = Path.resolve

    def explode_resolve(path: Path, strict: bool = False) -> Path:
        if path.name == "explode-resolve":
            raise OSError("private resolution detail")
        return original_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", explode_resolve)
    with pytest.raises(ConfigError, match=r"\[maintenance\]\.force_main_file"):
        load_config(resolve_path)


@pytest.mark.parametrize(
    ("section", "expected_path"),
    [
        ("proxy", "[proxy]"),
        ("main", "[main]"),
        ("healthcheck", "[healthcheck]"),
        ("connection", "[connection]"),
        ("logging", "[logging]"),
        ("maintenance", "[maintenance]"),
        ("proxy_protocol", "[proxy_protocol]"),
        ("monitoring", "[monitoring]"),
        ("circuit_breaker", "[circuit_breaker]"),
    ],
)
def test_validate_config_rejects_wrong_component_types(
    valid_config: AppConfig, section: str, expected_path: str
) -> None:
    _assert_invalid(replace(valid_config, **{section: cast(Any, object())}), expected_path)


def test_validate_config_rejects_wrong_root_and_strict_flag_types(valid_config: AppConfig) -> None:
    _assert_invalid(object(), "config")
    _assert_invalid(
        replace(valid_config, strict_unknown_keys=cast(Any, 1)), "[config].strict_unknown_keys"
    )


@pytest.mark.parametrize(
    ("section", "field", "value", "expected_path"),
    [
        ("proxy", "listen_host", " padded.example", "[proxy].listen_host"),
        ("proxy", "listen_port", 0, "[proxy].listen_port"),
        ("proxy", "backlog", False, "[proxy].backlog"),
        ("main", "host", "bad:hostname", "[main].host"),
        ("main", "host", ".", "[main].host"),
        ("main", "host", "\ud800", "[main].host"),
        ("main", "host", "-bad.example", "[main].host"),
        ("main", "host", "0.0.0.0", "[main].host"),
        ("healthcheck", "enabled", 1, "[healthcheck].enabled"),
        ("healthcheck", "mode", "http", "[healthcheck].mode"),
        ("healthcheck", "interval_seconds", 10**1000, "[healthcheck].interval_seconds"),
        ("healthcheck", "require_valid_json", 1, "[healthcheck].require_valid_json"),
        (
            "healthcheck",
            "reject_uninitialized_protocol",
            1,
            "[healthcheck].reject_uninitialized_protocol",
        ),
        ("healthcheck", "motd_must_contain", " bad", "[healthcheck].motd_must_contain"),
        (
            "connection",
            "connect_fallback_on_main_connect_failure",
            1,
            "[connection].connect_fallback_on_main_connect_failure",
        ),
        ("connection", "tcp_keepalive", 1, "[connection].tcp_keepalive"),
        ("logging", "level", "TRACE", "[logging].level"),
        ("logging", "access_log", 1, "[logging].access_log"),
        ("maintenance", "mode", "auto", "[maintenance].mode"),
        ("maintenance", "force_main_file", "flag", "[maintenance].force_main_file"),
        ("proxy_protocol", "accept", 1, "[proxy_protocol].accept"),
        ("proxy_protocol", "accept_version", 3, "[proxy_protocol].accept_version"),
        ("monitoring", "enabled", 1, "[monitoring].enabled"),
        ("circuit_breaker", "enabled", 1, "[circuit_breaker].enabled"),
    ],
)
def test_validate_config_defensively_rechecks_nested_field_types_and_ranges(
    valid_config: AppConfig,
    section: str,
    field: str,
    value: Any,
    expected_path: str,
) -> None:
    _assert_invalid(_replace_section(valid_config, section, **{field: value}), expected_path)


def test_validate_config_rejects_impossible_cross_field_combinations(
    valid_config: AppConfig,
) -> None:
    too_many_per_ip = _replace_section(
        valid_config,
        "connection",
        max_connections=10,
        max_connections_per_ip=11,
    )
    _assert_invalid(too_many_per_ip, "[connection]")

    filtered_without_json = _replace_section(
        valid_config,
        "healthcheck",
        mode="minecraft_status",
        require_valid_json=False,
        motd_must_contain="online",
    )
    _assert_invalid(filtered_without_json, "[healthcheck]")

    uninitialized_filter_without_json = _replace_section(
        valid_config,
        "healthcheck",
        mode="minecraft_status",
        require_valid_json=False,
        reject_uninitialized_protocol=True,
    )
    _assert_invalid(
        uninitialized_filter_without_json,
        "[healthcheck].reject_uninitialized_protocol",
    )

    filtered_tcp = _replace_section(
        valid_config,
        "healthcheck",
        mode="tcp",
        require_valid_json=True,
        min_players_max=1,
    )
    _assert_invalid(filtered_tcp, "[healthcheck]")

    proxy_v2_header_too_short = _replace_section(
        valid_config,
        "proxy_protocol",
        accept=True,
        accept_version=2,
        trust_all_proxies=True,
        max_header_bytes=15,
    )
    _assert_invalid(proxy_v2_header_too_short, "[proxy_protocol].max_header_bytes")


@pytest.mark.parametrize(
    "trusted_proxy_ips",
    [
        cast(Any, ["127.0.0.1"]),
        (cast(Any, 123),),
        (" 127.0.0.1",),
        ("invalid-network",),
    ],
)
def test_validate_config_rechecks_trusted_proxy_tuple_entries(
    valid_config: AppConfig, trusted_proxy_ips: Any
) -> None:
    broken = _replace_section(
        valid_config,
        "proxy_protocol",
        trusted_proxy_ips=trusted_proxy_ips,
    )
    _assert_invalid(broken, "[proxy_protocol].trusted_proxy_ips")


@pytest.mark.parametrize("token", [cast(Any, 123), "", " secret", "line\nbreak", "non-ascii-ä"])
def test_validate_config_redacts_invalid_bearer_tokens(valid_config: AppConfig, token: Any) -> None:
    broken = _replace_section(valid_config, "monitoring", bearer_token=token)
    with pytest.raises(ConfigError) as captured:
        validate_config(broken)
    assert "<redacted>" in str(captured.value)
    if token:
        assert str(token) not in str(captured.value)


@pytest.mark.parametrize(
    ("target", "listener", "expected"),
    [
        ("127.0.0.1", "127.0.0.1", True),
        ("127.0.0.2", "0.0.0.0", True),
        ("localhost", "0.0.0.0", True),
        ("::1", "0.0.0.0", False),
        ("localhost", "example.test", False),
    ],
)
def test_target_listener_collision_handles_names_wildcards_and_ip_families(
    target: str, listener: str, expected: bool
) -> None:
    assert config_module._target_hits_listener(target, listener) is expected


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ("127.0.0.1", "127.0.0.1", True),
        ("localhost", "127.0.0.2", True),
        ("localhost", "example.test", False),
        ("proxy.example", "monitor.example", False),
        ("0.0.0.0", "127.0.0.2", True),
        ("0.0.0.0", "::1", False),
    ],
)
def test_listener_overlap_handles_names_wildcards_and_ip_families(
    first: str, second: str, expected: bool
) -> None:
    assert config_module._listeners_overlap(first, second) is expected


@pytest.mark.parametrize(
    ("host", "expected"),
    [("localhost", True), ("127.0.0.2", True), ("monitor.example", False)],
)
def test_local_monitor_host_classification(host: str, expected: bool) -> None:
    assert config_module._is_local_monitor_host(host) is expected


def test_healthcheck_cannot_point_at_monitoring_listener(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"\[fallback_healthcheck\].*Monitoring-Listener"):
        load_config(
            write_config(
                tmp_path,
                {
                    "monitoring": {"enabled": True, "listen_port": 8080},
                    "fallback_healthcheck": {
                        "target_host": "127.0.0.1",
                        "target_port": 8080,
                    },
                },
            )
        )


@pytest.mark.parametrize(
    "legacy_literal",
    ["127.1", "2130706433", "0x7f000001", "0177.0.0.1", "127.0.0.1."],
)
def test_legacy_ipv4_literals_cannot_bypass_loop_validation(
    tmp_path: Path,
    legacy_literal: str,
) -> None:
    with pytest.raises(ConfigError, match=r"\[main\]\.host.*Legacy-IPv4"):
        load_config(
            write_config(
                tmp_path,
                {
                    "main": {"host": legacy_literal, "port": 25565},
                },
            )
        )


def test_validate_config_rejects_noncanonical_programmatic_log_level(
    valid_config: AppConfig,
) -> None:
    broken = _replace_section(valid_config, "logging", level="info")
    _assert_invalid(broken, "[logging].level")
