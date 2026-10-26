from __future__ import annotations

from dataclasses import replace

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.config import ConfigError
from tests.conftest import make_config


@pytest.mark.parametrize("name", ["enabled", "listen_host", "listen_port", "max_connections"])
def test_monitoring_listener_reload_requires_restart(name: str) -> None:
    config = make_config(25561, 25562)
    app = FailoverApplication(config)
    options = {
        "enabled": replace(config.monitoring, enabled=True),
        "listen_host": replace(config.monitoring, listen_host="::1"),
        "listen_port": replace(config.monitoring, listen_port=8081),
        "max_connections": replace(config.monitoring, max_connections=32),
    }
    candidate = replace(config, monitoring=options[name])
    with pytest.raises(ConfigError, match="restart_required: monitoring"):
        app._check_reload_lifecycle(candidate)


@pytest.mark.parametrize(
    "name",
    [
        "max_connections",
        "max_connections_per_ip",
        "new_connections_per_second",
        "new_connections_burst",
    ],
)
def test_admission_limit_reload_requires_restart(name: str) -> None:
    config = make_config(25561, 25562)
    options = {
        "max_connections": replace(config.connection, max_connections=2),
        "max_connections_per_ip": replace(config.connection, max_connections_per_ip=2),
        "new_connections_per_second": replace(config.connection, new_connections_per_second=2),
        "new_connections_burst": replace(config.connection, new_connections_burst=2),
    }
    candidate = replace(config, connection=options[name])
    with pytest.raises(ConfigError, match="restart_required: connection"):
        FailoverApplication(config)._check_reload_lifecycle(candidate)


def test_proxy_protocol_trust_boundary_reload_requires_restart() -> None:
    config = make_config(25561, 25562)
    candidate = replace(config, proxy_protocol=replace(config.proxy_protocol, accept=True))
    with pytest.raises(ConfigError, match="restart_required: PROXY protocol"):
        FailoverApplication(config)._check_reload_lifecycle(candidate)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ConfigError("Ungültiges TOML: secret-value"), "invalid_toml"),
        (ConfigError("Datei nicht gefunden: secret-value"), "config_file_unreadable"),
        (ConfigError("[status].motd_online: secret-value"), "invalid_setting:status.motd_online"),
        (ConfigError("Doppelte Backend-ID secret-value"), "duplicate_backend_id"),
        (ConfigError("force_target secret-value"), "invalid_setting:maintenance.force_target"),
        (ConfigError("other secret-value"), "validation_failed"),
        (RuntimeError("secret-value"), "preparation_failed:RuntimeError"),
    ],
)
def test_reload_rejection_diagnostics_never_echo_input_values(
    error: Exception, expected: str
) -> None:
    reason = FailoverApplication._reload_rejection_reason(error)
    assert reason == expected
    assert "secret-value" not in reason
