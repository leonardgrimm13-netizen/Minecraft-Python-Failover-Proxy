from __future__ import annotations

import argparse
import asyncio
import json
import logging
import runpy
import signal
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from mc_failover import __version__, cli
from mc_failover.config import AppConfig, ConfigError
from mc_failover.models import HealthCheckResult, MaintenanceMode
from tests.conftest import make_config


def _arguments(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "config": Path("test.toml"),
        "check_config": False,
        "print_effective_config": False,
        "probe_live": False,
        "test_main": False,
        "test_fallback": False,
        "test_healthcheck": False,
        "test_fallback_healthcheck": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_parser_defaults_flags_and_version(capsys: pytest.CaptureFixture[str]) -> None:
    defaults = cli.parse_args([])
    assert defaults.config == Path("config.toml")
    assert not defaults.check_config
    assert not defaults.probe_live

    probe = cli.parse_args(["--config", "probe.toml", "--probe-live"])
    assert probe.config == Path("probe.toml")
    assert probe.probe_live
    assert cli._liveness_probe_host("0.0.0.0") == "127.0.0.1"
    assert cli._liveness_probe_host("::") == "::1"
    assert cli._liveness_probe_host("127.0.0.2") == "127.0.0.2"

    selected = cli.parse_args(
        [
            "--config",
            "custom.toml",
            "--check-config",
            "--print-effective-config",
            "--test-main",
            "--test-fallback",
            "--test-healthcheck",
            "--test-fallback-healthcheck",
        ]
    )
    assert selected.config == Path("custom.toml")
    assert all(
        (
            selected.check_config,
            selected.print_effective_config,
            selected.test_main,
            selected.test_fallback,
            selected.test_healthcheck,
            selected.test_fallback_healthcheck,
        )
    )

    with pytest.raises(SystemExit) as raised:
        cli.parse_args(["--version"])
    assert raised.value.code == 0
    assert capsys.readouterr().out.strip() == f"mc-failover {__version__}"


def test_setup_logging_uses_configured_level(monkeypatch: pytest.MonkeyPatch) -> None:
    config = make_config(25565, 25566)
    basic_config = MagicMock()
    monkeypatch.setattr(logging, "basicConfig", basic_config)

    cli.setup_logging(replace(config, logging=replace(config.logging, level="ERROR")))

    basic_config.assert_called_once_with(
        level=logging.ERROR,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )


def test_config_to_dict_serializes_values_and_redacts_token() -> None:
    config = make_config(25565, 25566)
    config = replace(
        config,
        monitoring=replace(config.monitoring, bearer_token="do-not-print"),
        maintenance=replace(
            config.maintenance,
            mode=MaintenanceMode.FORCE_MAIN,
            force_main_file=Path("relative.flag"),
        ),
    )

    converted = cli.config_to_dict(config)

    assert converted["monitoring"]["bearer_token"] == "<redacted>"
    assert converted["maintenance"]["mode"] == "force_main"
    assert converted["maintenance"]["force_main_file"] == "relative.flag"
    assert converted["proxy_protocol"]["trusted_proxy_ips"] == []
    assert "do-not-print" not in json.dumps(converted)


def test_config_to_dict_rejects_non_mapping() -> None:
    with pytest.raises(TypeError, match="did not produce a mapping"):
        cli.config_to_dict(cast(AppConfig, 42))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("label", "configured", "result", "expected_stream", "expected_latency"),
    [
        ("MAIN", True, HealthCheckResult(True, "healthy", 1.25), "stdout", "1.25"),
        ("FALLBACK", True, HealthCheckResult(False, "down"), "stderr", "n/a"),
        ("MAIN", False, HealthCheckResult(True, "tcp_connect_ok"), "stdout", "n/a"),
        ("FALLBACK", False, HealthCheckResult(True, "tcp_connect_ok"), "stdout", "n/a"),
    ],
)
async def test_run_check_selects_configuration_and_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    label: str,
    configured: bool,
    result: HealthCheckResult,
    expected_stream: str,
    expected_latency: str,
) -> None:
    config = make_config(25565, 25566)
    perform = AsyncMock(return_value=result)
    monkeypatch.setattr(cli, "perform_health_check", perform)
    target = config.main if label == "MAIN" else config.fallback

    assert await cli._run_check(label, target, config, configured=configured) is result.ok

    assert perform.await_args is not None
    called_target, called_check = perform.await_args.args
    assert called_target is target
    if configured:
        expected = config.healthcheck if label == "MAIN" else config.fallback_healthcheck
        assert called_check is expected
    else:
        assert called_check.enabled
        assert called_check.mode == "tcp"
        assert called_check.target_host is None
        assert called_check.target_port is None
        assert not called_check.require_valid_json
        assert called_check.expected_version_contains == ""
        assert called_check.motd_must_contain == ""
        assert called_check.motd_must_not_contain == ""
        assert called_check.min_players_max == 0
    captured = capsys.readouterr()
    output = captured.out if expected_stream == "stdout" else captured.err
    assert ("OK" if result.ok else "ERROR") in output
    assert f"reason={result.reason}" in output
    assert f"latency_ms={expected_latency}" in output


@pytest.mark.asyncio
async def test_run_cli_checks_returns_none_when_no_action_requested() -> None:
    assert await cli.run_cli_checks(_arguments(), make_config(25565, 25566)) is None


@pytest.mark.asyncio
async def test_run_cli_checks_runs_all_actions_and_accumulates_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = replace(
        make_config(25565, 25566),
        monitoring=replace(make_config(25565, 25566).monitoring, bearer_token="secret"),
    )
    outcomes = iter((True, False, True, True))
    run_check = AsyncMock(side_effect=lambda *_args, **_kwargs: next(outcomes))
    monkeypatch.setattr(cli, "_run_check", run_check)
    args = _arguments(
        check_config=True,
        print_effective_config=True,
        test_main=True,
        test_fallback=True,
        test_healthcheck=True,
        test_fallback_healthcheck=True,
    )

    assert await cli.run_cli_checks(args, config) == 1

    assert [call.args[0] for call in run_check.await_args_list] == [
        "MAIN",
        "FALLBACK",
        "MAIN",
        "FALLBACK",
    ]
    assert [call.kwargs["configured"] for call in run_check.await_args_list] == [
        False,
        False,
        True,
        True,
    ]
    output = capsys.readouterr().out
    assert "OK: configuration is valid: test.toml" in output
    assert '"bearer_token": "<redacted>"' in output
    assert "secret" not in output


@pytest.mark.asyncio
async def test_run_cli_checks_success_exit() -> None:
    assert (
        await cli.run_cli_checks(
            _arguments(check_config=True),
            make_config(25565, 25566),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_async_main_reports_config_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "load_config", MagicMock(side_effect=ConfigError("unsafe value")))

    assert await cli.async_main(["--config", "broken.toml"]) == 2
    assert capsys.readouterr().err == "Configuration error: unsafe value\n"


@pytest.mark.asyncio
@pytest.mark.parametrize(("probe_ok", "expected"), [(True, 0), (False, 1)])
async def test_async_main_liveness_probe_skips_backend_endpoint_validation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    probe_ok: bool,
    expected: int,
) -> None:
    base = make_config(25565, 25566)
    config = replace(base, monitoring=replace(base.monitoring, enabled=True))
    endpoint_guard = MagicMock()
    probe = AsyncMock(return_value=probe_ok)
    monkeypatch.setattr(cli, "load_config", MagicMock(return_value=config))
    monkeypatch.setattr(cli, "EndpointLoopGuard", endpoint_guard)
    monkeypatch.setattr(cli, "probe_liveness", probe)

    assert await cli.async_main(["--config", "probe.toml", "--probe-live"]) == expected
    probe.assert_awaited_once_with(config)
    endpoint_guard.assert_not_called()
    captured = capsys.readouterr()
    if probe_ok:
        assert captured.out == "OK: monitoring /live responded\n"
        assert captured.err == ""
    else:
        assert captured.out == ""
        assert captured.err == "ERROR: monitoring /live did not respond successfully\n"


@pytest.mark.asyncio
async def test_async_main_liveness_probe_requires_monitoring_and_is_exclusive(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = make_config(25565, 25566)
    monkeypatch.setattr(cli, "load_config", MagicMock(return_value=config))
    probe = AsyncMock()
    monkeypatch.setattr(cli, "probe_liveness", probe)

    assert await cli.async_main(["--probe-live"]) == 1
    probe.assert_not_awaited()
    assert capsys.readouterr().err == "ERROR: liveness probe requires [monitoring].enabled = true\n"

    assert await cli.async_main(["--probe-live", "--check-config"]) == 2
    assert (
        capsys.readouterr().err == "--probe-live cannot be combined with other diagnostic actions\n"
    )


@pytest.mark.asyncio
async def test_async_main_returns_requested_check_result(monkeypatch: pytest.MonkeyPatch) -> None:
    config = make_config(25565, 25566)
    monkeypatch.setattr(cli, "load_config", MagicMock(return_value=config))
    checks = AsyncMock(return_value=1)
    monkeypatch.setattr(cli, "run_cli_checks", checks)
    application = MagicMock()
    monkeypatch.setattr(cli, "FailoverApplication", application)

    assert await cli.async_main(["--check-config"]) == 1
    application.assert_not_called()


class _FakeSignalLoop:
    def __init__(self, *, failure: type[Exception] | None = None) -> None:
        self.failure = failure
        self.added: list[signal.Signals] = []
        self.removed: list[signal.Signals] = []

    def add_signal_handler(self, caught: signal.Signals, _callback: object) -> None:
        if self.failure is not None:
            raise self.failure
        self.added.append(caught)

    def remove_signal_handler(self, caught: signal.Signals) -> bool:
        self.removed.append(caught)
        return True


def _patch_async_main_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    config: AppConfig,
    application: object,
    loop: _FakeSignalLoop,
    logger: MagicMock,
) -> None:
    monkeypatch.setattr(cli, "load_config", MagicMock(return_value=config))
    monkeypatch.setattr(cli, "run_cli_checks", AsyncMock(return_value=None))
    monkeypatch.setattr(cli, "setup_logging", MagicMock())
    monkeypatch.setattr(logging, "getLogger", MagicMock(return_value=logger))
    monkeypatch.setattr(cli, "FailoverApplication", MagicMock(return_value=application))
    monkeypatch.setattr(asyncio, "get_running_loop", MagicMock(return_value=loop))


@pytest.mark.asyncio
async def test_async_main_runs_application_registers_signals_and_warns_for_dangerous_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25565, 25566)
    config = replace(
        base,
        proxy_protocol=replace(
            base.proxy_protocol,
            accept=True,
            trust_all_proxies=True,
        ),
        monitoring=replace(
            base.monitoring,
            enabled=True,
            allow_remote=True,
            allow_unauthenticated_remote=True,
        ),
    )
    application = SimpleNamespace(
        request_shutdown=MagicMock(),
        run_until_stopped=AsyncMock(),
        shutdown=AsyncMock(),
    )
    loop = _FakeSignalLoop()
    logger = MagicMock()
    _patch_async_main_dependencies(monkeypatch, config, application, loop, logger)

    assert await cli.async_main([]) == 0
    application.run_until_stopped.assert_awaited_once_with()
    application.shutdown.assert_not_awaited()
    assert loop.added == [signal.SIGINT, signal.SIGTERM]
    assert loop.removed == loop.added
    assert logger.critical.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("exception_type", [NotImplementedError, RuntimeError])
async def test_async_main_tolerates_unavailable_signal_handlers(
    monkeypatch: pytest.MonkeyPatch, exception_type: type[Exception]
) -> None:
    config = make_config(25565, 25566)
    application = SimpleNamespace(
        request_shutdown=MagicMock(),
        run_until_stopped=AsyncMock(),
        shutdown=AsyncMock(),
    )
    loop = _FakeSignalLoop(failure=exception_type)
    logger = MagicMock()
    _patch_async_main_dependencies(monkeypatch, config, application, loop, logger)

    assert await cli.async_main([]) == 0
    assert logger.warning.call_count == 2
    assert loop.removed == []


@pytest.mark.asyncio
async def test_async_main_handles_listener_error_and_removes_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(25565, 25566)
    application = SimpleNamespace(
        request_shutdown=MagicMock(),
        run_until_stopped=AsyncMock(side_effect=OSError("bind failed")),
        shutdown=AsyncMock(),
    )
    loop = _FakeSignalLoop()
    logger = MagicMock()
    _patch_async_main_dependencies(monkeypatch, config, application, loop, logger)

    assert await cli.async_main([]) == 1
    application.shutdown.assert_awaited_once_with()
    logger.error.assert_called_once_with("Listener startup failed error=%s", "OSError")
    assert loop.removed == [signal.SIGINT, signal.SIGTERM]


@pytest.mark.asyncio
async def test_async_main_rejects_dns_resolved_listener_loop_before_checks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = make_config(25_565, 25_566, proxy_port=25_565)
    checks = AsyncMock()
    monkeypatch.setattr(cli, "load_config", MagicMock(return_value=config))
    monkeypatch.setattr(cli, "run_cli_checks", checks)

    assert await cli.async_main([]) == 2
    checks.assert_not_awaited()
    assert "local proxy listener" in capsys.readouterr().err


def test_main_raises_exit_with_async_result(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_async_main(_argv: object = None) -> int:
        return 7

    monkeypatch.setattr(cli, "async_main", fake_async_main)
    with pytest.raises(SystemExit) as raised:
        cli.main()
    assert raised.value.code == 7


def test_package_module_entrypoint_calls_cli_main(monkeypatch: pytest.MonkeyPatch) -> None:
    called = MagicMock()
    monkeypatch.setattr(cli, "main", called)

    runpy.run_module("mc_failover.__main__", run_name="__main__")

    called.assert_called_once_with()
