from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.config import ConfigError, RoutingConfig, load_config, validate_config
from mc_failover.models import MaintenanceMode, Target
from tests.conftest import make_config
from tests.unit.test_backend_registry import new_toml
from tests.unit.test_config import write_config
from tests.unit.test_proxy_edges import FakeWriter, stream_writer


def test_omitted_legacy_budget_preserves_full_configured_attempts(tmp_path: Path) -> None:
    config = load_config(
        write_config(
            tmp_path,
            {
                "connection": {
                    "timeout_seconds": 12,
                    "connect_fallback_on_main_connect_failure": True,
                }
            },
        )
    )
    assert config.routing.connect_budget_seconds is None
    assert config.effective_connect_budget_seconds == 24
    validate_config(config)


def test_omitted_multi_backend_budget_keeps_bounded_default(tmp_path: Path) -> None:
    path = tmp_path / "multi.toml"
    path.write_text(new_toml(3), encoding="utf-8")
    config = load_config(path)
    assert config.routing.connect_budget_seconds is None
    assert config.effective_connect_budget_seconds == 10
    validate_config(config)


@pytest.mark.parametrize("explicit_budget", [0.01, 8, 600])
def test_explicit_budget_overrides_legacy_derived_budget(
    tmp_path: Path, explicit_budget: float
) -> None:
    config = load_config(
        write_config(
            tmp_path,
            {
                "connection": {"timeout_seconds": 60},
                "routing": {"connect_budget_seconds": explicit_budget},
            },
        )
    )
    assert config.routing.connect_budget_seconds == explicit_budget
    assert config.effective_connect_budget_seconds == explicit_budget


@pytest.mark.parametrize("invalid", [0, -1, 600.01, True, float("nan"), float("inf")])
def test_explicit_budget_still_requires_finite_positive_bounded_number(
    tmp_path: Path, invalid: object
) -> None:
    with pytest.raises(ConfigError, match=r"\[routing\]\.connect_budget_seconds"):
        load_config(write_config(tmp_path, {"routing": {"connect_budget_seconds": invalid}}))


@pytest.mark.parametrize("send_proxy", [False, True])
@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.parametrize("forced", [False, True])
def test_legacy_derived_budget_counts_connects_and_optional_proxy_writes(
    send_proxy: bool, fallback: bool, forced: bool
) -> None:
    config = make_config(25561, 25562)
    config = replace(
        config,
        connection=replace(
            config.connection,
            timeout_seconds=12,
            write_timeout_seconds=20,
            connect_fallback_on_main_connect_failure=fallback,
        ),
        proxy_protocol=replace(config.proxy_protocol, send=send_proxy),
        maintenance=replace(
            config.maintenance,
            mode=MaintenanceMode.FORCE_MAIN if forced else MaintenanceMode.AUTO,
        ),
    )
    attempts = 2 if fallback and not forced else 1
    assert config.effective_connect_budget_seconds == attempts * (12 + (20 if send_proxy else 0))
    limited = replace(config, routing=RoutingConfig(max_connect_attempts=1))
    assert limited.effective_connect_budget_seconds == 12 + (20 if send_proxy else 0)


class _LoopClock:
    """Advance only the connect loop's elapsed time, without slow wall-clock tests."""

    value = 100.0

    def time(self) -> float:
        return self.value


@pytest.mark.asyncio
@pytest.mark.parametrize("send_proxy", [False, True])
@pytest.mark.parametrize("explicit_budget", [None, 10.0])
async def test_legacy_fallback_receives_full_budget_unless_explicitly_capped(
    monkeypatch: pytest.MonkeyPatch, send_proxy: bool, explicit_budget: float | None
) -> None:
    base = make_config(25561, 25562)
    config = replace(
        base,
        healthcheck=replace(base.healthcheck, enabled=False),
        fallback_healthcheck=replace(base.fallback_healthcheck, enabled=False),
        connection=replace(base.connection, timeout_seconds=12, write_timeout_seconds=7),
        proxy_protocol=replace(base.proxy_protocol, send=send_proxy),
    )
    if explicit_budget is not None:
        config = replace(
            config, routing=replace(config.routing, connect_budget_seconds=explicit_budget)
        )
    app = FailoverApplication(config)
    calls: list[str] = []
    clock = _LoopClock()
    phase_seconds = 12 + (7 if send_proxy else 0)
    backend_writer = FakeWriter()

    async def connect(
        target: Target, *_args: Any
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        calls.append(target.id)
        clock.value += phase_seconds
        if target.id == "MAIN":
            raise asyncio.TimeoutError
        return asyncio.StreamReader(), stream_writer(backend_writer)

    monkeypatch.setattr(app.proxy, "_connect", connect)
    decision = await app.router.select_for_connection()
    try:
        with monkeypatch.context() as elapsed:
            elapsed.setattr(asyncio, "get_running_loop", lambda: clock)
            connected = await app.proxy._connect_candidates(
                decision, app.router, None, stream_writer(FakeWriter())
            )
        if explicit_budget is None:
            assert calls == ["MAIN", "FALLBACK"]
            assert connected is not None
            assert connected.entry.config.id == "FALLBACK"
        else:
            assert calls == ["MAIN"]
            assert connected is None
    finally:
        backend_writer.close()
        await app.shutdown()


@pytest.mark.asyncio
async def test_explicit_total_budget_cancels_stalled_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25561, 25562)
    config = replace(
        base,
        healthcheck=replace(base.healthcheck, enabled=False),
        fallback_healthcheck=replace(base.fallback_healthcheck, enabled=False),
        routing=replace(base.routing, connect_budget_seconds=0.01),
    )
    app = FailoverApplication(config)
    cancelled = asyncio.Event()

    async def stalled(*_args: Any) -> Any:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(app.proxy, "_connect", stalled)
    try:
        decision = await app.router.select_for_connection()
        connected = await asyncio.wait_for(
            app.proxy._connect_candidates(decision, app.router, None, stream_writer(FakeWriter())),
            timeout=1,
        )
        assert connected is None
        assert cancelled.is_set()
        assert app.registry.entries["MAIN"].connect_failures == 1
        assert app.registry.entries["FALLBACK"].connect_failures == 0
        assert app.proxy._writers == set()
        assert app.circuit_breaker.snapshot_now().half_open_in_flight == 0
    finally:
        await app.shutdown()
