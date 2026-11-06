from __future__ import annotations

import asyncio
import errno
from dataclasses import replace
from typing import Any, cast

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.models import CircuitState
from tests.conftest import make_config
from tests.integration.test_deferred_source_limits import FakeWriter, reader_with


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_number", [errno.EMFILE, errno.ENFILE, errno.ENOMEM, errno.ENOBUFS, errno.EADDRNOTAVAIL]
)
async def test_proxy_resource_errors_do_not_quarantine_healthy_backends(
    monkeypatch: pytest.MonkeyPatch,
    error_number: int,
) -> None:
    base = make_config(25561, 25562)
    config = replace(
        base,
        healthcheck=replace(base.healthcheck, enabled=False),
        fallback_healthcheck=replace(base.fallback_healthcheck, enabled=False),
        passive_health=replace(base.passive_health, failure_threshold=1),
        circuit_breaker=replace(base.circuit_breaker, failure_threshold=1),
    )
    app = FailoverApplication(config)
    calls = 0

    async def fail(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        raise OSError(error_number, "local proxy resource problem")

    monkeypatch.setattr(app.proxy, "_connect", fail)
    await app.proxy.handle_client(
        reader_with(b"client bytes"), cast(asyncio.StreamWriter, FakeWriter())
    )
    assert all(entry.passive_failures == 0 for entry in app.registry.ordered)
    assert all(entry.routable for entry in app.registry.ordered)
    assert all(
        entry.circuit.snapshot_now().state is CircuitState.CLOSED for entry in app.registry.ordered
    )
    assert calls == 1
    assert app.limiter.active == app.runtime.active_connections == 0
    await app.shutdown()
