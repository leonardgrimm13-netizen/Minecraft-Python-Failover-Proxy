from __future__ import annotations

import asyncio

import pytest

from mc_failover.circuit_breaker import CircuitBreaker, CircuitPermit
from mc_failover.config import CircuitBreakerConfig
from mc_failover.models import CircuitState
from tests.conftest import FakeClock


def circuit(clock: FakeClock, *, enabled: bool = True) -> CircuitBreaker:
    return CircuitBreaker(CircuitBreakerConfig(enabled, 2, 30.0, 5.0, 1), clock=clock)


@pytest.mark.asyncio
async def test_accepted_tcp_connections_do_not_erase_established_session_failures() -> None:
    breaker = circuit(FakeClock())
    for count in (1, 2):
        permit = await breaker.acquire()
        await breaker.record_success(permit)
        await breaker.record_session_failure(permit)
        snapshot = await breaker.snapshot()
        assert snapshot.failures_in_window == count
    assert snapshot.state is CircuitState.OPEN
    assert not (await breaker.acquire()).allowed


@pytest.mark.asyncio
async def test_session_failure_window_is_bounded_and_expires_independently() -> None:
    clock = FakeClock()
    breaker = circuit(clock)
    await breaker.record_session_failure(await breaker.acquire())
    clock.advance(31)
    assert (await breaker.snapshot()).failures_in_window == 0
    for _ in range(100):
        await breaker.record_session_failure()
    assert len(breaker._session_failures) == 2
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.OPEN
    assert snapshot.open_total == 1
    clock.advance(1)
    await breaker.record_session_failure()
    assert (await breaker.snapshot()).retry_after_seconds == 4


@pytest.mark.asyncio
async def test_failed_recovery_probe_reopens_before_or_after_connect_success() -> None:
    for record_connect_success in (False, True):
        clock = FakeClock()
        breaker = circuit(clock)
        await breaker.record_session_failure()
        await breaker.record_session_failure()
        clock.advance(5)
        permit = await breaker.acquire()
        assert permit.probe
        if record_connect_success:
            await breaker.record_success(permit)
            assert (await breaker.snapshot()).state is CircuitState.CLOSED
        await breaker.record_session_failure(permit)
        snapshot = await breaker.snapshot()
        assert snapshot.state is CircuitState.OPEN
        assert snapshot.open_total == 2
        assert snapshot.half_open_in_flight == 0


@pytest.mark.asyncio
async def test_old_session_and_probe_failures_cannot_reverse_a_later_recovery() -> None:
    clock = FakeClock()
    breaker = circuit(clock)
    old_session = await breaker.acquire()
    await breaker.record_session_failure(old_session)
    await breaker.record_session_failure(old_session)
    clock.advance(5)
    old_probe = await breaker.acquire()
    await breaker.record_success(old_probe)
    await breaker.record_session_failure(old_probe)
    clock.advance(5)
    await breaker.record_health_success()
    assert (await breaker.snapshot()).state is CircuitState.CLOSED
    await breaker.record_session_failure(old_session)
    await breaker.record_session_failure(old_probe)
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.failures_in_window == 0


@pytest.mark.asyncio
async def test_active_checks_preserve_window_until_actual_open_circuit_recovery() -> None:
    clock = FakeClock()
    breaker = circuit(clock)
    await breaker.record_session_failure()
    await breaker.record_health_success()
    assert (await breaker.snapshot()).failures_in_window == 1
    await breaker.record_session_failure()
    await breaker.record_health_success()
    assert (await breaker.snapshot()).state is CircuitState.OPEN
    clock.advance(5)
    await breaker.record_health_success()
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.failures_in_window == 0


@pytest.mark.asyncio
async def test_disabled_denied_and_stale_half_open_session_verdicts_are_ignored() -> None:
    clock = FakeClock()
    disabled = circuit(clock, enabled=False)
    await disabled.record_session_failure()
    assert (await disabled.snapshot()).failures_in_window == 0
    breaker = circuit(clock)
    await breaker.record_session_failure(CircuitPermit(False, False))
    assert (await breaker.snapshot()).failures_in_window == 0
    await breaker.record_session_failure()
    await breaker.record_session_failure()
    clock.advance(5)
    probe = await breaker.acquire()
    await breaker.record_session_failure(CircuitPermit(True, True, -1, -1))
    assert (await breaker.snapshot()).state is CircuitState.HALF_OPEN
    await breaker.release(probe)
    await breaker.record_session_failure(probe)
    assert (await breaker.snapshot()).state is CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_many_concurrent_session_failures_open_once_with_bounded_evidence() -> None:
    breaker = circuit(FakeClock())
    permits = [await breaker.acquire() for _ in range(200)]
    await asyncio.gather(*(breaker.record_session_failure(permit) for permit in permits))
    snapshot = await breaker.snapshot()
    assert snapshot.open_total == 1
    assert snapshot.failures_in_window == 2
    assert len(breaker._session_failures) == 2
