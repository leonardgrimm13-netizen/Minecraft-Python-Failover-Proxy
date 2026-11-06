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
    await breaker.record_session_failure(await breaker.acquire())
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


@pytest.mark.parametrize("threshold", [2, 3, 5])
@pytest.mark.parametrize(
    "order",
    [
        ("connect", "connect"),
        ("session", "session"),
        ("connect", "session"),
        ("session", "connect"),
    ],
)
@pytest.mark.asyncio
async def test_connect_and_session_failure_threshold_is_order_independent(
    threshold: int, order: tuple[str, str]
) -> None:
    breaker = CircuitBreaker(CircuitBreakerConfig(True, threshold, 30, 5, 1), clock=FakeClock())
    # All connections are admitted before any failure, so only error order varies.
    kinds = [*([order[0]] * (threshold - 1)), order[1]]
    permits = [await breaker.acquire() for _ in kinds]
    for index, (kind, permit) in enumerate(zip(kinds, permits, strict=True), 1):
        if kind == "connect":
            await breaker.record_failure(permit)
        else:
            await breaker.record_session_failure(permit)
        snapshot = await breaker.snapshot()
        assert snapshot.failures_in_window == index
        assert snapshot.state is (CircuitState.OPEN if index == threshold else CircuitState.CLOSED)
    assert snapshot.open_total == 1


@pytest.mark.parametrize("first", ["connect", "session"])
@pytest.mark.asyncio
async def test_mixed_failure_window_expires_before_threshold(first: str) -> None:
    clock = FakeClock()
    breaker = circuit(clock)
    if first == "connect":
        await breaker.record_failure(await breaker.acquire())
    else:
        await breaker.record_session_failure(await breaker.acquire())
    clock.advance(30.01)
    if first == "connect":
        await breaker.record_session_failure(await breaker.acquire())
    else:
        await breaker.record_failure(await breaker.acquire())
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.failures_in_window == 1


@pytest.mark.parametrize("kind", ["connect", "session"])
@pytest.mark.asyncio
async def test_one_permit_cannot_report_multiple_failures(kind: str) -> None:
    breaker = circuit(FakeClock())
    permit = await breaker.acquire()
    report = breaker.record_failure if kind == "connect" else breaker.record_session_failure
    await asyncio.gather(*(report(permit) for _ in range(20)))
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.failures_in_window == 1
    # A failure cannot also be counted in the other category or undone by a
    # late, duplicate success for that same connection.
    await breaker.record_failure(permit)
    await breaker.record_session_failure(permit)
    await breaker.record_success(permit)
    assert (await breaker.snapshot()).failures_in_window == 1


@pytest.mark.asyncio
async def test_duplicate_connect_success_does_not_erase_newer_connect_failure() -> None:
    breaker = circuit(FakeClock())
    success = await breaker.acquire()
    await breaker.record_success(success)
    await breaker.record_failure(await breaker.acquire())
    await breaker.record_success(success)
    assert (await breaker.snapshot()).failures_in_window == 1


@pytest.mark.parametrize("probe", [False, True])
@pytest.mark.asyncio
async def test_released_permit_cannot_later_report_any_verdict(probe: bool) -> None:
    clock = FakeClock()
    breaker = circuit(clock)
    if probe:
        await breaker.record_session_failure()
        await breaker.record_session_failure()
        clock.advance(5)
    permit = await breaker.acquire()
    await breaker.release(permit)
    before = await breaker.snapshot()
    await breaker.record_failure(permit)
    await breaker.record_success(permit)
    await breaker.record_session_failure(permit)
    assert await breaker.snapshot() == before


@pytest.mark.asyncio
async def test_an_unresolved_sibling_probe_cannot_reverse_successful_probe() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(CircuitBreakerConfig(True, 1, 30, 5, 2), clock=clock)
    await breaker.record_failure(await breaker.acquire())
    clock.advance(5)
    success, unresolved = await breaker.acquire(), await breaker.acquire()
    await breaker.record_success(success)
    await breaker.record_session_failure(unresolved)
    assert (await breaker.snapshot()).state is CircuitState.CLOSED
    await breaker.record_session_failure(success)
    assert (await breaker.snapshot()).state is CircuitState.OPEN


@pytest.mark.asyncio
async def test_concurrent_mixed_failures_open_once_without_duplicate_counting() -> None:
    breaker = CircuitBreaker(CircuitBreakerConfig(True, 6, 30, 5, 1), clock=FakeClock())
    permits = [await breaker.acquire() for _ in range(100)]
    await asyncio.gather(
        *(
            breaker.record_failure(permit) if index % 2 else breaker.record_session_failure(permit)
            for index, permit in enumerate(permits)
        )
    )
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.OPEN
    assert snapshot.open_total == 1
    assert snapshot.failures_in_window == 6
    assert snapshot.half_open_in_flight == 0


@pytest.mark.asyncio
async def test_failure_history_stays_bounded_across_failed_probe_cycles() -> None:
    clock = FakeClock()
    breaker = circuit(clock)
    await breaker.record_failure(await breaker.acquire())
    await breaker.record_failure(await breaker.acquire())
    for _ in range(20):
        clock.advance(5)
        probe = await breaker.acquire()
        await breaker.record_failure(probe)
        assert len(breaker._failures) <= breaker.failure_threshold
        assert (await breaker.snapshot()).half_open_in_flight == 0


@pytest.mark.asyncio
async def test_release_of_resolved_success_keeps_its_session_evidence_valid() -> None:
    breaker = circuit(FakeClock())
    permit = await breaker.acquire()
    await breaker.record_success(permit)
    await breaker.release(permit)
    await breaker.record_session_failure(permit)
    assert (await breaker.snapshot()).failures_in_window == 1
