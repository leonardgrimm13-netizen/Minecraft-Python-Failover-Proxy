from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError, dataclass
from datetime import datetime, timedelta, timezone

import pytest

from mc_failover.circuit_breaker import CircuitBreaker, CircuitPermit
from mc_failover.models import CircuitState

UTC = timezone.utc


class FakeClock:
    def __init__(self) -> None:
        self.monotonic_value = 100.0
        self.utc_value = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)

    def monotonic(self) -> float:
        return self.monotonic_value

    def utc_now(self) -> datetime:
        return self.utc_value

    def advance(self, seconds: float) -> None:
        self.monotonic_value += seconds
        self.utc_value += timedelta(seconds=seconds)

    def jump_wall_clock(self, seconds: float) -> None:
        self.utc_value += timedelta(seconds=seconds)


def make_breaker(
    clock: FakeClock,
    *,
    enabled: bool = True,
    failure_threshold: int = 3,
    failure_window_seconds: float = 10.0,
    open_seconds: float = 15.0,
    half_open_max_attempts: int = 1,
) -> CircuitBreaker:
    return CircuitBreaker(
        enabled=enabled,
        failure_threshold=failure_threshold,
        failure_window_seconds=failure_window_seconds,
        open_seconds=open_seconds,
        half_open_max_attempts=half_open_max_attempts,
        clock=clock,
    )


@pytest.mark.parametrize("field", ["failure_window_seconds", "open_seconds"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_constructor_rejects_non_finite_durations(field: str, value: float) -> None:
    values = {
        "enabled": True,
        "failure_threshold": 1,
        "failure_window_seconds": 1.0,
        "open_seconds": 1.0,
        "half_open_max_attempts": 1,
    }
    values[field] = value
    with pytest.raises(ValueError, match=field):
        CircuitBreaker(**values)  # type: ignore[arg-type]


async def fail_once(breaker: CircuitBreaker) -> None:
    permit = await breaker.acquire()
    assert permit.allowed
    await breaker.record_failure(permit)


@pytest.mark.asyncio
async def test_threshold_opens_and_sliding_window_discards_old_failures() -> None:
    clock = FakeClock()
    breaker = make_breaker(clock)

    await fail_once(breaker)
    await fail_once(breaker)
    clock.advance(10.01)
    await fail_once(breaker)
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.failures_in_window == 1

    await fail_once(breaker)
    await fail_once(breaker)
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.OPEN
    assert snapshot.failures_in_window == 3
    assert snapshot.open_total == 1
    assert snapshot.retry_after_seconds == pytest.approx(15.0)


@pytest.mark.asyncio
async def test_half_open_probe_failure_reopens_for_a_full_interval() -> None:
    clock = FakeClock()
    breaker = make_breaker(clock, failure_threshold=1, open_seconds=5.0)
    await fail_once(breaker)

    clock.advance(5.0)
    probe = await breaker.acquire()
    assert probe == CircuitPermit(allowed=True, probe=True)
    assert (await breaker.snapshot()).state is CircuitState.HALF_OPEN

    clock.advance(1.0)
    await breaker.record_failure(probe)
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.OPEN
    assert snapshot.open_total == 2
    assert snapshot.retry_after_seconds == pytest.approx(5.0)
    assert snapshot.opened_at == clock.utc_now()


@pytest.mark.asyncio
async def test_half_open_limits_parallel_probes_and_release_returns_capacity() -> None:
    clock = FakeClock()
    breaker = make_breaker(
        clock,
        failure_threshold=1,
        open_seconds=2.0,
        half_open_max_attempts=2,
    )
    await fail_once(breaker)
    clock.advance(2.0)

    permits = await asyncio.gather(*(breaker.acquire() for _ in range(20)))
    allowed = [permit for permit in permits if permit.allowed]
    denied = [permit for permit in permits if not permit.allowed]
    assert len(allowed) == 2
    assert len(denied) == 18
    first, second = allowed
    assert first.allowed and first.probe
    assert second.allowed and second.probe
    assert all(permit == CircuitPermit(allowed=False, probe=False) for permit in denied)
    assert (await breaker.snapshot()).half_open_in_flight == 2

    await breaker.release(first)
    assert (await breaker.snapshot()).half_open_in_flight == 1
    replacement = await breaker.acquire()
    assert replacement.allowed and replacement.probe
    assert (await breaker.snapshot()).half_open_in_flight == 2

    await breaker.release(first)
    assert (await breaker.snapshot()).half_open_in_flight == 2


@pytest.mark.asyncio
async def test_success_resets_closed_failures_and_closes_half_open() -> None:
    clock = FakeClock()
    breaker = make_breaker(clock, failure_threshold=2, open_seconds=3.0)
    await fail_once(breaker)

    success = await breaker.acquire()
    await breaker.record_success(success)
    assert (await breaker.snapshot()).failures_in_window == 0

    await fail_once(breaker)
    await fail_once(breaker)
    clock.advance(3.0)
    probe = await breaker.acquire()
    await breaker.record_success(probe)

    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.opened_at is None
    assert snapshot.failures_in_window == 0
    assert (await breaker.acquire()).allowed


@pytest.mark.asyncio
async def test_health_success_cannot_shorten_open_period_but_recovers_after_it() -> None:
    clock = FakeClock()
    breaker = make_breaker(clock, failure_threshold=1, open_seconds=10.0)
    await fail_once(breaker)

    clock.advance(9.999)
    await breaker.record_health_success()
    assert (await breaker.snapshot()).state is CircuitState.OPEN
    assert not (await breaker.acquire()).allowed

    clock.advance(0.001)
    await breaker.record_health_success()
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.opened_at is None


@pytest.mark.asyncio
async def test_wall_clock_jump_does_not_change_monotonic_retry_deadline() -> None:
    clock = FakeClock()
    breaker = make_breaker(clock, failure_threshold=1, open_seconds=8.0)
    expected_opened_at = clock.utc_now()
    await fail_once(breaker)
    assert (await breaker.snapshot()).opened_at == expected_opened_at

    clock.jump_wall_clock(-86400.0)
    clock.advance(3.0)
    snapshot = await breaker.snapshot()
    assert snapshot.retry_after_seconds == pytest.approx(5.0)
    assert snapshot.opened_at == expected_opened_at
    assert snapshot.opened_at is not None
    assert snapshot.opened_at.tzinfo is UTC

    clock.jump_wall_clock(604800.0)
    snapshot = await breaker.snapshot()
    assert snapshot.retry_after_seconds == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_disabled_breaker_always_allows_and_stays_closed() -> None:
    clock = FakeClock()
    breaker = make_breaker(clock, enabled=False, failure_threshold=1)

    permit = await breaker.acquire()
    assert permit == CircuitPermit(allowed=True, probe=False)
    await breaker.record_failure(permit)
    await breaker.record_health_success()
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.open_total == 0
    assert snapshot.failures_in_window == 0


@dataclass(frozen=True)
class ConfigLike:
    enabled: bool = True
    failure_threshold: int = 2
    failure_window_seconds: float = 4.0
    open_seconds: float = 6.0
    half_open_max_attempts: int = 1


@pytest.mark.asyncio
async def test_accepts_structural_config_and_snapshots_are_immutable() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(ConfigLike(), clock=clock)
    await fail_once(breaker)
    await fail_once(breaker)

    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.OPEN
    assert snapshot.retry_after_seconds == pytest.approx(6.0)
    field_name = "open_total"
    with pytest.raises(FrozenInstanceError):
        setattr(snapshot, field_name, 99)


@pytest.mark.asyncio
async def test_stale_probe_result_cannot_corrupt_new_closed_generation() -> None:
    clock = FakeClock()
    breaker = make_breaker(
        clock,
        failure_threshold=1,
        open_seconds=1.0,
        half_open_max_attempts=2,
    )
    await fail_once(breaker)
    clock.advance(1.0)
    successful_probe = await breaker.acquire()
    stale_probe = await breaker.acquire()

    await breaker.record_success(successful_probe)
    await breaker.record_failure(stale_probe)
    snapshot = await breaker.snapshot()
    assert snapshot.state is CircuitState.CLOSED
    assert snapshot.open_total == 1
