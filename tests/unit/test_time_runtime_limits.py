from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timedelta, timezone

import pytest

from mc_failover.limits import ConnectionLimiter
from mc_failover.models import RejectionReason, TargetName
from mc_failover.runtime import RuntimeState, TaskTracker
from mc_failover.time_utils import as_utc, format_utc, non_negative_elapsed


class FakeClock:
    def __init__(self) -> None:
        self.monotonic_value = 100.0
        self.utc_value = datetime(2026, 7, 19, 12, 34, 56, 123_456, tzinfo=timezone.utc)

    def monotonic(self) -> float:
        return self.monotonic_value

    def utc_now(self) -> datetime:
        return self.utc_value

    def advance(self, seconds: float) -> None:
        self.monotonic_value += seconds
        self.utc_value += timedelta(seconds=seconds)


def limiter(
    clock: FakeClock,
    *,
    maximum: int = 10,
    per_ip: int = 0,
    rate: float = 0.0,
    burst: int = 0,
) -> ConnectionLimiter:
    return ConnectionLimiter(
        maximum,
        per_ip,
        rate,
        burst,
        clock=clock,
    )


def test_utc_formatting_is_stable_and_normalizes_offsets() -> None:
    local = datetime(
        2026,
        7,
        19,
        14,
        34,
        56,
        987_654,
        tzinfo=timezone(timedelta(hours=2)),
    )
    assert as_utc(local) == datetime(2026, 7, 19, 12, 34, 56, 987_654, tzinfo=timezone.utc)
    assert format_utc(local) == "2026-07-19T12:34:56.987Z"
    assert format_utc(None) is None
    with pytest.raises(ValueError, match="timezone-aware"):
        format_utc(datetime(2026, 7, 19, 12, 0))  # noqa: DTZ001 - deliberately naive


def test_elapsed_time_is_monotonic_non_negative_and_optional() -> None:
    assert non_negative_elapsed(105.5, 100.0) == 5.5
    assert non_negative_elapsed(90.0, 100.0) == 0.0
    assert non_negative_elapsed(100.0, None) is None


def test_runtime_uses_separate_wall_and_monotonic_start_times() -> None:
    clock = FakeClock()
    runtime = RuntimeState(clock=clock)
    started_at = clock.utc_now()

    clock.monotonic_value += 12.5
    clock.utc_value -= timedelta(days=30)
    assert runtime.started_at == started_at
    assert runtime.uptime_seconds == 12.5

    clock.monotonic_value = runtime.started_monotonic - 1.0
    assert runtime.uptime_seconds == 0.0


def test_runtime_connection_rejection_and_target_counters_are_consistent() -> None:
    runtime = RuntimeState(clock=FakeClock())
    runtime.incoming_connection_received()
    runtime.incoming_connection_received()
    runtime.incoming_connection_received()
    runtime.connection_admitted()
    runtime.connection_admitted()
    runtime.backend_connection_started()
    runtime.backend_connection_started()
    runtime.backend_connection_finished()
    assert runtime.incoming_connections_total == 3
    assert runtime.active_connections == 1
    assert runtime.backend_connections_established_total == 2
    assert runtime.total_connections == 2

    runtime.backend_connection_finished()
    runtime.backend_connection_finished()
    assert runtime.active_connections == 0
    assert runtime.total_connections == 2

    runtime.reject(RejectionReason.GLOBAL_LIMIT)
    runtime.reject(RejectionReason.GLOBAL_LIMIT)
    runtime.reject(RejectionReason.MONITORING_LIMIT)
    assert runtime.rejected_connections == 2
    assert runtime.connections_rejected_total == 2
    assert runtime.monitoring_rejected_connections == 1
    assert runtime.rejection_reasons[RejectionReason.GLOBAL_LIMIT] == 2
    assert RejectionReason.MONITORING_LIMIT not in runtime.rejection_reasons

    runtime.connect_succeeded(TargetName.MAIN)
    runtime.connect_succeeded(TargetName.FALLBACK)
    runtime.connect_succeeded(TargetName.NONE)
    runtime.connect_failed(TargetName.MAIN)
    runtime.connect_failed(TargetName.FALLBACK)
    runtime.connect_failed(TargetName.NONE)
    assert runtime.main_connect_successes == 1
    assert runtime.fallback_connect_successes == 1
    assert runtime.main_connect_failures == 1
    assert runtime.fallback_connect_failures == 1


def test_runtime_legacy_connection_lifecycle_helpers_remain_compatible() -> None:
    runtime = RuntimeState(clock=FakeClock())

    runtime.connection_started()
    runtime.connection_finished()

    assert runtime.total_connections == 1
    assert runtime.backend_connections_established_total == 1
    assert runtime.active_connections == 0


@pytest.mark.asyncio
async def test_task_tracker_gracefully_drains_completed_work() -> None:
    tracker = TaskTracker(name="graceful")
    started = asyncio.Event()
    finish = asyncio.Event()

    async def worker() -> str:
        started.set()
        await finish.wait()
        return "done"

    task = tracker.create(worker())
    await started.wait()
    assert tracker.active_count == 1
    finish.set()
    await tracker.drain(grace_seconds=1.0, cancel_timeout_seconds=0.0)
    assert await task == "done"
    assert tracker.active_count == 0


@pytest.mark.asyncio
async def test_task_tracker_cancels_and_collects_pending_work_with_cleanup() -> None:
    tracker = TaskTracker(name="cancel")
    started = asyncio.Event()
    blocker = asyncio.Event()
    cleaned = asyncio.Event()

    async def worker() -> None:
        try:
            started.set()
            await blocker.wait()
        finally:
            cleaned.set()

    task = tracker.create(worker())
    await started.wait()
    await tracker.cancel_all(timeout_seconds=0.0)
    assert task.cancelled()
    assert cleaned.is_set()
    assert tracker.active_count == 0


@pytest.mark.asyncio
async def test_task_tracker_cancel_timeout_never_waits_forever_for_resistant_task() -> None:
    tracker = TaskTracker(name="resistant")
    started = asyncio.Event()
    ignored_cancel = asyncio.Event()
    release = asyncio.Event()

    async def worker() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            ignored_cancel.set()
            await release.wait()

    task = tracker.create(worker())
    await started.wait()
    await asyncio.wait_for(tracker.cancel_all(timeout_seconds=0.001), timeout=0.1)
    assert ignored_cancel.is_set()
    assert not task.done()
    release.set()
    assert not await tracker.wait_remaining(timeout_seconds=0.1)
    await task
    assert tracker.active_count == 0


@pytest.mark.asyncio
async def test_task_tracker_closes_rejected_coroutine_after_shutdown() -> None:
    tracker = TaskTracker(name="closed")
    await tracker.drain(grace_seconds=0.0, cancel_timeout_seconds=0.0)

    async def never_started() -> None:
        raise AssertionError("closed coroutine must not run")

    coroutine = never_started()
    with pytest.raises(RuntimeError, match="is closed"):
        tracker.create(coroutine)
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED


@pytest.mark.asyncio
async def test_connection_limiter_global_limit_and_cleanup() -> None:
    connection_limiter = limiter(FakeClock(), maximum=2)
    first, reason = await connection_limiter.try_acquire("192.0.2.1")
    second, _ = await connection_limiter.try_acquire("192.0.2.2")
    rejected, rejected_reason = await connection_limiter.try_acquire("192.0.2.3")

    assert first is not None and second is not None
    assert reason is None
    assert rejected is None
    assert rejected_reason is RejectionReason.GLOBAL_LIMIT
    assert connection_limiter.active == 2
    assert connection_limiter.tracked_active_ips == 2

    await connection_limiter.release(first)
    await connection_limiter.release(second)
    assert connection_limiter.active == 0
    assert connection_limiter.tracked_active_ips == 0


@pytest.mark.asyncio
async def test_connection_limiter_enforces_per_ip_without_affecting_other_ips() -> None:
    connection_limiter = limiter(FakeClock(), maximum=5, per_ip=1)
    first, _ = await connection_limiter.try_acquire("2001:db8::1")
    duplicate, duplicate_reason = await connection_limiter.try_acquire("2001:db8::1")
    other, other_reason = await connection_limiter.try_acquire("2001:db8::2")

    assert first is not None and other is not None
    assert duplicate is None
    assert duplicate_reason is RejectionReason.PER_IP_LIMIT
    assert other_reason is None
    await connection_limiter.release(first)
    await connection_limiter.release(other)
    assert connection_limiter.tracked_active_ips == 0


@pytest.mark.asyncio
async def test_connection_limiter_treats_ipv4_mapped_ipv6_as_the_same_source() -> None:
    connection_limiter = limiter(FakeClock(), maximum=5, per_ip=1)

    ipv4, reason = await connection_limiter.try_acquire("192.0.2.1")
    mapped, mapped_reason = await connection_limiter.try_acquire("::ffff:192.0.2.1")

    assert ipv4 is not None
    assert reason is None
    assert mapped is None
    assert mapped_reason is RejectionReason.PER_IP_LIMIT
    assert connection_limiter.active == 1
    assert connection_limiter.tracked_active_ips == 1

    await connection_limiter.release(ipv4)
    mapped_after_release, reason = await connection_limiter.try_acquire("::ffff:192.0.2.1")
    duplicate_ipv4, duplicate_reason = await connection_limiter.try_acquire("192.0.2.1")

    assert mapped_after_release is not None
    assert reason is None
    assert duplicate_ipv4 is None
    assert duplicate_reason is RejectionReason.PER_IP_LIMIT

    await connection_limiter.release(mapped_after_release)
    assert connection_limiter.active == 0
    assert connection_limiter.tracked_active_ips == 0


@pytest.mark.asyncio
async def test_connection_limiter_token_bucket_uses_injected_monotonic_clock() -> None:
    clock = FakeClock()
    connection_limiter = limiter(clock, maximum=10, rate=2.0, burst=2)
    first, _ = await connection_limiter.try_acquire("198.51.100.9")
    second, _ = await connection_limiter.try_acquire("198.51.100.9")
    limited, limited_reason = await connection_limiter.try_acquire("198.51.100.9")
    assert first is not None and second is not None
    assert limited is None
    assert limited_reason is RejectionReason.RATE_LIMIT

    clock.monotonic_value += 0.5
    replenished, reason = await connection_limiter.try_acquire("198.51.100.9")
    assert replenished is not None
    assert reason is None

    clock.monotonic_value -= 1000.0
    still_limited, reason = await connection_limiter.try_acquire("198.51.100.9")
    assert still_limited is None
    assert reason is RejectionReason.RATE_LIMIT

    await connection_limiter.release(first)
    await connection_limiter.release(second)
    await connection_limiter.release(replenished)


@pytest.mark.asyncio
async def test_connection_limiter_detects_double_release() -> None:
    connection_limiter = limiter(FakeClock())
    lease, _ = await connection_limiter.try_acquire("203.0.113.1")
    assert lease is not None
    await connection_limiter.release(lease)
    with pytest.raises(RuntimeError, match="more than once"):
        await connection_limiter.release(lease)
    assert connection_limiter.active == 0


@pytest.mark.asyncio
async def test_rate_tracking_dictionary_is_bounded_under_many_source_ips() -> None:
    connection_limiter = limiter(FakeClock(), maximum=1, rate=1.0, burst=1)
    for index in range(1100):
        lease, reason = await connection_limiter.try_acquire(f"198.51.{index // 256}.{index % 256}")
        assert lease is not None
        assert reason is None
        await connection_limiter.release(lease)

    assert connection_limiter.tracked_rate_ips == 1024
