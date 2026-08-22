from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from datetime import datetime, timezone
from typing import Any, TypeVar

import pytest

from mc_failover.limits import ConnectionLease, ConnectionLimiter
from mc_failover.models import RejectionReason

_T = TypeVar("_T")
UTC = timezone.utc


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def monotonic(self) -> float:
        return self.now

    def utc_now(self) -> datetime:
        return datetime(2026, 7, 19, tzinfo=UTC)


def run(coroutine: Coroutine[Any, Any, _T]) -> _T:
    return asyncio.run(coroutine)


def make_limiter(
    *,
    maximum: int = 8,
    per_ip: int = 1,
    rate: float = 0.0,
    burst: int = 0,
) -> ConnectionLimiter:
    return ConnectionLimiter(maximum, per_ip, rate, burst, clock=FakeClock())


def test_deferred_acquire_reserves_global_capacity_before_proxy_identity_is_known() -> None:
    async def scenario() -> None:
        limiter = make_limiter(maximum=1, per_ip=1, rate=1.0, burst=1)
        lease, reason = await limiter.try_acquire(
            "127.0.0.1",
            defer_peer_limits=True,
        )
        assert lease == ConnectionLease(peer_ip=None, released=False)
        assert reason is None
        assert limiter.active == 1
        assert limiter.tracked_active_ips == 0
        assert limiter.tracked_rate_ips == 0

        rejected, rejected_reason = await limiter.try_acquire(
            "127.0.0.2",
            defer_peer_limits=True,
        )
        assert rejected is None
        assert rejected_reason is RejectionReason.GLOBAL_LIMIT
        assert limiter.active == 1
        assert limiter.tracked_active_ips == 0
        assert limiter.tracked_rate_ips == 0

        await limiter.release(lease)
        assert limiter.active == 0

    run(scenario())


def test_apply_peer_limits_normalizes_equivalent_ipv6_before_bucket_lookup() -> None:
    async def scenario() -> None:
        limiter = make_limiter(maximum=3, per_ip=1)
        first, _ = await limiter.try_acquire("127.0.0.1", defer_peer_limits=True)
        duplicate, _ = await limiter.try_acquire("127.0.0.1", defer_peer_limits=True)
        assert first is not None and duplicate is not None

        assert await limiter.apply_peer_limits(first, "2001:0db8:0:0:0:0:0:1") is None
        assert first.peer_ip == "2001:db8::1"
        assert limiter.tracked_active_ips == 1

        rejection = await limiter.apply_peer_limits(duplicate, "2001:db8::1")
        assert rejection is RejectionReason.PER_IP_LIMIT
        assert duplicate.peer_ip is None
        assert limiter.active == 2
        assert limiter.tracked_active_ips == 1

        await limiter.release(duplicate)
        assert limiter.active == 1
        assert limiter.tracked_active_ips == 1
        await limiter.release(first)
        assert limiter.active == 0
        assert limiter.tracked_active_ips == 0

    run(scenario())


def test_apply_peer_limits_uses_canonical_ipv4_key_and_keeps_other_sources_separate() -> None:
    async def scenario() -> None:
        limiter = make_limiter(maximum=3, per_ip=1)
        first, _ = await limiter.try_acquire("127.0.0.1", defer_peer_limits=True)
        second, _ = await limiter.try_acquire("127.0.0.1", defer_peer_limits=True)
        assert first is not None and second is not None

        assert await limiter.apply_peer_limits(first, "192.0.2.10") is None
        assert await limiter.apply_peer_limits(second, "192.0.2.11") is None
        assert first.peer_ip == "192.0.2.10"
        assert second.peer_ip == "192.0.2.11"
        assert limiter.active == 2
        assert limiter.tracked_active_ips == 2

        await limiter.release(first)
        assert limiter.active == 1
        assert limiter.tracked_active_ips == 1
        await limiter.release(second)
        assert limiter.active == 0
        assert limiter.tracked_active_ips == 0

    run(scenario())


def test_rejected_deferred_rate_limit_still_requires_global_lease_release() -> None:
    async def scenario() -> None:
        limiter = make_limiter(maximum=3, per_ip=0, rate=1.0, burst=1)
        first, _ = await limiter.try_acquire("127.0.0.1", defer_peer_limits=True)
        limited, _ = await limiter.try_acquire("127.0.0.1", defer_peer_limits=True)
        assert first is not None and limited is not None

        assert await limiter.apply_peer_limits(first, "198.51.100.20") is None
        assert (
            await limiter.apply_peer_limits(limited, "198.51.100.20") is RejectionReason.RATE_LIMIT
        )
        assert limited.peer_ip is None
        assert limiter.active == 2
        assert limiter.tracked_active_ips == 1
        assert limiter.tracked_rate_ips == 1

        await limiter.release(limited)
        assert limiter.active == 1
        assert limiter.tracked_active_ips == 1
        await limiter.release(first)
        assert limiter.active == 0
        assert limiter.tracked_active_ips == 0

    run(scenario())


def test_deferred_lease_cannot_be_accounted_twice_or_after_release() -> None:
    async def scenario() -> None:
        limiter = make_limiter(maximum=2, per_ip=2)
        lease, _ = await limiter.try_acquire("127.0.0.1", defer_peer_limits=True)
        assert lease is not None
        assert await limiter.apply_peer_limits(lease, "203.0.113.7") is None
        with pytest.raises(RuntimeError, match="already has source-IP accounting"):
            await limiter.apply_peer_limits(lease, "203.0.113.8")

        await limiter.release(lease)
        with pytest.raises(RuntimeError, match="inactive connection lease"):
            await limiter.apply_peer_limits(lease, "203.0.113.8")
        assert limiter.active == 0
        assert limiter.tracked_active_ips == 0

    run(scenario())


def test_concurrent_deferred_applications_are_serialized_at_per_ip_boundary() -> None:
    async def scenario() -> None:
        limiter = make_limiter(maximum=16, per_ip=1)
        leases: list[ConnectionLease] = []
        for _ in range(8):
            lease, reason = await limiter.try_acquire("127.0.0.1", defer_peer_limits=True)
            assert lease is not None and reason is None
            leases.append(lease)

        results = await asyncio.gather(
            *(limiter.apply_peer_limits(lease, "2001:db8::42") for lease in leases)
        )
        assert results.count(None) == 1
        assert results.count(RejectionReason.PER_IP_LIMIT) == 7
        assert sum(lease.peer_ip == "2001:db8::42" for lease in leases) == 1
        assert limiter.active == 8
        assert limiter.tracked_active_ips == 1

        await asyncio.gather(*(limiter.release(lease) for lease in leases))
        assert limiter.active == 0
        assert limiter.tracked_active_ips == 0

    run(scenario())
