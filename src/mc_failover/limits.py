"""Lightweight connection, per-IP and token-bucket admission controls."""

from __future__ import annotations

import asyncio
import ipaddress
from collections import OrderedDict
from dataclasses import dataclass

from .models import RejectionReason
from .time_utils import SYSTEM_CLOCK, Clock


@dataclass(slots=True)
class ConnectionLease:
    peer_ip: str | None
    released: bool = False


@dataclass(slots=True)
class _Bucket:
    tokens: float
    updated_at: float


class ConnectionLimiter:
    def __init__(
        self,
        max_connections: int,
        max_connections_per_ip: int,
        new_connections_per_second: float,
        new_connections_burst: int,
        *,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        self.max_connections = max_connections
        self.max_connections_per_ip = max_connections_per_ip
        self.rate = new_connections_per_second
        self.burst = new_connections_burst
        self.clock = clock
        self._active = 0
        self._per_ip: dict[str, int] = {}
        self._buckets: OrderedDict[str, _Bucket] = OrderedDict()
        self._maximum_buckets = min(100_000, max(1024, max_connections * 4))
        self._lock = asyncio.Lock()

    @property
    def active(self) -> int:
        return self._active

    @property
    def tracked_active_ips(self) -> int:
        return len(self._per_ip)

    @property
    def tracked_rate_ips(self) -> int:
        return len(self._buckets)

    def _consume_rate_token(self, peer_ip: str, now: float) -> bool:
        if self.rate <= 0:
            return True
        bucket = self._buckets.pop(peer_ip, None)
        if bucket is None:
            if len(self._buckets) >= self._maximum_buckets:
                self._buckets.popitem(last=False)
            bucket = _Bucket(tokens=float(self.burst), updated_at=now)
        else:
            elapsed = max(0.0, now - bucket.updated_at)
            bucket.tokens = min(float(self.burst), bucket.tokens + elapsed * self.rate)
            bucket.updated_at = now
        allowed = bucket.tokens >= 1.0
        if allowed:
            bucket.tokens -= 1.0
        self._buckets[peer_ip] = bucket
        return allowed

    @staticmethod
    def _normalize_peer_ip(peer_ip: str) -> str:
        """Use one stable key for equivalent textual IP address forms."""

        try:
            address = ipaddress.ip_address(peer_ip)
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
                return address.ipv4_mapped.compressed
            return address.compressed
        except ValueError:
            # The listener normally supplies a validated socket address.  Keep a
            # bounded opaque key for unusual transports instead of failing open.
            return peer_ip[:128]

    def _try_apply_peer_limits(
        self,
        peer_ip: str,
        *,
        now: float,
    ) -> tuple[str | None, RejectionReason | None]:
        normalized = self._normalize_peer_ip(peer_ip)
        if not self._consume_rate_token(normalized, now):
            return None, RejectionReason.RATE_LIMIT
        per_ip = self._per_ip.get(normalized, 0)
        if self.max_connections_per_ip > 0 and per_ip >= self.max_connections_per_ip:
            return None, RejectionReason.PER_IP_LIMIT
        self._per_ip[normalized] = per_ip + 1
        return normalized, None

    async def try_acquire(
        self,
        peer_ip: str,
        *,
        defer_peer_limits: bool = False,
    ) -> tuple[ConnectionLease | None, RejectionReason | None]:
        """Reserve a global slot and, normally, the source-IP limits.

        A trusted PROXY-protocol connection can defer source-IP accounting
        until its bounded header has been authenticated and parsed.  The
        global slot is still reserved immediately, so slow headers cannot
        bypass the global connection cap.
        """

        async with self._lock:
            if self._active >= self.max_connections:
                return None, RejectionReason.GLOBAL_LIMIT
            normalized: str | None = None
            if not defer_peer_limits:
                normalized, rejection = self._try_apply_peer_limits(
                    peer_ip,
                    now=self.clock.monotonic(),
                )
                if rejection is not None:
                    return None, rejection
            self._active += 1
            return ConnectionLease(normalized), None

    async def apply_peer_limits(
        self,
        lease: ConnectionLease,
        peer_ip: str,
    ) -> RejectionReason | None:
        """Attach authenticated client-IP accounting to a deferred lease."""

        async with self._lock:
            if lease.released or self._active <= 0:
                raise RuntimeError("cannot update an inactive connection lease")
            if lease.peer_ip is not None:
                raise RuntimeError("connection lease already has source-IP accounting")
            normalized, rejection = self._try_apply_peer_limits(
                peer_ip,
                now=self.clock.monotonic(),
            )
            if rejection is None:
                lease.peer_ip = normalized
            return rejection

    async def release(self, lease: ConnectionLease) -> None:
        async with self._lock:
            if lease.released:
                raise RuntimeError("connection lease released more than once")
            if self._active <= 0:
                raise RuntimeError("connection limiter state is inconsistent")
            lease.released = True
            self._active -= 1
            if lease.peer_ip is None:
                return
            current = self._per_ip.get(lease.peer_ip)
            if current is None or current <= 0:
                raise RuntimeError("connection limiter state is inconsistent")
            if current == 1:
                del self._per_ip[lease.peer_ip]
            else:
                self._per_ip[lease.peer_ip] = current - 1
