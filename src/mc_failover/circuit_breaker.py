"""Concurrency-safe passive connection failure circuit breaker."""

from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from .models import CircuitState
from .time_utils import SYSTEM_CLOCK, Clock, as_utc


class CircuitBreakerSettings(Protocol):
    """Structural type accepted by :class:`CircuitBreaker`.

    ``CircuitBreakerConfig`` implements this protocol without coupling this
    state machine to the configuration parser.
    """

    @property
    def enabled(self) -> bool: ...

    @property
    def failure_threshold(self) -> int: ...

    @property
    def failure_window_seconds(self) -> float: ...

    @property
    def open_seconds(self) -> float: ...

    @property
    def half_open_max_attempts(self) -> int: ...


@dataclass(frozen=True, slots=True)
class CircuitPermit:
    """Immutable result of an attempt to acquire circuit capacity."""

    allowed: bool
    probe: bool
    _generation: int | None = field(default=None, repr=False, compare=False)
    _probe_id: int | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class CircuitSnapshot:
    """Immutable, point-in-time monitoring view of a circuit breaker."""

    state: CircuitState
    opened_at: datetime | None
    retry_after_seconds: float
    failures_in_window: int
    half_open_in_flight: int
    open_total: int


class CircuitBreaker:
    """Bounded sliding-window circuit breaker for real upstream connects.

    All state transitions happen under one ``asyncio.Lock``. Permits carry a
    private generation marker so a connect result that arrives after a newer
    transition cannot corrupt that newer state.
    """

    def __init__(
        self,
        config: CircuitBreakerSettings | None = None,
        *,
        enabled: bool | None = None,
        failure_threshold: int | None = None,
        failure_window_seconds: float | None = None,
        open_seconds: float | None = None,
        half_open_max_attempts: int | None = None,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        explicit_values = (
            enabled,
            failure_threshold,
            failure_window_seconds,
            open_seconds,
            half_open_max_attempts,
        )
        if config is not None:
            if any(value is not None for value in explicit_values):
                raise TypeError("pass either config or explicit circuit breaker values, not both")
            enabled = config.enabled
            failure_threshold = config.failure_threshold
            failure_window_seconds = config.failure_window_seconds
            open_seconds = config.open_seconds
            half_open_max_attempts = config.half_open_max_attempts
        elif any(value is None for value in explicit_values):
            raise TypeError("all explicit circuit breaker values are required")

        if not isinstance(enabled, bool):
            raise TypeError("enabled must be bool")
        if (
            not isinstance(failure_threshold, int)
            or isinstance(failure_threshold, bool)
            or failure_threshold < 1
        ):
            raise ValueError("failure_threshold must be an integer >= 1")
        if (
            isinstance(failure_window_seconds, bool)
            or not isinstance(failure_window_seconds, (int, float))
            or not math.isfinite(failure_window_seconds)
            or failure_window_seconds <= 0
        ):
            raise ValueError("failure_window_seconds must be > 0")
        if (
            isinstance(open_seconds, bool)
            or not isinstance(open_seconds, (int, float))
            or not math.isfinite(open_seconds)
            or open_seconds <= 0
        ):
            raise ValueError("open_seconds must be > 0")
        if (
            not isinstance(half_open_max_attempts, int)
            or isinstance(half_open_max_attempts, bool)
            or half_open_max_attempts < 1
        ):
            raise ValueError("half_open_max_attempts must be an integer >= 1")

        self.enabled = enabled
        self.failure_threshold = failure_threshold
        self.failure_window_seconds = float(failure_window_seconds)
        self.open_seconds = float(open_seconds)
        self.half_open_max_attempts = half_open_max_attempts
        self._clock = clock

        self._lock = asyncio.Lock()
        self._state = CircuitState.CLOSED
        self._failures: deque[float] = deque()
        self._opened_at: datetime | None = None
        self._open_deadline: float | None = None
        self._open_total = 0
        self._generation = 0
        self._next_probe_id = 0
        self._active_probe_ids: set[int] = set()

    async def acquire(self) -> CircuitPermit:
        """Acquire permission for one real connection attempt."""

        async with self._lock:
            if not self.enabled:
                return CircuitPermit(allowed=True, probe=False)

            now = self._clock.monotonic()
            self._prune_failures(now)
            if self._state is CircuitState.CLOSED:
                return CircuitPermit(
                    allowed=True,
                    probe=False,
                    _generation=self._generation,
                )

            if self._state is CircuitState.OPEN:
                if self._open_deadline is not None and now < self._open_deadline:
                    return CircuitPermit(allowed=False, probe=False)
                self._state = CircuitState.HALF_OPEN
                self._generation += 1
                self._active_probe_ids.clear()

            if len(self._active_probe_ids) >= self.half_open_max_attempts:
                return CircuitPermit(allowed=False, probe=False)

            self._next_probe_id += 1
            probe_id = self._next_probe_id
            self._active_probe_ids.add(probe_id)
            return CircuitPermit(
                allowed=True,
                probe=True,
                _generation=self._generation,
                _probe_id=probe_id,
            )

    async def record_failure(self, permit: CircuitPermit) -> None:
        """Record a failed attempt represented by ``permit``."""

        async with self._lock:
            if not self.enabled or not permit.allowed:
                return

            now = self._clock.monotonic()
            if permit.probe:
                if not self._consume_current_probe(permit):
                    return
                self._failures.append(now)
                self._prune_failures(now)
                self._open(now)
                return

            if self._state is not CircuitState.CLOSED or permit._generation != self._generation:
                return
            self._failures.append(now)
            self._prune_failures(now)
            if len(self._failures) >= self.failure_threshold:
                self._open(now)

    async def record_success(self, permit: CircuitPermit) -> None:
        """Record a successful real connection and reset eligible failures."""

        async with self._lock:
            if not self.enabled or not permit.allowed:
                return

            if permit.probe:
                if not self._consume_current_probe(permit):
                    return
                self._close()
                return

            if self._state is CircuitState.CLOSED and permit._generation == self._generation:
                self._failures.clear()

    async def release(self, permit: CircuitPermit) -> None:
        """Release an unresolved/cancelled half-open probe without a verdict."""

        async with self._lock:
            if not self.enabled or not permit.allowed or not permit.probe:
                return
            self._consume_current_probe(permit)

    async def record_health_success(self) -> None:
        """Recover an expired open circuit using a successful active check.

        A health result never shortens the configured open interval. Once that
        interval has elapsed, a fresh successful healthcheck is sufficient
        evidence to close either an open or half-open circuit.
        """

        async with self._lock:
            if not self.enabled or self._state is CircuitState.CLOSED:
                return
            now = self._clock.monotonic()
            if (
                self._state is CircuitState.OPEN
                and self._open_deadline is not None
                and now < self._open_deadline
            ):
                return
            self._close()

    async def snapshot(self) -> CircuitSnapshot:
        """Return a consistent monitoring snapshot."""

        async with self._lock:
            return self.snapshot_now()

    def snapshot_now(self) -> CircuitSnapshot:
        """Return a non-awaiting view for same-event-loop pure render paths.

        State mutations never yield while holding the internal lock, so a
        synchronous read is atomic within the owning asyncio event loop.
        """

        if not self.enabled:
            return CircuitSnapshot(
                state=CircuitState.CLOSED,
                opened_at=None,
                retry_after_seconds=0.0,
                failures_in_window=0,
                half_open_in_flight=0,
                open_total=0,
            )

        now = self._clock.monotonic()
        self._prune_failures(now)
        retry_after = 0.0
        if self._state is CircuitState.OPEN and self._open_deadline is not None:
            retry_after = max(0.0, self._open_deadline - now)
        return CircuitSnapshot(
            state=self._state,
            opened_at=self._opened_at,
            retry_after_seconds=retry_after,
            failures_in_window=len(self._failures),
            half_open_in_flight=len(self._active_probe_ids),
            open_total=self._open_total,
        )

    def _prune_failures(self, now: float) -> None:
        oldest_allowed = now - self.failure_window_seconds
        while self._failures and self._failures[0] < oldest_allowed:
            self._failures.popleft()

    def _consume_current_probe(self, permit: CircuitPermit) -> bool:
        if (
            self._state is not CircuitState.HALF_OPEN
            or permit._generation != self._generation
            or permit._probe_id is None
            or permit._probe_id not in self._active_probe_ids
        ):
            return False
        self._active_probe_ids.remove(permit._probe_id)
        return True

    def _open(self, now: float) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = as_utc(self._clock.utc_now())
        self._open_deadline = now + self.open_seconds
        self._open_total += 1
        self._generation += 1
        self._active_probe_ids.clear()

    def _close(self) -> None:
        self._state = CircuitState.CLOSED
        self._opened_at = None
        self._open_deadline = None
        self._failures.clear()
        self._generation += 1
        self._active_probe_ids.clear()
