"""Generation-owned backend policy with bounded, independently monitored state.

The event loop owns all synchronous state updates. Circuit transitions retain
their existing lock and generation checks. Old sessions keep their entry alive
after a reload, so removing a backend never closes unrelated sockets.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import deque
from dataclasses import dataclass, field, replace

from .circuit_breaker import CircuitBreaker
from .config import AppConfig, BackendConfig, TargetConfig
from .endpoint_safety import EndpointLoopGuard
from .health import HealthMonitor, HealthState
from .models import HealthCheckResult, HealthStatus, Target, TargetName
from .time_utils import SYSTEM_CLOCK, Clock

log = logging.getLogger("mc-failover.registry")


@dataclass(slots=True)
class BackendStatistics:
    active_connections: int = 0
    connect_failures: int = 0
    connect_successes: int = 0
    passive_failures: int = 0
    failures: deque[float] = field(default_factory=deque)
    quarantine_until: float | None = None
    recovery_started: float | None = None
    recovery_successes: int = 0
    last_check_request: float | None = None
    last_passive_reason: str | None = None


class BackendRuntime:
    def __init__(
        self,
        config: BackendConfig,
        health: HealthState,
        circuit: CircuitBreaker,
        monitor: HealthMonitor,
        *,
        statistics: BackendStatistics | None = None,
        passive_enabled: bool = True,
        passive_failure_window_seconds: float = 30.0,
    ) -> None:
        self.config = config
        self.health = health
        self.circuit = circuit
        self.monitor = monitor
        self.statistics = statistics or BackendStatistics()
        self._passive_enabled = passive_enabled
        self._passive_failure_window_seconds = passive_failure_window_seconds
        self.check_requested = asyncio.Event()

    @property
    def target(self) -> Target:
        role = TargetName.MAIN if self.config.id == "MAIN" else TargetName.FALLBACK
        return Target(role, self.config.host, self.config.port, self.config.id)

    @property
    def routable(self) -> bool:
        if not self._passive_enabled:
            return self.config.enabled and self.health.routable
        until = self.statistics.quarantine_until
        if until is not None and not self.health.config.enabled:
            if self.health.clock.monotonic() >= until:
                self.statistics.quarantine_until = None
        return (
            self.config.enabled
            and self.health.routable
            and self.statistics.quarantine_until is None
        )

    @property
    def quarantine_remaining_seconds(self) -> float:
        if not self._passive_enabled:
            return 0.0
        until = self.statistics.quarantine_until
        return max(0.0, until - self.health.clock.monotonic()) if until is not None else 0.0

    @property
    def passive_failures_in_window(self) -> int:
        oldest = self.health.clock.monotonic() - self._passive_failure_window_seconds
        while self.statistics.failures and self.statistics.failures[0] < oldest:
            self.statistics.failures.popleft()
        return len(self.statistics.failures)

    @property
    def active_connections(self) -> int:
        return self.statistics.active_connections

    @active_connections.setter
    def active_connections(self, value: int) -> None:
        self.statistics.active_connections = value

    @property
    def connect_failures(self) -> int:
        return self.statistics.connect_failures

    @connect_failures.setter
    def connect_failures(self, value: int) -> None:
        self.statistics.connect_failures = value

    @property
    def connect_successes(self) -> int:
        return self.statistics.connect_successes

    @connect_successes.setter
    def connect_successes(self, value: int) -> None:
        self.statistics.connect_successes = value

    @property
    def passive_failures(self) -> int:
        return self.statistics.passive_failures

    def report_active(self, result: HealthCheckResult) -> None:
        stats = self.statistics
        _ = self.passive_failures_in_window
        until = stats.quarantine_until
        if until is None:
            return
        if not result.ok:
            stats.recovery_started = None
            stats.recovery_successes = 0
            return
        now = self.health.clock.monotonic()
        if now < until:
            return
        if stats.recovery_started is None:
            stats.recovery_started = now
        stats.recovery_successes += 1
        if (
            stats.recovery_successes >= self.config.healthcheck.recover_after
            and now - stats.recovery_started >= self.config.healthcheck.min_recovery_seconds
        ):
            stats.quarantine_until = None
            stats.recovery_started = None
            stats.recovery_successes = 0
            stats.failures.clear()


class BackendRegistry:
    def __init__(
        self,
        config: AppConfig,
        *,
        clock: Clock = SYSTEM_CLOCK,
        endpoint_guard: EndpointLoopGuard | None = None,
        main_health: HealthState | None = None,
        fallback_health: HealthState | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        previous: BackendRegistry | None = None,
    ) -> None:
        self.config = config
        self.clock = clock
        self.entries: dict[str, BackendRuntime] = {}
        self._reused: set[str] = set()
        self._semaphore = asyncio.Semaphore(config.routing.max_parallel_healthchecks)
        for backend in config.effective_backends:
            old = previous.entries.get(backend.id) if previous is not None else None
            statistics: BackendStatistics | None
            policy_only = (
                old is not None
                and replace(
                    old.config,
                    priority=backend.priority,
                    connect_timeout_seconds=backend.connect_timeout_seconds,
                )
                == backend
            )
            if policy_only and old is not None:
                health, circuit, statistics = old.health, old.circuit, old.statistics
                self._reused.add(backend.id)
            else:
                injected_health = (
                    main_health
                    if backend.id == "MAIN" and not config.backends
                    else fallback_health
                    if backend.id == "FALLBACK" and not config.backends
                    else None
                )
                health = injected_health or HealthState(
                    backend.id, backend.healthcheck, clock=clock
                )
                circuit = (
                    circuit_breaker
                    if backend.id == "MAIN" and circuit_breaker is not None and not config.backends
                    else CircuitBreaker(backend.circuit_breaker, clock=clock)
                )
                statistics = (
                    old.statistics
                    if old is not None
                    and (old.config.host, old.config.port) == (backend.host, backend.port)
                    else None
                )
            monitor = HealthMonitor(
                TargetConfig(backend.host, backend.port), health, endpoint_guard=endpoint_guard
            )
            self.entries[backend.id] = BackendRuntime(
                backend,
                health,
                circuit,
                monitor,
                statistics=statistics,
                passive_enabled=config.passive_health.enabled,
                passive_failure_window_seconds=config.passive_health.failure_window_seconds,
            )
            entry = self.entries[backend.id]

            async def on_result(result: HealthCheckResult, entry: BackendRuntime = entry) -> None:
                entry.report_active(result)
                if result.ok:
                    await entry.circuit.record_health_success()

            monitor.on_result = on_result
        self.ordered = tuple(sorted(self.entries.values(), key=lambda entry: entry.config.priority))

    async def _check(
        self, entry: BackendRuntime, *, initial: bool = False, preparing: bool = False
    ) -> HealthCheckResult:
        async with self._semaphore:
            callback = entry.monitor.on_result
            if preparing:
                # Fresh per-generation monitors allow staging candidate health
                # without mutating counters/quarantine shared with live sessions.
                entry.monitor.on_result = None
            try:
                result = await entry.monitor.check_once(initial=initial)
            finally:
                if preparing:
                    entry.monitor.on_result = callback
        return result

    def _report_active(self, entry: BackendRuntime, result: HealthCheckResult) -> None:
        entry.report_active(result)

    def _prune(self, entry: BackendRuntime) -> None:
        oldest = self.clock.monotonic() - self.config.passive_health.failure_window_seconds
        while entry.statistics.failures and entry.statistics.failures[0] < oldest:
            entry.statistics.failures.popleft()

    def record_passive_failure(self, backend_id: str, reason: str) -> bool:
        """Record proven backend I/O evidence, never bare EOF/client/idle events.

        Callers supply a bounded classification value, rather than exception
        text. No tasks or network operations are started by an observation.
        """
        entry = self.entries.get(backend_id)
        if entry is None or not self.config.passive_health.enabled:
            return False
        stats = entry.statistics
        stats.passive_failures += 1
        stats.last_passive_reason = reason[:64]
        self._prune(entry)
        threshold = self.config.passive_health.failure_threshold
        stats.failures.append(self.clock.monotonic())
        while len(stats.failures) > threshold:
            stats.failures.popleft()
        self.request_check(backend_id)
        if len(stats.failures) < threshold:
            return False
        if stats.quarantine_until is None:
            stats.quarantine_until = (
                self.clock.monotonic() + self.config.passive_health.quarantine_seconds
            )
            stats.recovery_started = None
            stats.recovery_successes = 0
        return True

    def request_check(self, backend_id: str) -> None:
        """Coalesce weak evidence into an existing, rate-bounded health worker."""
        entry = self.entries.get(backend_id)
        if entry is None or not entry.config.enabled or not entry.config.healthcheck.enabled:
            return
        now = self.clock.monotonic()
        last = entry.statistics.last_check_request
        if last is None or now - last >= min(1.0, entry.config.healthcheck.interval_seconds):
            entry.statistics.last_check_request = now
            entry.check_requested.set()

    async def check_all(
        self, *, initial: bool = False, include_reused: bool = False, preparing: bool = False
    ) -> None:
        await asyncio.gather(
            *(
                self._check(entry, initial=initial, preparing=preparing)
                for entry in self.ordered
                if entry.config.enabled and (include_reused or entry.config.id not in self._reused)
            )
        )

    async def _run_entry(self, entry: BackendRuntime, stop_event: asyncio.Event) -> None:
        check = entry.config.healthcheck
        if not entry.config.enabled or not check.enabled:
            return
        while not stop_event.is_set():
            delay = check.interval_seconds + (
                random.SystemRandom().uniform(0, check.jitter_seconds)
                if check.jitter_seconds
                else 0
            )
            try:
                await asyncio.wait_for(entry.check_requested.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            entry.check_requested.clear()
            if stop_event.is_set():
                return
            try:
                await self._check(entry, initial=entry.health.status is HealthStatus.UNKNOWN)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Backend health worker failed id=%s", entry.config.id)

    async def run(self, stop_event: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(
                self._run_entry(entry, stop_event), name=f"health-{entry.config.id}"
            )
            for entry in self.ordered
        ]
        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
