"""Process-wide runtime counters and graceful task tracking."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Coroutine
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar

from .models import RejectionReason, TargetName
from .time_utils import SYSTEM_CLOCK, Clock, non_negative_elapsed

_T = TypeVar("_T")
log = logging.getLogger("mc-failover.runtime")


@dataclass(slots=True)
class RuntimeState:
    """Mutable state owned by a single asyncio event loop."""

    clock: Clock = SYSTEM_CLOCK
    started_monotonic: float = field(init=False)
    started_at: datetime = field(init=False)
    active_connections: int = 0
    incoming_connections_total: int = 0
    backend_connections_established_total: int = 0
    connections_rejected_total: int = 0
    # Deprecated admission counter retained for monitoring compatibility.
    total_connections: int = 0
    monitoring_rejected_connections: int = 0
    main_connect_failures: int = 0
    fallback_connect_failures: int = 0
    main_connect_successes: int = 0
    fallback_connect_successes: int = 0
    shutting_down: bool = False
    rejection_reasons: Counter[RejectionReason] = field(default_factory=Counter)

    def __post_init__(self) -> None:
        self.started_monotonic = self.clock.monotonic()
        self.started_at = self.clock.utc_now()

    @property
    def uptime_seconds(self) -> float:
        elapsed = non_negative_elapsed(self.clock.monotonic(), self.started_monotonic)
        return 0.0 if elapsed is None else elapsed

    @property
    def rejected_connections(self) -> int:
        """Deprecated JSON/runtime alias for connections_rejected_total."""

        return self.connections_rejected_total

    @rejected_connections.setter
    def rejected_connections(self, value: int) -> None:
        self.connections_rejected_total = value

    def incoming_connection_received(self) -> None:
        self.incoming_connections_total += 1

    def connection_admitted(self) -> None:
        """Record a granted global limiter lease for the legacy counter."""

        self.total_connections += 1

    def backend_connection_started(self) -> None:
        """Record a backend prepared for relay and mark its session active."""

        self.backend_connections_established_total += 1
        self.active_connections += 1

    def backend_connection_finished(self) -> None:
        self.active_connections = max(0, self.active_connections - 1)

    def connection_started(self) -> None:
        """Compatibility helper for callers that treated admission as establishment."""

        self.connection_admitted()
        self.backend_connection_started()

    def connection_finished(self) -> None:
        """Compatibility alias for backend_connection_finished()."""

        self.backend_connection_finished()

    def reject(self, reason: RejectionReason) -> None:
        if reason is RejectionReason.MONITORING_LIMIT:
            self.monitoring_rejected_connections += 1
            return
        self.connections_rejected_total += 1
        self.rejection_reasons[reason] += 1

    def connect_succeeded(self, target: TargetName) -> None:
        if target is TargetName.MAIN:
            self.main_connect_successes += 1
        elif target is TargetName.FALLBACK:
            self.fallback_connect_successes += 1

    def connect_failed(self, target: TargetName) -> None:
        if target is TargetName.MAIN:
            self.main_connect_failures += 1
        elif target is TargetName.FALLBACK:
            self.fallback_connect_failures += 1


class TaskTracker:
    """Own a bounded set of tasks and always retrieve their exceptions."""

    def __init__(self, *, name: str) -> None:
        self.name = name
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closed = False

    @property
    def tasks(self) -> frozenset[asyncio.Task[Any]]:
        return frozenset(self._tasks)

    @property
    def active_count(self) -> int:
        return len(self._tasks)

    def create(self, coroutine: Coroutine[object, object, _T]) -> asyncio.Task[_T]:
        if self._closed:
            coroutine.close()
            raise RuntimeError(f"task tracker {self.name!r} is closed")
        task = asyncio.create_task(coroutine, name=f"{self.name}-{len(self._tasks) + 1}")
        self._tasks.add(task)
        task.add_done_callback(self._discard)
        return task

    def _discard(self, task: asyncio.Task[Any]) -> None:
        if task not in self._tasks:
            return
        self._tasks.discard(task)
        if not task.cancelled():
            exception = task.exception()
            if exception is not None:
                log.error(
                    "Tracked task failed tracker=%s task=%s",
                    self.name,
                    task.get_name(),
                    exc_info=(type(exception), exception, exception.__traceback__),
                )

    async def drain(self, grace_seconds: float, cancel_timeout_seconds: float) -> None:
        self._closed = True
        if self._tasks and grace_seconds > 0:
            done, pending = await asyncio.wait(self._tasks, timeout=grace_seconds)
        else:
            done = set()
            pending = set(self._tasks)
        for task in done:
            self._discard(task)
        for task in pending:
            task.cancel()
        if pending:
            # Give cancellation-safe coroutines one scheduling turn even when
            # the configured cleanup timeout is zero.
            await asyncio.sleep(0)
            done = {task for task in pending if task.done()}
            still_pending = pending - done
            if still_pending and cancel_timeout_seconds > 0:
                newly_done, still_pending = await asyncio.wait(
                    still_pending,
                    timeout=cancel_timeout_seconds,
                )
                done.update(newly_done)
            for task in done:
                self._discard(task)
            if still_pending:
                names = ",".join(sorted(task.get_name() for task in still_pending))
                log.error(
                    "Tasks ignored cancellation after shutdown timeout tracker=%s tasks=%s",
                    self.name,
                    names,
                )

    async def cancel_all(self, timeout_seconds: float) -> None:
        await self.drain(0.0, timeout_seconds)

    async def wait_remaining(self, timeout_seconds: float) -> frozenset[asyncio.Task[Any]]:
        """Collect tasks unblocked by owner-specific resource cleanup."""

        if not self._tasks:
            return frozenset()
        await asyncio.sleep(0)
        done = {task for task in self._tasks if task.done()}
        pending = set(self._tasks) - done
        if pending and timeout_seconds > 0:
            newly_done, pending = await asyncio.wait(pending, timeout=timeout_seconds)
            done.update(newly_done)
        for task in done:
            self._discard(task)
        return frozenset(pending)
