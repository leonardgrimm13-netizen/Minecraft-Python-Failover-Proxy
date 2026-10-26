"""Structured routing decisions for automatic and maintenance modes."""

from __future__ import annotations

import asyncio
import logging
import stat
from dataclasses import dataclass
from pathlib import Path

from .circuit_breaker import CircuitBreaker, CircuitPermit
from .config import AppConfig, MaintenanceConfig
from .health import HealthState
from .models import (
    CircuitState,
    MaintenanceMode,
    RoutingReason,
    Target,
    TargetName,
)
from .registry import BackendRegistry, BackendRuntime

log = logging.getLogger("mc-failover.routing")


@dataclass(frozen=True, slots=True)
class MaintenanceSnapshot:
    mode: MaintenanceMode
    source: str


class MaintenanceWatcher:
    """Cache maintenance files so filesystem I/O never runs in connection handlers."""

    def __init__(self, config: MaintenanceConfig) -> None:
        self.config = config
        self._snapshot = MaintenanceSnapshot(config.mode, "config")

    @property
    def snapshot(self) -> MaintenanceSnapshot:
        return self._snapshot

    async def refresh(self) -> MaintenanceSnapshot:
        if self.config.mode is not MaintenanceMode.AUTO:
            self._snapshot = MaintenanceSnapshot(self.config.mode, "config")
            return self._snapshot

        fallback_file = self.config.force_fallback_file
        main_file = self.config.force_main_file

        def flag_exists(path: Path | None) -> bool:
            if path is None:
                return False
            try:
                return stat.S_ISREG(path.stat().st_mode)
            except (FileNotFoundError, NotADirectoryError):
                return False

        def check_files() -> tuple[bool, bool]:
            # Preserve cached policy on access/I/O errors, including Python
            # 3.14 where Path.is_file() silently suppresses those errors.
            if flag_exists(fallback_file):
                return True, False
            return False, flag_exists(main_file)

        fallback_exists, main_exists = await asyncio.to_thread(check_files)
        if fallback_exists:
            current = MaintenanceSnapshot(MaintenanceMode.FORCE_FALLBACK, "force_fallback_file")
        elif main_exists:
            current = MaintenanceSnapshot(MaintenanceMode.FORCE_MAIN, "force_main_file")
        else:
            current = MaintenanceSnapshot(MaintenanceMode.AUTO, "auto")
        self._snapshot = current
        return current

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except OSError as exc:
                log.warning("Maintenance file check failed error=%s", type(exc).__name__)
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self.config.file_check_interval_seconds
                )
            except asyncio.TimeoutError:
                continue


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    target: Target | None
    requested_target: TargetName
    reason: RoutingReason
    maintenance_mode: MaintenanceMode
    maintenance_source: str
    ready: bool
    degraded: bool
    circuit_permit: CircuitPermit | None = None

    @property
    def backend_id(self) -> str | None:
        return self.target.id if self.target is not None else None

    @property
    def active_target(self) -> TargetName:
        return self.target.name if self.target is not None else TargetName.NONE


class Router:
    """One priority policy for legacy and explicit backend configurations.

    Smaller priorities win; equal priorities retain declaration order. Forced
    modes only consider their named backend and always fail closed.
    """

    def __init__(
        self,
        config: AppConfig,
        main_health: HealthState,
        fallback_health: HealthState,
        circuit_breaker: CircuitBreaker,
        maintenance: MaintenanceWatcher,
        *,
        registry: BackendRegistry | None = None,
    ) -> None:
        self.config = config
        self.main_health = main_health
        self.fallback_health = fallback_health
        self.circuit_breaker = circuit_breaker
        self.maintenance = maintenance
        self.registry = registry or BackendRegistry(
            config,
            main_health=main_health,
            fallback_health=fallback_health,
            circuit_breaker=circuit_breaker,
            clock=main_health.clock,
        )

    def target(self, name: TargetName) -> Target:
        entry = self.registry.entries.get(name.value)
        if entry is not None:
            return entry.target
        configured = self.config.main if name is TargetName.MAIN else self.config.fallback
        return Target(name, configured.host, configured.port, name.value)

    def _forced_id(self, mode: MaintenanceMode) -> str | None:
        if mode is MaintenanceMode.FORCE_MAIN:
            return "MAIN"
        if mode is MaintenanceMode.FORCE_FALLBACK:
            return "FALLBACK"
        if mode is MaintenanceMode.FORCE_TARGET:
            return self.config.maintenance.force_target
        return None

    def _candidates(self, mode: MaintenanceMode) -> tuple[BackendRuntime, ...]:
        if mode is MaintenanceMode.AUTO:
            return self.registry.ordered
        forced = self._forced_id(mode)
        entry = self.registry.entries.get(forced) if forced is not None else None
        return (entry,) if entry is not None else ()

    def _reason(
        self, entry: BackendRuntime, mode: MaintenanceMode, *, probe: bool = False
    ) -> RoutingReason:
        if mode is MaintenanceMode.FORCE_TARGET:
            return RoutingReason.FORCE_TARGET
        if mode is MaintenanceMode.FORCE_FALLBACK:
            return RoutingReason.FORCE_FALLBACK
        if probe and entry.config.id == "MAIN":
            return RoutingReason.MAIN_CIRCUIT_HALF_OPEN
        if mode is MaintenanceMode.FORCE_MAIN:
            return RoutingReason.FORCE_MAIN
        if entry.config.id == "MAIN":
            return RoutingReason.MAIN_HEALTHY
        main = self.registry.entries.get("MAIN")
        if main is None:
            return RoutingReason.BACKEND_HEALTHY
        return (
            RoutingReason.MAIN_CIRCUIT_OPEN_FALLBACK_HEALTHY
            if main.circuit.snapshot_now().state is not CircuitState.CLOSED
            else RoutingReason.MAIN_UNAVAILABLE_FALLBACK_HEALTHY
        )

    def _decision(
        self,
        entry: BackendRuntime | None,
        mode: MaintenanceSnapshot,
        *,
        permit: CircuitPermit | None = None,
        shutting_down: bool = False,
    ) -> RoutingDecision:
        if shutting_down:
            reason, requested = RoutingReason.SHUTTING_DOWN, TargetName.NONE
        elif entry is not None:
            reason = self._reason(entry, mode.mode, probe=bool(permit and permit.probe))
            requested = entry.target.name
        elif mode.mode is MaintenanceMode.FORCE_MAIN:
            reason, requested = RoutingReason.FORCED_MAIN_UNAVAILABLE, TargetName.MAIN
        elif mode.mode is MaintenanceMode.FORCE_FALLBACK:
            reason, requested = RoutingReason.FORCED_FALLBACK_UNAVAILABLE, TargetName.FALLBACK
        elif mode.mode is MaintenanceMode.FORCE_TARGET:
            reason, requested = RoutingReason.FORCED_TARGET_UNAVAILABLE, TargetName.NONE
        else:
            reason, requested = RoutingReason.NO_TARGET_AVAILABLE, TargetName.NONE
        preferred = next(
            (candidate for candidate in self.registry.ordered if candidate.config.enabled), None
        )
        degraded = (
            entry is None
            or mode.mode is not MaintenanceMode.AUTO
            or entry is not preferred
            or bool(permit and permit.probe)
            or any(
                candidate.health.healthy is not True
                for candidate in self.registry.ordered
                if candidate.config.enabled
            )
        )
        return RoutingDecision(
            entry.target if entry else None,
            requested,
            reason,
            mode.mode,
            mode.source,
            entry is not None,
            degraded,
            permit,
        )

    def snapshot(self, *, shutting_down: bool = False) -> RoutingDecision:
        mode = self.maintenance.snapshot
        if shutting_down:
            return self._decision(None, mode, shutting_down=True)
        for entry in self._candidates(mode.mode):
            if entry.routable and entry.circuit.snapshot_now().state is CircuitState.CLOSED:
                return self._decision(entry, mode)
        return self._decision(None, mode)

    async def select_for_connection(
        self, *, shutting_down: bool = False, excluded: frozenset[str] = frozenset()
    ) -> RoutingDecision:
        mode = self.maintenance.snapshot
        if shutting_down:
            return self._decision(None, mode, shutting_down=True)
        for entry in self._candidates(mode.mode):
            if entry.config.id in excluded or not entry.routable:
                continue
            permit = await entry.circuit.acquire()
            if permit.allowed:
                return self._decision(entry, mode, permit=permit if entry.circuit.enabled else None)
        return self._decision(None, mode)
