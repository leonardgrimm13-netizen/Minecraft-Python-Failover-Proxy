"""Structured routing decisions for automatic and maintenance modes."""

from __future__ import annotations

import asyncio
import logging
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

        def check_files() -> tuple[bool, bool]:
            return (
                fallback_file is not None and Path(fallback_file).is_file(),
                main_file is not None and Path(main_file).is_file(),
            )

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
    def active_target(self) -> TargetName:
        return self.target.name if self.target is not None else TargetName.NONE


class Router:
    def __init__(
        self,
        config: AppConfig,
        main_health: HealthState,
        fallback_health: HealthState,
        circuit_breaker: CircuitBreaker,
        maintenance: MaintenanceWatcher,
    ) -> None:
        self.config = config
        self.main_health = main_health
        self.fallback_health = fallback_health
        self.circuit_breaker = circuit_breaker
        self.maintenance = maintenance

    def target(self, name: TargetName) -> Target:
        configured = self.config.main if name is TargetName.MAIN else self.config.fallback
        return Target(name, configured.host, configured.port)

    def snapshot(self, *, shutting_down: bool = False) -> RoutingDecision:
        mode = self.maintenance.snapshot
        if shutting_down:
            return RoutingDecision(
                None,
                TargetName.NONE,
                RoutingReason.SHUTTING_DOWN,
                mode.mode,
                mode.source,
                False,
                True,
            )
        circuit = self.circuit_breaker.snapshot_now()
        if mode.mode is MaintenanceMode.FORCE_MAIN:
            available = self.main_health.routable and circuit.state is CircuitState.CLOSED
            return RoutingDecision(
                self.target(TargetName.MAIN) if available else None,
                TargetName.MAIN,
                RoutingReason.FORCE_MAIN if available else RoutingReason.FORCED_MAIN_UNAVAILABLE,
                mode.mode,
                mode.source,
                available,
                True,
            )
        if mode.mode is MaintenanceMode.FORCE_FALLBACK:
            available = self.fallback_health.routable
            return RoutingDecision(
                self.target(TargetName.FALLBACK) if available else None,
                TargetName.FALLBACK,
                (
                    RoutingReason.FORCE_FALLBACK
                    if available
                    else RoutingReason.FORCED_FALLBACK_UNAVAILABLE
                ),
                mode.mode,
                mode.source,
                available,
                True,
            )
        if self.main_health.routable and circuit.state is CircuitState.CLOSED:
            return RoutingDecision(
                self.target(TargetName.MAIN),
                TargetName.MAIN,
                RoutingReason.MAIN_HEALTHY,
                mode.mode,
                mode.source,
                True,
                self.main_health.healthy is not True or self.fallback_health.healthy is not True,
            )
        if self.fallback_health.routable:
            reason = (
                RoutingReason.MAIN_CIRCUIT_OPEN_FALLBACK_HEALTHY
                if circuit.state is not CircuitState.CLOSED
                else RoutingReason.MAIN_UNAVAILABLE_FALLBACK_HEALTHY
            )
            return RoutingDecision(
                self.target(TargetName.FALLBACK),
                TargetName.FALLBACK,
                reason,
                mode.mode,
                mode.source,
                True,
                True,
            )
        return RoutingDecision(
            None,
            TargetName.NONE,
            RoutingReason.NO_TARGET_AVAILABLE,
            mode.mode,
            mode.source,
            False,
            True,
        )

    async def select_for_connection(self, *, shutting_down: bool = False) -> RoutingDecision:
        base = self.snapshot(shutting_down=shutting_down)
        if shutting_down:
            return base
        mode = self.maintenance.snapshot
        should_try_main = self.main_health.routable and mode.mode in {
            MaintenanceMode.AUTO,
            MaintenanceMode.FORCE_MAIN,
        }
        if should_try_main:
            permit = await self.circuit_breaker.acquire()
            if permit.allowed:
                reason = (
                    RoutingReason.MAIN_CIRCUIT_HALF_OPEN
                    if permit.probe
                    else (
                        RoutingReason.FORCE_MAIN
                        if mode.mode is MaintenanceMode.FORCE_MAIN
                        else RoutingReason.MAIN_HEALTHY
                    )
                )
                return RoutingDecision(
                    self.target(TargetName.MAIN),
                    TargetName.MAIN,
                    reason,
                    mode.mode,
                    mode.source,
                    True,
                    permit.probe
                    or mode.mode is MaintenanceMode.FORCE_MAIN
                    or self.main_health.healthy is not True
                    or self.fallback_health.healthy is not True,
                    permit,
                )
            if mode.mode is MaintenanceMode.FORCE_MAIN:
                return RoutingDecision(
                    None,
                    TargetName.MAIN,
                    RoutingReason.FORCED_MAIN_UNAVAILABLE,
                    mode.mode,
                    mode.source,
                    False,
                    True,
                    permit,
                )
        elif mode.mode is MaintenanceMode.FORCE_MAIN:
            return RoutingDecision(
                None,
                TargetName.MAIN,
                RoutingReason.FORCED_MAIN_UNAVAILABLE,
                mode.mode,
                mode.source,
                False,
                True,
            )
        if mode.mode is MaintenanceMode.FORCE_FALLBACK:
            return base
        if self.fallback_health.routable:
            reason = (
                RoutingReason.MAIN_CIRCUIT_OPEN_FALLBACK_HEALTHY
                if self.main_health.routable
                else RoutingReason.MAIN_UNAVAILABLE_FALLBACK_HEALTHY
            )
            return RoutingDecision(
                self.target(TargetName.FALLBACK),
                TargetName.FALLBACK,
                reason,
                mode.mode,
                mode.source,
                True,
                True,
            )
        return RoutingDecision(
            None,
            TargetName.NONE,
            RoutingReason.NO_TARGET_AVAILABLE,
            mode.mode,
            mode.source,
            False,
            True,
        )
