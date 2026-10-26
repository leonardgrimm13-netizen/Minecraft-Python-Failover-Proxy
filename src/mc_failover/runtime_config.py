"""Immutable references captured once for each admitted connection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import AppConfig
    from .endpoint_safety import EndpointLoopGuard
    from .local_status import LocalStatusService
    from .registry import BackendRegistry
    from .routing import MaintenanceWatcher, Router


@dataclass(frozen=True, slots=True)
class RuntimeConfiguration:
    generation: int
    config: AppConfig
    registry: BackendRegistry
    router: Router
    maintenance: MaintenanceWatcher
    endpoint_guard: EndpointLoopGuard
    status_service: LocalStatusService
