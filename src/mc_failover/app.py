"""Top-level ownership of listeners, health tasks and graceful shutdown."""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from .circuit_breaker import CircuitBreaker
from .config import AppConfig, ConfigError, load_config, validate_config
from .endpoint_safety import EndpointLoopGuard
from .health import HealthMonitor, HealthState
from .limits import ConnectionLimiter
from .local_status import LocalStatusService
from .models import TargetName
from .monitoring import MonitoringServer
from .proxy import ProxyServer
from .registry import BackendRegistry
from .routing import MaintenanceWatcher, Router
from .runtime import RuntimeState, TaskTracker
from .runtime_config import RuntimeConfiguration
from .time_utils import SYSTEM_CLOCK, Clock

log = logging.getLogger("mc-failover.app")


class FailoverApplication:
    def __init__(
        self, config: AppConfig, *, clock: Clock = SYSTEM_CLOCK, config_path: Path | None = None
    ) -> None:
        self.config = config
        self.config_path = config_path
        self.runtime = RuntimeState(clock=clock)
        self.stop_event = asyncio.Event()
        self.main_health = HealthState(TargetName.MAIN, config.healthcheck, clock=clock)
        self.fallback_health = HealthState(
            TargetName.FALLBACK, config.fallback_healthcheck, clock=clock
        )
        self.circuit_breaker = CircuitBreaker(config.circuit_breaker, clock=clock)
        self.maintenance = MaintenanceWatcher(config.maintenance)
        self.endpoint_guard = EndpointLoopGuard(config)
        self.registry = BackendRegistry(
            config,
            clock=clock,
            endpoint_guard=self.endpoint_guard,
            main_health=self.main_health,
            fallback_health=self.fallback_health,
            circuit_breaker=self.circuit_breaker,
        )
        main_entry = self.registry.entries.get("MAIN")
        fallback_entry = self.registry.entries.get("FALLBACK")
        if main_entry is not None:
            self.main_health, self.circuit_breaker = main_entry.health, main_entry.circuit
        if fallback_entry is not None:
            self.fallback_health = fallback_entry.health
        self.router = Router(
            config,
            self.main_health,
            self.fallback_health,
            self.circuit_breaker,
            self.maintenance,
            registry=self.registry,
        )
        self.limiter = ConnectionLimiter(
            config.connection.max_connections,
            config.connection.max_connections_per_ip,
            config.connection.new_connections_per_second,
            config.connection.new_connections_burst,
            clock=clock,
        )
        self.proxy = ProxyServer(
            config,
            self.runtime,
            self.router,
            self.main_health,
            self.fallback_health,
            self.circuit_breaker,
            self.limiter,
            self.endpoint_guard,
        )
        self.monitoring = MonitoringServer(
            config,
            self.runtime,
            self.main_health,
            self.fallback_health,
            self.circuit_breaker,
            self.router,
            self.endpoint_guard,
        )
        self.background = TaskTracker(name="background")
        self._start_lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()
        self._startup_task: asyncio.Task[None] | None = None
        self._started = False
        self._shutdown_complete = False

        self.main_monitor = (
            self.registry.entries["MAIN"].monitor
            if "MAIN" in self.registry.entries
            else HealthMonitor(config.main, self.main_health)
        )
        self.fallback_monitor = (
            self.registry.entries["FALLBACK"].monitor
            if "FALLBACK" in self.registry.entries
            else HealthMonitor(config.fallback, self.fallback_health)
        )
        self._reload_lock = asyncio.Lock()
        self._reload_event = asyncio.Event()
        self._reload_preparation: asyncio.Task[RuntimeConfiguration] | None = None
        self._generation_stop = asyncio.Event()
        self._generation_tasks: tuple[asyncio.Task[None], ...] = ()
        self.snapshot = RuntimeConfiguration(
            1,
            config,
            self.registry,
            self.router,
            self.maintenance,
            self.endpoint_guard,
            LocalStatusService(config.status),
        )
        self.proxy.activate(self.snapshot)

    async def start(self) -> None:
        async with self._start_lock:
            await self._start_locked()

    async def _start_locked(self) -> None:
        if self._started:
            raise RuntimeError("application is already started")
        if self.stop_event.is_set():
            self._started = True
            return

        async def prepare() -> None:
            await self.maintenance.refresh()
            await self.endpoint_guard.validate_all()
            await self.registry.check_all(initial=True)

        self._startup_task = asyncio.create_task(prepare(), name="initial-healthchecks")
        try:
            await self._startup_task
        except asyncio.CancelledError:
            if not self.stop_event.is_set():
                raise
            self._started = True
            return
        finally:
            self._startup_task = None
        if self.stop_event.is_set():
            self._started = True
            return
        proxy_server: asyncio.Server | None = None
        monitoring_server: asyncio.Server | None = None

        async def open_listeners() -> None:
            nonlocal proxy_server, monitoring_server
            try:
                proxy_server = await self.proxy.start()
                monitoring_server = await self.monitoring.start()
            except BaseException:
                await self.proxy.stop_accepting()
                await self.monitoring.stop()
                await self.proxy.wait_listener_closed()
                raise

        self._startup_task = asyncio.create_task(open_listeners(), name="listener-startup")
        try:
            await self._startup_task
        except asyncio.CancelledError:
            if not self.stop_event.is_set():
                raise
            self._started = True
            return
        finally:
            self._startup_task = None

        if self.stop_event.is_set():
            await self.proxy.stop_accepting()
            await self.monitoring.stop()
            await self.proxy.wait_listener_closed()
            self._started = True
            return

        if proxy_server is None:
            raise RuntimeError("proxy listener startup returned no server")

        self._generation_tasks = (
            self.background.create(self.registry.run(self._generation_stop)),
            self.background.create(self.maintenance.run(self._generation_stop)),
        )
        self.background.create(self._reload_worker())
        self._started = True
        proxy_sockets = ", ".join(str(sock.getsockname()) for sock in proxy_server.sockets or [])
        log.info("Proxy listening sockets=%s", proxy_sockets)
        if monitoring_server is not None:
            monitoring_sockets = ", ".join(
                str(sock.getsockname()) for sock in monitoring_server.sockets or []
            )
            log.info("Monitoring listening sockets=%s", monitoring_sockets)

    def request_reload(self) -> None:
        """Coalesce local SIGHUP requests into one bounded worker."""
        if not self.stop_event.is_set():
            self._reload_event.set()

    async def _reload_worker(self) -> None:
        while not self.stop_event.is_set():
            await self._reload_event.wait()
            self._reload_event.clear()
            if not self.stop_event.is_set():
                await self.reload_config()

    def _check_reload_lifecycle(self, candidate: AppConfig) -> None:
        old = self.config
        if candidate.proxy != old.proxy:
            raise ConfigError("restart_required: proxy listener settings")
        for name in ("enabled", "listen_host", "listen_port", "max_connections"):
            if getattr(candidate.monitoring, name) != getattr(old.monitoring, name):
                raise ConfigError(f"restart_required: monitoring.{name}")
        if candidate.proxy_protocol != old.proxy_protocol:
            raise ConfigError("restart_required: PROXY protocol trust and framing settings")
        for name in (
            "max_connections",
            "max_connections_per_ip",
            "new_connections_per_second",
            "new_connections_burst",
        ):
            if getattr(candidate.connection, name) != getattr(old.connection, name):
                raise ConfigError(f"restart_required: connection.{name}")

    async def _prepare_reload(self, source: AppConfig | Path | None) -> RuntimeConfiguration:
        if isinstance(source, AppConfig):
            candidate = source
            validate_config(candidate)
        else:
            path = source or self.config_path
            if path is None:
                raise ConfigError("reload requires a configuration file path")
            candidate = await asyncio.to_thread(load_config, path)
        self._check_reload_lifecycle(candidate)
        guard = EndpointLoopGuard(candidate)
        for name, listener in (
            ("proxy", self.proxy.server),
            ("monitoring", self.monitoring.server),
        ):
            if listener is not None:
                from .endpoint_safety import sockaddr_host

                guard.register_bound_addresses(
                    name,
                    (
                        address
                        for sock in listener.sockets or ()
                        if (address := sockaddr_host(sock.getsockname())) is not None
                    ),
                )
        await guard.validate_all()
        maintenance = MaintenanceWatcher(candidate.maintenance)
        await maintenance.refresh()
        registry = BackendRegistry(
            candidate, clock=self.runtime.clock, endpoint_guard=guard, previous=self.registry
        )
        # Only new/changed resources are probed. Reused live health state remains
        # untouched until the prepared generation is committed.
        await registry.check_all(initial=True, preparing=True)
        main = registry.entries.get("MAIN")
        fallback = registry.entries.get("FALLBACK")
        router = Router(
            candidate,
            main.health
            if main is not None
            else HealthState(TargetName.MAIN, candidate.healthcheck, clock=self.runtime.clock),
            fallback.health
            if fallback is not None
            else HealthState(
                TargetName.FALLBACK, candidate.fallback_healthcheck, clock=self.runtime.clock
            ),
            main.circuit
            if main is not None
            else CircuitBreaker(candidate.circuit_breaker, clock=self.runtime.clock),
            maintenance,
            registry=registry,
        )
        return RuntimeConfiguration(
            self.runtime.config_generation + 1,
            candidate,
            registry,
            router,
            maintenance,
            guard,
            LocalStatusService(candidate.status),
        )

    @staticmethod
    def _reload_rejection_reason(error: Exception) -> str:
        """Expose bounded field names and error categories, never input values."""
        if not isinstance(error, ConfigError):
            return f"preparation_failed:{type(error).__name__}"
        message = str(error)
        if message.startswith("restart_required:"):
            return message
        if "TOML" in message or "UTF-8 enthalten" in message:
            return "invalid_toml"
        if "nicht gefunden" in message or "gelesen werden" in message:
            return "config_file_unreadable"
        field = re.search(r"\[([a-z_]+)\](?:\.([a-z_]+))?", message)
        if field is not None:
            return "invalid_setting:" + ".".join(value for value in field.groups() if value)
        if "Doppelte Backend-ID" in message:
            return "duplicate_backend_id"
        if "force_target" in message:
            return "invalid_setting:maintenance.force_target"
        return "validation_failed"

    async def reload_config(self, source: AppConfig | Path | None = None) -> bool:
        """Prepare completely, then commit without yielding to another task.

        Existing handlers retain their captured generation. There is no listener
        replacement and no socket belonging to an established session is closed.
        """
        async with self._reload_lock:
            if self.stop_event.is_set() or not self.config.reload.enabled:
                return False
            prepared_tasks: list[asyncio.Task[None]] = []
            activated = False
            try:
                self._reload_preparation = asyncio.create_task(
                    self._prepare_reload(source), name="config-reload-prepare"
                )
                snapshot = await self._reload_preparation
                if self.stop_event.is_set():
                    return False
                generation_stop = asyncio.Event()
                activate = asyncio.Event()

                async def run_registry() -> None:
                    await activate.wait()
                    await snapshot.registry.run(generation_stop)

                async def run_maintenance() -> None:
                    await activate.wait()
                    await snapshot.maintenance.run(generation_stop)

                prepared_tasks.append(self.background.create(run_registry()))
                prepared_tasks.append(self.background.create(run_maintenance()))
                old_tasks = self._generation_tasks
                old_stop = self._generation_stop
                # Atomic activation: all referenced objects are prepared and no
                # operation in this section awaits, opens sockets or reads files.
                self.snapshot = snapshot
                self.config = snapshot.config
                self.registry = snapshot.registry
                self.router = snapshot.router
                self.maintenance = snapshot.maintenance
                self.endpoint_guard = snapshot.endpoint_guard
                main = self.registry.entries.get("MAIN")
                fallback = self.registry.entries.get("FALLBACK")
                self.main_health = self.router.main_health
                self.fallback_health = self.router.fallback_health
                self.circuit_breaker = self.router.circuit_breaker
                self.main_monitor = (
                    main.monitor
                    if main is not None
                    else HealthMonitor(self.config.main, self.main_health)
                )
                self.fallback_monitor = (
                    fallback.monitor
                    if fallback is not None
                    else HealthMonitor(self.config.fallback, self.fallback_health)
                )
                self.proxy.activate(snapshot)
                self.monitoring.reconfigure(
                    self.config,
                    self.main_health,
                    self.fallback_health,
                    self.circuit_breaker,
                    self.router,
                    self.endpoint_guard,
                )
                self._generation_stop = generation_stop
                self._generation_tasks = tuple(prepared_tasks)
                self.runtime.config_generation = snapshot.generation
                self.runtime.reload_successes += 1
                self.runtime.last_reload_at = self.runtime.clock.utc_now()
                self.runtime.last_reload_result = "success"
                logging.getLogger().setLevel(self.config.logging.level)
                old_stop.set()
                for task in old_tasks:
                    task.cancel()
                activate.set()
                activated = True
                log.info("Configuration reload activated generation=%s", snapshot.generation)
                # TaskTracker continues owning old tasks until their cleanup
                # completes. No unbounded retired-generation list is retained.
                if old_tasks:
                    await asyncio.wait(
                        old_tasks, timeout=self.config.connection.shutdown_cancel_timeout_seconds
                    )
                return True
            except asyncio.CancelledError:
                if activated:
                    raise
                self.runtime.reload_failures += 1
                self.runtime.last_reload_result = "cancelled"
                if not self.stop_event.is_set():
                    raise
                return False
            except Exception as exc:
                self.runtime.reload_failures += 1
                detail = self._reload_rejection_reason(exc)
                self.runtime.last_reload_result = detail
                log.warning("Configuration reload rejected reason=%s", detail)
                return False
            finally:
                self._reload_preparation = None
                if not activated:
                    for task in prepared_tasks:
                        task.cancel()
                    if prepared_tasks:
                        await asyncio.gather(*prepared_tasks, return_exceptions=True)

    def request_shutdown(self) -> None:
        self.runtime.shutting_down = True
        self.stop_event.set()
        self._generation_stop.set()
        self._reload_event.set()
        if self._reload_preparation is not None:
            self._reload_preparation.cancel()
        if self._startup_task is not None and not self._startup_task.done():
            self._startup_task.cancel()

    async def run_until_stopped(self) -> None:
        try:
            await self.start()
            await self.stop_event.wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        async with self._shutdown_lock:
            if self._shutdown_complete:
                return
            self.request_shutdown()
            async with self._start_lock:
                log.info(
                    "Graceful shutdown started active_connections=%s",
                    self.runtime.active_connections,
                )
                await self.proxy.stop_accepting()
                component_names = ("monitoring", "background", "connections")
                results = await asyncio.gather(
                    self.monitoring.stop(),
                    self.background.cancel_all(
                        self.config.connection.shutdown_cancel_timeout_seconds
                    ),
                    self.proxy.shutdown_connections(),
                    return_exceptions=True,
                )
                failure: BaseException | None = None
                for component, result in zip(component_names, results, strict=True):
                    if isinstance(result, BaseException):
                        log.error(
                            "Graceful shutdown component failed component=%s error=%s",
                            component,
                            type(result).__name__,
                        )
                        failure = failure or result
                if failure is not None:
                    raise RuntimeError("graceful shutdown component failed") from failure
                self._shutdown_complete = True
                log.info(
                    "Graceful shutdown complete active_connections=%s "
                    "incoming_connections_total=%s backend_connections_established_total=%s "
                    "connections_rejected_total=%s",
                    self.runtime.active_connections,
                    self.runtime.incoming_connections_total,
                    self.runtime.backend_connections_established_total,
                    self.runtime.connections_rejected_total,
                )
