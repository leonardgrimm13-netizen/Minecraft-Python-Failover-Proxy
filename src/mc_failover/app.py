"""Top-level ownership of listeners, health tasks and graceful shutdown."""

from __future__ import annotations

import asyncio
import logging

from .circuit_breaker import CircuitBreaker
from .config import AppConfig
from .endpoint_safety import EndpointLoopGuard
from .health import HealthMonitor, HealthState
from .limits import ConnectionLimiter
from .models import HealthCheckResult, TargetName
from .monitoring import MonitoringServer
from .proxy import ProxyServer
from .routing import MaintenanceWatcher, Router
from .runtime import RuntimeState, TaskTracker
from .time_utils import SYSTEM_CLOCK, Clock

log = logging.getLogger("mc-failover.app")


class FailoverApplication:
    def __init__(self, config: AppConfig, *, clock: Clock = SYSTEM_CLOCK) -> None:
        self.config = config
        self.runtime = RuntimeState(clock=clock)
        self.stop_event = asyncio.Event()
        self.main_health = HealthState(TargetName.MAIN, config.healthcheck, clock=clock)
        self.fallback_health = HealthState(
            TargetName.FALLBACK, config.fallback_healthcheck, clock=clock
        )
        self.circuit_breaker = CircuitBreaker(config.circuit_breaker, clock=clock)
        self.maintenance = MaintenanceWatcher(config.maintenance)
        self.router = Router(
            config,
            self.main_health,
            self.fallback_health,
            self.circuit_breaker,
            self.maintenance,
        )
        self.limiter = ConnectionLimiter(
            config.connection.max_connections,
            config.connection.max_connections_per_ip,
            config.connection.new_connections_per_second,
            config.connection.new_connections_burst,
            clock=clock,
        )
        self.endpoint_guard = EndpointLoopGuard(config)
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

        async def main_result(result: HealthCheckResult) -> None:
            if result.ok:
                await self.circuit_breaker.record_health_success()

        self.main_monitor = HealthMonitor(
            config.main,
            self.main_health,
            on_result=main_result,
            endpoint_guard=self.endpoint_guard,
        )
        self.fallback_monitor = HealthMonitor(
            config.fallback,
            self.fallback_health,
            endpoint_guard=self.endpoint_guard,
        )

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
            await asyncio.gather(
                self.main_monitor.check_once(initial=True),
                self.fallback_monitor.check_once(initial=True),
            )

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

        self.background.create(self.main_monitor.run(self.stop_event))
        self.background.create(self.fallback_monitor.run(self.stop_event))
        self.background.create(self.maintenance.run(self.stop_event))
        self._started = True
        proxy_sockets = ", ".join(str(sock.getsockname()) for sock in proxy_server.sockets or [])
        log.info("Proxy listening sockets=%s", proxy_sockets)
        if monitoring_server is not None:
            monitoring_sockets = ", ".join(
                str(sock.getsockname()) for sock in monitoring_server.sockets or []
            )
            log.info("Monitoring listening sockets=%s", monitoring_sockets)

    def request_shutdown(self) -> None:
        self.runtime.shutting_down = True
        self.stop_event.set()
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
