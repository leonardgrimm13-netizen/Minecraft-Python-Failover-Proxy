"""Minecraft listener, upstream connection handling and connection ownership."""

from __future__ import annotations

import asyncio
import errno
import logging
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from .circuit_breaker import CircuitBreaker, CircuitPermit
from .config import AppConfig
from .endpoint_safety import EndpointLoopGuard, sockaddr_host
from .health import HealthState
from .limits import ConnectionLease, ConnectionLimiter
from .models import MaintenanceMode, RejectionReason, RoutingReason, Target, TargetName
from .proxy_protocol import (
    ProxyProtocolInfo,
    build_proxy_header_for_version,
    build_proxy_unknown_header_for_version,
    is_trusted_proxy,
    read_proxy_header_for_version,
)
from .registry import BackendRuntime
from .relay import (
    PipeEnd,
    RelayErrorKind,
    _error_result,
    classify_relay_result,
    close_stream_writer,
    configure_tcp_writer,
    relay_streams,
)
from .routing import Router, RoutingDecision
from .runtime import RuntimeState, TaskTracker
from .runtime_config import RuntimeConfiguration

_CONNECTION_SNAPSHOT: ContextVar[RuntimeConfiguration | None] = ContextVar(
    "mc_failover_connection_snapshot", default=None
)

log = logging.getLogger("mc-failover.proxy")


@dataclass(frozen=True, slots=True)
class _BackendConnection:
    decision: RoutingDecision
    entry: BackendRuntime
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    permit: CircuitPermit | None


def _endpoint(value: Any) -> tuple[str, int] | None:
    if not isinstance(value, tuple) or len(value) < 2:
        return None
    host, port = value[0], value[1]
    if not isinstance(host, str) or isinstance(port, bool) or not isinstance(port, int):
        return None
    return host, port


def _accept_version(config: AppConfig) -> int:
    return config.proxy_protocol.accept_version or config.proxy_protocol.version


def _send_version(config: AppConfig) -> int:
    return config.proxy_protocol.send_version or config.proxy_protocol.version


class ProxyServer:
    def __init__(
        self,
        config: AppConfig,
        runtime: RuntimeState,
        router: Router,
        main_health: HealthState,
        fallback_health: HealthState,
        circuit_breaker: CircuitBreaker,
        limiter: ConnectionLimiter,
        endpoint_guard: EndpointLoopGuard | None = None,
    ) -> None:
        self.config = config
        self.runtime = runtime
        self.router = router
        self.main_health = main_health
        self.fallback_health = fallback_health
        self.circuit_breaker = circuit_breaker
        self.limiter = limiter
        self.endpoint_guard = endpoint_guard or EndpointLoopGuard(config)
        self.server: asyncio.Server | None = None
        self._closing_server: asyncio.Server | None = None
        self.tasks = TaskTracker(name="minecraft-client")
        self._writers: set[asyncio.StreamWriter] = set()
        self._snapshot: RuntimeConfiguration | None = None

    def activate(self, snapshot: RuntimeConfiguration) -> None:
        self._snapshot = snapshot
        self.config = snapshot.config
        self.router = snapshot.router
        self.main_health = snapshot.router.main_health
        self.fallback_health = snapshot.router.fallback_health
        self.circuit_breaker = snapshot.router.circuit_breaker
        self.endpoint_guard = snapshot.endpoint_guard

    def _connection_config(self) -> AppConfig:
        snapshot = _CONNECTION_SNAPSHOT.get()
        return snapshot.config if snapshot is not None else self.config

    def _accepted(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.runtime.incoming_connection_received()
        if self.runtime.shutting_down:
            self.runtime.reject(RejectionReason.SHUTTING_DOWN)
            writer.close()
            return
        # Bound queued handlers and their cleanup too, before any coroutine
        # has a chance to acquire a limiter lease. Rejecting in the handler
        # alone permits an unbounded backlog of rejection/close tasks.
        if self.tasks.active_count >= self.config.connection.max_connections:
            self.runtime.reject(RejectionReason.GLOBAL_LIMIT)
            writer.close()
            return
        self._writers.add(writer)

        def finished(_task: asyncio.Task[None]) -> None:
            # Canceled-before-start coroutines never enter handle_client's finally.
            writer.close()
            self._writers.discard(writer)

        try:
            task = self.tasks.create(self.handle_client(reader, writer))
            task.add_done_callback(finished)
        except RuntimeError:
            self._writers.discard(writer)
            self.runtime.reject(RejectionReason.SHUTTING_DOWN)
            writer.close()

    async def start(self) -> asyncio.Server:
        stream_limit = max(65_536, self.config.proxy_protocol.max_header_bytes + 1)
        self.server = await asyncio.start_server(
            self._accepted,
            self.config.proxy.listen_host,
            self.config.proxy.listen_port,
            backlog=self.config.proxy.backlog,
            limit=stream_limit,
            start_serving=False,
        )
        bound_addresses = (
            address
            for sock in self.server.sockets or ()
            if (address := sockaddr_host(sock.getsockname())) is not None
        )
        self.endpoint_guard.register_bound_addresses("proxy", bound_addresses)
        await self.server.start_serving()
        return self.server

    async def stop_accepting(self) -> None:
        if self.server is not None:
            closing_server = self.server
            closing_server.close()
            self.server = None
            self._closing_server = closing_server

    async def wait_listener_closed(self) -> None:
        if self._closing_server is not None:
            try:
                await asyncio.wait_for(
                    self._closing_server.wait_closed(),
                    timeout=self.config.connection.shutdown_cancel_timeout_seconds,
                )
            except asyncio.TimeoutError:
                log.error("Minecraft listener did not close before the shutdown deadline")
            self._closing_server = None

    async def shutdown_connections(self) -> None:
        await self.tasks.drain(
            self.config.connection.shutdown_grace_seconds,
            self.config.connection.shutdown_cancel_timeout_seconds,
        )
        remaining = tuple(self._writers)
        if remaining:
            await asyncio.gather(
                *(
                    close_stream_writer(
                        writer,
                        timeout_seconds=self.config.connection.shutdown_cancel_timeout_seconds,
                    )
                    for writer in remaining
                ),
                return_exceptions=True,
            )
        still_pending = await self.tasks.wait_remaining(
            self.config.connection.shutdown_cancel_timeout_seconds
        )
        if still_pending:
            log.error(
                "Connection tasks remained after forced writer cleanup count=%s",
                len(still_pending),
            )
        await self.wait_listener_closed()
        if self.limiter.active:
            log.error("Connection limiter non-zero after shutdown active=%s", self.limiter.active)

    async def _read_inbound_proxy(
        self,
        reader: asyncio.StreamReader,
        peer_ip: str,
    ) -> ProxyProtocolInfo | None:
        config = self._connection_config()
        protocol = config.proxy_protocol
        if not protocol.accept:
            return None
        if not is_trusted_proxy(
            peer_ip,
            protocol.trusted_proxy_ips,
            trust_all=protocol.trust_all_proxies,
        ):
            raise PermissionError("untrusted_proxy")
        return await read_proxy_header_for_version(
            _accept_version(config),
            reader,
            protocol.header_timeout_seconds,
            max_header_bytes=protocol.max_header_bytes,
        )

    def _outbound_proxy_header(
        self,
        inbound: ProxyProtocolInfo | None,
        client_writer: asyncio.StreamWriter,
    ) -> bytes:
        version = _send_version(self._connection_config())
        if inbound is not None:
            if inbound.family == "UNKNOWN":
                return build_proxy_unknown_header_for_version(version)
            source_ip = inbound.source_ip
            source_port = inbound.source_port
            destination_ip = inbound.destination_ip
            destination_port = inbound.destination_port
        else:
            peer = _endpoint(client_writer.get_extra_info("peername"))
            local = _endpoint(client_writer.get_extra_info("sockname"))
            if peer is None or local is None:
                return build_proxy_unknown_header_for_version(version)
            source_ip, source_port = peer
            destination_ip, destination_port = local
        try:
            return build_proxy_header_for_version(
                version,
                source_ip,
                destination_ip,
                source_port,
                destination_port,
            )
        except ValueError:
            return build_proxy_unknown_header_for_version(version)

    async def _connect(
        self,
        target: Target,
        inbound: ProxyProtocolInfo | None,
        client_writer: asyncio.StreamWriter,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        server_writer: asyncio.StreamWriter | None = None
        config = self._connection_config()
        snapshot = _CONNECTION_SNAPSHOT.get()
        guard = snapshot.endpoint_guard if snapshot is not None else self.endpoint_guard
        entry = (
            snapshot.registry.entries.get(target.backend_id or target.name.value)
            if snapshot is not None
            else None
        )
        timeout = entry.config.connect_timeout_seconds if entry is not None else None
        try:
            server_reader, connected_writer = await guard.open_connection(
                target.host,
                target.port,
                timeout_seconds=timeout or config.connection.timeout_seconds,
            )
            server_writer = connected_writer
            self._writers.add(connected_writer)
            configure_tcp_writer(connected_writer, keepalive=config.connection.tcp_keepalive)
            if config.proxy_protocol.send:
                connected_writer.write(self._outbound_proxy_header(inbound, client_writer))
                await asyncio.wait_for(
                    connected_writer.drain(), timeout=config.connection.write_timeout_seconds
                )
            return server_reader, connected_writer
        except BaseException:
            if server_writer is not None:
                await close_stream_writer(
                    server_writer,
                    timeout_seconds=config.connection.write_timeout_seconds,
                )
                self._writers.discard(server_writer)
            raise

    async def _fallback_after_main_failure(
        self,
        decision: RoutingDecision,
        inbound: ProxyProtocolInfo | None,
        client_writer: asyncio.StreamWriter,
    ) -> tuple[Target, asyncio.StreamReader, asyncio.StreamWriter] | None:
        if (
            decision.maintenance_mode is not MaintenanceMode.AUTO
            or not self.config.connection.connect_fallback_on_main_connect_failure
            or not self.fallback_health.routable
        ):
            return None
        fallback = self.router.target(TargetName.FALLBACK)
        try:
            reader, writer = await self._connect(fallback, inbound, client_writer)
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
            self.runtime.connect_failed(TargetName.FALLBACK)
            log.warning(
                "Upstream connect failed target=FALLBACK host=%s port=%s error=%s",
                fallback.host,
                fallback.port,
                type(exc).__name__,
            )
            return None
        self.runtime.connect_succeeded(TargetName.FALLBACK)
        log.warning("MAIN connect failed; routed connection directly to FALLBACK")
        return fallback, reader, writer

    async def _connect_candidates(
        self,
        decision: RoutingDecision,
        router: Router,
        inbound: ProxyProtocolInfo | None,
        client_writer: asyncio.StreamWriter,
    ) -> _BackendConnection | None:
        config = self._connection_config()
        registry = router.registry
        attempted: set[str] = set()
        maximum = min(config.routing.max_connect_attempts, len(registry.entries))
        deadline = asyncio.get_running_loop().time() + config.routing.connect_budget_seconds
        for attempt in range(maximum):
            selected = decision.target
            if selected is None:
                return None
            target = selected
            backend_id = target.id
            entry = registry.entries[backend_id]
            permit = decision.circuit_permit
            resolved = False
            transferred = False
            writer: asyncio.StreamWriter | None = None
            attempted.add(backend_id)

            async def connect_owned(
                selected_target: Target = target,
            ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
                nonlocal writer
                # Capture ownership inside wait_for's operation before its
                # result crosses a task boundary on Python 3.10.
                reader, writer = await self._connect(selected_target, inbound, client_writer)
                return reader, writer

            try:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return None
                try:
                    reader, connected_writer = await asyncio.wait_for(
                        connect_owned(), timeout=remaining
                    )
                except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
                    self.runtime.connect_failed(target.name)
                    entry.connect_failures += 1
                    evidence = _error_result("client_to_server", PipeEnd.WRITE_ERROR, 0, exc)
                    if evidence.error_kind in {
                        RelayErrorKind.RESET,
                        RelayErrorKind.BROKEN_PIPE,
                        RelayErrorKind.NETWORK,
                        RelayErrorKind.TIMEOUT,
                    }:
                        registry.record_passive_failure(backend_id, "connect_failed")
                        if permit is not None:
                            await entry.circuit.record_failure(permit)
                            resolved = True
                    else:
                        registry.request_check(backend_id)
                    log.warning(
                        "Upstream connect failed backend=%s error=%s",
                        backend_id,
                        type(exc).__name__,
                    )
                    local_resource_failure = isinstance(exc, OSError) and exc.errno in {
                        errno.EMFILE,
                        errno.ENFILE,
                        errno.ENOMEM,
                        errno.ENOBUFS,
                        errno.EADDRNOTAVAIL,
                    }
                    if (
                        local_resource_failure
                        or decision.maintenance_mode is not MaintenanceMode.AUTO
                        or (
                            not config.backends
                            and not config.connection.connect_fallback_on_main_connect_failure
                        )
                        or attempt + 1 >= maximum
                        or asyncio.get_running_loop().time() >= deadline
                    ):
                        return None
                else:
                    self.runtime.connect_succeeded(target.name)
                    entry.connect_successes += 1
                    if permit is not None:
                        await entry.circuit.record_success(permit)
                        resolved = True
                    connected = _BackendConnection(
                        decision, entry, reader, connected_writer, permit
                    )
                    transferred = True
                    return connected
            finally:
                try:
                    if writer is not None and not transferred:
                        try:
                            await close_stream_writer(
                                writer,
                                timeout_seconds=config.connection.shutdown_cancel_timeout_seconds,
                            )
                        finally:
                            self._writers.discard(writer)
                finally:
                    if permit is not None and permit.allowed and not resolved:
                        await entry.circuit.release(permit)
            decision = await router.select_for_connection(
                shutting_down=self.runtime.shutting_down, excluded=frozenset(attempted)
            )
        return None

    async def _local_status(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        snapshot: RuntimeConfiguration,
        state: str,
    ) -> tuple[bool, bytes]:
        service = snapshot.status_service
        if not service.try_acquire():
            self.runtime.reject(RejectionReason.GLOBAL_LIMIT)
            return True, b""
        try:
            inspection = await service.inspect(reader)
            if inspection.next_state == 1:
                await service.respond(reader, writer, inspection, state)
                return True, b""
            return False, inspection.prefix
        finally:
            service.release()

    async def handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        token = _CONNECTION_SNAPSHOT.set(self._snapshot)
        try:
            await self._handle_client(client_reader, client_writer)
        finally:
            _CONNECTION_SNAPSHOT.reset(token)

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        snapshot = _CONNECTION_SNAPSHOT.get()
        config = self._connection_config()
        router = snapshot.router if snapshot is not None else self.router
        registry = router.registry
        entry: BackendRuntime | None = None
        prefix = b""
        status_inspected = False
        peer = _endpoint(client_writer.get_extra_info("peername"))
        peer_ip = peer[0] if peer is not None else "unknown"
        lease: ConnectionLease | None = None
        server_writer: asyncio.StreamWriter | None = None
        backend_started = False
        permit: CircuitPermit | None = None
        permit_resolved = False
        try:
            if peer is None:
                self.runtime.reject(RejectionReason.NO_TARGET)
                return
            trusted_proxy = config.proxy_protocol.accept and is_trusted_proxy(
                peer_ip,
                config.proxy_protocol.trusted_proxy_ips,
                trust_all=config.proxy_protocol.trust_all_proxies,
            )
            lease, rejection = await self.limiter.try_acquire(
                peer_ip,
                defer_peer_limits=trusted_proxy,
            )
            if lease is None:
                self.runtime.reject(rejection or RejectionReason.GLOBAL_LIMIT)
                return
            self.runtime.connection_admitted()
            configure_tcp_writer(client_writer, keepalive=config.connection.tcp_keepalive)

            try:
                inbound = await self._read_inbound_proxy(client_reader, peer_ip)
            except PermissionError:
                self.runtime.reject(RejectionReason.UNTRUSTED_PROXY)
                log.warning("Rejected PROXY header from untrusted peer=%s", peer_ip)
                return
            except asyncio.CancelledError:
                raise
            except (
                asyncio.TimeoutError,
                TimeoutError,
                OSError,
                ValueError,
                asyncio.IncompleteReadError,
            ):
                if lease.peer_ip is None:
                    rejection = await self.limiter.apply_peer_limits(lease, peer_ip)
                    if rejection is not None:
                        self.runtime.reject(rejection)
                        return
                self.runtime.reject(RejectionReason.INVALID_PROXY_HEADER)
                log.debug("Rejected invalid or incomplete PROXY header peer=%s", peer_ip)
                return

            if lease.peer_ip is None:
                effective_ip = (
                    inbound.source_ip
                    if inbound is not None and inbound.family != "UNKNOWN"
                    else peer_ip
                )
                rejection = await self.limiter.apply_peer_limits(lease, effective_ip)
                if rejection is not None:
                    self.runtime.reject(rejection)
                    return

            preview = router.snapshot(shutting_down=self.runtime.shutting_down)
            status_policy = config.status.enabled and (
                not preview.ready
                or config.status.always
                or (
                    config.status.maintenance
                    and preview.maintenance_mode is not MaintenanceMode.AUTO
                )
            )
            if status_policy and snapshot is not None:
                status_inspected = True
                preferred = next((item for item in registry.ordered if item.config.enabled), None)
                state = (
                    "maintenance"
                    if preview.maintenance_mode is not MaintenanceMode.AUTO
                    else "offline"
                    if not preview.ready
                    else "fallback"
                    if preferred is not None and preview.backend_id != preferred.config.id
                    else "online"
                )
                handled, prefix = await self._local_status(
                    client_reader, client_writer, snapshot, state
                )
                if handled:
                    return

            decision = await router.select_for_connection(shutting_down=self.runtime.shutting_down)
            if decision.target is None:
                if (
                    config.status.enabled
                    and snapshot is not None
                    and not status_inspected
                    and not self.runtime.shutting_down
                ):
                    mode = snapshot.maintenance.snapshot.mode
                    state = "offline" if mode is MaintenanceMode.AUTO else "maintenance"
                    handled, _prefix = await self._local_status(
                        client_reader, client_writer, snapshot, state
                    )
                    if handled:
                        return
                reason = (
                    RejectionReason.SHUTTING_DOWN
                    if self.runtime.shutting_down
                    else RejectionReason.NO_TARGET
                )
                self.runtime.reject(reason)
                return

            connected = await self._connect_candidates(decision, router, inbound, client_writer)
            if connected is None:
                if config.status.enabled and snapshot is not None and not status_inspected:
                    mode = snapshot.maintenance.snapshot.mode
                    state = "offline" if mode is MaintenanceMode.AUTO else "maintenance"
                    handled, _prefix = await self._local_status(
                        client_reader, client_writer, snapshot, state
                    )
                    if handled:
                        return
                self.runtime.reject(RejectionReason.BACKEND_CONNECT_FAILED)
                return
            decision = connected.decision
            entry = connected.entry
            target = entry.target
            server_reader, server_writer = connected.reader, connected.writer
            permit, permit_resolved = connected.permit, True
            if not backend_started:
                self.runtime.backend_connection_started()
                backend_started = True
            if entry is not None:
                entry.active_connections += 1
            if prefix:
                try:
                    server_writer.write(prefix)
                    await asyncio.wait_for(
                        server_writer.drain(), timeout=config.connection.write_timeout_seconds
                    )
                except asyncio.TimeoutError:
                    if entry is not None and registry is not None:
                        registry.request_check(entry.config.id)
                    return
                except (ConnectionError, OSError) as exc:
                    outcome = classify_relay_result(
                        (_error_result("client_to_server", PipeEnd.WRITE_ERROR, 0, exc),),
                        duration_seconds=0,
                    )
                    if outcome.backend_failure and config.passive_health.enabled:
                        registry.record_passive_failure(entry.config.id, outcome.kind.value)
                        await entry.circuit.record_session_failure(permit)
                    elif outcome.confirm_health:
                        registry.request_check(entry.config.id)
                    return
            if config.logging.access_log:
                log.info(
                    "Connection opened peer=%s target=%s reason=%s",
                    lease.peer_ip or peer_ip,
                    target.backend_id or target.name.value,
                    (
                        RoutingReason.MAIN_CONNECT_FAILED_FALLBACK.value
                        if target.name is TargetName.FALLBACK
                        and decision.target is not None
                        and decision.target.name is TargetName.MAIN
                        else decision.reason.value
                    ),
                )
            relay_started = self.runtime.clock.monotonic()
            results = await relay_streams(
                client_reader,
                client_writer,
                server_reader,
                server_writer,
                buffer_size=config.connection.buffer_size,
                idle_timeout_seconds=config.connection.idle_timeout_seconds,
                write_timeout_seconds=config.connection.write_timeout_seconds,
                drain_timeout_seconds=config.connection.relay_drain_timeout_seconds,
                clock=self.runtime.clock,
            )
            if entry is not None and registry is not None:
                outcome = classify_relay_result(
                    results,
                    duration_seconds=max(0.0, self.runtime.clock.monotonic() - relay_started),
                    early_session_seconds=config.passive_health.early_session_seconds,
                    shutting_down=self.runtime.shutting_down,
                )
                if outcome.backend_failure:
                    registry.record_passive_failure(entry.config.id, outcome.kind.value)
                    if config.passive_health.enabled:
                        await entry.circuit.record_session_failure(permit)
                elif outcome.confirm_health:
                    registry.request_check(entry.config.id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Unexpected internal connection handler failure")
        finally:
            # Once relay_streams has returned (or failed), this session is no
            # longer an active backend relay.  Keep this synchronous accounting
            # ahead of fallible async cleanup so a cleanup error cannot leak the
            # active gauge.
            if backend_started:
                self.runtime.backend_connection_finished()
                if entry is not None:
                    entry.active_connections = max(0, entry.active_connections - 1)
            cleanup_cancelled: asyncio.CancelledError | None = None
            if permit is not None and permit.allowed and not permit_resolved:
                try:
                    breaker = entry.circuit if entry is not None else self.circuit_breaker
                    await breaker.release(permit)
                except asyncio.CancelledError as exc:
                    cleanup_cancelled = exc
                except Exception:
                    log.exception("Unexpected circuit-permit cleanup failure")
            try:
                close_results = await asyncio.gather(
                    close_stream_writer(
                        server_writer,
                        timeout_seconds=config.connection.shutdown_cancel_timeout_seconds,
                    ),
                    close_stream_writer(
                        client_writer,
                        timeout_seconds=config.connection.shutdown_cancel_timeout_seconds,
                    ),
                    return_exceptions=True,
                )
            except asyncio.CancelledError as exc:
                cleanup_cancelled = cleanup_cancelled or exc
            else:
                for result in close_results:
                    if isinstance(result, asyncio.CancelledError):
                        cleanup_cancelled = cleanup_cancelled or result
                    elif isinstance(result, BaseException):
                        log.error("Unexpected stream-writer cleanup failure", exc_info=result)
            if server_writer is not None:
                self._writers.discard(server_writer)
            self._writers.discard(client_writer)
            if lease is not None:
                try:
                    await self.limiter.release(lease)
                except asyncio.CancelledError as exc:
                    cleanup_cancelled = cleanup_cancelled or exc
                except Exception:
                    log.exception("Unexpected connection-limiter cleanup failure")
            if config.logging.access_log:
                log.info("Connection closed peer=%s", lease.peer_ip if lease else peer_ip)
            if cleanup_cancelled is not None:
                raise cleanup_cancelled
