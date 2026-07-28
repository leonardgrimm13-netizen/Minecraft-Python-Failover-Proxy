"""Minecraft listener, upstream connection handling and connection ownership."""

from __future__ import annotations

import asyncio
import logging
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
from .relay import close_stream_writer, configure_tcp_writer, relay_streams
from .routing import Router, RoutingDecision
from .runtime import RuntimeState, TaskTracker

log = logging.getLogger("mc-failover.proxy")


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

    def _accepted(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.runtime.incoming_connection_received()
        if self.runtime.shutting_down:
            self.runtime.reject(RejectionReason.SHUTTING_DOWN)
            writer.close()
            return
        self._writers.add(writer)
        try:
            self.tasks.create(self.handle_client(reader, writer))
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
        protocol = self.config.proxy_protocol
        if not protocol.accept:
            return None
        if not is_trusted_proxy(
            peer_ip,
            protocol.trusted_proxy_ips,
            trust_all=protocol.trust_all_proxies,
        ):
            raise PermissionError("untrusted_proxy")
        return await read_proxy_header_for_version(
            _accept_version(self.config),
            reader,
            protocol.header_timeout_seconds,
            max_header_bytes=protocol.max_header_bytes,
        )

    def _outbound_proxy_header(
        self,
        inbound: ProxyProtocolInfo | None,
        client_writer: asyncio.StreamWriter,
    ) -> bytes:
        version = _send_version(self.config)
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
        try:
            server_reader, connected_writer = await self.endpoint_guard.open_connection(
                target.host,
                target.port,
                timeout_seconds=self.config.connection.timeout_seconds,
            )
            server_writer = connected_writer
            self._writers.add(connected_writer)
            configure_tcp_writer(connected_writer, keepalive=self.config.connection.tcp_keepalive)
            if self.config.proxy_protocol.send:
                connected_writer.write(self._outbound_proxy_header(inbound, client_writer))
                await asyncio.wait_for(
                    connected_writer.drain(), timeout=self.config.connection.write_timeout_seconds
                )
            return server_reader, connected_writer
        except BaseException:
            if server_writer is not None:
                await close_stream_writer(
                    server_writer,
                    timeout_seconds=self.config.connection.write_timeout_seconds,
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

    async def handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
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
            trusted_proxy = self.config.proxy_protocol.accept and is_trusted_proxy(
                peer_ip,
                self.config.proxy_protocol.trusted_proxy_ips,
                trust_all=self.config.proxy_protocol.trust_all_proxies,
            )
            lease, rejection = await self.limiter.try_acquire(
                peer_ip,
                defer_peer_limits=trusted_proxy,
            )
            if lease is None:
                self.runtime.reject(rejection or RejectionReason.GLOBAL_LIMIT)
                return
            self.runtime.connection_admitted()
            configure_tcp_writer(client_writer, keepalive=self.config.connection.tcp_keepalive)

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

            decision = await self.router.select_for_connection(
                shutting_down=self.runtime.shutting_down
            )
            permit = decision.circuit_permit
            if decision.target is None:
                reason = (
                    RejectionReason.SHUTTING_DOWN
                    if self.runtime.shutting_down
                    else RejectionReason.NO_TARGET
                )
                self.runtime.reject(reason)
                return

            target = decision.target
            try:
                server_reader, server_writer = await self._connect(target, inbound, client_writer)
            except asyncio.CancelledError:
                raise
            except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
                self.runtime.connect_failed(target.name)
                log.warning(
                    "Upstream connect failed target=%s host=%s port=%s error=%s",
                    target.name.value,
                    target.host,
                    target.port,
                    type(exc).__name__,
                )
                if target.name is TargetName.MAIN and permit is not None:
                    await self.circuit_breaker.record_failure(permit)
                    permit_resolved = True
                    fallback_result = await self._fallback_after_main_failure(
                        decision, inbound, client_writer
                    )
                    if fallback_result is None:
                        self.runtime.reject(RejectionReason.BACKEND_CONNECT_FAILED)
                        return
                    target, server_reader, server_writer = fallback_result
                else:
                    self.runtime.reject(RejectionReason.BACKEND_CONNECT_FAILED)
                    return
            else:
                self.runtime.connect_succeeded(target.name)
                self.runtime.backend_connection_started()
                backend_started = True
                if target.name is TargetName.MAIN and permit is not None:
                    await self.circuit_breaker.record_success(permit)
                    permit_resolved = True

            if not backend_started:
                self.runtime.backend_connection_started()
                backend_started = True
            if self.config.logging.access_log:
                log.info(
                    "Connection opened peer=%s target=%s reason=%s",
                    lease.peer_ip or peer_ip,
                    target.name.value,
                    (
                        RoutingReason.MAIN_CONNECT_FAILED_FALLBACK.value
                        if target.name is TargetName.FALLBACK
                        and decision.target.name is TargetName.MAIN
                        else decision.reason.value
                    ),
                )
            await relay_streams(
                client_reader,
                client_writer,
                server_reader,
                server_writer,
                buffer_size=self.config.connection.buffer_size,
                idle_timeout_seconds=self.config.connection.idle_timeout_seconds,
                write_timeout_seconds=self.config.connection.write_timeout_seconds,
                drain_timeout_seconds=self.config.connection.relay_drain_timeout_seconds,
                clock=self.runtime.clock,
            )
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
            cleanup_cancelled: asyncio.CancelledError | None = None
            if permit is not None and permit.allowed and not permit_resolved:
                try:
                    await self.circuit_breaker.release(permit)
                except asyncio.CancelledError as exc:
                    cleanup_cancelled = exc
                except Exception:
                    log.exception("Unexpected circuit-permit cleanup failure")
            try:
                close_results = await asyncio.gather(
                    close_stream_writer(
                        server_writer,
                        timeout_seconds=self.config.connection.shutdown_cancel_timeout_seconds,
                    ),
                    close_stream_writer(
                        client_writer,
                        timeout_seconds=self.config.connection.shutdown_cancel_timeout_seconds,
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
            if self.config.logging.access_log:
                log.info("Connection closed peer=%s", lease.peer_ip if lease else peer_ip)
            if cleanup_cancelled is not None:
                raise cleanup_cancelled
