"""Small hardened HTTP/1 monitoring server."""

from __future__ import annotations

import asyncio
import hmac
import ipaddress
import json
import logging
import re
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit

from .circuit_breaker import CircuitBreaker
from .config import AppConfig
from .endpoint_safety import EndpointLoopGuard, sockaddr_host
from .health import HealthSnapshot, HealthState
from .metrics import render_metrics
from .models import RejectionReason
from .routing import Router
from .runtime import RuntimeState, TaskTracker
from .time_utils import format_utc

log = logging.getLogger("mc-failover.monitoring")

MAX_REQUEST_LINE_BYTES = 4096
MAX_HEADER_LINE_BYTES = 4096
MAX_HEADER_LINES = 64
MAX_HEADER_BYTES = 16 * 1024
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def _is_loopback_address(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value.split("%", 1)[0])
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_loopback


@dataclass(frozen=True, slots=True)
class HttpRequest:
    method: str
    path: str
    version: str
    headers: dict[str, tuple[str, ...]]


class HttpError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


async def _readline(
    reader: asyncio.StreamReader,
    *,
    deadline: float,
    maximum: int,
) -> bytes:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise asyncio.TimeoutError
    try:
        line = await asyncio.wait_for(reader.readuntil(b"\n"), timeout=remaining)
    except (asyncio.LimitOverrunError, ValueError) as exc:
        raise HttpError(400, "line too long") from exc
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            raise HttpError(400, "incomplete request") from exc
        line = exc.partial
    if len(line) > maximum:
        raise HttpError(400, "line too long")
    if not line.endswith(b"\r\n"):
        raise HttpError(400, "HTTP lines must use CRLF")
    return line[:-2]


async def read_http_request(
    reader: asyncio.StreamReader,
    *,
    timeout_seconds: float,
) -> HttpRequest:
    """Read one header-only request under one absolute deadline."""

    deadline = asyncio.get_running_loop().time() + timeout_seconds
    request_line = await _readline(reader, deadline=deadline, maximum=MAX_REQUEST_LINE_BYTES)
    try:
        method_raw, target_raw, version_raw = request_line.split(b" ")
        method = method_raw.decode("ascii")
        target = target_raw.decode("ascii")
        version = version_raw.decode("ascii")
    except (ValueError, UnicodeDecodeError) as exc:
        raise HttpError(400, "invalid request line") from exc
    if version not in {"HTTP/1.0", "HTTP/1.1"}:
        raise HttpError(400, "unsupported HTTP version")
    if not _HEADER_NAME.fullmatch(method):
        raise HttpError(400, "invalid request method")
    if (
        not target.startswith("/")
        or "\\" in target
        or "%" in target
        or any(ord(character) < 0x21 or ord(character) == 0x7F for character in target)
    ):
        raise HttpError(400, "invalid request target")
    parsed_target = urlsplit(target)
    if parsed_target.scheme or parsed_target.netloc or parsed_target.fragment:
        raise HttpError(400, "invalid request target")
    path = parsed_target.path
    if "//" in path or "/./" in path or "/../" in path or path.endswith(("/.", "/..")):
        raise HttpError(400, "non-normalized request path")

    collected: dict[str, list[str]] = {}
    total_bytes = 0
    lines = 0
    while True:
        raw = await _readline(reader, deadline=deadline, maximum=MAX_HEADER_LINE_BYTES)
        if not raw:
            break
        lines += 1
        total_bytes += len(raw) + 2
        if lines > MAX_HEADER_LINES or total_bytes > MAX_HEADER_BYTES:
            raise HttpError(400, "too many request headers")
        if raw[:1] in {b" ", b"\t"} or b":" not in raw:
            raise HttpError(400, "invalid request header")
        name_raw, value_raw = raw.split(b":", 1)
        try:
            name = name_raw.decode("ascii")
            value = value_raw.decode("latin-1").strip(" \t")
        except UnicodeDecodeError as exc:
            raise HttpError(400, "invalid request header") from exc
        if not _HEADER_NAME.fullmatch(name):
            raise HttpError(400, "invalid request header name")
        if any(
            (ord(character) < 0x20 and character != "\t") or ord(character) == 0x7F
            for character in value
        ):
            raise HttpError(400, "invalid request header value")
        collected.setdefault(name.lower(), []).append(value)

    hosts = collected.get("host", [])
    if len(hosts) > 1 or (hosts and not hosts[0]):
        raise HttpError(400, "invalid Host header")
    if version == "HTTP/1.1" and not hosts:
        raise HttpError(400, "Host header required")
    if "transfer-encoding" in collected:
        raise HttpError(400, "request bodies are not supported")
    lengths = collected.get("content-length", [])
    if lengths:
        if any(not value.isdigit() for value in lengths) or len(set(lengths)) != 1:
            raise HttpError(400, "invalid Content-Length")
        if int(lengths[0]) != 0:
            raise HttpError(400, "request bodies are not supported")
    return HttpRequest(
        method=method,
        path=path,
        version=version,
        headers={key: tuple(values) for key, values in collected.items()},
    )


_REASONS = {
    200: "OK",
    400: "Bad Request",
    401: "Unauthorized",
    404: "Not Found",
    405: "Method Not Allowed",
    408: "Request Timeout",
    503: "Service Unavailable",
}


def response_bytes(
    status: int,
    body: bytes,
    content_type: str,
    *,
    extra_headers: tuple[tuple[str, str], ...] = (),
) -> bytes:
    headers = [
        f"HTTP/1.1 {status} {_REASONS.get(status, 'Error')}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
        "Connection: close",
        "X-Content-Type-Options: nosniff",
    ]
    headers.extend(f"{name}: {value}" for name, value in extra_headers)
    return ("\r\n".join(headers) + "\r\n\r\n").encode("ascii") + body


def json_response(status: int, payload: dict[str, Any]) -> bytes:
    # ASCII escaping keeps the response valid even if an upstream status JSON
    # contained escaped lone surrogates that Python can represent internally
    # but UTF-8 cannot encode directly.
    body = (json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n").encode("ascii")
    return response_bytes(status, body, "application/json; charset=utf-8")


def _health_payload(snapshot: HealthSnapshot) -> dict[str, Any]:
    result = snapshot.last_result
    return {
        "status": snapshot.status.value,
        "healthy": snapshot.healthy,
        "routable": snapshot.routable,
        "consecutive_successes": snapshot.successes,
        "consecutive_failures": snapshot.failures,
        "last_check_at": snapshot.last_check_at_iso,
        "seconds_since_last_check": snapshot.seconds_since_last_check,
        "last_result": asdict(result) if result is not None else None,
        "status_changed_at": format_utc(snapshot.status_changed_at),
    }


def build_monitoring_payload(
    config: AppConfig,
    runtime: RuntimeState,
    main_health: HealthState,
    fallback_health: HealthState,
    circuit_breaker: CircuitBreaker,
    router: Router,
    *,
    include_sensitive: bool,
) -> dict[str, Any]:
    decision = router.snapshot(shutting_down=runtime.shutting_down)
    main = main_health.snapshot()
    fallback = fallback_health.snapshot()
    circuit = circuit_breaker.snapshot_now()
    overall = (
        "unavailable" if not decision.ready else "degraded" if decision.degraded else "healthy"
    )
    payload: dict[str, Any] = {
        "service": "mc-failover",
        "status": overall,
        "ready": decision.ready,
        "started_at": format_utc(runtime.started_at),
        "uptime_seconds": runtime.uptime_seconds,
        "active_connections": runtime.active_connections,
        "incoming_connections_total": runtime.incoming_connections_total,
        "backend_connections_established_total": runtime.backend_connections_established_total,
        "connections_rejected_total": runtime.connections_rejected_total,
        # Deprecated aliases retained for monitoring clients during migration.
        "total_connections": runtime.total_connections,
        "rejected_connections": runtime.rejected_connections,
        "active_target": decision.active_target.value,
        "requested_target": decision.requested_target.value,
        "routing_reason": decision.reason.value,
        "maintenance_mode": decision.maintenance_mode.value,
        "maintenance_source": decision.maintenance_source,
        "main_healthy": main.healthy,
        "fallback_healthy": fallback.healthy,
        "main": _health_payload(main),
        "fallback": _health_payload(fallback),
        "circuit_breaker": {
            "state": circuit.state.value,
            "opened_at": format_utc(circuit.opened_at),
            "retry_after_seconds": circuit.retry_after_seconds,
            "failures_in_window": circuit.failures_in_window,
            "half_open_in_flight": circuit.half_open_in_flight,
            "open_total": circuit.open_total,
        },
        "shutting_down": runtime.shutting_down,
    }
    if include_sensitive:
        payload["targets"] = {
            "main": {"host": config.main.host, "port": config.main.port},
            "fallback": {"host": config.fallback.host, "port": config.fallback.port},
        }
    return payload


class MonitoringServer:
    def __init__(
        self,
        config: AppConfig,
        runtime: RuntimeState,
        main_health: HealthState,
        fallback_health: HealthState,
        circuit_breaker: CircuitBreaker,
        router: Router,
        endpoint_guard: EndpointLoopGuard | None = None,
    ) -> None:
        self.config = config
        self.runtime = runtime
        self.main_health = main_health
        self.fallback_health = fallback_health
        self.circuit_breaker = circuit_breaker
        self.router = router
        self.endpoint_guard = endpoint_guard
        self.server: asyncio.Server | None = None
        self.tasks = TaskTracker(name="monitoring-client")
        self._active = 0

    def _accepted(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.runtime.shutting_down:
            writer.close()
            return
        if self._active >= self.config.monitoring.max_connections:
            self.runtime.reject(RejectionReason.MONITORING_LIMIT)
            writer.close()
            return
        self._active += 1
        try:
            self.tasks.create(self._handle_counted(reader, writer))
        except RuntimeError:
            # A callback already queued by the event loop may race with stop().
            self._active = max(0, self._active - 1)
            writer.close()

    async def _handle_counted(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await self.handle(reader, writer)
        finally:
            self._active = max(0, self._active - 1)

    def _authorized(self, request: HttpRequest) -> bool:
        expected = self.config.monitoring.bearer_token
        if expected is None:
            return True
        values = request.headers.get("authorization", ())
        if len(values) != 1:
            return False
        scheme, separator, supplied = values[0].partition(" ")
        if not supplied.isascii():
            return False
        return (
            bool(separator)
            and scheme.lower() == "bearer"
            and hmac.compare_digest(supplied, expected)
        )

    def _dispatch(self, request: HttpRequest) -> bytes:
        if request.method != "GET":
            return response_bytes(
                405,
                b"method not allowed\n",
                "text/plain; charset=utf-8",
                extra_headers=(("Allow", "GET"),),
            )
        if not self._authorized(request):
            return response_bytes(
                401,
                b"unauthorized\n",
                "text/plain; charset=utf-8",
                extra_headers=(("WWW-Authenticate", 'Bearer realm="mc-failover"'),),
            )
        if request.path == "/live":
            return json_response(200, {"live": True, "service": "mc-failover"})

        full = build_monitoring_payload(
            self.config,
            self.runtime,
            self.main_health,
            self.fallback_health,
            self.circuit_breaker,
            self.router,
            include_sensitive=(
                request.path == "/state" and self.config.monitoring.expose_sensitive_state
            ),
        )
        status = 200 if full["ready"] else 503
        if request.path == "/ready":
            return json_response(
                status,
                {
                    "ready": full["ready"],
                    "status": full["status"],
                    "active_target": full["active_target"],
                    "main_healthy": full["main_healthy"],
                    "fallback_healthy": full["fallback_healthy"],
                    "routing_reason": full["routing_reason"],
                },
            )
        if request.path == "/health":
            return json_response(status, full)
        if request.path == "/state":
            return json_response(200, full)
        if request.path == "/metrics":
            body = render_metrics(
                self.runtime,
                self.main_health,
                self.fallback_health,
                self.circuit_breaker,
                self.router,
            ).encode("utf-8")
            return response_bytes(200, body, "text/plain; version=0.0.4; charset=utf-8")
        return response_bytes(404, b"not found\n", "text/plain; charset=utf-8")

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        response: bytes | None = None
        try:
            try:
                request = await read_http_request(
                    reader, timeout_seconds=self.config.monitoring.request_timeout_seconds
                )
                response = self._dispatch(request)
            except asyncio.TimeoutError:
                response = response_bytes(408, b"request timeout\n", "text/plain; charset=utf-8")
            except HttpError as exc:
                response = response_bytes(
                    exc.status, f"{exc.message}\n".encode(), "text/plain; charset=utf-8"
                )
            except asyncio.CancelledError:
                raise
            except (ConnectionError, OSError):
                log.debug("Monitoring client disconnected during request")
            except Exception:
                log.exception("Unexpected monitoring handler failure")
            try:
                if response is not None and not writer.is_closing():
                    writer.write(response)
                    await asyncio.wait_for(
                        writer.drain(), timeout=self.config.monitoring.write_timeout_seconds
                    )
            except asyncio.CancelledError:
                raise
            except (asyncio.TimeoutError, ConnectionError, OSError):
                log.debug("Monitoring response could not be delivered")
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
            except asyncio.CancelledError:
                writer.transport.abort()
                raise
            except (asyncio.TimeoutError, ConnectionError, OSError):
                writer.transport.abort()

    async def start(self) -> asyncio.Server | None:
        if not self.config.monitoring.enabled:
            return None
        self.server = await asyncio.start_server(
            self._accepted,
            self.config.monitoring.listen_host,
            self.config.monitoring.listen_port,
            limit=MAX_HEADER_LINE_BYTES + 2,
            start_serving=False,
        )
        bound_addresses = []
        for sock in self.server.sockets or ():
            address = sockaddr_host(sock.getsockname())
            if address is not None:
                bound_addresses.append(address)
        if not self.config.monitoring.allow_remote and any(
            not _is_loopback_address(address) for address in bound_addresses
        ):
            unsafe_server = self.server
            self.server = None
            unsafe_server.close()
            await unsafe_server.wait_closed()
            raise OSError("monitoring listener resolved to a non-loopback address")
        if self.endpoint_guard is not None:
            self.endpoint_guard.register_bound_addresses("monitoring", bound_addresses)
        await self.server.start_serving()
        return self.server

    async def stop(self) -> None:
        closing_server: asyncio.Server | None = None
        if self.server is not None:
            closing_server = self.server
            closing_server.close()
            self.server = None
        await self.tasks.cancel_all(self.config.monitoring.write_timeout_seconds)
        still_pending = await self.tasks.wait_remaining(
            self.config.monitoring.write_timeout_seconds
        )
        if still_pending:
            log.error(
                "Monitoring tasks remained after forced cleanup count=%s",
                len(still_pending),
            )
        if closing_server is not None:
            try:
                await asyncio.wait_for(
                    closing_server.wait_closed(),
                    timeout=self.config.monitoring.write_timeout_seconds,
                )
            except asyncio.TimeoutError:
                log.error("Monitoring listener did not close before the shutdown deadline")
