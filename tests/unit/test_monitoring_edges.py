from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import cast
from unittest.mock import AsyncMock

import pytest

import mc_failover.monitoring as monitoring
from mc_failover.circuit_breaker import CircuitBreaker
from mc_failover.health import HealthState
from mc_failover.models import HealthCheckResult, RejectionReason, TargetName
from mc_failover.monitoring import HttpError, HttpRequest, MonitoringServer
from mc_failover.routing import MaintenanceWatcher, Router
from mc_failover.runtime import RuntimeState
from tests.conftest import make_config


def feed_reader(raw: bytes, *, limit: int = 65_536) -> asyncio.StreamReader:
    reader = asyncio.StreamReader(limit=limit)
    reader.feed_data(raw)
    reader.feed_eof()
    return reader


def make_monitoring_server(*, token: str | None = None) -> MonitoringServer:
    config = make_config(25565, 25566, monitoring=True)
    config = replace(config, monitoring=replace(config.monitoring, bearer_token=token))
    runtime = RuntimeState()
    main = HealthState(TargetName.MAIN, config.healthcheck)
    fallback = HealthState(TargetName.FALLBACK, config.fallback_healthcheck)
    main.report(HealthCheckResult(True, "initial"), initial=True)
    fallback.report(HealthCheckResult(True, "initial"), initial=True)
    circuit = CircuitBreaker(config.circuit_breaker)
    watcher = MaintenanceWatcher(config.maintenance)
    router = Router(config, main, fallback, circuit, watcher)
    return MonitoringServer(config, runtime, main, fallback, circuit, router)


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("127.0.0.1", True),
        ("::1", True),
        ("::ffff:127.0.0.1", True),
        ("0.0.0.0", False),
        ("192.0.2.1", False),
        ("invalid", False),
    ],
)
def test_actual_monitoring_bind_address_classification(address: str, expected: bool) -> None:
    assert monitoring._is_loopback_address(address) is expected


class _BoundSocket:
    def __init__(self, host: str) -> None:
        self.host = host

    def getsockname(self) -> tuple[str, int]:
        return self.host, 8080


class _UnservedServer:
    def __init__(self, host: str) -> None:
        self.sockets = [_BoundSocket(host)]
        self.closed = False
        self.serving = False

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None

    async def start_serving(self) -> None:
        self.serving = True


class FakeTransport:
    def __init__(self) -> None:
        self.aborted = False

    def abort(self) -> None:
        self.aborted = True


class FakeWriter:
    def __init__(
        self,
        *,
        drain_error: BaseException | None = None,
        wait_error: BaseException | None = None,
        wait_forever: bool = False,
    ) -> None:
        self.transport = FakeTransport()
        self.drain_error = drain_error
        self.wait_error = wait_error
        self.wait_forever = wait_forever
        self.closed = asyncio.Event()
        self.wait_started = asyncio.Event()
        self.written = bytearray()
        self._closing = False

    def write(self, data: bytes) -> None:
        self.written.extend(data)

    async def drain(self) -> None:
        if self.drain_error is not None:
            raise self.drain_error

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        self._closing = True
        self.closed.set()

    async def wait_closed(self) -> None:
        self.wait_started.set()
        if self.wait_error is not None:
            raise self.wait_error
        if self.wait_forever:
            await asyncio.Event().wait()


async def parse(raw: bytes) -> HttpRequest:
    return await monitoring.read_http_request(feed_reader(raw), timeout_seconds=0.2)


@pytest.mark.asyncio
async def test_readline_deadline_limits_and_incomplete_input() -> None:
    loop = asyncio.get_running_loop()
    with pytest.raises(asyncio.TimeoutError):
        await monitoring._readline(feed_reader(b""), deadline=loop.time() - 1, maximum=10)

    with pytest.raises(HttpError, match="line too long"):
        await monitoring._readline(
            feed_reader(b"abcdefgh\n", limit=4), deadline=loop.time() + 1, maximum=20
        )
    with pytest.raises(HttpError, match="line too long"):
        await monitoring._readline(feed_reader(b"abcd\r\n"), deadline=loop.time() + 1, maximum=3)
    with pytest.raises(HttpError, match="incomplete request"):
        await monitoring._readline(feed_reader(b""), deadline=loop.time() + 1, maximum=10)
    with pytest.raises(HttpError, match="CRLF"):
        await monitoring._readline(feed_reader(b"partial"), deadline=loop.time() + 1, maximum=10)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (b"broken\r\n\r\n", "invalid request line"),
        (b"G\xffT / HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request line"),
        (b"GET / HTTP/9.9\r\nHost: x\r\n\r\n", "unsupported HTTP version"),
        (b"G@T / HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request method"),
        (b"GET x HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request target"),
        (b"GET /a\\b HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request target"),
        (b"GET /%20 HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request target"),
        (b"GET /\x01x HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request target"),
        (b"GET /\x7fx HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request target"),
        (b"GET /x#fragment HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request target"),
        (b"GET //authority HTTP/1.1\r\nHost: x\r\n\r\n", "invalid request target"),
        (b"GET /a//b HTTP/1.1\r\nHost: x\r\n\r\n", "non-normalized"),
        (b"GET /a/./b HTTP/1.1\r\nHost: x\r\n\r\n", "non-normalized"),
        (b"GET /a/../b HTTP/1.1\r\nHost: x\r\n\r\n", "non-normalized"),
        (b"GET /a/. HTTP/1.1\r\nHost: x\r\n\r\n", "non-normalized"),
        (b"GET / HTTP/1.1\r\n folded: x\r\n\r\n", "invalid request header"),
        (b"GET / HTTP/1.1\r\nNoColon\r\n\r\n", "invalid request header"),
        (b"GET / HTTP/1.1\r\nH\xff: x\r\n\r\n", "invalid request header"),
        (b"GET / HTTP/1.1\r\nBad@: x\r\n\r\n", "invalid request header name"),
        (b"GET / HTTP/1.1\r\nX: a\x00b\r\n\r\n", "invalid request header value"),
        (b"GET / HTTP/1.1\r\nX: a\x7fb\r\n\r\n", "invalid request header value"),
        (b"GET / HTTP/1.1\r\nHost: one\r\nHost: two\r\n\r\n", "invalid Host"),
        (b"GET / HTTP/1.1\r\nHost: \r\n\r\n", "invalid Host"),
        (b"GET / HTTP/1.1\r\n\r\n", "Host header required"),
        (b"GET / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n", "bodies"),
        (b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: nope\r\n\r\n", "Content-Length"),
        (
            b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n",
            "Content-Length",
        ),
        (b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\n", "bodies"),
    ],
)
async def test_http_parser_rejects_each_invalid_method_target_header_and_host_path(
    raw: bytes, message: str
) -> None:
    with pytest.raises(HttpError, match=message):
        await parse(raw)


@pytest.mark.asyncio
async def test_http_parser_header_count_size_and_valid_http10_paths() -> None:
    too_many = b"GET / HTTP/1.1\r\nHost: x\r\n" + b"X: y\r\n" * 64 + b"\r\n"
    with pytest.raises(HttpError, match="too many"):
        await parse(too_many)

    large_value = b"x" * 4088
    too_large = (
        b"GET / HTTP/1.1\r\nHost: x\r\n"
        + b"X: "
        + large_value
        + b"\r\n"
        + b"Y: "
        + large_value
        + b"\r\n"
        + b"Z: "
        + large_value
        + b"\r\n"
        + b"W: "
        + large_value
        + b"\r\n"
        + b"V: "
        + large_value
        + b"\r\n\r\n"
    )
    with pytest.raises(HttpError, match="too many"):
        await parse(too_large)

    request = await parse(b"GET /live?full=1 HTTP/1.0\r\nContent-Length: 0\r\n\r\n")
    assert request.path == "/live"
    assert request.version == "HTTP/1.0"
    assert request.headers["content-length"] == ("0",)


def test_http_helpers_and_authorization_edges() -> None:
    unknown = monitoring.response_bytes(599, b"x", "text/plain")
    assert unknown.startswith(b"HTTP/1.1 599 Error\r\n")
    assert monitoring.json_response(200, {"snowman": "☃"}).endswith(b'"snowman":"\\u2603"}\n')
    surrogate_response = monitoring.json_response(200, {"external": "\ud800"})
    assert surrogate_response.endswith(b'"external":"\\ud800"}\n')

    open_server = make_monitoring_server()
    assert open_server._authorized(HttpRequest("GET", "/", "HTTP/1.1", {}))

    server = make_monitoring_server(token="secret")
    assert not server._authorized(HttpRequest("GET", "/", "HTTP/1.1", {}))
    assert not server._authorized(
        HttpRequest("GET", "/", "HTTP/1.1", {"authorization": ("Bearer a", "Bearer b")})
    )
    assert not server._authorized(
        HttpRequest("GET", "/", "HTTP/1.1", {"authorization": ("Bearer\xa0secret",)})
    )
    assert not server._authorized(
        HttpRequest("GET", "/", "HTTP/1.1", {"authorization": ("secret",)})
    )
    assert not server._authorized(
        HttpRequest("GET", "/", "HTTP/1.1", {"authorization": ("Basic secret",)})
    )
    assert server._authorized(
        HttpRequest("GET", "/", "HTTP/1.1", {"authorization": ("bEaReR secret",)})
    )


@pytest.mark.asyncio
async def test_monitoring_cancel_and_slowloris_always_close_writer() -> None:
    server = make_monitoring_server()
    server.config = replace(
        server.config,
        monitoring=replace(server.config.monitoring, request_timeout_seconds=0.01),
    )
    slow_writer = FakeWriter()
    await server.handle(asyncio.StreamReader(), cast(asyncio.StreamWriter, slow_writer))
    assert b"408 Request Timeout" in slow_writer.written
    assert slow_writer.closed.is_set()

    blocking_writer = FakeWriter(wait_forever=True)
    task = asyncio.create_task(
        server.handle(asyncio.StreamReader(), cast(asyncio.StreamWriter, blocking_writer))
    )
    await asyncio.sleep(0)
    task.cancel()
    await blocking_writer.wait_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert blocking_writer.closed.is_set()
    assert blocking_writer.transport.aborted


@pytest.mark.asyncio
async def test_monitoring_accept_shutdown_tracker_race_and_limit_cleanup() -> None:
    server = make_monitoring_server()
    reader = asyncio.StreamReader()

    server.runtime.shutting_down = True
    shutting_down_writer = FakeWriter()
    server._accepted(reader, cast(asyncio.StreamWriter, shutting_down_writer))
    assert shutting_down_writer.closed.is_set()
    assert server._active == 0

    server.runtime.shutting_down = False
    server._active = server.config.monitoring.max_connections
    limited_writer = FakeWriter()
    server._accepted(reader, cast(asyncio.StreamWriter, limited_writer))
    assert limited_writer.closed.is_set()
    assert server.runtime.monitoring_rejected_connections == 1
    assert server.runtime.rejection_reasons[RejectionReason.MONITORING_LIMIT] == 0

    server._active = 0
    await server.tasks.cancel_all(0)
    raced_writer = FakeWriter()
    server._accepted(reader, cast(asyncio.StreamWriter, raced_writer))
    assert raced_writer.closed.is_set()
    assert server._active == 0


@pytest.mark.asyncio
async def test_monitoring_handler_error_write_error_and_wait_closed_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_monitoring_server()

    async def unexpected_read(
        _reader: asyncio.StreamReader, *, timeout_seconds: float
    ) -> HttpRequest:
        assert timeout_seconds > 0
        raise RuntimeError("test failure")

    monkeypatch.setattr(monitoring, "read_http_request", unexpected_read)
    writer = FakeWriter(wait_error=OSError("already gone"))
    await server.handle(asyncio.StreamReader(), cast(asyncio.StreamWriter, writer))
    assert writer.closed.is_set()
    assert writer.transport.aborted

    async def valid_read(_reader: asyncio.StreamReader, *, timeout_seconds: float) -> HttpRequest:
        return HttpRequest("GET", "/live", "HTTP/1.1", {"host": ("x",)})

    monkeypatch.setattr(monitoring, "read_http_request", valid_read)
    write_error = FakeWriter(drain_error=OSError("peer reset"))
    await server.handle(asyncio.StreamReader(), cast(asyncio.StreamWriter, write_error))
    assert write_error.written.startswith(b"HTTP/1.1 200")
    assert write_error.closed.is_set()


@pytest.mark.asyncio
async def test_monitoring_disabled_start_and_idempotent_stop() -> None:
    server = make_monitoring_server()
    server.config = replace(
        server.config, monitoring=replace(server.config.monitoring, enabled=False)
    )
    assert await server.start() is None
    await server.stop()
    assert server.server is None


@pytest.mark.asyncio
async def test_monitoring_rejects_actual_remote_bind_before_serving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_monitoring_server()
    listener = _UnservedServer("192.0.2.10")
    monkeypatch.setattr(
        asyncio,
        "start_server",
        AsyncMock(return_value=cast(asyncio.Server, listener)),
    )

    with pytest.raises(OSError, match="non-loopback"):
        await server.start()

    assert listener.closed
    assert not listener.serving
    assert server.server is None


@pytest.mark.asyncio
async def test_remote_opt_in_serves_actual_remote_bind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_monitoring_server(token="protected")
    server.config = replace(
        server.config,
        monitoring=replace(server.config.monitoring, allow_remote=True),
    )
    listener = _UnservedServer("192.0.2.10")
    monkeypatch.setattr(
        asyncio,
        "start_server",
        AsyncMock(return_value=cast(asyncio.Server, listener)),
    )

    returned = await server.start()
    assert returned is not None
    assert cast(object, returned) is listener
    assert listener.serving
    assert not listener.closed
