"""Cancellation-safe bidirectional TCP relay with half-close support."""

from __future__ import annotations

import asyncio
import errno
import logging
import socket
from dataclasses import dataclass
from enum import Enum

from .time_utils import SYSTEM_CLOCK, Clock

log = logging.getLogger("mc-failover.relay")


class PipeEnd(str, Enum):
    EOF = "eof"
    IDLE_TIMEOUT = "idle_timeout"
    READ_ERROR = "read_error"
    WRITE_ERROR = "write_error"


class RelayErrorKind(str, Enum):
    """Socket evidence, captured before exceptions leave the data path."""

    RESET = "reset"
    BROKEN_PIPE = "broken_pipe"
    NETWORK = "network"
    TIMEOUT = "timeout"
    OTHER_IO = "other_io"


class RelayEndpoint(str, Enum):
    CLIENT = "client"
    BACKEND = "backend"
    UNKNOWN = "unknown"


class RelayOutcomeKind(str, Enum):
    EOF = "eof"
    CLIENT_DISCONNECT = "client_disconnect"
    BACKEND_RESET = "backend_reset"
    BACKEND_IO_ERROR = "backend_io_error"
    AMBIGUOUS = "ambiguous"
    EARLY_BACKEND_END = "early_backend_end"
    IDLE_TIMEOUT = "idle_timeout"
    SHUTDOWN = "shutdown"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class PipeResult:
    direction: str
    end: PipeEnd
    bytes_forwarded: int
    error_type: str | None = None
    error_kind: RelayErrorKind | None = None
    error_errno: int | None = None
    eof_propagation: bool = False

    @property
    def error_endpoint(self) -> RelayEndpoint:
        if self.end not in {PipeEnd.READ_ERROR, PipeEnd.WRITE_ERROR}:
            return RelayEndpoint.UNKNOWN
        if self.direction == "client_to_server":
            return RelayEndpoint.CLIENT if self.end is PipeEnd.READ_ERROR else RelayEndpoint.BACKEND
        if self.direction == "server_to_client":
            return RelayEndpoint.BACKEND if self.end is PipeEnd.READ_ERROR else RelayEndpoint.CLIENT
        return RelayEndpoint.UNKNOWN


@dataclass(frozen=True, slots=True)
class RelayOutcome:
    kind: RelayOutcomeKind
    backend_failure: bool = False
    confirm_health: bool = False
    early: bool = False


def _error_result(
    direction: str,
    end: PipeEnd,
    forwarded: int,
    error: BaseException,
    *,
    eof_propagation: bool = False,
) -> PipeResult:
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        kind = RelayErrorKind.TIMEOUT
    elif isinstance(error, ConnectionResetError):
        kind = RelayErrorKind.RESET
    elif isinstance(error, BrokenPipeError):
        kind = RelayErrorKind.BROKEN_PIPE
    elif isinstance(error, ConnectionError) or (
        isinstance(error, OSError)
        and error.errno
        in {
            errno.ECONNABORTED,
            errno.ECONNREFUSED,
            errno.EHOSTUNREACH,
            errno.ENETDOWN,
            errno.ENETUNREACH,
            errno.ENOTCONN,
            errno.ETIMEDOUT,
        }
    ):
        kind = RelayErrorKind.NETWORK
    else:
        kind = RelayErrorKind.OTHER_IO
    return PipeResult(
        direction,
        end,
        forwarded,
        type(error).__name__,
        kind,
        error.errno if isinstance(error, OSError) else None,
        eof_propagation,
    )


def classify_relay_result(
    results: tuple[PipeResult, ...],
    *,
    duration_seconds: float,
    early_session_seconds: float = 0.0,
    shutting_down: bool = False,
    cancelled: bool = False,
) -> RelayOutcome:
    """Attribute errors by the socket operation, never by exception messages.

    EOF and inactivity are normal. A short backend EOF without any response
    merely requests an active health confirmation when explicitly enabled.
    Failed half-close propagation and write deadlines are also ambiguous.
    """

    early = early_session_seconds > 0 and duration_seconds <= early_session_seconds
    if shutting_down:
        return RelayOutcome(RelayOutcomeKind.SHUTDOWN, early=early)
    if cancelled:
        return RelayOutcome(RelayOutcomeKind.CANCELLED, early=early)
    client_errors: list[PipeResult] = []
    backend_errors: list[PipeResult] = []
    for result in results:
        if result.end not in {PipeEnd.READ_ERROR, PipeEnd.WRITE_ERROR}:
            continue
        (
            backend_errors if result.error_endpoint is RelayEndpoint.BACKEND else client_errors
        ).append(result)
    # A client abort can cause a failed write_eof to the backend as cleanup.
    direct_backend_errors = [result for result in backend_errors if not result.eof_propagation]
    if direct_backend_errors:
        if any(result.error_kind is RelayErrorKind.RESET for result in direct_backend_errors):
            return RelayOutcome(RelayOutcomeKind.BACKEND_RESET, True, early=early)
        if any(
            result.error_kind in {RelayErrorKind.NETWORK, RelayErrorKind.BROKEN_PIPE}
            for result in direct_backend_errors
        ):
            return RelayOutcome(RelayOutcomeKind.BACKEND_IO_ERROR, True, early=early)
    if client_errors:
        return RelayOutcome(RelayOutcomeKind.CLIENT_DISCONNECT, early=early)
    if backend_errors:
        return RelayOutcome(RelayOutcomeKind.AMBIGUOUS, confirm_health=True, early=early)
    if any(result.end is PipeEnd.IDLE_TIMEOUT for result in results):
        return RelayOutcome(RelayOutcomeKind.IDLE_TIMEOUT, early=early)
    if (
        early
        and any(
            result.direction == "server_to_client"
            and result.end is PipeEnd.EOF
            and result.bytes_forwarded == 0
            for result in results
        )
        and not any(
            result.direction == "client_to_server" and result.end is PipeEnd.EOF
            for result in results
        )
    ):
        return RelayOutcome(RelayOutcomeKind.EARLY_BACKEND_END, confirm_health=True, early=True)
    return RelayOutcome(RelayOutcomeKind.EOF, early=early)


@dataclass(slots=True)
class _Activity:
    clock: Clock
    last_at: float

    def touch(self) -> None:
        self.last_at = self.clock.monotonic()

    def idle_for(self) -> float:
        return max(0.0, self.clock.monotonic() - self.last_at)


async def close_stream_writer(
    writer: asyncio.StreamWriter | None,
    *,
    timeout_seconds: float = 1.0,
) -> None:
    """Close a writer and abort its transport if graceful close stalls."""

    if writer is None:
        return
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=timeout_seconds)
    except asyncio.CancelledError:
        writer.transport.abort()
        raise
    except (asyncio.TimeoutError, ConnectionError, OSError):
        writer.transport.abort()


def configure_tcp_writer(
    writer: asyncio.StreamWriter,
    *,
    keepalive: bool,
) -> None:
    raw_socket = writer.get_extra_info("socket")
    if raw_socket is None:
        return
    try:
        raw_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if keepalive:
            raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except OSError as exc:
        log.debug("Unable to configure TCP socket error=%s", type(exc).__name__)


async def _read_with_idle_deadline(
    source: asyncio.StreamReader,
    size: int,
    idle_timeout_seconds: float,
    activity: _Activity,
) -> bytes:
    if idle_timeout_seconds <= 0:
        return await source.read(size)
    while True:
        remaining = idle_timeout_seconds - activity.idle_for()
        if remaining <= 0:
            raise asyncio.TimeoutError
        try:
            return await asyncio.wait_for(source.read(size), timeout=remaining)
        except asyncio.TimeoutError:
            # The opposite direction may have refreshed shared activity while
            # this read was waiting. Only end the connection if it is globally idle.
            if activity.idle_for() >= idle_timeout_seconds:
                raise


async def _write_eof(
    destination: asyncio.StreamWriter,
    *,
    write_timeout_seconds: float,
) -> None:
    if not destination.can_write_eof() or destination.is_closing():
        return
    destination.write_eof()
    if write_timeout_seconds > 0:
        await asyncio.wait_for(destination.drain(), timeout=write_timeout_seconds)
    else:
        await destination.drain()


async def pipe(
    source: asyncio.StreamReader,
    destination: asyncio.StreamWriter,
    *,
    direction: str,
    buffer_size: int,
    idle_timeout_seconds: float,
    write_timeout_seconds: float,
    activity: _Activity,
) -> PipeResult:
    """Copy one direction and propagate EOF as a TCP half-close."""

    forwarded = 0
    while True:
        try:
            data = await _read_with_idle_deadline(
                source, buffer_size, idle_timeout_seconds, activity
            )
        except asyncio.TimeoutError:
            return PipeResult(direction, PipeEnd.IDLE_TIMEOUT, forwarded)
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError, ConnectionError, OSError) as exc:
            return _error_result(direction, PipeEnd.READ_ERROR, forwarded, exc)

        if not data:
            try:
                await _write_eof(
                    destination,
                    write_timeout_seconds=write_timeout_seconds,
                )
            except asyncio.CancelledError:
                raise
            except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
                return _error_result(
                    direction, PipeEnd.WRITE_ERROR, forwarded, exc, eof_propagation=True
                )
            return PipeResult(direction, PipeEnd.EOF, forwarded)

        activity.touch()
        try:
            destination.write(data)
            # A zero transport buffer means write() handed everything to the
            # kernel. Avoid allocating a timeout for a drain that cannot block.
            # Buffered writes retain normal backpressure and the same deadline;
            # closing writers and EOF still go through drain's error checks.
            if destination.transport.get_write_buffer_size() or destination.is_closing():
                if write_timeout_seconds > 0:
                    await asyncio.wait_for(destination.drain(), timeout=write_timeout_seconds)
                else:
                    await destination.drain()
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, OSError) as exc:
            return _error_result(direction, PipeEnd.WRITE_ERROR, forwarded, exc)
        forwarded += len(data)
        activity.touch()


async def relay_streams(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    server_reader: asyncio.StreamReader,
    server_writer: asyncio.StreamWriter,
    *,
    buffer_size: int,
    idle_timeout_seconds: float,
    write_timeout_seconds: float,
    drain_timeout_seconds: float,
    clock: Clock = SYSTEM_CLOCK,
) -> tuple[PipeResult, ...]:
    """Relay both directions and drain the peer direction after a clean EOF."""

    activity = _Activity(clock=clock, last_at=clock.monotonic())
    tasks = {
        asyncio.create_task(
            pipe(
                client_reader,
                server_writer,
                direction="client_to_server",
                buffer_size=buffer_size,
                idle_timeout_seconds=idle_timeout_seconds,
                write_timeout_seconds=write_timeout_seconds,
                activity=activity,
            ),
            name="relay-client-to-server",
        ),
        asyncio.create_task(
            pipe(
                server_reader,
                client_writer,
                direction="server_to_client",
                buffer_size=buffer_size,
                idle_timeout_seconds=idle_timeout_seconds,
                write_timeout_seconds=write_timeout_seconds,
                activity=activity,
            ),
            name="relay-server-to-client",
        ),
    }
    results: list[PipeResult] = []
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        first_results = [task.result() for task in done]
        results.extend(first_results)
        clean_eof = all(result.end is PipeEnd.EOF for result in first_results)
        if pending and clean_eof:
            drained, pending = await asyncio.wait(pending, timeout=drain_timeout_seconds)
            results.extend(task.result() for task in drained)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for result in results:
            if result.end in {PipeEnd.READ_ERROR, PipeEnd.WRITE_ERROR}:
                log.debug(
                    "Relay ended direction=%s result=%s error=%s",
                    result.direction,
                    result.end.value,
                    result.error_type,
                )
            elif result.end is PipeEnd.IDLE_TIMEOUT:
                log.info("Relay idle timeout direction=%s", result.direction)
        return tuple(results)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except Exception:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
