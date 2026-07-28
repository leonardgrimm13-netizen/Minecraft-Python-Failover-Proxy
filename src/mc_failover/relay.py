"""Cancellation-safe bidirectional TCP relay with half-close support."""

from __future__ import annotations

import asyncio
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


@dataclass(frozen=True, slots=True)
class PipeResult:
    direction: str
    end: PipeEnd
    bytes_forwarded: int
    error_type: str | None = None


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
            return PipeResult(direction, PipeEnd.READ_ERROR, forwarded, type(exc).__name__)

        if not data:
            try:
                await _write_eof(
                    destination,
                    write_timeout_seconds=write_timeout_seconds,
                )
            except asyncio.CancelledError:
                raise
            except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
                return PipeResult(direction, PipeEnd.WRITE_ERROR, forwarded, type(exc).__name__)
            return PipeResult(direction, PipeEnd.EOF, forwarded)

        activity.touch()
        try:
            destination.write(data)
            if write_timeout_seconds > 0:
                await asyncio.wait_for(destination.drain(), timeout=write_timeout_seconds)
            else:
                await destination.drain()
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, OSError) as exc:
            return PipeResult(direction, PipeEnd.WRITE_ERROR, forwarded, type(exc).__name__)
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
