from __future__ import annotations

import asyncio
import socket
from collections.abc import Awaitable, Coroutine
from typing import cast

import pytest

import mc_failover.relay as relay
from mc_failover.relay import PipeEnd, PipeResult
from tests.conftest import FakeClock


class FakeTransport:
    def __init__(self) -> None:
        self.aborted = False

    def abort(self) -> None:
        self.aborted = True


class FakeSocket:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.options: list[tuple[int, int, int]] = []

    def setsockopt(self, level: int, option: int, value: int) -> None:
        if self.fail:
            raise OSError("unsupported")
        self.options.append((level, option, value))


class FakeWriter:
    def __init__(
        self,
        *,
        can_eof: bool = True,
        closing: bool = False,
        socket_value: FakeSocket | None = None,
        write_error: BaseException | None = None,
        drain_error: BaseException | None = None,
        drain_forever: bool = False,
        wait_error: BaseException | None = None,
        wait_forever: bool = False,
    ) -> None:
        self.transport = FakeTransport()
        self.can_eof = can_eof
        self.closing = closing
        self.socket_value = socket_value
        self.write_error = write_error
        self.drain_error = drain_error
        self.drain_forever = drain_forever
        self.wait_error = wait_error
        self.wait_forever = wait_forever
        self.wait_started = asyncio.Event()
        self.drain_started = asyncio.Event()
        self.writes: list[bytes] = []
        self.eof_writes = 0

    def get_extra_info(self, name: str) -> object | None:
        return self.socket_value if name == "socket" else None

    def can_write_eof(self) -> bool:
        return self.can_eof

    def is_closing(self) -> bool:
        return self.closing

    def write_eof(self) -> None:
        self.eof_writes += 1

    def write(self, data: bytes) -> None:
        if self.write_error is not None:
            raise self.write_error
        self.writes.append(data)

    async def drain(self) -> None:
        self.drain_started.set()
        if self.drain_error is not None:
            raise self.drain_error
        if self.drain_forever:
            await asyncio.Event().wait()

    def close(self) -> None:
        self.closing = True

    async def wait_closed(self) -> None:
        self.wait_started.set()
        if self.wait_error is not None:
            raise self.wait_error
        if self.wait_forever:
            await asyncio.Event().wait()


class ScriptedReader:
    def __init__(self, *items: bytes | BaseException | Awaitable[bytes]) -> None:
        self.items = list(items)

    async def read(self, _size: int) -> bytes:
        if not self.items:
            return b""
        item = self.items.pop(0)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, bytes):
            return item
        return await item


def stream_writer(writer: FakeWriter) -> asyncio.StreamWriter:
    return cast(asyncio.StreamWriter, writer)


def stream_reader(reader: ScriptedReader) -> asyncio.StreamReader:
    return cast(asyncio.StreamReader, reader)


@pytest.mark.asyncio
async def test_close_writer_none_success_errors_timeout_and_cancellation() -> None:
    await relay.close_stream_writer(None)

    success = FakeWriter()
    await relay.close_stream_writer(stream_writer(success))
    assert success.closing
    assert not success.transport.aborted

    errored = FakeWriter(wait_error=ConnectionResetError("gone"))
    await relay.close_stream_writer(stream_writer(errored))
    assert errored.transport.aborted

    timed_out = FakeWriter(wait_forever=True)
    await relay.close_stream_writer(stream_writer(timed_out), timeout_seconds=0.001)
    assert timed_out.transport.aborted

    cancelled = FakeWriter(wait_forever=True)
    task = asyncio.create_task(relay.close_stream_writer(stream_writer(cancelled)))
    await cancelled.wait_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.transport.aborted


def test_configure_tcp_writer_without_socket_with_keepalive_and_oserror() -> None:
    no_socket = FakeWriter()
    relay.configure_tcp_writer(stream_writer(no_socket), keepalive=True)

    raw_socket = FakeSocket()
    configured = FakeWriter(socket_value=raw_socket)
    relay.configure_tcp_writer(stream_writer(configured), keepalive=False)
    assert raw_socket.options == [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]

    raw_socket = FakeSocket()
    configured = FakeWriter(socket_value=raw_socket)
    relay.configure_tcp_writer(stream_writer(configured), keepalive=True)
    assert raw_socket.options == [
        (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    ]

    relay.configure_tcp_writer(
        stream_writer(FakeWriter(socket_value=FakeSocket(fail=True))), keepalive=True
    )


def test_activity_uses_monotonic_clock_and_clamps_negative_age() -> None:
    clock = FakeClock()
    activity = relay._Activity(clock, 101.0)
    assert activity.idle_for() == 0.0
    activity.touch()
    clock.advance(2.5)
    assert activity.idle_for() == 2.5


@pytest.mark.asyncio
async def test_idle_reader_disabled_expired_and_opposite_direction_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    activity = relay._Activity(clock, clock.monotonic())
    assert (
        await relay._read_with_idle_deadline(
            stream_reader(ScriptedReader(b"data")), 4, 0.0, activity
        )
        == b"data"
    )

    clock.advance(2)
    with pytest.raises(asyncio.TimeoutError):
        await relay._read_with_idle_deadline(
            stream_reader(ScriptedReader(b"unused")), 4, 1.0, activity
        )

    clock = FakeClock()
    activity = relay._Activity(clock, clock.monotonic())
    real_wait_for = asyncio.wait_for

    async def timeout_without_refresh(awaitable: Awaitable[bytes], *, timeout: float) -> bytes:
        assert timeout > 0
        cast(Coroutine[object, object, bytes], awaitable).close()
        clock.advance(1.0)
        raise asyncio.TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", timeout_without_refresh)
    with pytest.raises(asyncio.TimeoutError):
        await relay._read_with_idle_deadline(
            stream_reader(ScriptedReader(b"unused")), 4, 1.0, activity
        )

    clock = FakeClock()
    activity = relay._Activity(clock, clock.monotonic())
    calls = 0

    async def first_timeout_then_read(awaitable: Awaitable[bytes], *, timeout: float) -> bytes:
        nonlocal calls
        assert timeout > 0
        calls += 1
        if calls == 1:
            cast(Coroutine[object, object, bytes], awaitable).close()
            activity.touch()
            raise asyncio.TimeoutError
        return await real_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr(asyncio, "wait_for", first_timeout_then_read)
    result = await relay._read_with_idle_deadline(
        stream_reader(ScriptedReader(b"refreshed")), 32, 1.0, activity
    )
    assert result == b"refreshed"
    assert calls == 2


@pytest.mark.asyncio
async def test_write_eof_capability_closing_and_timeout_modes() -> None:
    unsupported = FakeWriter(can_eof=False)
    await relay._write_eof(stream_writer(unsupported), write_timeout_seconds=1.0)
    assert unsupported.eof_writes == 0

    closing = FakeWriter(closing=True)
    await relay._write_eof(stream_writer(closing), write_timeout_seconds=1.0)
    assert closing.eof_writes == 0

    timed = FakeWriter()
    await relay._write_eof(stream_writer(timed), write_timeout_seconds=1.0)
    assert timed.eof_writes == 1

    untimed = FakeWriter()
    await relay._write_eof(stream_writer(untimed), write_timeout_seconds=0.0)
    assert untimed.eof_writes == 1


def activity() -> relay._Activity:
    clock = FakeClock()
    return relay._Activity(clock, clock.monotonic())


@pytest.mark.asyncio
async def test_pipe_forwards_then_half_closes_with_and_without_write_timeout() -> None:
    for timeout in (0.0, 0.2):
        writer = FakeWriter()
        result = await relay.pipe(
            stream_reader(ScriptedReader(b"abc", b"def", b"")),
            stream_writer(writer),
            direction="test",
            buffer_size=4,
            idle_timeout_seconds=0.0,
            write_timeout_seconds=timeout,
            activity=activity(),
        )
        assert result == PipeResult("test", PipeEnd.EOF, 6)
        assert writer.writes == [b"abc", b"def"]
        assert writer.eof_writes == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reader", "writer", "expected_end", "error_type"),
    [
        (
            ScriptedReader(ConnectionResetError("reset")),
            FakeWriter(),
            PipeEnd.READ_ERROR,
            "ConnectionResetError",
        ),
        (
            ScriptedReader(b""),
            FakeWriter(drain_error=BrokenPipeError("broken")),
            PipeEnd.WRITE_ERROR,
            "BrokenPipeError",
        ),
        (
            ScriptedReader(b"data"),
            FakeWriter(write_error=BrokenPipeError("broken")),
            PipeEnd.WRITE_ERROR,
            "BrokenPipeError",
        ),
        (
            ScriptedReader(b"data"),
            FakeWriter(drain_error=asyncio.TimeoutError()),
            PipeEnd.WRITE_ERROR,
            "TimeoutError",
        ),
    ],
)
async def test_pipe_classifies_expected_read_and_write_errors(
    reader: ScriptedReader,
    writer: FakeWriter,
    expected_end: PipeEnd,
    error_type: str,
) -> None:
    result = await relay.pipe(
        stream_reader(reader),
        stream_writer(writer),
        direction="edge",
        buffer_size=4,
        idle_timeout_seconds=0.0,
        write_timeout_seconds=0.0,
        activity=activity(),
    )
    assert result.end is expected_end
    assert result.error_type == error_type
    assert result.bytes_forwarded == 0


@pytest.mark.asyncio
async def test_pipe_idle_timeout_and_cancellation_at_read_eof_and_drain() -> None:
    clock = FakeClock()
    expired = relay._Activity(clock, clock.monotonic() - 2)
    result = await relay.pipe(
        stream_reader(ScriptedReader(b"unused")),
        stream_writer(FakeWriter()),
        direction="idle",
        buffer_size=4,
        idle_timeout_seconds=1.0,
        write_timeout_seconds=1.0,
        activity=expired,
    )
    assert result.end is PipeEnd.IDLE_TIMEOUT

    blocked_read: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
    task = asyncio.create_task(
        relay.pipe(
            stream_reader(ScriptedReader(blocked_read)),
            stream_writer(FakeWriter()),
            direction="read-cancel",
            buffer_size=4,
            idle_timeout_seconds=0.0,
            write_timeout_seconds=1.0,
            activity=activity(),
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    eof_writer = FakeWriter(drain_forever=True)
    task = asyncio.create_task(
        relay.pipe(
            stream_reader(ScriptedReader(b"")),
            stream_writer(eof_writer),
            direction="eof-cancel",
            buffer_size=4,
            idle_timeout_seconds=0.0,
            write_timeout_seconds=0.0,
            activity=activity(),
        )
    )
    await eof_writer.drain_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    data_writer = FakeWriter(drain_forever=True)
    task = asyncio.create_task(
        relay.pipe(
            stream_reader(ScriptedReader(b"data")),
            stream_writer(data_writer),
            direction="drain-cancel",
            buffer_size=4,
            idle_timeout_seconds=0.0,
            write_timeout_seconds=0.0,
            activity=activity(),
        )
    )
    await data_writer.drain_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_relay_nonclean_result_cancels_peer_and_logs_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    peer_cancelled = asyncio.Event()

    async def fake_pipe(*_args: object, direction: str, **_kwargs: object) -> PipeResult:
        if direction == "client_to_server":
            return PipeResult(direction, PipeEnd.READ_ERROR, 4, "ConnectionResetError")
        try:
            await asyncio.Event().wait()
        finally:
            peer_cancelled.set()
        raise AssertionError("unreachable")

    monkeypatch.setattr(relay, "pipe", fake_pipe)
    results = await relay.relay_streams(
        cast(asyncio.StreamReader, object()),
        stream_writer(FakeWriter()),
        cast(asyncio.StreamReader, object()),
        stream_writer(FakeWriter()),
        buffer_size=4,
        idle_timeout_seconds=1.0,
        write_timeout_seconds=1.0,
        drain_timeout_seconds=1.0,
        clock=FakeClock(),
    )
    assert results == (
        PipeResult("client_to_server", PipeEnd.READ_ERROR, 4, "ConnectionResetError"),
    )
    assert peer_cancelled.is_set()


@pytest.mark.asyncio
async def test_relay_clean_eof_drains_peer_and_reports_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_peer = asyncio.Event()
    asyncio.get_running_loop().call_soon(release_peer.set)

    async def fake_pipe(*_args: object, direction: str, **_kwargs: object) -> PipeResult:
        if direction == "client_to_server":
            return PipeResult(direction, PipeEnd.EOF, 1)
        await release_peer.wait()
        return PipeResult(direction, PipeEnd.IDLE_TIMEOUT, 2)

    monkeypatch.setattr(relay, "pipe", fake_pipe)
    results = await relay.relay_streams(
        cast(asyncio.StreamReader, object()),
        stream_writer(FakeWriter()),
        cast(asyncio.StreamReader, object()),
        stream_writer(FakeWriter()),
        buffer_size=4,
        idle_timeout_seconds=1.0,
        write_timeout_seconds=1.0,
        drain_timeout_seconds=1.0,
        clock=FakeClock(),
    )
    assert {result.end for result in results} == {PipeEnd.EOF, PipeEnd.IDLE_TIMEOUT}


@pytest.mark.asyncio
async def test_relay_outer_cancellation_and_unexpected_task_error_collect_all_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled_directions: set[str] = set()

    async def blocked_pipe(*_args: object, direction: str, **_kwargs: object) -> PipeResult:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled_directions.add(direction)
        raise AssertionError("unreachable")

    monkeypatch.setattr(relay, "pipe", blocked_pipe)
    task = asyncio.create_task(
        relay.relay_streams(
            cast(asyncio.StreamReader, object()),
            stream_writer(FakeWriter()),
            cast(asyncio.StreamReader, object()),
            stream_writer(FakeWriter()),
            buffer_size=4,
            idle_timeout_seconds=1.0,
            write_timeout_seconds=1.0,
            drain_timeout_seconds=1.0,
            clock=FakeClock(),
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled_directions == {"client_to_server", "server_to_client"}

    peer_cancelled = asyncio.Event()

    async def exploding_pipe(*_args: object, direction: str, **_kwargs: object) -> PipeResult:
        if direction == "client_to_server":
            raise RuntimeError("relay test explosion")
        try:
            await asyncio.Event().wait()
        finally:
            peer_cancelled.set()
        raise AssertionError("unreachable")

    monkeypatch.setattr(relay, "pipe", exploding_pipe)
    with pytest.raises(RuntimeError, match="explosion"):
        await relay.relay_streams(
            cast(asyncio.StreamReader, object()),
            stream_writer(FakeWriter()),
            cast(asyncio.StreamReader, object()),
            stream_writer(FakeWriter()),
            buffer_size=4,
            idle_timeout_seconds=1.0,
            write_timeout_seconds=1.0,
            drain_timeout_seconds=1.0,
            clock=FakeClock(),
        )
    assert peer_cancelled.is_set()
