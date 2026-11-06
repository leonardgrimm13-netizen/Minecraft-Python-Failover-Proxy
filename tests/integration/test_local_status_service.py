from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from mc_failover.config import LocalStatusConfig
from mc_failover.local_status import LocalStatusService, StatusState
from mc_failover.minecraft_status import decode_varint, read_status_packet
from mc_failover.relay import close_stream_writer
from tests.conftest import server_port
from tests.unit.test_local_status import handshake


class StatusHarness:
    def __init__(self, config: LocalStatusConfig, state: StatusState) -> None:
        self.service = LocalStatusService(config)
        self.state = state
        self.tasks: set[asyncio.Task[None]] = set()
        self.writers: set[asyncio.StreamWriter] = set()
        self.completed = 0
        self.rejected = 0
        self.started = asyncio.Event()

    def accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if not self.service.try_acquire():
            self.rejected += 1
            writer.close()
            return
        self.writers.add(writer)
        task = asyncio.create_task(self.handle(reader, writer))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.started.set()
        try:
            inspection = await self.service.inspect(reader)
            if inspection.next_state == 1:
                completed = await self.service.respond(reader, writer, inspection, self.state)
                self.completed += completed
            elif inspection.next_state == 2:
                # A transparent consumer replays the inspected prefix once.
                writer.write(inspection.prefix)
                await writer.drain()
                while data := await reader.read(4096):
                    writer.write(data)
                    await writer.drain()
        finally:
            self.service.release()
            await close_stream_writer(writer)
            self.writers.discard(writer)


@asynccontextmanager
async def status_server(
    config: LocalStatusConfig | None = None,
    *,
    state: StatusState = StatusState.OFFLINE,
    host: str = "127.0.0.1",
) -> AsyncIterator[tuple[StatusHarness, int]]:
    harness = StatusHarness(config or LocalStatusConfig(), state)
    server = await asyncio.start_server(harness.accept, host, 0)
    try:
        yield harness, server_port(server)
    finally:
        server.close()
        await asyncio.gather(*(close_stream_writer(writer) for writer in tuple(harness.writers)))
        if harness.tasks:
            await asyncio.gather(*tuple(harness.tasks))
        await server.wait_closed()
        assert harness.service.active == 0
        assert not harness.tasks
        assert not harness.writers


async def query(port: int, *, protocol: int = 767, host: str = "127.0.0.1") -> dict[str, object]:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(handshake(protocol=protocol) + b"\x01\x00")
        await writer.drain()
        payload = await asyncio.wait_for(read_status_packet(reader), 1)
        assert payload[0] == 0
        size, offset = decode_varint(payload, 1)
        assert size == len(payload) - offset
        status = json.loads(payload[offset:])
        assert isinstance(status, dict)
        ping = b"\x09\x01" + (-(2**63)).to_bytes(8, "big", signed=True)
        writer.write(ping)
        await writer.drain()
        assert await asyncio.wait_for(reader.readexactly(len(ping)), 1) == ping
        assert await asyncio.wait_for(reader.read(), 1) == b""
        return status
    finally:
        await close_stream_writer(writer)


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", [47, 767, -1, 99999])
@pytest.mark.parametrize("state", list(StatusState))
async def test_real_tcp_java_status_and_ping_pong_for_each_operating_state(
    protocol: int, state: StatusState
) -> None:
    async with status_server(state=state) as (harness, port):
        result = await query(port, protocol=protocol)
        assert "description" in result
        assert "players" not in result
        assert harness.completed == 1


@pytest.mark.asyncio
async def test_real_tcp_partial_handshake_coalesced_request_and_exact_login_replay() -> None:
    async with status_server() as (_harness, port):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        wire = handshake(state=2) + b"\x00\xffbinary-login-payload"
        try:
            for byte in wire:
                writer.write(bytes((byte,)))
                await writer.drain()
                await asyncio.sleep(0)
            writer.write_eof()
            assert await asyncio.wait_for(reader.read(), 1) == wire
        finally:
            await close_stream_writer(writer)


@pytest.mark.asyncio
async def test_slow_and_malformed_clients_release_status_capacity() -> None:
    config = replace(
        LocalStatusConfig(),
        max_concurrent=1,
        handshake_timeout_seconds=0.05,
        session_timeout_seconds=0.1,
    )
    async with status_server(config) as (harness, port):
        slow_reader, slow_writer = await asyncio.open_connection("127.0.0.1", port)
        slow_writer.write(b"\x80")
        await slow_writer.drain()
        await harness.started.wait()
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            assert await asyncio.wait_for(reader.read(), 1) == b""
            assert harness.rejected == 1
            assert await asyncio.wait_for(slow_reader.read(), 1) == b""
        finally:
            await close_stream_writer(writer)
            await close_stream_writer(slow_writer)
        for malicious in (b"\xff\xff\xff\xff\xff", b"\xff\xff\x7f"):
            bad_reader, bad_writer = await asyncio.open_connection("127.0.0.1", port)
            try:
                bad_writer.write(malicious)
                await bad_writer.drain()
                assert await asyncio.wait_for(bad_reader.read(), 1) == b""
            finally:
                await close_stream_writer(bad_writer)
        assert "description" in await query(port)


@pytest.mark.asyncio
async def test_many_parallel_status_requests_leave_no_background_tasks_or_sockets() -> None:
    config = replace(LocalStatusConfig(), max_concurrent=128)
    async with status_server(config) as (harness, port):
        results = await asyncio.gather(*(query(port) for _ in range(100)))
        assert len(results) == 100
        assert harness.completed == 100
        assert harness.rejected == 0
        assert harness.service.active == 0


@pytest.mark.asyncio
async def test_ipv6_java_status_ping() -> None:
    if not socket.has_ipv6:
        pytest.skip("Python reports no IPv6 support")
    try:
        probe = await asyncio.start_server(lambda _r, w: w.close(), "::1", 0)
    except OSError as exc:
        pytest.skip(f"IPv6 loopback unavailable: {exc}")
    probe.close()
    await probe.wait_closed()
    async with status_server(host="::1") as (_harness, port):
        assert "description" in await query(port, host="::1")
