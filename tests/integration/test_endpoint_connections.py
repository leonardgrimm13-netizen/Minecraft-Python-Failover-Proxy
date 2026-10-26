"""Real TCP regressions for ownership during DNS connection races."""

from __future__ import annotations

import asyncio
import errno
import socket
from typing import Any

import pytest

from mc_failover.endpoint_safety import EndpointLoopGuard
from tests.conftest import close_writer, make_config, running_server, server_port


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(1024):
            writer.write(data)
            await writer.drain()
    finally:
        await close_writer(writer)


def answer(port: int) -> tuple[Any, ...]:
    return socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", port)


@pytest.mark.asyncio
async def test_dns_candidate_cap_retains_both_address_families(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with running_server(echo) as backend:
        port = server_port(backend)
        loop = asyncio.get_running_loop()
        original_connect = loop.sock_connect

        async def resolve(*_args: Any, **_kwargs: Any) -> list[tuple[Any, ...]]:
            return [
                (
                    socket.AF_INET6,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP,
                    "",
                    ("::1", port, 0, index),
                )
                for index in range(33)
            ] + [answer(port)]

        async def connect(sock: socket.socket, address: Any) -> None:
            if sock.family == socket.AF_INET6:
                raise OSError(errno.ENETUNREACH, "test IPv6 route unavailable")
            await original_connect(sock, address)

        monkeypatch.setattr(loop, "getaddrinfo", resolve)
        monkeypatch.setattr(loop, "sock_connect", connect)
        guard = EndpointLoopGuard(make_config(port, port))
        reader, writer = await guard.open_connection("backend.test", port, timeout_seconds=1)
        try:
            writer.write(b"both families retained")
            await writer.drain()
            assert await reader.readexactly(22) == b"both families retained"
        finally:
            await close_writer(writer)


@pytest.mark.asyncio
async def test_dns_connect_cancellation_closes_raw_winner(monkeypatch: pytest.MonkeyPatch) -> None:
    async with running_server(echo) as backend:
        port = server_port(backend)
        loop = asyncio.get_running_loop()
        original_connect = loop.sock_connect
        observed: list[socket.socket] = []
        children: list[asyncio.Task[Any]] = []
        operation: asyncio.Task[tuple[asyncio.StreamReader, asyncio.StreamWriter]]

        async def resolve(*_args: Any, **_kwargs: Any) -> list[tuple[Any, ...]]:
            return [answer(port)]

        async def connect(sock: socket.socket, address: Any) -> None:
            observed.append(sock)
            child = asyncio.current_task()
            if child is not None:
                children.append(child)
            await original_connect(sock, address)
            operation.cancel()

        monkeypatch.setattr(loop, "getaddrinfo", resolve)
        monkeypatch.setattr(loop, "sock_connect", connect)
        guard = EndpointLoopGuard(make_config(port, port))
        operation = asyncio.create_task(
            guard.open_connection("backend.test", port, timeout_seconds=1)
        )
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert observed and all(sock.fileno() == -1 for sock in observed)
        assert children and all(child.done() for child in children)


@pytest.mark.asyncio
async def test_stream_initialization_failure_closes_dns_winner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with running_server(echo) as backend:
        port = server_port(backend)
        observed: list[socket.socket] = []

        async def resolve(*_args: Any, **_kwargs: Any) -> list[tuple[Any, ...]]:
            return [answer(port)]

        async def fail_stream(*_args: Any, **kwargs: Any) -> Any:
            observed.append(kwargs["sock"])
            raise OSError("injected stream initialization failure")

        monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
        monkeypatch.setattr(asyncio, "open_connection", fail_stream)
        guard = EndpointLoopGuard(make_config(port, port))
        with pytest.raises(OSError, match="stream initialization"):
            await guard.open_connection("backend.test", port, timeout_seconds=1)
        assert observed and all(sock.fileno() == -1 for sock in observed)


@pytest.mark.asyncio
async def test_dns_connect_timeout_closes_all_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = asyncio.get_running_loop()
    observed: list[socket.socket] = []

    async def resolve(*_args: Any, **_kwargs: Any) -> list[tuple[Any, ...]]:
        return [answer(25561), answer(25562)]

    async def blocked(sock: socket.socket, _address: Any) -> None:
        observed.append(sock)
        await asyncio.Event().wait()

    monkeypatch.setattr(loop, "getaddrinfo", resolve)
    monkeypatch.setattr(loop, "sock_connect", blocked)
    guard = EndpointLoopGuard(make_config(25561, 25562))
    with pytest.raises(asyncio.TimeoutError):
        await guard.open_connection("backend.test", 25561, timeout_seconds=0.3)
    assert len(observed) == 2
    assert all(sock.fileno() == -1 for sock in observed)


@pytest.mark.asyncio
async def test_two_stalled_dns_addresses_do_not_starve_reachable_third(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with running_server(echo) as backend:
        port = server_port(backend)
        loop = asyncio.get_running_loop()
        original_connect = loop.sock_connect
        observed: list[socket.socket] = []

        async def resolve(*_args: Any, **_kwargs: Any) -> list[tuple[Any, ...]]:
            return [
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (host, port))
                for host in ("192.0.2.1", "192.0.2.2", "127.0.0.1")
            ]

        async def connect(sock: socket.socket, address: Any) -> None:
            observed.append(sock)
            if address[0] != "127.0.0.1":
                await asyncio.Event().wait()
            await original_connect(sock, address)

        monkeypatch.setattr(loop, "getaddrinfo", resolve)
        monkeypatch.setattr(loop, "sock_connect", connect)
        guard = EndpointLoopGuard(make_config(port, port))
        reader, writer = await guard.open_connection("backend.test", port, timeout_seconds=0.9)
        try:
            writer.write(b"reachable third")
            await writer.drain()
            assert await reader.readexactly(15) == b"reachable third"
            assert len(observed) == 3
            assert all(sock.fileno() == -1 for sock in observed[:2])
        finally:
            await close_writer(writer)
