from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest

from mc_failover.app import FailoverApplication
from mc_failover.config import BackendConfig, LocalStatusConfig
from mc_failover.minecraft_status import (
    decode_varint,
    make_minecraft_status_packet,
    read_status_packet,
)
from mc_failover.models import MaintenanceMode
from mc_failover.proxy_protocol import build_proxy_header_for_version
from tests.conftest import close_writer, closed_local_port, make_config, running_server, server_port
from tests.integration.test_hot_reload import echo, exchange


@pytest.mark.asyncio
async def test_multi_proxy_retries_once_per_target_and_forced_target_fails_closed() -> None:
    async with running_server(echo) as healthy:
        base = make_config(await closed_local_port(), await closed_local_port())
        check = replace(base.healthcheck, enabled=False)
        backends = tuple(
            BackendConfig(name, "127.0.0.1", port, check, base.circuit_breaker, priority=index)
            for index, (name, port) in enumerate(
                (
                    ("MAIN", base.main.port),
                    ("FALLBACK_1", base.fallback.port),
                    ("FALLBACK_2", server_port(healthy)),
                )
            )
        )
        config = replace(base, backends=backends)
        app = FailoverApplication(config)
        await app.start()
        try:
            assert app.proxy.server is not None
            reader, writer = await asyncio.open_connection(
                "127.0.0.1", server_port(app.proxy.server)
            )
            try:
                await exchange(reader, writer, b"third backend receives every byte")
                assert app.registry.entries["MAIN"].connect_failures == 1
                assert app.registry.entries["FALLBACK_1"].connect_failures == 1
                assert app.registry.entries["FALLBACK_2"].active_connections == 1
            finally:
                await close_writer(writer)
        finally:
            await app.shutdown()

        forced = replace(
            config,
            maintenance=replace(
                config.maintenance, mode=MaintenanceMode.FORCE_TARGET, force_target="FALLBACK_1"
            ),
        )
        app = FailoverApplication(forced)
        await app.start()
        try:
            assert app.proxy.server is not None
            reader, writer = await asyncio.open_connection(
                "127.0.0.1", server_port(app.proxy.server)
            )
            try:
                assert await asyncio.wait_for(reader.read(), 1) == b""
                assert app.registry.entries["MAIN"].connect_failures == 0
                assert app.registry.entries["FALLBACK_2"].connect_successes == 0
            finally:
                await close_writer(writer)
        finally:
            await app.shutdown()


@pytest.mark.asyncio
async def test_removed_backend_session_survives_and_new_sessions_follow_new_priority() -> None:
    async with running_server(echo) as main, running_server(echo) as fallback:
        config = make_config(
            server_port(main), server_port(fallback), proxy_port=await closed_local_port()
        )
        config = replace(config, monitoring=replace(config.monitoring, listen_port=18080))
        backends = config.effective_backends
        app = FailoverApplication(replace(config, backends=backends))
        await app.start()
        held: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []
        try:
            held = list(
                await asyncio.gather(
                    *(
                        asyncio.open_connection("127.0.0.1", config.proxy.listen_port)
                        for _ in range(30)
                    )
                )
            )
            await asyncio.gather(*(exchange(r, w, b"before") for r, w in held))
            old_main = app.registry.entries["MAIN"]
            candidate = replace(config, backends=(replace(backends[1], priority=0),))
            assert await app.reload_config(candidate)
            assert "MAIN" not in app.registry.entries
            await asyncio.gather(*(exchange(r, w, b"after removal") for r, w in held))
            assert old_main.active_connections == 30
            reader, writer = await asyncio.open_connection("127.0.0.1", config.proxy.listen_port)
            try:
                await exchange(reader, writer, b"new fallback")
                assert app.registry.entries["FALLBACK"].active_connections == 1
            finally:
                await close_writer(writer)
        finally:
            await asyncio.gather(*(close_writer(w) for _, w in held))
            await app.shutdown()
        assert (
            app.runtime.active_connections
            == app.limiter.active
            == app.proxy.tasks.active_count
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("health_enabled", [True, False])
async def test_proxy_offline_status_after_trusted_proxy_header(
    version: int, health_enabled: bool
) -> None:
    config = make_config(await closed_local_port(), await closed_local_port())
    config = replace(
        config,
        status=LocalStatusConfig(enabled=True, motd_offline='§cOffline\n"try later"'),
        healthcheck=replace(config.healthcheck, enabled=health_enabled),
        fallback_healthcheck=replace(config.fallback_healthcheck, enabled=health_enabled),
        proxy_protocol=replace(
            config.proxy_protocol, accept=True, version=version, trusted_proxy_ips=("127.0.0.1",)
        ),
    )
    app = FailoverApplication(config)
    await app.start()
    try:
        assert app.proxy.server is not None
        port = server_port(app.proxy.server)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            header = build_proxy_header_for_version(
                version, "203.0.113.10", "127.0.0.1", 45678, port
            )
            ping = b"\x09\x01\x00\x01\x02\x03\x04\x05\x06\x07"
            writer.write(header + make_minecraft_status_packet("example.test", port, 767) + ping)
            await writer.drain()
            packet = await asyncio.wait_for(read_status_packet(reader), 1)
            packet_id, offset = decode_varint(packet)
            size, offset = decode_varint(packet, offset)
            assert packet_id == 0
            status = json.loads(packet[offset : offset + size])
            assert status["description"]["text"] == '§cOffline\n"try later"'
            assert "players" not in status
            assert await reader.readexactly(len(ping)) == ping
            assert await reader.read() == b""
            assert app.runtime.backend_connections_established_total == 0
        finally:
            await close_writer(writer)
    finally:
        await app.shutdown()


@pytest.mark.asyncio
async def test_status_inspection_preserves_login_and_pipeline_with_outbound_proxy() -> None:
    received: asyncio.Queue[bytes] = asyncio.Queue()

    async def capture(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            data = await reader.readuntil(b"\n")
            received.put_nowait(data)
            while chunk := await reader.read(4096):
                writer.write(chunk)
                await writer.drain()
        finally:
            await close_writer(writer)

    async with running_server(capture) as backend:
        config = make_config(server_port(backend), server_port(backend))
        config = replace(
            config,
            healthcheck=replace(config.healthcheck, enabled=False),
            fallback_healthcheck=replace(config.fallback_healthcheck, enabled=False),
            status=LocalStatusConfig(enabled=True, always=True),
            proxy_protocol=replace(config.proxy_protocol, send=True),
        )
        app = FailoverApplication(config)
        await app.start()
        try:
            assert app.proxy.server is not None
            port = server_port(app.proxy.server)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            try:
                status_handshake = make_minecraft_status_packet("test", port, 767)[:-2]
                login = status_handshake[:-1] + b"\x02" + b"\x05\x00user"
                writer.write(login)
                await writer.drain()
                assert await asyncio.wait_for(reader.readexactly(len(login)), 1) == login
                assert (await asyncio.wait_for(received.get(), 1)).startswith(b"PROXY TCP4 ")
                assert app.snapshot.status_service.active == 0
            finally:
                await close_writer(writer)
        finally:
            await app.shutdown()
