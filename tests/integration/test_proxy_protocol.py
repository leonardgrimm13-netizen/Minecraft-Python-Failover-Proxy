from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from mc_failover.proxy_protocol import (
    PROXY_V2_SIGNATURE,
    ProxyProtocolTLV,
    build_proxy_v1_header,
    build_proxy_v2_header,
    parse_proxy_v1_header,
    parse_proxy_v2_header,
)
from tests.conftest import (
    close_writer,
    closed_local_port,
    make_config,
    running_server,
    server_port,
)
from tests.integration.test_proxy_and_relay import ProxyHarness


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2])
async def test_accept_and_send_bridges_validated_client_identity(version: int) -> None:
    received: asyncio.Future[tuple[bytes, bytes]] = asyncio.get_running_loop().create_future()

    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if version == 1:
            header = await reader.readuntil(b"\r\n")
        else:
            base = await reader.readexactly(16)
            payload_length = int.from_bytes(base[14:16], "big")
            header = base + await reader.readexactly(payload_length)
        payload = await reader.readexactly(5)
        received.set_result((header, payload))
        writer.write(b"OK")
        await writer.drain()
        await close_writer(writer)

    async with running_server(backend) as target:
        port = server_port(target)
        config = make_config(port, port)
        config = replace(
            config,
            proxy_protocol=replace(
                config.proxy_protocol,
                accept=True,
                send=True,
                version=version,
                trusted_proxy_ips=("127.0.0.0/8", "::1/128"),
            ),
        )
        harness = ProxyHarness(config)
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        if version == 1:
            inbound = build_proxy_v1_header("203.0.113.10", "198.51.100.20", 54321, 25565)
        else:
            inbound = build_proxy_v2_header(
                "203.0.113.10",
                "198.51.100.20",
                54321,
                25565,
                tlvs=(ProxyProtocolTLV(0xEE, b"not-forwarded"),),
            )
        writer.write(inbound + b"HELLO")
        await writer.drain()
        assert await asyncio.wait_for(reader.readexactly(2), 1.0) == b"OK"
        outbound, payload = await asyncio.wait_for(received, 1.0)
        parsed = (
            parse_proxy_v1_header(outbound) if version == 1 else parse_proxy_v2_header(outbound)
        )
        assert parsed.source_ip == "203.0.113.10"
        assert parsed.destination_ip == "198.51.100.20"
        assert parsed.source_port == 54321
        assert parsed.destination_port == 25565
        assert parsed.tlvs == ()
        assert payload == b"HELLO"
        await close_writer(writer)
        await harness.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2])
async def test_accept_consumes_header_before_forwarding_payload(version: int) -> None:
    received: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        received.set_result(await reader.readexactly(4))
        await close_writer(writer)

    async with running_server(backend) as target:
        port = server_port(target)
        config = make_config(port, port)
        config = replace(
            config,
            proxy_protocol=replace(
                config.proxy_protocol,
                accept=True,
                version=version,
                trusted_proxy_ips=("127.0.0.1",),
            ),
        )
        harness = ProxyHarness(config)
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        header = (
            build_proxy_v1_header("203.0.113.1", "198.51.100.1", 1234, 25565)
            if version == 1
            else build_proxy_v2_header("203.0.113.1", "198.51.100.1", 1234, 25565)
        )
        writer.write(header + b"DATA")
        await writer.drain()
        assert await asyncio.wait_for(received, 1.0) == b"DATA"
        await reader.read()
        await close_writer(writer)
        await harness.stop()


@pytest.mark.asyncio
async def test_direct_client_send_adds_v1_header_before_payload() -> None:
    received: asyncio.Future[tuple[bytes, bytes]] = asyncio.get_running_loop().create_future()

    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        header = await reader.readuntil(b"\r\n")
        payload = await reader.readexactly(1)
        received.set_result((header, payload))
        await close_writer(writer)

    async with running_server(backend) as target:
        port = server_port(target)
        config = make_config(port, port)
        config = replace(
            config,
            proxy_protocol=replace(config.proxy_protocol, send=True),
        )
        harness = ProxyHarness(config)
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(b"X")
        await writer.drain()
        header, payload = await asyncio.wait_for(received, 1.0)
        parsed = parse_proxy_v1_header(header)
        assert parsed.source_ip == "127.0.0.1"
        assert parsed.destination_ip == "127.0.0.1"
        assert payload == b"X"
        await reader.read()
        await close_writer(writer)
        await harness.stop()


@pytest.mark.asyncio
async def test_untrusted_peer_is_closed_before_header_read_and_releases_limit() -> None:
    port_without_backend = await closed_local_port()
    config = make_config(port_without_backend, port_without_backend)
    config = replace(
        config,
        proxy_protocol=replace(
            config.proxy_protocol,
            accept=True,
            trusted_proxy_ips=("10.0.0.0/8",),
        ),
    )
    harness = ProxyHarness(config)
    proxy_port = await harness.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    assert await asyncio.wait_for(reader.read(), 1.0) == b""
    await close_writer(writer)
    await harness.stop()
    assert harness.runtime.incoming_connections_total == 1
    assert harness.runtime.rejected_connections == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.runtime.active_connections == 0
    assert harness.limiter.active == 0
    assert harness.runtime.main_connect_failures == 0


@pytest.mark.asyncio
async def test_incomplete_slow_header_hits_single_deadline_and_releases_resources() -> None:
    unavailable = await closed_local_port()
    config = make_config(unavailable, unavailable)
    config = replace(
        config,
        proxy_protocol=replace(
            config.proxy_protocol,
            accept=True,
            trusted_proxy_ips=("127.0.0.1",),
            header_timeout_seconds=0.05,
        ),
    )
    harness = ProxyHarness(config)
    proxy_port = await harness.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    writer.write(b"PROXY TCP4 203.0")
    await writer.drain()
    await asyncio.sleep(0)
    assert harness.runtime.active_connections == 0
    assert await asyncio.wait_for(reader.read(), 1.0) == b""
    await close_writer(writer)
    await harness.stop()
    assert harness.runtime.incoming_connections_total == 1
    assert harness.runtime.rejected_connections == 1
    assert harness.runtime.backend_connections_established_total == 0
    assert harness.limiter.active == 0
    assert harness.runtime.active_connections == 0


@pytest.mark.asyncio
async def test_v2_signature_is_not_forwarded_when_accepting_header() -> None:
    received: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    async def backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        received.set_result(await reader.readexactly(1))
        await close_writer(writer)

    async with running_server(backend) as target:
        port = server_port(target)
        config = make_config(port, port)
        config = replace(
            config,
            proxy_protocol=replace(
                config.proxy_protocol,
                accept=True,
                version=2,
                trusted_proxy_ips=("127.0.0.1",),
            ),
        )
        harness = ProxyHarness(config)
        proxy_port = await harness.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(build_proxy_v2_header("2001:db8::1", "2001:db8::2", 1234, 25565) + b"Z")
        await writer.drain()
        assert await asyncio.wait_for(received, 1.0) == b"Z"
        assert PROXY_V2_SIGNATURE not in b"Z"
        await reader.read()
        await close_writer(writer)
        await harness.stop()
