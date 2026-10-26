from __future__ import annotations

import asyncio
import base64
import json
import struct
import zlib
from dataclasses import replace
from typing import cast

import pytest

from mc_failover.config import LocalStatusConfig
from mc_failover.local_status import (
    MAX_FAVICON_BYTES,
    HandshakeInspection,
    InspectionError,
    LocalStatusService,
    StatusState,
    _decode_protocol,
    _png_scanlines,
    make_status_response,
    validate_favicon,
)
from mc_failover.minecraft_status import decode_varint, write_varint

PNG = b"\x89PNG\r\n\x1a\n"


def chunk(kind: bytes, payload: bytes) -> bytes:
    return (
        len(payload).to_bytes(4, "big")
        + kind
        + payload
        + (zlib.crc32(kind + payload) & 0xFFFF_FFFF).to_bytes(4, "big")
    )


def png_header(
    *,
    width: int = 64,
    height: int = 64,
    depth: int = 8,
    color: int = 6,
    compression: int = 0,
    filtering: int = 0,
    interlace: int = 0,
) -> bytes:
    return struct.pack("!IIBBBBB", width, height, depth, color, compression, filtering, interlace)


def png_image(*, depth: int = 8, color: int = 6, interlace: int = 0) -> bytes:
    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}[color]
    rows = _png_scanlines(64, 64, channels * depth, interlace)
    raw = b"".join(b"\x00" + bytes(size) for size in rows)
    palette = chunk(b"PLTE", b"\0\0\0") if color == 3 else b""
    return (
        PNG
        + chunk(b"IHDR", png_header(depth=depth, color=color, interlace=interlace))
        + palette
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


def favicon(data: bytes) -> str:
    return "data:image/png;base64," + base64.b64encode(data).decode("ascii")


def handshake(
    *, protocol: int = 767, state: int = 1, host: bytes = b"localhost", extra: bytes = b""
) -> bytes:
    encoded_protocol = write_varint(protocol) if protocol >= 0 else b"\xff\xff\xff\xff\x0f"
    packet = (
        b"\0"
        + encoded_protocol
        + write_varint(len(host))
        + host
        + b"\x63\xdd"
        + write_varint(state)
        + extra
    )
    return write_varint(len(packet)) + packet


def memory_reader(data: bytes, *, eof: bool = True) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    if eof:
        reader.feed_eof()
    return reader


def parse_response(response: bytes) -> dict[str, object]:
    size, offset = decode_varint(response)
    assert size == len(response) - offset
    assert response[offset] == 0
    size, offset = decode_varint(response, offset + 1)
    assert size == len(response) - offset
    result = json.loads(response[offset:])
    assert isinstance(result, dict)
    return result


@pytest.mark.parametrize("state", list(StatusState))
def test_status_response_json_escaping_formatting_unknown_counts_and_dynamic_hint(
    state: StatusState,
) -> None:
    motd = '§cOffline\n"Try again" \\ ünicode'
    config = replace(
        LocalStatusConfig(),
        motd_online=motd,
        motd_maintenance=motd,
        motd_fallback=motd,
        motd_offline=motd,
        dynamic_hint=True,
    )
    status = parse_response(make_status_response(config, state))
    assert status["description"] == {"text": motd + f"\nStatus: {state.value}"}
    assert "players" not in status
    assert "favicon" not in status
    assert status["version"] == {"name": "Failover Proxy", "protocol": -1}


def test_explicit_static_counts_and_favicon_are_included_and_response_size_is_bounded() -> None:
    icon = favicon(png_image())
    config = replace(LocalStatusConfig(), players_online=0, players_max=100, favicon=icon)
    status = parse_response(make_status_response(config, StatusState.OFFLINE))
    assert status["players"] == {"online": 0, "max": 100}
    assert status["favicon"] == icon
    with pytest.raises(ValueError, match="size limit"):
        make_status_response(replace(config, motd_offline="x" * 65_536), StatusState.OFFLINE)


@pytest.mark.parametrize(
    ("depth", "color", "interlace"),
    [(8, 6, 0), (8, 2, 0), (1, 0, 0), (2, 3, 0), (8, 4, 0), (16, 6, 1)],
)
def test_favicon_accepts_complete_64_square_png(depth: int, color: int, interlace: int) -> None:
    validate_favicon(favicon(png_image(depth=depth, color=color, interlace=interlace)))


@pytest.mark.parametrize(
    "value",
    [
        "",
        "https://example.test/icon.png",
        "data:image/png;base64,%%%%",
        "data:image/png;base64,ü",
        favicon(b"not a PNG"),
        favicon(PNG + bytes(MAX_FAVICON_BYTES)),
        "data:image/png;base64," + "A" * 30_000,
    ],
)
def test_favicon_rejects_unbounded_or_invalid_base64_input(value: str) -> None:
    with pytest.raises(ValueError, match=r"status\.favicon"):
        validate_favicon(value)


@pytest.mark.parametrize(
    "data",
    [
        PNG,
        PNG + b"truncated",
        PNG + chunk(b"IEND", b""),
        PNG + chunk(b"IHDR", b"short"),
        PNG + chunk(b"IHDR", png_header(width=65)),
        PNG + chunk(b"IHDR", png_header(color=9)),
        PNG + chunk(b"IHDR", png_header(depth=3)),
        PNG + chunk(b"IHDR", png_header(compression=1)),
        PNG + chunk(b"IHDR", png_header(filtering=1)),
        PNG + chunk(b"IHDR", png_header(interlace=2)),
        PNG + chunk(b"IHDR", png_header()) * 2,
        PNG + chunk(b"IHDR", png_header())[:-1],
        PNG + chunk(b"IHDR", png_header())[:-1] + b"\0",
        PNG + chunk(b"IHDR", png_header()) + chunk(b"NOPE", b""),
        PNG + chunk(b"IHDR", png_header()) + chunk(b"IEND", b"x"),
        PNG + chunk(b"IHDR", png_header()) + chunk(b"IEND", b"") + b"trailing",
        PNG + chunk(b"IHDR", png_header()) + chunk(b"PLTE", b"bad!") + chunk(b"IEND", b""),
    ],
)
def test_favicon_rejects_png_framing_crc_and_header_errors(data: bytes) -> None:
    with pytest.raises(ValueError, match=r"status\.favicon"):
        validate_favicon(favicon(data))


@pytest.mark.parametrize(
    "idat",
    [
        b"invalid zlib",
        zlib.compress(b"short"),
        zlib.compress(b"x" * 100_000),
        zlib.compress((b"\x05" + bytes(256)) * 64),
        zlib.compress((b"\x00" + bytes(256)) * 64) + b"tail",
    ],
)
def test_favicon_rejects_bad_or_excessive_decompression(idat: bytes) -> None:
    data = PNG + chunk(b"IHDR", png_header()) + chunk(b"IDAT", idat) + chunk(b"IEND", b"")
    with pytest.raises(ValueError, match=r"status\.favicon"):
        validate_favicon(favicon(data))


def test_favicon_requires_palette_and_consecutive_image_data() -> None:
    header = PNG + chunk(b"IHDR", png_header(color=3))
    compressed = zlib.compress((b"\0" + bytes(64)) * 64)
    image = chunk(b"IDAT", compressed) + chunk(b"IEND", b"")
    for data in (
        header + image,
        header + chunk(b"PLTE", b"\0\0\0") * 2 + image,
        header + chunk(b"IDAT", compressed) + chunk(b"PLTE", b"\0\0\0") + image,
        header
        + chunk(b"PLTE", b"\0\0\0")
        + chunk(b"IDAT", compressed[:4])
        + chunk(b"tEXt", b"name\0value")
        + chunk(b"IDAT", compressed[4:])
        + chunk(b"IEND", b""),
    ):
        with pytest.raises(ValueError, match=r"status\.favicon"):
            validate_favicon(favicon(data))


@pytest.mark.parametrize("protocol", [0, 47, 767, 2**31 - 1, -1])
@pytest.mark.asyncio
async def test_inspect_status_and_signed_unknown_protocol_without_consuming_following_packet(
    protocol: int,
) -> None:
    service = LocalStatusService(LocalStatusConfig())
    prefix = handshake(protocol=protocol)
    reader = memory_reader(prefix + b"\x01\x00")
    inspected = await service.inspect(reader)
    assert inspected.prefix == prefix
    assert inspected.protocol == protocol
    assert inspected.next_state == 1
    assert inspected.error is None
    assert await reader.read() == b"\x01\x00"


@pytest.mark.asyncio
async def test_inspect_login_returns_exact_prefix_for_unchanged_forwarding() -> None:
    service = LocalStatusService(LocalStatusConfig())
    prefix = handshake(state=2, host="münchen.test".encode())
    reader = memory_reader(prefix + b"login payload")
    inspected = await service.inspect(reader)
    assert inspected.next_state == 2
    assert inspected.prefix + await reader.read() == prefix + b"login payload"


@pytest.mark.parametrize(
    "wire",
    [
        b"\x00",
        b"\xff\xff\xff\xff\x7f",
        b"\xff\xff\xff\xff\xff",
        b"\x80\x00",
        b"\x01\x01",
        handshake(host=b""),
        handshake(host=b"x" * 1021),
        handshake(host=b"\xff"),
        handshake(host=b"x" * 256),
        handshake(state=3),
        handshake(extra=b"\0"),
        b"\x03\0\xff\xff",
        b"\x07\0\x80\0\x01a\0\0",
    ],
)
@pytest.mark.asyncio
async def test_malformed_inspection_preserves_all_consumed_bytes(wire: bytes) -> None:
    service = LocalStatusService(LocalStatusConfig())
    reader = memory_reader(wire)
    inspected = await service.inspect(reader)
    assert inspected.error is InspectionError.MALFORMED
    assert inspected.next_state is None
    assert inspected.prefix + await reader.read() == wire


@pytest.mark.asyncio
async def test_inspect_size_limit_incomplete_and_timeout_preserve_bytes() -> None:
    service = LocalStatusService(replace(LocalStatusConfig(), handshake_timeout_seconds=0.01))
    for wire, eof, error in (
        (write_varint(20_000) + b"remainder", True, InspectionError.TOO_LARGE),
        (b"\x05ab", True, InspectionError.INCOMPLETE),
        (b"\x05ab", False, InspectionError.TIMEOUT),
    ):
        reader = memory_reader(wire, eof=eof)
        inspected = await service.inspect(reader)
        assert inspected.error is error
        if not eof:
            reader.feed_eof()
        assert inspected.prefix + await reader.read() == wire


@pytest.mark.parametrize("data", [b"", b"\x80\x00", b"\xff\xff\xff\xff\xff"])
def test_protocol_decoder_rejects_incomplete_noncanonical_and_overflow(data: bytes) -> None:
    with pytest.raises(ValueError):
        _decode_protocol(data, 0)


def test_status_concurrency_cap_and_double_release_protection() -> None:
    service = LocalStatusService(replace(LocalStatusConfig(), max_concurrent=2))
    assert service.try_acquire()
    assert service.try_acquire()
    assert not service.try_acquire()
    assert service.active == 2
    service.release()
    assert service.try_acquire()
    service.release()
    service.release()
    with pytest.raises(RuntimeError, match="released"):
        service.release()


@pytest.mark.asyncio
async def test_inspection_socket_error_and_cancellation_propagation() -> None:
    class FailedReader:
        async def readexactly(self, _size: int) -> bytes:
            raise ConnectionResetError()

    service = LocalStatusService(LocalStatusConfig())
    failed = await service.inspect(cast(asyncio.StreamReader, FailedReader()))
    assert failed.error is InspectionError.IO_ERROR
    task = asyncio.create_task(service.inspect(memory_reader(b"", eof=False)))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class CaptureWriter:
    def __init__(self, error: OSError | None = None) -> None:
        self.data = bytearray()
        self.error = error

    def write(self, data: bytes) -> None:
        self.data.extend(data)

    async def drain(self) -> None:
        if self.error is not None:
            raise self.error


@pytest.mark.parametrize(
    ("wire_request", "expected"),
    [
        (b"\x01\x00\x09\x01" + bytes(range(8)), True),
        (b"\x01\x00", True),
        (b"\x02\x00", False),
        (b"\x01", False),
        (b"\x01\x00\x09", False),
        (b"\x01\x00\x09\x02" + bytes(8), False),
    ],
)
@pytest.mark.asyncio
async def test_status_exchange_accepts_only_status_request_and_exact_ping(
    wire_request: bytes, expected: bool
) -> None:
    service = LocalStatusService(LocalStatusConfig())
    reader = memory_reader(handshake() + wire_request)
    inspected = await service.inspect(reader)
    writer = CaptureWriter()
    assert (
        await service.respond(reader, cast(asyncio.StreamWriter, writer), inspected, "offline")
        is expected
    )
    if expected:
        response = make_status_response(service.config, StatusState.OFFLINE)
        assert bytes(writer.data).startswith(response)
        assert bytes(writer.data)[len(response) :] == (
            wire_request[2:] if len(wire_request) > 2 else b""
        )


@pytest.mark.asyncio
async def test_response_timeout_expired_deadline_io_errors_and_cancel() -> None:
    service = LocalStatusService(replace(LocalStatusConfig(), session_timeout_seconds=0.01))
    reader = memory_reader(handshake() + b"\x01\x00", eof=False)
    inspected = await service.inspect(reader)
    writer = cast(asyncio.StreamWriter, CaptureWriter())
    assert not await service.respond(reader, writer, inspected, "offline")
    assert not await service.respond(reader, writer, replace(inspected, deadline=0), "offline")
    assert not await service.respond(reader, writer, replace(inspected, next_state=2), "offline")
    assert not await service.respond(
        memory_reader(b"\x01\x00"),
        cast(asyncio.StreamWriter, CaptureWriter(ConnectionResetError())),
        replace(inspected, deadline=asyncio.get_running_loop().time() + 1),
        "offline",
    )
    pending_inspection = HandshakeInspection(b"", 1, 767, asyncio.get_running_loop().time() + 5)
    pending = asyncio.create_task(
        service.respond(memory_reader(b"", eof=False), writer, pending_inspection, "offline")
    )
    await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending


@pytest.mark.parametrize("kind", [b"iCCP", b"zTXt", b"iTXt"])
def test_favicon_rejects_compressed_metadata_before_client_delivery(kind: bytes) -> None:
    image = png_image()
    header_end = len(PNG) + 12 + 13
    metadata = chunk(kind, b"metadata\x00\x00" + zlib.compress(b"x" * 1_000_000))
    with pytest.raises(ValueError, match="compressed metadata"):
        validate_favicon(favicon(image[:header_end] + metadata + image[header_end:]))
