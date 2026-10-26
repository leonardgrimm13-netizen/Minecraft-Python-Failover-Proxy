"""Bounded, opt-in Java status responder with lossless handshake inspection.

Only the initial handshake is inspected. Established gameplay remains an
opaque byte stream. The caller authenticates PROXY metadata and acquires the
normal admission lease before entering this service, and owns stream cleanup.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import struct
import zlib
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from .minecraft_status import decode_varint, write_varint

if TYPE_CHECKING:
    from .config import LocalStatusConfig

MAX_FAVICON_BYTES = 16_384
MAX_RESPONSE_BYTES = 65_536
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_FAVICON_PREFIX = "data:image/png;base64,"


class StatusState(str, Enum):
    ONLINE = "online"
    MAINTENANCE = "maintenance"
    FALLBACK = "fallback"
    OFFLINE = "offline"


class InspectionError(str, Enum):
    MALFORMED = "malformed"
    TOO_LARGE = "too_large"
    TIMEOUT = "timeout"
    INCOMPLETE = "incomplete"
    IO_ERROR = "io_error"


@dataclass(frozen=True, slots=True)
class HandshakeInspection:
    prefix: bytes
    next_state: int | None
    protocol: int | None
    deadline: float
    error: InspectionError | None = None


def _png_scanlines(width: int, height: int, bits_per_pixel: int, interlace: int) -> list[int]:
    if not interlace:
        return [(width * bits_per_pixel + 7) // 8] * height
    rows: list[int] = []
    # PNG's seven Adam7 passes, including each pass's own filter bytes.
    for x, y, dx, dy in (
        (0, 0, 8, 8),
        (4, 0, 8, 8),
        (0, 4, 4, 8),
        (2, 0, 4, 4),
        (0, 2, 2, 4),
        (1, 0, 2, 2),
        (0, 1, 1, 2),
    ):
        pass_width = max(0, (width - x + dx - 1) // dx)
        pass_height = max(0, (height - y + dy - 1) // dy)
        if pass_width:
            rows.extend([(pass_width * bits_per_pixel + 7) // 8] * pass_height)
    return rows


def validate_favicon(value: str) -> None:
    """Validate a complete 64 by 64 PNG without unbounded decompression.

    Validation happens when loading configuration, never on a status request.
    A bounded image also bounds ancillary chunks and any client image work.
    """

    if not isinstance(value, str) or not value.startswith(_FAVICON_PREFIX):
        raise ValueError("status.favicon must be a PNG base64 data URI")
    encoded = value[len(_FAVICON_PREFIX) :]
    if len(encoded) > ((MAX_FAVICON_BYTES + 2) // 3) * 4:
        raise ValueError("status.favicon exceeds the 16 KiB image limit")
    try:
        data = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("status.favicon contains invalid base64") from exc
    if len(data) > MAX_FAVICON_BYTES or not data.startswith(_PNG_SIGNATURE):
        raise ValueError("status.favicon must contain a bounded PNG")
    offset = len(_PNG_SIGNATURE)
    image_data = bytearray()
    header: tuple[int, int, int] | None = None
    palette = False
    ended = False
    idat_ended = False
    while offset < len(data):
        if len(data) - offset < 12:
            raise ValueError("status.favicon contains a truncated PNG chunk")
        size = int.from_bytes(data[offset : offset + 4], "big")
        kind = data[offset + 4 : offset + 8]
        end = offset + 12 + size
        if end > len(data):
            raise ValueError("status.favicon contains a truncated PNG chunk")
        payload = data[offset + 8 : end - 4]
        checksum = int.from_bytes(data[end - 4 : end], "big")
        if zlib.crc32(kind + payload) & 0xFFFF_FFFF != checksum:
            raise ValueError("status.favicon contains an invalid PNG checksum")
        if header is None and kind != b"IHDR":
            raise ValueError("status.favicon PNG must start with IHDR")
        if kind == b"IHDR":
            if header is not None or size != 13:
                raise ValueError("status.favicon contains an invalid PNG header")
            width, height, depth, color, compression, filtering, interlace = struct.unpack(
                "!IIBBBBB", payload
            )
            valid_depths = {
                0: {1, 2, 4, 8, 16},
                2: {8, 16},
                3: {1, 2, 4, 8},
                4: {8, 16},
                6: {8, 16},
            }
            if (
                width != 64
                or height != 64
                or depth not in valid_depths.get(color, set())
                or compression != 0
                or filtering != 0
                or interlace not in {0, 1}
            ):
                raise ValueError("status.favicon must be a valid 64x64 PNG")
            header = (depth, color, interlace)
        elif kind == b"PLTE":
            if palette or image_data or not 3 <= size <= 768 or size % 3:
                raise ValueError("status.favicon contains an invalid PNG palette")
            palette = True
        elif kind == b"IDAT":
            if idat_ended:
                raise ValueError("status.favicon PNG image chunks must be consecutive")
            image_data.extend(payload)
        elif kind == b"IEND":
            if size or end != len(data):
                raise ValueError("status.favicon contains an invalid PNG end")
            ended = True
        elif kind in {b"iCCP", b"zTXt", b"iTXt"}:
            raise ValueError("status.favicon compressed metadata is not supported")
        elif kind and not kind[0] & 0x20:
            raise ValueError("status.favicon contains an unknown critical PNG chunk")
        if image_data and kind != b"IDAT":
            idat_ended = True
        offset = end
    if header is None or not ended or not image_data:
        raise ValueError("status.favicon contains an incomplete PNG")
    depth, color, interlace = header
    if color == 3 and not palette:
        raise ValueError("status.favicon indexed PNG requires a palette")
    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}[color]
    rows = _png_scanlines(64, 64, channels * depth, interlace)
    expected = sum(size + 1 for size in rows)
    decoder = zlib.decompressobj()
    try:
        raw = decoder.decompress(bytes(image_data), expected + 1)
    except zlib.error as exc:
        raise ValueError("status.favicon contains invalid PNG image data") from exc
    if len(raw) != expected or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError("status.favicon contains invalid PNG image data")
    offset = 0
    for size in rows:
        if raw[offset] > 4:
            raise ValueError("status.favicon contains an invalid PNG row filter")
        offset += size + 1


def _decode_protocol(data: bytes, offset: int) -> tuple[int, int]:
    """Read signed int32 VarInt; lengths continue using the unsigned parser."""

    value = 0
    start = offset
    for position in range(5):
        if offset >= len(data):
            raise ValueError("incomplete_protocol")
        byte = data[offset]
        offset += 1
        if position == 4 and byte & 0xF0:
            raise ValueError("invalid_protocol")
        value |= (byte & 0x7F) << (position * 7)
        if not byte & 0x80:
            # Positive fields retain the same canonical encoding constraint.
            if value <= 0x7FFF_FFFF:
                if data[start:offset] != write_varint(value):
                    raise ValueError("non_canonical_protocol")
                return value, offset
            if position != 4:
                raise ValueError("invalid_protocol")
            return value - 2**32, offset
    raise ValueError("invalid_protocol")


def _parse_handshake(packet: bytes) -> tuple[int, int]:
    packet_id, offset = decode_varint(packet)
    if packet_id != 0:
        raise ValueError("invalid_handshake_packet")
    protocol, offset = _decode_protocol(packet, offset)
    host_length, offset = decode_varint(packet, offset)
    if not 1 <= host_length <= 1020:
        raise ValueError("invalid_handshake_host")
    host_end = offset + host_length
    if host_end + 2 >= len(packet):
        raise ValueError("incomplete_handshake")
    host = packet[offset:host_end].decode("utf-8")
    if len(host) > 255:
        raise ValueError("invalid_handshake_host")
    next_state, end = decode_varint(packet, host_end + 2)
    if next_state not in {1, 2} or end != len(packet):
        raise ValueError("invalid_handshake_state")
    return protocol, next_state


def make_status_response(config: LocalStatusConfig, state: StatusState) -> bytes:
    motd = {
        StatusState.ONLINE: config.motd_online,
        StatusState.MAINTENANCE: config.motd_maintenance,
        StatusState.FALLBACK: config.motd_fallback,
        StatusState.OFFLINE: config.motd_offline,
    }[state]
    if config.dynamic_hint:
        motd += f"\nStatus: {state.value}"
    status: dict[str, object] = {
        "version": {"name": config.version_name, "protocol": config.protocol_version},
        "description": {"text": motd},
    }
    # TCP connections are not an authoritative Minecraft player count.
    if config.players_online is not None and config.players_max is not None:
        status["players"] = {"online": config.players_online, "max": config.players_max}
    if config.favicon is not None:
        status["favicon"] = config.favicon
    encoded = json.dumps(status, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_RESPONSE_BYTES:
        raise ValueError("status response exceeds its size limit")
    payload = b"\x00" + write_varint(len(encoded)) + encoded
    return write_varint(len(payload)) + payload


class LocalStatusService:
    """One service per configuration generation; no background tasks."""

    def __init__(self, config: LocalStatusConfig) -> None:
        self.config = config
        self.active = 0
        self._responses = {state: make_status_response(config, state) for state in StatusState}

    def try_acquire(self) -> bool:
        if self.active >= self.config.max_concurrent:
            return False
        self.active += 1
        return True

    def release(self) -> None:
        if self.active <= 0:
            raise RuntimeError("local status lease released more than once")
        self.active -= 1

    async def inspect(self, reader: asyncio.StreamReader) -> HandshakeInspection:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.config.session_timeout_seconds
        prefix = bytearray()
        error: InspectionError | None = None
        next_state: int | None = None
        protocol: int | None = None

        async def read(size: int) -> bytes:
            try:
                fragment = await reader.readexactly(size)
            except asyncio.IncompleteReadError as exc:
                prefix.extend(exc.partial)
                raise
            prefix.extend(fragment)
            return fragment

        async def handshake() -> tuple[int, int]:
            for _position in range(5):
                byte = (await read(1))[0]
                if not byte & 0x80:
                    break
            length, _end = decode_varint(bytes(prefix))
            if length < 1:
                raise ValueError("invalid_handshake_length")
            if length + len(prefix) > self.config.max_handshake_bytes:
                raise OverflowError("handshake_size_limit")
            return _parse_handshake(await read(length))

        try:
            protocol, next_state = await asyncio.wait_for(
                handshake(),
                timeout=min(
                    self.config.handshake_timeout_seconds,
                    max(0.0, deadline - loop.time()),
                ),
            )
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            error = InspectionError.TIMEOUT
        except asyncio.IncompleteReadError:
            error = InspectionError.INCOMPLETE
        except OverflowError:
            error = InspectionError.TOO_LARGE
        except (ValueError, UnicodeError):
            error = InspectionError.MALFORMED
        except (ConnectionError, OSError):
            error = InspectionError.IO_ERROR
        return HandshakeInspection(bytes(prefix), next_state, protocol, deadline, error)

    async def respond(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        inspection: HandshakeInspection,
        state: str | StatusState,
    ) -> bool:
        if inspection.next_state != 1 or inspection.error is not None:
            return False

        async def exchange() -> bool:
            # Only two exact packets are accepted, bounding framing and CPU work.
            if await reader.readexactly(2) != b"\x01\x00":
                return False
            writer.write(self._responses[StatusState(state)])
            await writer.drain()
            try:
                ping = await reader.readexactly(10)
            except asyncio.IncompleteReadError as exc:
                # A client may intentionally omit latency measurement.
                return not exc.partial
            if ping[:2] != b"\x09\x01":
                return False
            writer.write(ping)
            await writer.drain()
            return True

        remaining = inspection.deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return False
        try:
            return await asyncio.wait_for(exchange(), timeout=remaining)
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError, OSError):
            return False
