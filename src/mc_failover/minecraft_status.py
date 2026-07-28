"""Bounded Minecraft status-ping encoding and response validation."""

from __future__ import annotations

import json
import unicodedata
from collections.abc import Mapping
from typing import Any, Protocol

from .models import HealthCheckResult

MAX_VARINT_BYTES = 5
MAX_VARINT_VALUE = 0x7FFF_FFFF
MIN_SIGNED_INT32 = -(2**31)
MAX_SIGNED_INT32 = 2**31 - 1
MAX_STATUS_JSON_BYTES = 256 * 1024
MAX_PACKET_BYTES = MAX_STATUS_JSON_BYTES + 4096
MAX_STATUS_HOST_CHARS = 255
MAX_STATUS_HOST_BYTES = MAX_STATUS_HOST_CHARS * 4
MAX_JSON_NESTING = 32
MAX_MOTD_NODES = 1024
MAX_MOTD_TEXT_CHARS = 4096


class ExactReader(Protocol):
    async def readexactly(self, size: int) -> bytes: ...


def write_varint(value: int) -> bytes:
    """Encode a non-negative Minecraft VarInt used for packet lengths."""

    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_VARINT_VALUE:
        raise ValueError("VarInt must be an integer in 0..2147483647")
    encoded = bytearray()
    remaining = value
    while True:
        current = remaining & 0x7F
        remaining >>= 7
        if remaining:
            current |= 0x80
        encoded.append(current)
        if not remaining:
            return bytes(encoded)


def decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Decode a canonical, non-negative VarInt from a byte string."""

    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ValueError("invalid_varint_offset")
    value = 0
    raw = bytearray()
    for position in range(MAX_VARINT_BYTES):
        index = offset + position
        if index >= len(data):
            raise ValueError("incomplete_varint")
        current = data[index]
        raw.append(current)
        if position == MAX_VARINT_BYTES - 1 and current & 0xF8:
            raise ValueError("varint_overflow")
        value |= (current & 0x7F) << (position * 7)
        if not current & 0x80:
            if bytes(raw) != write_varint(value):
                raise ValueError("non_canonical_varint")
            return value, index + 1
    raise ValueError("varint_too_long")


async def read_varint(reader: ExactReader) -> int:
    """Read a canonical VarInt without introducing a per-byte timeout."""

    raw = bytearray()
    for position in range(MAX_VARINT_BYTES):
        current = (await reader.readexactly(1))[0]
        raw.append(current)
        if position == MAX_VARINT_BYTES - 1 and current & 0xF8:
            raise ValueError("varint_overflow")
        if not current & 0x80:
            value, consumed = decode_varint(bytes(raw))
            if consumed != len(raw):
                raise ValueError("invalid_varint")
            return value
    raise ValueError("varint_too_long")


def validate_status_hostname(host: str) -> bytes:
    if not isinstance(host, str) or not host:
        raise ValueError("status_hostname_empty")
    if len(host) > MAX_STATUS_HOST_CHARS:
        raise ValueError("status_hostname_too_long")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in host):
        raise ValueError("status_hostname_contains_control_character")
    encoded = host.encode("utf-8")
    if len(encoded) > MAX_STATUS_HOST_BYTES:
        raise ValueError("status_hostname_utf8_too_long")
    return encoded


def make_minecraft_status_packet(host: str, port: int, protocol_version: int) -> bytes:
    """Build a handshake followed by a status request."""

    host_bytes = validate_status_hostname(host)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("invalid_status_port")
    if (
        isinstance(protocol_version, bool)
        or not isinstance(protocol_version, int)
        or not 0 <= protocol_version <= MAX_VARINT_VALUE
    ):
        raise ValueError("invalid_protocol_version")
    handshake = b"".join(
        (
            write_varint(0),
            write_varint(protocol_version),
            write_varint(len(host_bytes)),
            host_bytes,
            port.to_bytes(2, "big"),
            write_varint(1),
        )
    )
    request = write_varint(0)
    return write_varint(len(handshake)) + handshake + write_varint(len(request)) + request


async def read_status_packet(reader: ExactReader) -> bytes:
    packet_length = await read_varint(reader)
    if not 1 <= packet_length <= MAX_PACKET_BYTES:
        raise ValueError("invalid_packet_length")
    return await reader.readexactly(packet_length)


def _json_nesting_is_safe(text: str, maximum: int = MAX_JSON_NESTING) -> bool:
    depth = 0
    in_string = False
    escaped = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > maximum:
                return False
        elif character in "]}":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0 and not in_string


def extract_motd_text(description: Any) -> str:
    """Extract text iteratively with explicit depth, node and size limits."""

    output: list[str] = []
    output_length = 0
    nodes = 0
    stack: list[tuple[Any, int]] = [(description, 0)]
    while stack:
        node, depth = stack.pop()
        nodes += 1
        if nodes > MAX_MOTD_NODES or depth > MAX_JSON_NESTING:
            raise ValueError("motd_structure_too_deep_or_large")
        if isinstance(node, str):
            output.append(node)
            output_length += len(node)
            if output_length > MAX_MOTD_TEXT_CHARS:
                raise ValueError("motd_text_too_long")
        elif isinstance(node, list):
            stack.extend((child, depth + 1) for child in reversed(node))
        elif isinstance(node, Mapping):
            text = node.get("text")
            if text is not None and not isinstance(text, str):
                raise ValueError("motd_text_invalid_type")
            translate = node.get("translate")
            if translate is not None and (not isinstance(translate, str) or not translate):
                raise ValueError("motd_translate_invalid_type")
            if text is not None and translate is not None:
                raise ValueError("motd_multiple_content_types")
            arguments = node.get("with")
            if arguments is not None and not isinstance(arguments, list):
                raise ValueError("motd_with_invalid_type")
            if arguments is not None and translate is None:
                raise ValueError("motd_with_without_translate")
            extra = node.get("extra")
            if extra is not None and not isinstance(extra, list):
                raise ValueError("motd_extra_invalid_type")
            if extra is not None:
                stack.extend((child, depth + 1) for child in reversed(extra))
            if arguments is not None:
                stack.extend((child, depth + 1) for child in reversed(arguments))
            if translate is not None:
                stack.append((translate, depth + 1))
            if text is not None:
                stack.append((text, depth + 1))
        else:
            raise ValueError("motd_invalid_type")
    return "".join(output)


def sanitize_log_value(value: str | None, *, limit: int = 160) -> str:
    """Remove control sequences and bound externally supplied log fields."""

    if not value:
        return "n/a"
    cleaned = "".join(
        " " if unicodedata.category(character).startswith("C") else character for character in value
    )
    cleaned = " ".join(cleaned.split())
    return cleaned if len(cleaned) <= limit else f"{cleaned[: limit - 3]}..."


def _invalid(reason: str, latency_ms: float | None) -> HealthCheckResult:
    return HealthCheckResult(ok=False, reason=reason, latency_ms=latency_ms)


def parse_status_payload(
    payload: bytes,
    *,
    latency_ms: float,
    require_valid_json: bool,
    reject_uninitialized_protocol: bool = True,
    max_latency_ms: float = 0.0,
    expected_version_contains: str = "",
    motd_must_contain: str = "",
    motd_must_not_contain: str = "",
    min_players_max: int = 0,
) -> HealthCheckResult:
    """Validate packet framing and, optionally, the complete status object."""

    try:
        packet_id, offset = decode_varint(payload)
        if packet_id != 0:
            return _invalid("invalid_packet_id", latency_ms)
        json_length, offset = decode_varint(payload, offset)
    except ValueError as exc:
        return _invalid(str(exc), latency_ms)
    if json_length > MAX_STATUS_JSON_BYTES:
        return _invalid("status_json_too_large", latency_ms)
    end = offset + json_length
    if end > len(payload):
        return _invalid("status_json_truncated", latency_ms)
    if end != len(payload):
        return _invalid("unexpected_packet_bytes", latency_ms)
    try:
        decoded = payload[offset:end].decode("utf-8")
    except UnicodeDecodeError:
        return _invalid("status_json_invalid_utf8", latency_ms)
    if max_latency_ms > 0 and latency_ms > max_latency_ms:
        return _invalid("latency_too_high", latency_ms)
    if not require_valid_json:
        return HealthCheckResult(ok=True, reason="status_packet_ok", latency_ms=latency_ms)
    if not _json_nesting_is_safe(decoded):
        return _invalid("status_json_nesting_invalid", latency_ms)

    def reject_nonstandard_constant(value: str) -> None:
        raise ValueError(f"nonstandard JSON constant: {value}")

    try:
        status = json.loads(decoded, parse_constant=reject_nonstandard_constant)
    except (json.JSONDecodeError, RecursionError, ValueError):
        return _invalid("status_json_invalid_json", latency_ms)
    if not isinstance(status, dict):
        return _invalid("status_json_not_object", latency_ms)

    version = status.get("version")
    if not isinstance(version, dict):
        return _invalid("status_version_invalid", latency_ms)
    version_name = version.get("name")
    version_protocol = version.get("protocol")
    if not isinstance(version_name, str) or not version_name or len(version_name) > 256:
        return _invalid("status_version_name_invalid", latency_ms)
    if (
        isinstance(version_protocol, bool)
        or not isinstance(version_protocol, int)
        or not MIN_SIGNED_INT32 <= version_protocol <= MAX_SIGNED_INT32
    ):
        return _invalid("status_version_protocol_invalid", latency_ms)

    players = status.get("players")
    if not isinstance(players, dict):
        return _invalid("status_players_invalid", latency_ms)
    online = players.get("online")
    maximum = players.get("max")
    if isinstance(online, bool) or not isinstance(online, int) or online < 0:
        return _invalid("status_players_online_invalid", latency_ms)
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0:
        return _invalid("status_players_max_invalid", latency_ms)
    if online > maximum:
        return _invalid("status_players_online_exceeds_max", latency_ms)

    if "description" not in status:
        return _invalid("status_description_missing", latency_ms)
    try:
        motd_text = extract_motd_text(status["description"])
    except ValueError as exc:
        return _invalid(str(exc), latency_ms)

    def result(ok: bool, reason: str) -> HealthCheckResult:
        return HealthCheckResult(
            ok=ok,
            reason=reason,
            latency_ms=latency_ms,
            version_name=version_name,
            players_online=online,
            players_max=maximum,
            motd_text=motd_text,
        )

    if reject_uninitialized_protocol and version_protocol == -1:
        return result(False, "status_server_not_initialized")
    if expected_version_contains and expected_version_contains not in version_name:
        return result(False, "version_mismatch")
    if motd_must_contain and motd_must_contain not in motd_text:
        return result(False, "motd_missing_required_text")
    if motd_must_not_contain and motd_must_not_contain in motd_text:
        return result(False, "motd_contains_forbidden_text")
    if min_players_max > 0 and maximum < min_players_max:
        return result(False, "players_max_too_low")
    return result(True, "status_json_ok")
