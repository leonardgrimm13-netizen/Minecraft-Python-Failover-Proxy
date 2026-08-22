from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from mc_failover.minecraft_status import (
    MAX_JSON_NESTING,
    MAX_MOTD_NODES,
    MAX_MOTD_TEXT_CHARS,
    MAX_PACKET_BYTES,
    MAX_SIGNED_INT32,
    MAX_STATUS_HOST_CHARS,
    MAX_STATUS_JSON_BYTES,
    MAX_VARINT_VALUE,
    MIN_SIGNED_INT32,
    decode_varint,
    extract_motd_text,
    make_minecraft_status_packet,
    parse_status_payload,
    read_status_packet,
    read_varint,
    sanitize_log_value,
    validate_status_hostname,
    write_varint,
)


class MemoryReader:
    def __init__(self, data: bytes) -> None:
        self.data = bytearray(data)
        self.calls: list[int] = []

    async def readexactly(self, size: int) -> bytes:
        self.calls.append(size)
        if len(self.data) < size:
            partial = bytes(self.data)
            self.data.clear()
            raise asyncio.IncompleteReadError(partial=partial, expected=size)
        result = bytes(self.data[:size])
        del self.data[:size]
        return result


def valid_status(*, description: object = "A Minecraft Server") -> dict[str, object]:
    return {
        "version": {"name": "1.21.1", "protocol": 767},
        "players": {"online": 3, "max": 20},
        "description": description,
    }


def status_payload(
    status: object | None = None,
    *,
    raw_json: bytes | None = None,
    packet_id: int = 0,
    declared_json_length: int | None = None,
    suffix: bytes = b"",
) -> bytes:
    encoded = (
        json.dumps(valid_status() if status is None else status, separators=(",", ":")).encode()
        if raw_json is None
        else raw_json
    )
    declared = len(encoded) if declared_json_length is None else declared_json_length
    return write_varint(packet_id) + write_varint(declared) + encoded + suffix


@pytest.mark.parametrize(
    ("value", "encoded"),
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (255, b"\xff\x01"),
        (2_097_151, b"\xff\xff\x7f"),
        (MAX_VARINT_VALUE, b"\xff\xff\xff\xff\x07"),
    ],
)
def test_varint_encoding_is_canonical_and_roundtrips(value: int, encoded: bytes) -> None:
    assert write_varint(value) == encoded
    assert decode_varint(encoded) == (value, len(encoded))


@pytest.mark.parametrize("value", [-1, MAX_VARINT_VALUE + 1, True, False, 1.0, "1", None])
def test_write_varint_rejects_negative_overflow_bool_and_non_integer(value: object) -> None:
    with pytest.raises(ValueError, match="VarInt"):
        write_varint(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "encoded",
    [
        b"\x80\x00",
        b"\x81\x00",
        b"\xff\x00",
        b"\x80\x80\x00",
    ],
)
def test_decode_varint_rejects_non_canonical_encodings(encoded: bytes) -> None:
    with pytest.raises(ValueError, match="non_canonical_varint"):
        decode_varint(encoded)


@pytest.mark.parametrize("encoded", [b"", b"\x80", b"\x80\x80", b"\x80\x80\x80\x80"])
def test_decode_varint_rejects_truncation(encoded: bytes) -> None:
    with pytest.raises(ValueError, match="incomplete_varint"):
        decode_varint(encoded)


@pytest.mark.parametrize(
    "encoded",
    [
        b"\xff\xff\xff\xff\x08",
        b"\x80\x80\x80\x80\x10",
        b"\xff\xff\xff\xff\xff",
    ],
)
def test_decode_varint_rejects_32_bit_overflow(encoded: bytes) -> None:
    with pytest.raises(ValueError, match="varint_overflow"):
        decode_varint(encoded)


def test_decode_varint_honors_offset_and_reports_absolute_end() -> None:
    assert decode_varint(b"prefix\xac\x02suffix", offset=6) == (300, 8)
    with pytest.raises(ValueError, match="incomplete_varint"):
        decode_varint(b"\x01", offset=1)


@pytest.mark.parametrize("offset", [-1, True, 1.5, "0"])
def test_decode_varint_rejects_invalid_offsets(offset: object) -> None:
    with pytest.raises(ValueError, match="invalid_varint_offset"):
        decode_varint(b"\x01", offset=offset)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_read_varint_reads_one_byte_at_a_time_without_overread() -> None:
    reader = MemoryReader(write_varint(300) + b"tail")
    assert await read_varint(reader) == 300
    assert reader.calls == [1, 1]
    assert bytes(reader.data) == b"tail"


@pytest.mark.asyncio
async def test_read_varint_propagates_truncated_stream() -> None:
    with pytest.raises(asyncio.IncompleteReadError):
        await read_varint(MemoryReader(b"\x80"))


@pytest.mark.asyncio
async def test_read_varint_rejects_overflow_before_sixth_byte() -> None:
    reader = MemoryReader(b"\x80\x80\x80\x80\x08extra")
    with pytest.raises(ValueError, match="varint_overflow"):
        await read_varint(reader)
    assert reader.calls == [1, 1, 1, 1, 1]
    assert bytes(reader.data) == b"extra"


def decode_handshake(packet: bytes) -> tuple[int, str, int, int, int]:
    handshake_length, offset = decode_varint(packet)
    handshake_end = offset + handshake_length
    packet_id, offset = decode_varint(packet, offset)
    assert packet_id == 0
    protocol_version, offset = decode_varint(packet, offset)
    host_length, offset = decode_varint(packet, offset)
    host_end = offset + host_length
    host = packet[offset:host_end].decode("utf-8")
    port = int.from_bytes(packet[host_end : host_end + 2], "big")
    next_state, offset = decode_varint(packet, host_end + 2)
    assert offset == handshake_end
    request_length, offset = decode_varint(packet, handshake_end)
    request_id, final_offset = decode_varint(packet, offset)
    assert request_length == 1
    assert final_offset == len(packet)
    return protocol_version, host, port, next_state, request_id


@pytest.mark.parametrize(
    ("host", "port", "protocol_version"),
    [
        ("mc.example.test", 25_565, 767),
        ("münchen.example", 1, 0),
        ("😀" * MAX_STATUS_HOST_CHARS, 65_535, MAX_VARINT_VALUE),
    ],
)
def test_status_handshake_encodes_utf8_byte_length_port_and_status_state(
    host: str, port: int, protocol_version: int
) -> None:
    packet = make_minecraft_status_packet(host, port, protocol_version)
    assert decode_handshake(packet) == (protocol_version, host, port, 1, 0)


@pytest.mark.parametrize(
    "host",
    [
        "",
        "a" * (MAX_STATUS_HOST_CHARS + 1),
        "line\nbreak",
        "nul\x00byte",
        "delete\x7f",
    ],
)
def test_status_hostname_rejects_empty_long_and_ascii_control_values(host: str) -> None:
    with pytest.raises(ValueError, match="status_hostname"):
        validate_status_hostname(host)


def test_status_hostname_length_is_based_on_characters_then_utf8_bytes() -> None:
    host = "😀" * MAX_STATUS_HOST_CHARS
    assert len(validate_status_hostname(host)) == MAX_STATUS_HOST_CHARS * 4
    with pytest.raises(UnicodeEncodeError):
        validate_status_hostname("\ud800")


@pytest.mark.parametrize("port", [0, -1, 65_536, True, False, 25_565.0, "25565"])
def test_status_handshake_rejects_invalid_ports(port: object) -> None:
    with pytest.raises(ValueError, match="invalid_status_port"):
        make_minecraft_status_packet("localhost", port, 767)  # type: ignore[arg-type]


@pytest.mark.parametrize("protocol_version", [-1, MAX_VARINT_VALUE + 1, True, False, 767.0, "767"])
def test_status_handshake_rejects_invalid_protocol_versions(protocol_version: object) -> None:
    with pytest.raises(ValueError, match="invalid_protocol_version"):
        make_minecraft_status_packet(
            "localhost",
            25_565,
            protocol_version,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_read_status_packet_reads_exact_bounded_payload() -> None:
    payload = status_payload()
    reader = MemoryReader(write_varint(len(payload)) + payload + b"next")
    assert await read_status_packet(reader) == payload
    assert bytes(reader.data) == b"next"


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [0, MAX_PACKET_BYTES + 1])
async def test_read_status_packet_rejects_invalid_outer_length(length: int) -> None:
    reader = MemoryReader(write_varint(length))
    with pytest.raises(ValueError, match="invalid_packet_length"):
        await read_status_packet(reader)


@pytest.mark.asyncio
async def test_read_status_packet_propagates_truncated_body() -> None:
    with pytest.raises(asyncio.IncompleteReadError):
        await read_status_packet(MemoryReader(write_varint(10) + b"short"))


def test_valid_status_payload_returns_bounded_details() -> None:
    result = parse_status_payload(status_payload(), latency_ms=12.5, require_valid_json=True)
    assert result.ok
    assert result.reason == "status_json_ok"
    assert result.latency_ms == 12.5
    assert result.version_name == "1.21.1"
    assert result.players_online == 3
    assert result.players_max == 20
    assert result.motd_text == "A Minecraft Server"


@pytest.mark.parametrize("packet_id", [1, 2, 127])
def test_status_payload_requires_response_packet_id_zero(packet_id: int) -> None:
    result = parse_status_payload(
        status_payload(packet_id=packet_id), latency_ms=1.0, require_valid_json=True
    )
    assert not result.ok
    assert result.reason == "invalid_packet_id"


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"\x80\x00", "non_canonical_varint"),
        (b"\x00\x80\x00", "non_canonical_varint"),
        (b"\x00", "incomplete_varint"),
        (b"\x00" + write_varint(MAX_STATUS_JSON_BYTES + 1), "status_json_too_large"),
        (status_payload(raw_json=b"{}", declared_json_length=3), "status_json_truncated"),
        (status_payload(raw_json=b"{}", suffix=b"x"), "unexpected_packet_bytes"),
        (status_payload(raw_json=b"\xff"), "status_json_invalid_utf8"),
    ],
)
def test_status_payload_rejects_varint_string_framing_and_utf8(payload: bytes, reason: str) -> None:
    result = parse_status_payload(payload, latency_ms=1.0, require_valid_json=True)
    assert not result.ok
    assert result.reason == reason


def test_status_payload_can_skip_json_semantics_but_not_framing_or_utf8() -> None:
    result = parse_status_payload(
        status_payload(raw_json=b"not json"),
        latency_ms=1.0,
        require_valid_json=False,
    )
    assert result.ok
    assert result.reason == "status_packet_ok"

    deeply_nested = ("[" * (MAX_JSON_NESTING + 10) + "]" * (MAX_JSON_NESTING + 10)).encode()
    assert parse_status_payload(
        status_payload(raw_json=deeply_nested),
        latency_ms=1.0,
        require_valid_json=False,
    ).ok


@pytest.mark.parametrize(
    ("raw_json", "reason"),
    [
        (b"not-json", "status_json_invalid_json"),
        (b"[]", "status_json_not_object"),
        (
            ("[" * (MAX_JSON_NESTING + 1) + "0" + "]" * (MAX_JSON_NESTING + 1)).encode(),
            "status_json_nesting_invalid",
        ),
        (b'{"unterminated": [}', "status_json_nesting_invalid"),
    ],
)
def test_status_payload_rejects_invalid_json_shape_and_depth(raw_json: bytes, reason: str) -> None:
    result = parse_status_payload(
        status_payload(raw_json=raw_json), latency_ms=1.0, require_valid_json=True
    )
    assert not result.ok
    assert result.reason == reason


@pytest.mark.parametrize("constant", [b"NaN", b"Infinity", b"-Infinity"])
def test_status_payload_rejects_nonstandard_json_numeric_constants(constant: bytes) -> None:
    raw = (
        b'{"version":{"name":"1.21.1","protocol":767},'
        b'"players":{"online":0,"max":20},"description":"ok","ignored":' + constant + b"}"
    )
    result = parse_status_payload(
        status_payload(raw_json=raw), latency_ms=1.0, require_valid_json=True
    )
    assert not result.ok
    assert result.reason == "status_json_invalid_json"


@pytest.mark.parametrize(
    ("version", "reason"),
    [
        (None, "status_version_invalid"),
        ("1.21", "status_version_invalid"),
        ({}, "status_version_name_invalid"),
        ({"name": "", "protocol": 767}, "status_version_name_invalid"),
        ({"name": "x" * 257, "protocol": 767}, "status_version_name_invalid"),
        ({"name": "1.21"}, "status_version_protocol_invalid"),
        ({"name": "1.21", "protocol": True}, "status_version_protocol_invalid"),
        ({"name": "1.21", "protocol": False}, "status_version_protocol_invalid"),
        (
            {"name": "1.21", "protocol": MIN_SIGNED_INT32 - 1},
            "status_version_protocol_invalid",
        ),
        (
            {"name": "1.21", "protocol": MAX_SIGNED_INT32 + 1},
            "status_version_protocol_invalid",
        ),
        ({"name": "1.21", "protocol": "767"}, "status_version_protocol_invalid"),
    ],
)
def test_status_payload_requires_valid_version_name_and_protocol(
    version: object, reason: str
) -> None:
    status = valid_status()
    if version is None:
        status.pop("version")
    else:
        status["version"] = version
    result = parse_status_payload(status_payload(status), latency_ms=1.0, require_valid_json=True)
    assert not result.ok
    assert result.reason == reason


@pytest.mark.parametrize("protocol", [MIN_SIGNED_INT32, 0, 767, MAX_SIGNED_INT32])
def test_status_payload_accepts_signed_int32_protocol_boundaries(protocol: int) -> None:
    status = valid_status()
    status["version"] = {"name": "1.21", "protocol": protocol}

    result = parse_status_payload(status_payload(status), latency_ms=1.0, require_valid_json=True)

    assert result.ok
    assert result.reason == "status_json_ok"


@pytest.mark.parametrize(
    ("reject_uninitialized_protocol", "expected_ok", "expected_reason"),
    [
        (True, False, "status_server_not_initialized"),
        (False, True, "status_json_ok"),
    ],
)
def test_status_payload_treats_uninitialized_protocol_as_a_readiness_filter(
    reject_uninitialized_protocol: bool,
    expected_ok: bool,
    expected_reason: str,
) -> None:
    status = valid_status()
    status["version"] = {"name": "Paper starting", "protocol": -1}

    result = parse_status_payload(
        status_payload(status),
        latency_ms=1.0,
        require_valid_json=True,
        reject_uninitialized_protocol=reject_uninitialized_protocol,
    )

    assert result.ok is expected_ok
    assert result.reason == expected_reason
    assert result.version_name == "Paper starting"


@pytest.mark.parametrize(
    ("players", "reason"),
    [
        (None, "status_players_invalid"),
        ([], "status_players_invalid"),
        ({"online": True, "max": 20}, "status_players_online_invalid"),
        ({"online": -1, "max": 20}, "status_players_online_invalid"),
        ({"online": 1.0, "max": 20}, "status_players_online_invalid"),
        ({"online": 1, "max": False}, "status_players_max_invalid"),
        ({"online": 1, "max": -1}, "status_players_max_invalid"),
        ({"online": 1, "max": 20.0}, "status_players_max_invalid"),
        ({"online": 21, "max": 20}, "status_players_online_exceeds_max"),
    ],
)
def test_status_payload_rejects_invalid_player_counts(players: object, reason: str) -> None:
    status = valid_status()
    if players is None:
        status.pop("players")
    else:
        status["players"] = players
    result = parse_status_payload(status_payload(status), latency_ms=1.0, require_valid_json=True)
    assert not result.ok
    assert result.reason == reason


@pytest.mark.parametrize(
    ("description", "expected"),
    [
        ("plain", "plain"),
        ({"text": "hello"}, "hello"),
        ({"text": "hello", "extra": [" ", {"text": "world"}]}, "hello world"),
        (
            {
                "translate": "chat.type.text",
                "with": [{"text": "Alex"}, "hello"],
                "extra": ["!"],
            },
            "chat.type.textAlexhello!",
        ),
        ([{"text": "a"}, "b", {"text": "c", "extra": ["d"]}], "abcd"),
        ({"extra": []}, ""),
    ],
)
def test_extract_motd_text_handles_supported_description_shapes(
    description: object, expected: str
) -> None:
    assert extract_motd_text(description) == expected
    result = parse_status_payload(
        status_payload(valid_status(description=description)),
        latency_ms=1.0,
        require_valid_json=True,
    )
    assert result.ok
    assert result.motd_text == expected


@pytest.mark.parametrize(
    ("description", "reason"),
    [
        (None, "motd_invalid_type"),
        (123, "motd_invalid_type"),
        ({"text": 1}, "motd_text_invalid_type"),
        ({"translate": 1}, "motd_translate_invalid_type"),
        ({"translate": "key", "text": "also"}, "motd_multiple_content_types"),
        ({"translate": "key", "with": "bad"}, "motd_with_invalid_type"),
        ({"with": []}, "motd_with_without_translate"),
        ({"text": "ok", "extra": "bad"}, "motd_extra_invalid_type"),
        (["ok", False], "motd_invalid_type"),
    ],
)
def test_status_payload_rejects_missing_or_invalid_descriptions(
    description: object, reason: str
) -> None:
    status = valid_status(description=description)
    result = parse_status_payload(status_payload(status), latency_ms=1.0, require_valid_json=True)
    assert not result.ok
    assert result.reason == reason

    if description is None:
        status.pop("description")
        missing = parse_status_payload(
            status_payload(status), latency_ms=1.0, require_valid_json=True
        )
        assert missing.reason == "status_description_missing"


def test_motd_depth_node_and_text_limits_are_iterative_and_bounded() -> None:
    nested: object = "leaf"
    for _ in range(MAX_JSON_NESTING + 1):
        nested = [nested]
    with pytest.raises(ValueError, match="motd_structure_too_deep_or_large"):
        extract_motd_text(nested)

    with pytest.raises(ValueError, match="motd_structure_too_deep_or_large"):
        extract_motd_text(["x"] * MAX_MOTD_NODES)

    with pytest.raises(ValueError, match="motd_text_too_long"):
        extract_motd_text("x" * (MAX_MOTD_TEXT_CHARS + 1))
    assert extract_motd_text("x" * MAX_MOTD_TEXT_CHARS) == "x" * MAX_MOTD_TEXT_CHARS


def test_status_payload_returns_motd_limit_failure_without_recursion_error() -> None:
    result = parse_status_payload(
        status_payload(valid_status(description=["x"] * MAX_MOTD_NODES)),
        latency_ms=1.0,
        require_valid_json=True,
    )
    assert not result.ok
    assert result.reason == "motd_structure_too_deep_or_large"


@pytest.mark.parametrize(
    ("options", "reason"),
    [
        ({"expected_version_contains": "1.20"}, "version_mismatch"),
        ({"motd_must_contain": "READY"}, "motd_missing_required_text"),
        ({"motd_must_not_contain": "Minecraft"}, "motd_contains_forbidden_text"),
        ({"min_players_max": 21}, "players_max_too_low"),
    ],
)
def test_status_filters_fail_with_stable_bounded_reasons(
    options: dict[str, Any], reason: str
) -> None:
    result = parse_status_payload(
        status_payload(), latency_ms=3.0, require_valid_json=True, **options
    )
    assert not result.ok
    assert result.reason == reason
    assert result.version_name == "1.21.1"
    assert result.players_online == 3
    assert result.players_max == 20


def test_status_filters_accept_matching_boundaries() -> None:
    result = parse_status_payload(
        status_payload(),
        latency_ms=10.0,
        require_valid_json=True,
        max_latency_ms=10.0,
        expected_version_contains="21.1",
        motd_must_contain="Minecraft",
        motd_must_not_contain="STARTING",
        min_players_max=20,
    )
    assert result.ok


def test_latency_limit_is_enforced_even_when_json_semantics_are_disabled() -> None:
    result = parse_status_payload(
        status_payload(raw_json=b"not json"),
        latency_ms=10.01,
        require_valid_json=False,
        max_latency_ms=10.0,
    )
    assert not result.ok
    assert result.reason == "latency_too_high"


def test_log_value_sanitization_removes_injection_controls_and_bounds_output() -> None:
    unsafe = " user\r\nforged\x00\tentry\u202e "
    assert sanitize_log_value(unsafe) == "user forged entry"
    assert sanitize_log_value(None) == "n/a"
    assert sanitize_log_value("") == "n/a"

    sanitized = sanitize_log_value("x" * 200, limit=20)
    assert sanitized == "x" * 17 + "..."
    assert len(sanitized) == 20
    assert "\n" not in sanitized and "\r" not in sanitized
