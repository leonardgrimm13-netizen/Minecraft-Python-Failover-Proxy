from __future__ import annotations

import asyncio
import struct
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import pytest

from mc_failover.proxy_protocol import (
    MAX_PROXY_V1_HEADER_BYTES,
    MAX_PROXY_V2_HEADER_BYTES,
    PROXY_V2_SIGNATURE,
    ProxyProtocolInfo,
    ProxyProtocolTLV,
    build_proxy_header_for_version,
    build_proxy_unknown_header,
    build_proxy_unknown_header_for_version,
    build_proxy_v1_header,
    build_proxy_v2_header,
    build_proxy_v2_local_header,
    build_proxy_v2_unknown_header,
    is_trusted_proxy,
    parse_proxy_v1_header,
    parse_proxy_v2_header,
    read_proxy_header_for_version,
    read_proxy_v1_header,
    read_proxy_v2_header,
)

_T = TypeVar("_T")


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


class StepClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class AdvancingReader(MemoryReader):
    def __init__(self, data: bytes, clock: StepClock, advance: Callable[[int], float]) -> None:
        super().__init__(data)
        self.clock = clock
        self.advance = advance

    async def readexactly(self, size: int) -> bytes:
        result = await super().readexactly(size)
        self.clock.now += self.advance(size)
        return result


def run(coroutine: Coroutine[Any, Any, _T]) -> _T:
    return asyncio.run(coroutine)


@pytest.mark.parametrize(
    ("peer", "entries"),
    [
        ("127.0.0.1", ("127.0.0.1",)),
        ("10.23.4.5", ("10.0.0.0/8",)),
        ("192.0.2.7", ("192.0.2.7/32",)),
        ("2001:db8::123", ("2001:db8::/32",)),
        ("::1", ("::1/128",)),
        ("::ffff:192.0.2.7", ("192.0.2.0/24",)),
        ("192.0.2.7", ("::ffff:192.0.2.7/128",)),
        ("192.0.2.7", ("::ffff:192.0.2.0/120",)),
        ("::ffff:192.0.2.7", ("::ffff:192.0.2.0/120",)),
    ],
)
def test_trusted_proxy_accepts_ipv4_ipv6_and_cidr(peer: str, entries: tuple[str, ...]) -> None:
    assert is_trusted_proxy(peer, entries)


@pytest.mark.parametrize(
    ("peer", "entries"),
    [
        ("10.0.0.1", ()),
        ("10.0.0.1", ("",)),
        ("10.0.0.1", ("not-a-network",)),
        ("10.0.0.1", ("2001:db8::/32",)),
        ("2001:db8::1", ("10.0.0.0/8",)),
        ("::ffff:192.0.3.7", ("192.0.2.0/24",)),
        ("192.0.3.7", ("::ffff:192.0.2.0/120",)),
        ("192.0.2.8", ("::ffff:192.0.2.7/128",)),
        ("192.0.2.7", ("::/0",)),
        ("not-an-ip", ("0.0.0.0/0",)),
        ("fe80::1%eth0", ("fe80::/10",)),
    ],
)
def test_trusted_proxy_fails_closed(peer: str, entries: tuple[str, ...]) -> None:
    assert not is_trusted_proxy(peer, entries)


def test_trust_all_requires_literal_true_and_valid_peer() -> None:
    assert is_trusted_proxy("203.0.113.10", (), trust_all=True)
    assert not is_trusted_proxy("invalid", (), trust_all=True)
    assert not is_trusted_proxy("203.0.113.10", (), trust_all=False)
    assert not is_trusted_proxy("203.0.113.10", (), trust_all=1)  # type: ignore[arg-type]


def test_trusted_entries_must_be_an_iterable_of_network_strings() -> None:
    assert not is_trusted_proxy("127.0.0.1", "127.0.0.1")
    assert not is_trusted_proxy("127.0.0.1", (None, 123))  # type: ignore[arg-type]
    assert is_trusted_proxy("127.0.0.1", ("bad", " 127.0.0.1/32 "))


@pytest.mark.parametrize(
    ("source", "destination", "family"),
    [
        ("203.0.113.10", "198.51.100.20", "TCP4"),
        ("2001:db8::1", "2001:db8::2", "TCP6"),
    ],
)
def test_proxy_v1_build_parse_roundtrip(source: str, destination: str, family: str) -> None:
    header = build_proxy_v1_header(source, destination, 54_321, 25_565)
    info = parse_proxy_v1_header(header)
    assert info == ProxyProtocolInfo(source, destination, 54_321, 25_565, family)


def test_proxy_v1_unknown_does_not_expose_addresses() -> None:
    assert build_proxy_unknown_header() == b"PROXY UNKNOWN\r\n"
    info = parse_proxy_v1_header(build_proxy_unknown_header())
    assert info.family == "UNKNOWN"
    assert info.command == "UNKNOWN"
    assert info.source_port == info.destination_port == 0
    assert info.tlvs == ()


@pytest.mark.parametrize(
    "header",
    [
        b"",
        b"PROXY UNKNOWN\n",
        b"PROXY UNKNOWN extra\r\n",
        b"PROXY  UNKNOWN\r\n",
        b"PROXY\tUNKNOWN\r\n",
        b"proxy UNKNOWN\r\n",
        b"PROXY TCP4 192.0.2.1 192.0.2.2 1 2",
        b"PROXY TCP4 192.0.2.1 192.0.2.2 1 2\r\nJUNK\r\n",
        b"PROXY TCP4 192.0.2.1 192.0.2.2 +1 2\r\n",
        b"PROXY TCP4 192.0.2.1 192.0.2.2 -1 2\r\n",
        b"PROXY TCP4 192.0.2.1 192.0.2.2 0 2\r\n",
        b"PROXY TCP4 192.0.2.1 192.0.2.2 65536 2\r\n",
        b"PROXY TCP4 2001:db8::1 2001:db8::2 1 2\r\n",
        b"PROXY TCP6 192.0.2.1 192.0.2.2 1 2\r\n",
        b"PROXY TCP4 999.0.0.1 192.0.2.2 1 2\r\n",
        b"PROXY UDP4 192.0.2.1 192.0.2.2 1 2\r\n",
        b"PROXY TCP4 192.0.2.1 192.0.2.2 1\r\n",
        b"PROXY TCP4 192.0.2.1\xff 192.0.2.2 1 2\r\n",
    ],
)
def test_proxy_v1_rejects_malformed_headers(header: bytes) -> None:
    with pytest.raises(ValueError):
        parse_proxy_v1_header(header)


def test_proxy_v1_enforces_configured_and_global_length_limits() -> None:
    header = build_proxy_v1_header("192.0.2.1", "192.0.2.2", 1, 2)
    with pytest.raises(ValueError, match="too long"):
        parse_proxy_v1_header(header, max_header_bytes=len(header) - 1)
    with pytest.raises(ValueError, match="max_header_bytes"):
        parse_proxy_v1_header(header, max_header_bytes=True)
    with pytest.raises(ValueError, match="too long"):
        parse_proxy_v1_header(b"PROXY " + b"A" * MAX_PROXY_V1_HEADER_BYTES + b"\r\n")


@pytest.mark.parametrize(
    ("source", "destination", "source_port", "destination_port"),
    [
        ("192.0.2.1", "2001:db8::1", 1, 2),
        ("invalid", "192.0.2.1", 1, 2),
        ("192.0.2.1", "192.0.2.2", 0, 2),
        ("192.0.2.1", "192.0.2.2", 1, 65_536),
        ("192.0.2.1", "192.0.2.2", True, 2),
    ],
)
def test_proxy_v1_builder_rejects_bad_family_addresses_and_ports(
    source: str,
    destination: str,
    source_port: int,
    destination_port: int,
) -> None:
    with pytest.raises(ValueError):
        build_proxy_v1_header(source, destination, source_port, destination_port)


@pytest.mark.parametrize(
    ("source", "destination", "family"),
    [
        ("203.0.113.10", "198.51.100.20", "TCP4"),
        ("2001:db8::1", "2001:db8::2", "TCP6"),
    ],
)
def test_proxy_v2_build_parse_roundtrip_with_tlvs(
    source: str, destination: str, family: str
) -> None:
    tlvs = (ProxyProtocolTLV(0x01, b"minecraft"), ProxyProtocolTLV(0xEE, b""))
    header = build_proxy_v2_header(
        source,
        destination,
        54_321,
        25_565,
        tlvs=tlvs,
    )
    info = parse_proxy_v2_header(header)
    assert info == ProxyProtocolInfo(source, destination, 54_321, 25_565, family, tlvs=tlvs)


def test_proxy_v2_unknown_preserves_valid_tlvs_but_not_addresses() -> None:
    tlvs = (ProxyProtocolTLV(0x05, b"unique-id"),)
    info = parse_proxy_v2_header(build_proxy_v2_unknown_header(tlvs=tlvs))
    assert info.family == "UNKNOWN"
    assert info.command == "PROXY"
    assert info.source_port == info.destination_port == 0
    assert info.tlvs == tlvs


def test_proxy_v2_local_ignores_all_address_and_tlv_shaped_payload_data() -> None:
    opaque = b"\xff\xff\xffnot-a-valid-tlv"
    info = parse_proxy_v2_header(build_proxy_v2_local_header(opaque))
    assert info.family == "UNKNOWN"
    assert info.command == "LOCAL"
    assert info.source_port == info.destination_port == 0
    assert info.tlvs == ()

    tcp4_shaped_local = PROXY_V2_SIGNATURE + b"\x20\x11\x00\x0c" + b"\xff" * 12
    assert parse_proxy_v2_header(tcp4_shaped_local).family == "UNKNOWN"


def mutate(header: bytes, offset: int, replacement: bytes) -> bytes:
    result = bytearray(header)
    result[offset : offset + len(replacement)] = replacement
    return bytes(result)


@pytest.mark.parametrize(
    "header",
    [
        b"",
        PROXY_V2_SIGNATURE,
        b"X" * 16,
        mutate(build_proxy_v2_unknown_header(), 12, b"\x10"),
        mutate(build_proxy_v2_unknown_header(), 12, b"\x22"),
        PROXY_V2_SIGNATURE + b"\x21\x01\x00\x00",
        PROXY_V2_SIGNATURE + b"\x21\x10\x00\x0c" + b"\x00" * 12,
        PROXY_V2_SIGNATURE + b"\x21\x12\x00\x0c" + b"\x00" * 12,
        PROXY_V2_SIGNATURE + b"\x21\x21\x00\x24" + b"\x00" * 35,
        PROXY_V2_SIGNATURE + b"\x21\x31\x00\x00",
        PROXY_V2_SIGNATURE + b"\x21\x11\x00\x0b" + b"\x00" * 11,
        PROXY_V2_SIGNATURE + b"\x21\x11\x00\x0d" + b"\x00" * 13,
        PROXY_V2_SIGNATURE + b"\x21\x11\x00\x0e" + b"\x00" * 14,
    ],
)
def test_proxy_v2_rejects_bad_base_fields_and_lengths(header: bytes) -> None:
    with pytest.raises(ValueError):
        parse_proxy_v2_header(header)


def test_proxy_v2_requires_declared_length_to_equal_actual_length() -> None:
    header = build_proxy_v2_header("192.0.2.1", "192.0.2.2", 1, 2)
    with pytest.raises(ValueError, match="length"):
        parse_proxy_v2_header(header[:-1])
    with pytest.raises(ValueError, match="length"):
        parse_proxy_v2_header(header + b"\x00")
    with pytest.raises(ValueError, match="too long"):
        parse_proxy_v2_header(header, max_header_bytes=len(header) - 1)


@pytest.mark.parametrize(
    "suffix",
    [
        b"\x01",
        b"\x01\x00",
        b"\x01\x00\x01",
        b"\x01\x00\x02x",
        b"\x01\xff\xff",
    ],
)
def test_proxy_v2_rejects_truncated_tlv_framing_and_values(suffix: bytes) -> None:
    header = build_proxy_v2_header("192.0.2.1", "192.0.2.2", 1, 2)
    payload_length = int.from_bytes(header[14:16], "big") + len(suffix)
    malformed = header[:14] + struct.pack("!H", payload_length) + header[16:] + suffix
    with pytest.raises(ValueError, match="TLV"):
        parse_proxy_v2_header(malformed)


def test_proxy_v2_rejects_zero_ports() -> None:
    header = bytearray(build_proxy_v2_header("192.0.2.1", "192.0.2.2", 1, 2))
    header[24:26] = b"\x00\x00"
    with pytest.raises(ValueError, match="ports"):
        parse_proxy_v2_header(bytes(header))


@pytest.mark.parametrize(
    ("source", "destination", "source_port", "destination_port"),
    [
        ("192.0.2.1", "2001:db8::1", 1, 2),
        ("invalid", "192.0.2.1", 1, 2),
        ("192.0.2.1", "192.0.2.2", 0, 2),
        ("192.0.2.1", "192.0.2.2", 1, 65_536),
        ("192.0.2.1", "192.0.2.2", 1, False),
    ],
)
def test_proxy_v2_builder_rejects_bad_family_addresses_and_ports(
    source: str,
    destination: str,
    source_port: int,
    destination_port: int,
) -> None:
    with pytest.raises(ValueError):
        build_proxy_v2_header(source, destination, source_port, destination_port)


@pytest.mark.parametrize(
    "tlv",
    [
        ProxyProtocolTLV(-1, b"x"),
        ProxyProtocolTLV(256, b"x"),
        ProxyProtocolTLV(True, b"x"),
        ProxyProtocolTLV(1, bytearray(b"x")),  # type: ignore[arg-type]
    ],
)
def test_proxy_v2_builder_validates_tlvs(tlv: ProxyProtocolTLV) -> None:
    with pytest.raises(ValueError):
        build_proxy_v2_header("192.0.2.1", "192.0.2.2", 1, 2, tlvs=(tlv,))


def test_proxy_v2_builder_rejects_payload_over_protocol_limit() -> None:
    oversized = ProxyProtocolTLV(1, b"x" * 65_535)
    with pytest.raises(ValueError, match="payload"):
        build_proxy_v2_header("192.0.2.1", "192.0.2.2", 1, 2, tlvs=(oversized,))
    with pytest.raises(ValueError, match="payload"):
        build_proxy_v2_local_header(b"x" * 65_536)


def test_v2_constants_cover_base_and_maximum_declared_payload() -> None:
    assert len(PROXY_V2_SIGNATURE) == 12
    assert MAX_PROXY_V2_HEADER_BYTES == 16 + 65_535


def test_read_proxy_v1_header_success_and_incomplete_input() -> None:
    header = build_proxy_v1_header("192.0.2.1", "192.0.2.2", 1, 2)
    info = run(read_proxy_v1_header(MemoryReader(header), 1.0))
    assert info.family == "TCP4"

    with pytest.raises(ValueError, match="Incomplete"):
        run(read_proxy_v1_header(MemoryReader(header[:-2]), 1.0))


def test_read_proxy_v1_header_has_one_absolute_deadline() -> None:
    clock = StepClock()
    header = build_proxy_v1_header("192.0.2.1", "192.0.2.2", 1, 2)
    reader = AdvancingReader(header, clock, lambda _size: 0.2)
    with pytest.raises(TimeoutError):
        run(read_proxy_v1_header(reader, 1.0, monotonic=clock))
    assert len(reader.calls) < len(header)


def test_read_proxy_v1_enforces_limit_while_streaming() -> None:
    reader = MemoryReader(b"PROXY " + b"A" * 100 + b"\r\n")
    with pytest.raises(ValueError, match="too long"):
        run(read_proxy_v1_header(reader, 1.0, max_header_bytes=20))
    assert len(reader.calls) == 20


def test_read_proxy_v2_success_and_incomplete_input() -> None:
    header = build_proxy_v2_header("2001:db8::1", "2001:db8::2", 1, 2)
    info = run(read_proxy_v2_header(MemoryReader(header), 1.0))
    assert info.family == "TCP6"

    with pytest.raises(ValueError, match="Incomplete"):
        run(read_proxy_v2_header(MemoryReader(header[:-1]), 1.0))


def test_read_proxy_v2_has_one_deadline_for_base_and_payload() -> None:
    clock = StepClock()
    header = build_proxy_v2_header("192.0.2.1", "192.0.2.2", 1, 2)
    reader = AdvancingReader(header, clock, lambda size: 0.7 if size == 16 else 0.4)
    with pytest.raises(TimeoutError):
        run(read_proxy_v2_header(reader, 1.0, monotonic=clock))
    assert reader.calls == [16, 12]


def test_read_proxy_v2_rejects_declared_oversize_before_payload_read() -> None:
    base = PROXY_V2_SIGNATURE + b"\x21\x11" + struct.pack("!H", 100)
    reader = MemoryReader(base)
    with pytest.raises(ValueError, match="too long"):
        run(read_proxy_v2_header(reader, 1.0, max_header_bytes=64))
    assert reader.calls == [16]


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True, "1"])
def test_read_helpers_reject_invalid_timeouts(timeout: object) -> None:
    with pytest.raises(ValueError, match="timeout"):
        run(read_proxy_v1_header(MemoryReader(b""), timeout))  # type: ignore[arg-type]


def test_version_build_and_read_dispatch_helpers() -> None:
    v1 = build_proxy_header_for_version(1, "192.0.2.1", "192.0.2.2", 1, 2)
    v2 = build_proxy_header_for_version(2, "192.0.2.1", "192.0.2.2", 1, 2)
    assert run(read_proxy_header_for_version(1, MemoryReader(v1), 1.0)).family == "TCP4"
    assert run(read_proxy_header_for_version(2, MemoryReader(v2), 1.0)).family == "TCP4"
    assert build_proxy_unknown_header_for_version(1) == build_proxy_unknown_header()
    assert build_proxy_unknown_header_for_version(2) == build_proxy_v2_unknown_header()


@pytest.mark.parametrize("version", [0, 3, -1, True])
def test_version_helpers_reject_unsupported_versions(version: int) -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        build_proxy_unknown_header_for_version(version)
    with pytest.raises(ValueError, match="Unsupported"):
        build_proxy_header_for_version(version, "192.0.2.1", "192.0.2.2", 1, 2)
    with pytest.raises(ValueError, match="Unsupported"):
        run(read_proxy_header_for_version(version, MemoryReader(b""), 1.0))


def test_v1_version_helper_rejects_tlvs_instead_of_dropping_them() -> None:
    with pytest.raises(ValueError, match="does not support TLVs"):
        build_proxy_header_for_version(
            1,
            "192.0.2.1",
            "192.0.2.2",
            1,
            2,
            tlvs=(ProxyProtocolTLV(1, b"x"),),
        )
