"""Strict, bounded helpers for HAProxy's PROXY protocol versions 1 and 2.

The parsing functions deliberately accept only TCP over IPv4/IPv6.  Version
2 ``LOCAL`` and unspecified (the v2 equivalent of ``UNKNOWN``) headers are
accepted, but never expose address information supplied in their payload.
"""

from __future__ import annotations

import asyncio
import ipaddress
import math
import struct
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Protocol

PROXY_V2_SIGNATURE = b"\r\n\r\n\x00\r\nQUIT\n"
MAX_PROXY_V1_HEADER_BYTES = 256
MAX_PROXY_V2_HEADER_BYTES = 16 + 65_535

_V2_BASE_BYTES = 16
_V2_COMMAND_LOCAL = 0x00
_V2_COMMAND_PROXY = 0x01
_V2_FAMILY_UNSPEC = 0x00
_V2_FAMILY_INET = 0x01
_V2_FAMILY_INET6 = 0x02
_V2_PROTOCOL_UNSPEC = 0x00
_V2_PROTOCOL_STREAM = 0x01


class _ExactReader(Protocol):
    async def readexactly(self, n: int) -> bytes:
        """Read exactly *n* bytes or raise ``IncompleteReadError``."""

        ...


@dataclass(frozen=True, slots=True)
class ProxyProtocolTLV:
    """One fully framed PROXY v2 type-length-value field."""

    type: int
    value: bytes


@dataclass(frozen=True, slots=True)
class ProxyProtocolInfo:
    """Validated address metadata from an inbound PROXY header."""

    source_ip: str
    destination_ip: str
    source_port: int
    destination_port: int
    family: str
    command: str = "PROXY"
    tlvs: tuple[ProxyProtocolTLV, ...] = ()


def is_trusted_proxy(
    peer_ip: str,
    trusted_entries: Iterable[str],
    *,
    trust_all: bool = False,
) -> bool:
    """Return whether a peer may supply identity-bearing PROXY metadata.

    Trust is fail-closed: an empty or wholly invalid allowlist never grants
    access.  The dangerous bypass is honored only for the literal boolean
    value ``True`` and only when *peer_ip* itself is a valid IP address.
    Invalid allowlist entries are ignored here; configuration validation is
    responsible for reporting them to an operator. IPv4-mapped IPv6 socket
    addresses are treated as the equivalent IPv4 identity, including for
    allowlist networks wholly contained in ``::ffff:0:0/96``.
    """

    if not isinstance(peer_ip, str) or not peer_ip or "%" in peer_ip:
        return False
    try:
        peer = ipaddress.ip_address(peer_ip)
    except ValueError:
        return False
    canonical_peer = (
        peer.ipv4_mapped
        if isinstance(peer, ipaddress.IPv6Address) and peer.ipv4_mapped is not None
        else peer
    )

    if trust_all is True:
        return True
    if isinstance(trusted_entries, (str, bytes)):
        return False

    try:
        entries = iter(trusted_entries)
    except TypeError:
        return False
    for entry in entries:
        if not isinstance(entry, str) or not entry.strip() or "%" in entry:
            continue
        try:
            network = ipaddress.ip_network(entry.strip(), strict=False)
        except (TypeError, ValueError):
            continue
        if peer.version == network.version and peer in network:
            return True
        if isinstance(network, ipaddress.IPv6Network) and network.prefixlen >= 96:
            mapped_network_address = network.network_address.ipv4_mapped
            if mapped_network_address is not None:
                network = ipaddress.IPv4Network(
                    (int(mapped_network_address), network.prefixlen - 96)
                )
        if canonical_peer.version == network.version and canonical_peer in network:
            return True
    return False


def _header_limit(value: int | None, *, default: int, minimum: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"max_header_bytes must be an integer >= {minimum}")
    return min(value, default)


def _valid_port(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 65_535


def _parse_port(value: bytes, *, version: int) -> int:
    if not value or not value.isdigit():
        raise ValueError(f"Invalid port in PROXY v{version} header")
    port = int(value)
    if not _valid_port(port):
        raise ValueError(f"Invalid port in PROXY v{version} header")
    return port


def _parse_text_ip(value: bytes, *, version: int) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    try:
        text = value.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError(f"Invalid non-ASCII address in PROXY v{version} header") from exc
    if not text or "%" in text:
        raise ValueError(f"Invalid address in PROXY v{version} header")
    try:
        return ipaddress.ip_address(text)
    except ValueError as exc:
        raise ValueError(f"Invalid address in PROXY v{version} header") from exc


def parse_proxy_v1_header(
    line: bytes,
    *,
    max_header_bytes: int | None = None,
) -> ProxyProtocolInfo:
    """Parse one complete, CRLF-terminated PROXY v1 header."""

    limit = _header_limit(
        max_header_bytes,
        default=MAX_PROXY_V1_HEADER_BYTES,
        minimum=len(b"PROXY UNKNOWN\r\n"),
    )
    if not isinstance(line, bytes):
        raise ValueError("PROXY v1 header must be bytes")
    if len(line) > limit:
        raise ValueError("PROXY v1 header too long")
    if not line.endswith(b"\r\n") or line.count(b"\r\n") != 1:
        raise ValueError("Invalid PROXY v1 header framing")
    if line == b"PROXY UNKNOWN\r\n":
        return ProxyProtocolInfo(
            "0.0.0.0",
            "0.0.0.0",
            0,
            0,
            "UNKNOWN",
            command="UNKNOWN",
        )

    fields = line[:-2].split(b" ")
    if len(fields) != 6 or any(not field for field in fields):
        raise ValueError("Invalid PROXY v1 header format")
    marker, family_raw, source_raw, destination_raw, source_port_raw, destination_port_raw = fields
    if marker != b"PROXY" or family_raw not in {b"TCP4", b"TCP6"}:
        raise ValueError("Invalid PROXY v1 header format")

    source = _parse_text_ip(source_raw, version=1)
    destination = _parse_text_ip(destination_raw, version=1)
    expected_version = 4 if family_raw == b"TCP4" else 6
    if source.version != expected_version or destination.version != expected_version:
        raise ValueError(f"{family_raw.decode('ascii')} requires matching IP addresses")
    source_port = _parse_port(source_port_raw, version=1)
    destination_port = _parse_port(destination_port_raw, version=1)
    return ProxyProtocolInfo(
        str(source),
        str(destination),
        source_port,
        destination_port,
        family_raw.decode("ascii"),
    )


def _validated_timeout(timeout: float) -> float:
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError("timeout must be a finite number > 0")
    result = float(timeout)
    if not math.isfinite(result) or result <= 0:
        raise ValueError("timeout must be a finite number > 0")
    return result


def _new_deadline(
    timeout: float,
    monotonic: Callable[[], float] | None,
) -> tuple[float, Callable[[], float]]:
    loop = asyncio.get_running_loop()
    clock = monotonic if monotonic is not None else loop.time
    return clock() + _validated_timeout(timeout), clock


async def _read_exactly_before(
    reader: _ExactReader,
    size: int,
    *,
    deadline: float,
    monotonic: Callable[[], float],
) -> bytes:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise TimeoutError("Timed out while reading PROXY header")
    try:
        data = await asyncio.wait_for(reader.readexactly(size), timeout=remaining)
    except asyncio.IncompleteReadError as exc:
        raise ValueError("Incomplete PROXY header") from exc
    if monotonic() > deadline:
        raise TimeoutError("Timed out while reading PROXY header")
    if not isinstance(data, bytes) or len(data) != size:
        raise ValueError("Reader returned an invalid PROXY header fragment")
    return data


async def _read_proxy_v1_before(
    reader: _ExactReader,
    *,
    deadline: float,
    monotonic: Callable[[], float],
    limit: int,
) -> ProxyProtocolInfo:
    header = bytearray()
    expected_prefix = b"PROXY "
    while len(header) < limit:
        header.extend(
            await _read_exactly_before(
                reader,
                1,
                deadline=deadline,
                monotonic=monotonic,
            )
        )
        prefix_length = min(len(header), len(expected_prefix))
        if bytes(header[:prefix_length]) != expected_prefix[:prefix_length]:
            raise ValueError("Invalid PROXY v1 signature")
        if header.endswith(b"\r\n"):
            return parse_proxy_v1_header(bytes(header), max_header_bytes=limit)
    raise ValueError("PROXY v1 header too long")


async def read_proxy_v1_header(
    reader: _ExactReader,
    timeout: float,
    *,
    max_header_bytes: int | None = None,
    monotonic: Callable[[], float] | None = None,
) -> ProxyProtocolInfo:
    """Read v1 with one absolute deadline and a bounded byte budget."""

    limit = _header_limit(
        max_header_bytes,
        default=MAX_PROXY_V1_HEADER_BYTES,
        minimum=len(b"PROXY UNKNOWN\r\n"),
    )
    deadline, clock = _new_deadline(timeout, monotonic)
    return await _read_proxy_v1_before(
        reader,
        deadline=deadline,
        monotonic=clock,
        limit=limit,
    )


def build_proxy_v1_header(
    source_ip: str,
    destination_ip: str,
    source_port: int,
    destination_port: int,
) -> bytes:
    """Build a canonical TCP4 or TCP6 PROXY v1 header."""

    source = _build_ip(source_ip, field="source_ip")
    destination = _build_ip(destination_ip, field="destination_ip")
    if source.version != destination.version:
        raise ValueError("source_ip and destination_ip must use the same family")
    _require_ports(source_port, destination_port)
    family = "TCP4" if source.version == 4 else "TCP6"
    header = f"PROXY {family} {source} {destination} {source_port} {destination_port}\r\n".encode(
        "ascii"
    )
    if len(header) > MAX_PROXY_V1_HEADER_BYTES:
        raise ValueError("PROXY v1 header too long")
    return header


def build_proxy_unknown_header() -> bytes:
    """Build the canonical v1 header that carries no address metadata."""

    return b"PROXY UNKNOWN\r\n"


def _inspect_proxy_v2_base(base: bytes, *, limit: int) -> tuple[int, int, int, int]:
    if len(base) != _V2_BASE_BYTES:
        raise ValueError("PROXY v2 header too short")
    if base[:12] != PROXY_V2_SIGNATURE:
        raise ValueError("Invalid PROXY v2 signature")

    version_command = base[12]
    if version_command >> 4 != 0x02:
        raise ValueError("Invalid PROXY v2 version")
    command = version_command & 0x0F
    if command not in {_V2_COMMAND_LOCAL, _V2_COMMAND_PROXY}:
        raise ValueError("Invalid PROXY v2 command")

    family_protocol = base[13]
    family = family_protocol >> 4
    protocol = family_protocol & 0x0F
    payload_length = int.from_bytes(base[14:16], "big")
    if _V2_BASE_BYTES + payload_length > limit:
        raise ValueError("PROXY v2 header too long")

    if command == _V2_COMMAND_LOCAL:
        return command, family, protocol, payload_length

    if family == _V2_FAMILY_UNSPEC:
        if protocol != _V2_PROTOCOL_UNSPEC:
            raise ValueError("Unspecified PROXY v2 family requires unspecified protocol")
        address_length = 0
    elif family == _V2_FAMILY_INET:
        if protocol != _V2_PROTOCOL_STREAM:
            raise ValueError("Only STREAM/TCP is supported for PROXY v2")
        address_length = 12
    elif family == _V2_FAMILY_INET6:
        if protocol != _V2_PROTOCOL_STREAM:
            raise ValueError("Only STREAM/TCP is supported for PROXY v2")
        address_length = 36
    else:
        raise ValueError("Unsupported PROXY v2 address family")

    if payload_length < address_length:
        raise ValueError("PROXY v2 payload is shorter than its address block")
    tlv_length = payload_length - address_length
    if tlv_length in {1, 2}:
        raise ValueError("Truncated PROXY v2 TLV framing")
    return command, family, protocol, payload_length


def _parse_tlvs(payload: bytes) -> tuple[ProxyProtocolTLV, ...]:
    result: list[ProxyProtocolTLV] = []
    offset = 0
    while offset < len(payload):
        if len(payload) - offset < 3:
            raise ValueError("Truncated PROXY v2 TLV framing")
        tlv_type = payload[offset]
        value_length = int.from_bytes(payload[offset + 1 : offset + 3], "big")
        offset += 3
        end = offset + value_length
        if end > len(payload):
            raise ValueError("Truncated PROXY v2 TLV value")
        result.append(ProxyProtocolTLV(tlv_type, payload[offset:end]))
        offset = end
    return tuple(result)


def parse_proxy_v2_header(
    data: bytes,
    *,
    max_header_bytes: int | None = None,
) -> ProxyProtocolInfo:
    """Parse one complete PROXY v2 header, including all outer TLVs."""

    limit = _header_limit(
        max_header_bytes,
        default=MAX_PROXY_V2_HEADER_BYTES,
        minimum=_V2_BASE_BYTES,
    )
    if not isinstance(data, bytes):
        raise ValueError("PROXY v2 header must be bytes")
    if len(data) < _V2_BASE_BYTES:
        raise ValueError("PROXY v2 header too short")
    if len(data) > limit:
        raise ValueError("PROXY v2 header too long")

    command, family, _protocol, payload_length = _inspect_proxy_v2_base(
        data[:_V2_BASE_BYTES],
        limit=limit,
    )
    if len(data) != _V2_BASE_BYTES + payload_length:
        raise ValueError("Invalid PROXY v2 length")
    payload = data[_V2_BASE_BYTES:]

    if command == _V2_COMMAND_LOCAL:
        return ProxyProtocolInfo(
            "0.0.0.0",
            "0.0.0.0",
            0,
            0,
            "UNKNOWN",
            command="LOCAL",
        )

    if family == _V2_FAMILY_UNSPEC:
        return ProxyProtocolInfo(
            "0.0.0.0",
            "0.0.0.0",
            0,
            0,
            "UNKNOWN",
            tlvs=_parse_tlvs(payload),
        )

    source: ipaddress.IPv4Address | ipaddress.IPv6Address
    destination: ipaddress.IPv4Address | ipaddress.IPv6Address
    if family == _V2_FAMILY_INET:
        address_length = 12
        source = ipaddress.IPv4Address(payload[:4])
        destination = ipaddress.IPv4Address(payload[4:8])
        source_port = int.from_bytes(payload[8:10], "big")
        destination_port = int.from_bytes(payload[10:12], "big")
        family_name = "TCP4"
    else:
        address_length = 36
        source = ipaddress.IPv6Address(payload[:16])
        destination = ipaddress.IPv6Address(payload[16:32])
        source_port = int.from_bytes(payload[32:34], "big")
        destination_port = int.from_bytes(payload[34:36], "big")
        family_name = "TCP6"

    if not _valid_port(source_port) or not _valid_port(destination_port):
        raise ValueError("Invalid ports in PROXY v2 header")
    return ProxyProtocolInfo(
        str(source),
        str(destination),
        source_port,
        destination_port,
        family_name,
        tlvs=_parse_tlvs(payload[address_length:]),
    )


async def _read_proxy_v2_before(
    reader: _ExactReader,
    *,
    deadline: float,
    monotonic: Callable[[], float],
    limit: int,
) -> ProxyProtocolInfo:
    base = await _read_exactly_before(
        reader,
        _V2_BASE_BYTES,
        deadline=deadline,
        monotonic=monotonic,
    )
    _command, _family, _protocol, payload_length = _inspect_proxy_v2_base(base, limit=limit)
    payload = b""
    if payload_length:
        payload = await _read_exactly_before(
            reader,
            payload_length,
            deadline=deadline,
            monotonic=monotonic,
        )
    return parse_proxy_v2_header(base + payload, max_header_bytes=limit)


async def read_proxy_v2_header(
    reader: _ExactReader,
    timeout: float,
    *,
    max_header_bytes: int | None = None,
    monotonic: Callable[[], float] | None = None,
) -> ProxyProtocolInfo:
    """Read v2 under one deadline, validating its base before its payload."""

    limit = _header_limit(
        max_header_bytes,
        default=MAX_PROXY_V2_HEADER_BYTES,
        minimum=_V2_BASE_BYTES,
    )
    deadline, clock = _new_deadline(timeout, monotonic)
    return await _read_proxy_v2_before(
        reader,
        deadline=deadline,
        monotonic=clock,
        limit=limit,
    )


def _build_ip(value: str, *, field: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    if not isinstance(value, str) or not value or "%" in value:
        raise ValueError(f"{field} must be an IPv4 or IPv6 address")
    try:
        return ipaddress.ip_address(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an IPv4 or IPv6 address") from exc


def _require_ports(source_port: int, destination_port: int) -> None:
    if not _valid_port(source_port) or not _valid_port(destination_port):
        raise ValueError("Ports must be integers in range 1..65535")


def _encode_tlvs(tlvs: Iterable[ProxyProtocolTLV]) -> bytes:
    encoded = bytearray()
    for tlv in tlvs:
        if not isinstance(tlv, ProxyProtocolTLV):
            raise ValueError("PROXY v2 TLVs must be ProxyProtocolTLV instances")
        if not _valid_tlv_type(tlv.type):
            raise ValueError("PROXY v2 TLV type must be an integer in range 0..255")
        if not isinstance(tlv.value, bytes):
            raise ValueError("PROXY v2 TLV value must be bytes")
        if len(tlv.value) > 65_535:
            raise ValueError("PROXY v2 TLV value is too long")
        encoded.append(tlv.type)
        encoded.extend(struct.pack("!H", len(tlv.value)))
        encoded.extend(tlv.value)
    return bytes(encoded)


def _valid_tlv_type(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 0xFF


def build_proxy_v2_unknown_header(
    *,
    tlvs: Iterable[ProxyProtocolTLV] = (),
) -> bytes:
    """Build a v2 PROXY/UNSPEC header, optionally carrying framed TLVs."""

    payload = _encode_tlvs(tlvs)
    return _build_v2_base(_V2_COMMAND_PROXY, 0x00, payload)


def build_proxy_v2_local_header(payload: bytes = b"") -> bytes:
    """Build a LOCAL header whose opaque payload must be ignored by receivers."""

    if not isinstance(payload, bytes):
        raise ValueError("PROXY v2 LOCAL payload must be bytes")
    return _build_v2_base(_V2_COMMAND_LOCAL, 0x00, payload)


def _build_v2_base(command: int, family_protocol: int, payload: bytes) -> bytes:
    if len(payload) > 65_535:
        raise ValueError("PROXY v2 payload is too long")
    return (
        PROXY_V2_SIGNATURE
        + bytes([0x20 | command, family_protocol])
        + struct.pack("!H", len(payload))
        + payload
    )


def build_proxy_v2_header(
    source_ip: str,
    destination_ip: str,
    source_port: int,
    destination_port: int,
    *,
    tlvs: Iterable[ProxyProtocolTLV] = (),
) -> bytes:
    """Build a canonical TCP4 or TCP6 PROXY v2 header with optional TLVs."""

    source = _build_ip(source_ip, field="source_ip")
    destination = _build_ip(destination_ip, field="destination_ip")
    if source.version != destination.version:
        raise ValueError("source_ip and destination_ip must use the same family")
    _require_ports(source_port, destination_port)
    address_payload = (
        source.packed + destination.packed + struct.pack("!HH", source_port, destination_port)
    )
    payload = address_payload + _encode_tlvs(tlvs)
    family_protocol = 0x11 if source.version == 4 else 0x21
    return _build_v2_base(_V2_COMMAND_PROXY, family_protocol, payload)


def build_proxy_unknown_header_for_version(version: int) -> bytes:
    _require_supported_version(version)
    if version == 1:
        return build_proxy_unknown_header()
    if version == 2:
        return build_proxy_v2_unknown_header()
    raise ValueError(f"Unsupported PROXY protocol version: {version}")


def build_proxy_header_for_version(
    version: int,
    source_ip: str,
    destination_ip: str,
    source_port: int,
    destination_port: int,
    *,
    tlvs: Iterable[ProxyProtocolTLV] = (),
) -> bytes:
    _require_supported_version(version)
    if version == 1:
        if tuple(tlvs):
            raise ValueError("PROXY v1 does not support TLVs")
        return build_proxy_v1_header(source_ip, destination_ip, source_port, destination_port)
    if version == 2:
        return build_proxy_v2_header(
            source_ip,
            destination_ip,
            source_port,
            destination_port,
            tlvs=tlvs,
        )
    raise ValueError(f"Unsupported PROXY protocol version: {version}")


async def read_proxy_header_for_version(
    version: int,
    reader: _ExactReader,
    timeout: float,
    *,
    max_header_bytes: int | None = None,
    monotonic: Callable[[], float] | None = None,
) -> ProxyProtocolInfo:
    """Dispatch to the selected parser without starting a second deadline."""

    _require_supported_version(version)
    deadline, clock = _new_deadline(timeout, monotonic)
    if version == 1:
        limit = _header_limit(
            max_header_bytes,
            default=MAX_PROXY_V1_HEADER_BYTES,
            minimum=len(b"PROXY UNKNOWN\r\n"),
        )
        return await _read_proxy_v1_before(
            reader,
            deadline=deadline,
            monotonic=clock,
            limit=limit,
        )
    if version == 2:
        limit = _header_limit(
            max_header_bytes,
            default=MAX_PROXY_V2_HEADER_BYTES,
            minimum=_V2_BASE_BYTES,
        )
        return await _read_proxy_v2_before(
            reader,
            deadline=deadline,
            monotonic=clock,
            limit=limit,
        )
    raise ValueError(f"Unsupported PROXY protocol version: {version}")


def _require_supported_version(version: int) -> None:
    if isinstance(version, bool) or not isinstance(version, int) or version not in {1, 2}:
        raise ValueError(f"Unsupported PROXY protocol version: {version}")


__all__ = [
    "MAX_PROXY_V1_HEADER_BYTES",
    "MAX_PROXY_V2_HEADER_BYTES",
    "PROXY_V2_SIGNATURE",
    "ProxyProtocolInfo",
    "ProxyProtocolTLV",
    "build_proxy_header_for_version",
    "build_proxy_unknown_header",
    "build_proxy_unknown_header_for_version",
    "build_proxy_v1_header",
    "build_proxy_v2_header",
    "build_proxy_v2_local_header",
    "build_proxy_v2_unknown_header",
    "is_trusted_proxy",
    "parse_proxy_v1_header",
    "parse_proxy_v2_header",
    "read_proxy_header_for_version",
    "read_proxy_v1_header",
    "read_proxy_v2_header",
]
