"""Runtime endpoint checks that prevent DNS aliases from routing back into this process."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Iterable
from dataclasses import dataclass

from .config import AppConfig

_MAX_SCOPE_ID = (2**32) - 1


class EndpointLoopError(OSError):
    """Raised when an upstream resolves to one of this process' listeners."""


@dataclass(frozen=True, slots=True)
class _Listener:
    name: str
    host: str
    port: int


def _normalize_address(value: str) -> str | None:
    candidate = value.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError:
        return None
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        return str(address.ipv4_mapped)
    return str(address)


def _canonical_scope_id(value: str) -> int | None:
    if not value:
        return None
    try:
        scope_id = int(value, 10)
    except ValueError:
        try:
            scope_id = socket.if_nametoindex(value)
        except OSError:
            return None
    if isinstance(scope_id, bool) or not 0 < scope_id <= _MAX_SCOPE_ID:
        return None
    return scope_id


def _scoped_address(value: str, scope_id: int = 0) -> str | None:
    normalized = _normalize_address(value)
    if normalized is None:
        return None
    _base, separator, textual_scope = value.partition("%")
    if isinstance(scope_id, bool) or not 0 <= scope_id <= _MAX_SCOPE_ID:
        return None
    if scope_id > 0:
        return f"{normalized}%{scope_id}"
    if not separator:
        return normalized
    canonical_scope = _canonical_scope_id(textual_scope)
    if canonical_scope is None:
        return None
    return f"{normalized}%{canonical_scope}"


def _address_object(value: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    return ipaddress.ip_address(value.split("%", 1)[0])


def sockaddr_host(value: object) -> str | None:
    """Return a numeric host with an IPv6 scope preserved from a sockaddr."""

    if not isinstance(value, tuple) or not value or not isinstance(value[0], str):
        return None
    scope_id = value[3] if len(value) >= 4 and isinstance(value[3], int) else 0
    return _scoped_address(value[0], scope_id)


def _is_local_address(value: str) -> bool:
    """Ask the kernel whether an address belongs to a local interface."""

    address = _address_object(value)
    if address.is_loopback or address.is_unspecified:
        return True
    family = socket.AF_INET if address.version == 4 else socket.AF_INET6
    bind_address: tuple[str, int] | tuple[str, int, int, int]
    if address.version == 4:
        bind_address = (str(address), 0)
    else:
        scope_text = value.partition("%")[2]
        scope_id = _canonical_scope_id(scope_text) if scope_text else 0
        if scope_id is None:
            return False
        bind_address = (str(address), 0, 0, scope_id)
    try:
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.bind(bind_address)
    except OSError:
        return False
    return True


class EndpointLoopGuard:
    """Resolve same-port endpoints and reject aliases of local listeners.

    Static textual collisions are rejected by :func:`validate_config`. This
    guard closes the remaining DNS-alias gap at startup and immediately before
    every active or passive upstream connection.
    """

    def __init__(self, config: AppConfig) -> None:
        listeners = [_Listener("proxy", config.proxy.listen_host, config.proxy.listen_port)]
        if config.monitoring.enabled:
            listeners.append(
                _Listener(
                    "monitoring",
                    config.monitoring.listen_host,
                    config.monitoring.listen_port,
                )
            )
        self._listeners = tuple(listeners)
        self._resolution_timeout = min(config.connection.timeout_seconds, 5.0)
        self._listener_addresses: dict[_Listener, frozenset[str]] = {}
        self._local_addresses: frozenset[str] | None = None
        self._cache_lock = asyncio.Lock()
        self._configured_endpoints = (
            ("main", config.main.host, config.main.port),
            ("fallback", config.fallback.host, config.fallback.port),
            (
                "healthcheck",
                config.healthcheck.target_host or config.main.host,
                config.healthcheck.target_port or config.main.port,
            ),
            (
                "fallback_healthcheck",
                config.fallback_healthcheck.target_host or config.fallback.host,
                config.fallback_healthcheck.target_port or config.fallback.port,
            ),
        )

    def register_bound_addresses(self, name: str, addresses: Iterable[str]) -> None:
        """Replace DNS-derived listener data with addresses of the bound sockets."""

        normalized = frozenset(
            value for address in addresses if (value := _scoped_address(address)) is not None
        )
        if not normalized:
            return
        for listener in self._listeners:
            if listener.name == name:
                self._listener_addresses[listener] = normalized
                self._local_addresses = None
                return

    async def _resolve(self, host: str) -> frozenset[str]:
        literal = _normalize_address(host)
        if literal is not None:
            scoped_literal = _scoped_address(host)
            return frozenset((scoped_literal,)) if scoped_literal is not None else frozenset()
        loop = asyncio.get_running_loop()
        try:
            results = await asyncio.wait_for(
                loop.getaddrinfo(
                    host,
                    0,
                    family=socket.AF_UNSPEC,
                    type=socket.SOCK_STREAM,
                ),
                timeout=self._resolution_timeout,
            )
        except (asyncio.TimeoutError, OSError, UnicodeError):
            return frozenset()
        addresses = {
            normalized for result in results if (normalized := sockaddr_host(result[4])) is not None
        }
        return frozenset(addresses)

    async def _listener_values(self, listener: _Listener) -> frozenset[str]:
        cached = self._listener_addresses.get(listener)
        if cached:
            return cached
        async with self._cache_lock:
            cached = self._listener_addresses.get(listener)
            if not cached:
                cached = await self._resolve(listener.host)
                if cached:
                    self._listener_addresses[listener] = cached
        return cached

    async def _local_values(self) -> frozenset[str]:
        if self._local_addresses is not None:
            return self._local_addresses
        async with self._cache_lock:
            if self._local_addresses is None:
                values = {"127.0.0.1", "::1"}
                try:
                    hostname = socket.gethostname()
                except OSError:
                    hostname = ""
                if hostname:
                    values.update(await self._resolve(hostname))
                for listener in self._listeners:
                    listener_values = self._listener_addresses.get(listener)
                    if listener_values is None:
                        listener_values = await self._resolve(listener.host)
                        self._listener_addresses[listener] = listener_values
                    values.update(
                        value
                        for value in listener_values
                        if not _address_object(value).is_unspecified
                    )
                self._local_addresses = frozenset(values)
        return self._local_addresses

    def _matching_listeners(self, port: int) -> tuple[_Listener, ...]:
        return tuple(
            listener for listener in self._listeners if listener.port != 0 and listener.port == port
        )

    async def _safe_addresses(self, host: str, port: int) -> frozenset[str]:
        matching = self._matching_listeners(port)
        if not matching:
            return frozenset()
        target_addresses = await self._resolve(host)
        if not target_addresses:
            raise EndpointLoopError(
                f"endpoint {host}:{port} could not be resolved for listener-loop validation"
            )
        for listener in matching:
            listener_addresses = await self._listener_values(listener)
            if not listener_addresses:
                raise EndpointLoopError(
                    f"local {listener.name} listener address could not be resolved safely"
                )
            wildcard_versions = {
                address.version
                for value in listener_addresses
                if (address := _address_object(value)).is_unspecified
            }
            collision = bool(target_addresses & listener_addresses)
            if wildcard_versions:
                local_values = await self._local_values()
                collision = collision or any(
                    (address := _address_object(value)).version in wildcard_versions
                    and (value in local_values or _is_local_address(value))
                    for value in target_addresses
                )
            if collision:
                raise EndpointLoopError(
                    f"endpoint {host}:{port} resolves to the local {listener.name} listener"
                )
        return target_addresses

    async def ensure_safe(self, host: str, port: int) -> None:
        """Reject an endpoint if its resolved address is accepted by a listener."""

        await self._safe_addresses(host, port)

    async def open_connection(
        self,
        host: str,
        port: int,
        *,
        timeout_seconds: float,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Connect without a second DNS lookup when loop validation is required."""

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        if not self._matching_listeners(port):
            return await asyncio.wait_for(
                asyncio.open_connection(
                    host,
                    port,
                    happy_eyeballs_delay=0.25,
                    interleave=1,
                ),
                timeout=timeout_seconds,
            )

        remaining = deadline - loop.time()
        if remaining <= 0:
            raise asyncio.TimeoutError
        addresses = sorted(
            await asyncio.wait_for(self._safe_addresses(host, port), timeout=remaining),
            key=lambda value: (_address_object(value).version, value),
        )
        last_error: Exception | None = None
        for index, address in enumerate(addresses):
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError from last_error
            attempts_left = len(addresses) - index
            attempt_timeout = remaining / attempts_left
            family = socket.AF_INET if _address_object(address).version == 4 else socket.AF_INET6
            try:
                return await asyncio.wait_for(
                    asyncio.open_connection(address, port, family=family),
                    timeout=attempt_timeout,
                )
            except asyncio.CancelledError:
                raise
            except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise OSError("upstream endpoint resolved to no usable addresses")

    async def validate_all(self) -> None:
        """Validate all configured routing and healthcheck endpoints."""

        for name, host, port in self._configured_endpoints:
            try:
                await self.ensure_safe(host, port)
            except EndpointLoopError as exc:
                raise EndpointLoopError(f"[{name}] {exc}") from exc
