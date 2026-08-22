from __future__ import annotations

import asyncio
import socket
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from mc_failover import endpoint_safety as endpoint_module
from mc_failover.endpoint_safety import (
    EndpointLoopError,
    EndpointLoopGuard,
    _normalize_address,
    _scoped_address,
    sockaddr_host,
)
from mc_failover.health import perform_health_check
from tests.conftest import make_config


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("127.0.0.1", "127.0.0.1"),
        ("::ffff:127.0.0.1", "127.0.0.1"),
        ("fe80::1%eth0", "fe80::1"),
        ("not-an-address", None),
    ],
)
def test_normalize_address(raw: str, expected: str | None) -> None:
    assert _normalize_address(raw) == expected


def test_scoped_address_canonicalizes_named_and_numeric_ipv6_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[str] = []

    def interface_index(name: str) -> int:
        requested.append(name)
        return 7

    monkeypatch.setattr(socket, "if_nametoindex", interface_index)

    assert _scoped_address("fe80::1%eth-test") == "fe80::1%7"
    assert _scoped_address("fe80::1%007") == "fe80::1%7"
    assert requested == ["eth-test"]


def test_scoped_address_rejects_unknown_or_invalid_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_interface(_name: str) -> int:
        raise OSError("unknown interface")

    monkeypatch.setattr(socket, "if_nametoindex", missing_interface)

    assert _scoped_address("fe80::1%missing-interface") is None
    assert _scoped_address("fe80::1%") is None
    assert _scoped_address("fe80::1%0") is None
    assert _scoped_address("fe80::1%-1") is None


def test_kernel_local_address_probe_recognizes_loopback() -> None:
    assert endpoint_module._is_local_address("127.0.0.42")
    assert endpoint_module._is_local_address("::1")


def test_kernel_local_address_probe_handles_bind_and_named_ipv6_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bound: list[object] = []

    class Probe:
        def __enter__(self) -> Probe:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def bind(self, address: object) -> None:
            bound.append(address)

    monkeypatch.setattr(socket, "socket", lambda *_args: Probe())
    monkeypatch.setattr(socket, "if_nametoindex", lambda name: 7 if name == "eth-test" else 0)

    assert endpoint_module._is_local_address("192.0.2.10")
    assert endpoint_module._is_local_address("fe80::1234%eth-test")
    assert bound == [("192.0.2.10", 0), ("fe80::1234", 0, 0, 7)]


def test_kernel_local_address_probe_handles_kernel_and_scope_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RejectedProbe:
        def __enter__(self) -> RejectedProbe:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def bind(self, _address: object) -> None:
            raise OSError("not local")

    monkeypatch.setattr(socket, "socket", lambda *_args: RejectedProbe())
    assert not endpoint_module._is_local_address("192.0.2.10")
    monkeypatch.setattr(socket, "if_nametoindex", lambda _name: (_ for _ in ()).throw(OSError()))
    assert not endpoint_module._is_local_address("fe80::1234%missing-interface")


def test_sockaddr_host_preserves_ipv6_scope() -> None:
    assert sockaddr_host(("fe80::1", 0, 0, 7)) == "fe80::1%7"
    assert sockaddr_host(("127.0.0.1", 0)) == "127.0.0.1"
    assert sockaddr_host("not-a-sockaddr") is None


def test_bound_listener_addresses_replace_dns_cache() -> None:
    base = make_config(25_565, 25_566, proxy_port=25_565)
    config = replace(base, proxy=replace(base.proxy, listen_host="bind-name.example"))
    guard = EndpointLoopGuard(config)
    guard._listener_addresses[next(iter(guard._listeners))] = frozenset(("192.0.2.1",))

    guard.register_bound_addresses("proxy", ("127.0.0.1", "invalid"))

    assert guard._listener_addresses[next(iter(guard._listeners))] == frozenset(("127.0.0.1",))


def test_bound_listener_registration_ignores_empty_and_unknown_updates() -> None:
    guard = EndpointLoopGuard(make_config(25_564, 25_566, proxy_port=25_565))
    guard.register_bound_addresses("proxy", ("invalid",))
    guard.register_bound_addresses("not-a-listener", ("127.0.0.1",))
    assert guard._listener_addresses == {}


@pytest.mark.asyncio
async def test_guard_rejects_literal_listener_target_and_identifies_section() -> None:
    config = make_config(25_565, 25_566, proxy_port=25_565)
    guard = EndpointLoopGuard(config)

    with pytest.raises(EndpointLoopError, match=r"\[main\].*local proxy listener"):
        await guard.validate_all()


@pytest.mark.asyncio
async def test_guard_rejects_dns_alias_of_explicit_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25_565, 25_566, proxy_port=25_565)
    config = replace(base, main=replace(base.main, host="proxy-internal.example"))
    guard = EndpointLoopGuard(config)

    async def resolve(host: str) -> frozenset[str]:
        values = {
            "proxy-internal.example": frozenset(("127.0.0.1",)),
            "127.0.0.1": frozenset(("127.0.0.1",)),
        }
        return values.get(host, frozenset(("203.0.113.8",)))

    monkeypatch.setattr(guard, "_resolve", resolve)
    with pytest.raises(EndpointLoopError, match=r"proxy-internal\.example:25565"):
        await guard.ensure_safe(config.main.host, config.main.port)


@pytest.mark.asyncio
async def test_guard_matches_named_scope_to_bound_numeric_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = EndpointLoopGuard(make_config(25_565, 25_566, proxy_port=25_565))
    guard.register_bound_addresses("proxy", ("fe80::1234%7",))
    monkeypatch.setattr(socket, "if_nametoindex", lambda name: 7 if name == "eth-test" else 0)

    with pytest.raises(EndpointLoopError, match="local proxy listener"):
        await guard.ensure_safe("fe80::1234%eth-test", 25_565)


@pytest.mark.asyncio
async def test_guard_rejects_unknown_named_scope_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = EndpointLoopGuard(make_config(25_565, 25_566, proxy_port=25_565))

    def missing_interface(_name: str) -> int:
        raise OSError("unknown interface")

    monkeypatch.setattr(socket, "if_nametoindex", missing_interface)

    with pytest.raises(EndpointLoopError, match="could not be resolved"):
        await guard.ensure_safe("fe80::1234%missing-interface", 25_565)


@pytest.mark.asyncio
async def test_wildcard_listener_rejects_local_address_in_same_family_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25_565, 25_566, proxy_port=25_565)
    config = replace(base, proxy=replace(base.proxy, listen_host="0.0.0.0"))
    guard = EndpointLoopGuard(config)
    monkeypatch.setattr(
        guard,
        "_local_values",
        AsyncMock(return_value=frozenset(("127.0.0.1", "::1"))),
    )

    with pytest.raises(EndpointLoopError):
        await guard.ensure_safe("127.0.0.42", 25_565)
    await guard.ensure_safe("::1", 25_565)


@pytest.mark.asyncio
async def test_wildcard_listener_uses_kernel_local_interface_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25_565, 25_566, proxy_port=25_565)
    config = replace(base, proxy=replace(base.proxy, listen_host="0.0.0.0"))
    guard = EndpointLoopGuard(config)
    monkeypatch.setattr(guard, "_local_values", AsyncMock(return_value=frozenset()))
    monkeypatch.setattr(
        endpoint_module,
        "_is_local_address",
        lambda value: value == "192.0.2.44",
    )

    with pytest.raises(EndpointLoopError, match="local proxy listener"):
        await guard.ensure_safe("192.0.2.44", 25_565)


@pytest.mark.asyncio
async def test_same_port_resolution_failure_is_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = EndpointLoopGuard(make_config(25_565, 25_566, proxy_port=25_565))
    monkeypatch.setattr(guard, "_resolve", AsyncMock(return_value=frozenset()))

    with pytest.raises(EndpointLoopError, match="could not be resolved"):
        await guard.ensure_safe("backend.example", 25_565)


@pytest.mark.asyncio
async def test_listener_resolution_failure_is_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25_565, 25_566, proxy_port=25_565)
    config = replace(base, main=replace(base.main, host="backend.example"))
    guard = EndpointLoopGuard(config)

    async def resolve(host: str) -> frozenset[str]:
        return frozenset(("192.0.2.10",)) if host == "backend.example" else frozenset()

    monkeypatch.setattr(guard, "_resolve", resolve)
    with pytest.raises(EndpointLoopError, match="listener address could not be resolved"):
        await guard.ensure_safe("backend.example", 25_565)


@pytest.mark.asyncio
async def test_listener_and_local_address_caches_are_reused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = EndpointLoopGuard(make_config(25_564, 25_566, proxy_port=25_565))
    listener = next(iter(guard._listeners))
    guard._listener_addresses[listener] = frozenset(("127.0.0.1",))
    assert await guard._listener_values(listener) == frozenset(("127.0.0.1",))

    monkeypatch.setattr(socket, "gethostname", lambda: (_ for _ in ()).throw(OSError()))
    assert await guard._local_values() == frozenset(("127.0.0.1", "::1"))
    assert await guard._local_values() == frozenset(("127.0.0.1", "::1"))


@pytest.mark.asyncio
async def test_safe_connection_uses_validated_numeric_addresses_and_falls_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25_565, 25_566, proxy_port=25_565)
    config = replace(base, main=replace(base.main, host="backend.example"))
    guard = EndpointLoopGuard(config)

    async def resolve(host: str) -> frozenset[str]:
        if host == "backend.example":
            return frozenset(("192.0.2.1", "198.51.100.2"))
        return frozenset(("127.0.0.1",))

    reader = asyncio.StreamReader()
    writer = AsyncMock(spec=asyncio.StreamWriter)
    attempted: list[str] = []

    async def connect(host: str, _port: int, **_kwargs: object):
        attempted.append(host)
        if host == "192.0.2.1":
            raise OSError("first address unavailable")
        return reader, writer

    monkeypatch.setattr(guard, "_resolve", resolve)
    monkeypatch.setattr(asyncio, "open_connection", connect)

    assert await guard.open_connection("backend.example", 25_565, timeout_seconds=0.2) == (
        reader,
        writer,
    )
    assert attempted == ["192.0.2.1", "198.51.100.2"]


@pytest.mark.asyncio
async def test_safe_connection_preserves_ipv6_scope_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25_565, 25_566, proxy_port=25_565)
    config = replace(base, main=replace(base.main, host="link-local-backend.example"))
    guard = EndpointLoopGuard(config)

    async def resolve(host: str) -> frozenset[str]:
        if host == "link-local-backend.example":
            return frozenset(("fe80::1234%7",))
        return frozenset(("127.0.0.1",))

    reader = asyncio.StreamReader()
    writer = AsyncMock(spec=asyncio.StreamWriter)
    connect = AsyncMock(return_value=(reader, writer))
    monkeypatch.setattr(guard, "_resolve", resolve)
    monkeypatch.setattr(asyncio, "open_connection", connect)

    assert await guard.open_connection(
        "link-local-backend.example", 25_565, timeout_seconds=0.2
    ) == (reader, writer)
    assert connect.await_args is not None
    assert connect.await_args.args[:2] == ("fe80::1234%7", 25_565)
    assert connect.await_args.kwargs["family"] == socket.AF_INET6


@pytest.mark.asyncio
async def test_safe_connection_propagates_cancellation_and_last_connect_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = make_config(25_565, 25_566, proxy_port=25_565)
    config = replace(base, main=replace(base.main, host="backend.example"))
    guard = EndpointLoopGuard(config)

    async def resolve(host: str) -> frozenset[str]:
        return (
            frozenset(("192.0.2.10",)) if host == "backend.example" else frozenset(("127.0.0.1",))
        )

    monkeypatch.setattr(guard, "_resolve", resolve)
    monkeypatch.setattr(asyncio, "open_connection", AsyncMock(side_effect=OSError("down")))
    with pytest.raises(OSError, match="down"):
        await guard.open_connection("backend.example", 25_565, timeout_seconds=0.2)

    monkeypatch.setattr(
        asyncio,
        "open_connection",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )
    with pytest.raises(asyncio.CancelledError):
        await guard.open_connection("backend.example", 25_565, timeout_seconds=0.2)


@pytest.mark.asyncio
async def test_safe_connection_enforces_one_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = EndpointLoopGuard(make_config(25_565, 25_566, proxy_port=25_565))
    monkeypatch.setattr(
        guard,
        "_safe_addresses",
        AsyncMock(return_value=frozenset(("192.0.2.1", "192.0.2.2"))),
    )

    async def stall(*_args: object, **_kwargs: object):
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio, "open_connection", stall)
    with pytest.raises(asyncio.TimeoutError):
        await guard.open_connection("backend.example", 25_565, timeout_seconds=0.01)

    with pytest.raises(asyncio.TimeoutError):
        await guard.open_connection("backend.example", 25_565, timeout_seconds=0.0)


@pytest.mark.asyncio
async def test_guard_skips_resolution_for_different_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = EndpointLoopGuard(make_config(25_564, 25_566, proxy_port=25_565))

    async def unexpected(_host: str) -> frozenset[str]:
        pytest.fail("different-port endpoint was resolved")

    monkeypatch.setattr(guard, "_resolve", unexpected)
    await guard.ensure_safe("backend.example", 25_564)


@pytest.mark.asyncio
async def test_resolution_failure_is_left_to_bounded_connection_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = EndpointLoopGuard(make_config(25_565, 25_566, proxy_port=25_565))
    loop = asyncio.get_running_loop()

    async def fail_resolution(*_args: object, **_kwargs: object) -> object:
        raise OSError("dns unavailable")

    monkeypatch.setattr(loop, "getaddrinfo", fail_resolution)
    assert await guard._resolve("unresolvable.example") == frozenset()


@pytest.mark.asyncio
async def test_dns_resolution_preserves_sockaddr_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard = EndpointLoopGuard(make_config(25_564, 25_566, proxy_port=25_565))
    loop = asyncio.get_running_loop()
    result = [
        (
            socket.AF_INET6,
            socket.SOCK_STREAM,
            6,
            "",
            ("fe80::1234", 0, 0, 7),
        )
    ]
    monkeypatch.setattr(loop, "getaddrinfo", AsyncMock(return_value=result))

    assert await guard._resolve("link-local.example") == frozenset(("fe80::1234%7",))


@pytest.mark.asyncio
async def test_active_healthcheck_reports_listener_loop_without_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(25_565, 25_566, proxy_port=25_565)
    guard = EndpointLoopGuard(config)
    connect = AsyncMock()
    monkeypatch.setattr(asyncio, "open_connection", connect)

    result = await perform_health_check(
        config.main,
        config.healthcheck,
        endpoint_guard=guard,
    )

    assert not result.ok
    assert result.reason == "unsafe_listener_loop"
    connect.assert_not_awaited()
