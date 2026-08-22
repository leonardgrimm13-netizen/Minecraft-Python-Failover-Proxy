from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mc_failover.config import (
    AppConfig,
    CircuitBreakerConfig,
    ConnectionConfig,
    HealthCheckConfig,
    LoggingConfig,
    MaintenanceConfig,
    MonitoringConfig,
    ProxyConfig,
    ProxyProtocolConfig,
    TargetConfig,
)
from mc_failover.models import MaintenanceMode

UTC = timezone.utc
StreamHandler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None] | None]


class FakeClock:
    def __init__(self) -> None:
        self.monotonic_value = 100.0
        self.utc_value = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)

    def monotonic(self) -> float:
        return self.monotonic_value

    def utc_now(self) -> datetime:
        return self.utc_value

    def advance(self, seconds: float) -> None:
        self.monotonic_value += seconds
        self.utc_value += timedelta(seconds=seconds)

    def jump_wall(self, seconds: float) -> None:
        self.utc_value += timedelta(seconds=seconds)


def health_config(*, enabled: bool = True, mode: str = "tcp") -> HealthCheckConfig:
    return HealthCheckConfig(
        enabled=enabled,
        mode=mode,
        interval_seconds=0.05,
        timeout_seconds=0.5,
        fail_after=1,
        recover_after=1,
        min_recovery_seconds=0.0,
        target_host=None,
        target_port=None,
        protocol_version=767,
        status_hostname=None,
        require_valid_json=True,
        reject_uninitialized_protocol=True,
        log_status_details=False,
        jitter_seconds=0.0,
        max_latency_ms=0.0,
        expected_version_contains="",
        motd_must_contain="",
        motd_must_not_contain="",
        min_players_max=0,
    )


def make_config(
    main_port: int,
    fallback_port: int,
    *,
    proxy_port: int = 0,
    monitoring: bool = False,
) -> AppConfig:
    return AppConfig(
        proxy=ProxyConfig("127.0.0.1", proxy_port, 128),
        main=TargetConfig("127.0.0.1", main_port),
        fallback=TargetConfig("127.0.0.1", fallback_port),
        healthcheck=health_config(),
        fallback_healthcheck=health_config(),
        connection=ConnectionConfig(
            timeout_seconds=0.3,
            buffer_size=4096,
            idle_timeout_seconds=1.0,
            write_timeout_seconds=0.5,
            relay_drain_timeout_seconds=0.5,
            shutdown_grace_seconds=0.5,
            shutdown_cancel_timeout_seconds=0.5,
            connect_fallback_on_main_connect_failure=True,
            tcp_keepalive=False,
            max_connections=64,
            max_connections_per_ip=0,
            new_connections_per_second=0.0,
            new_connections_burst=0,
        ),
        logging=LoggingConfig("DEBUG", False),
        maintenance=MaintenanceConfig(MaintenanceMode.AUTO, None, None, 0.05),
        proxy_protocol=ProxyProtocolConfig(
            accept=False,
            send=False,
            version=1,
            accept_version=None,
            send_version=None,
            trust_all_proxies=False,
            trusted_proxy_ips=(),
            header_timeout_seconds=0.2,
            max_header_bytes=4096,
        ),
        monitoring=MonitoringConfig(
            enabled=monitoring,
            listen_host="127.0.0.1",
            listen_port=0,
            allow_remote=False,
            bearer_token=None,
            allow_unauthenticated_remote=False,
            expose_sensitive_state=False,
            max_connections=16,
            request_timeout_seconds=0.3,
            write_timeout_seconds=0.3,
        ),
        circuit_breaker=CircuitBreakerConfig(True, 2, 5.0, 0.2, 1),
        strict_unknown_keys=True,
    )


async def close_writer(writer: asyncio.StreamWriter | None) -> None:
    if writer is None:
        return
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
    except (asyncio.TimeoutError, ConnectionError, OSError):
        writer.transport.abort()


@asynccontextmanager
async def running_server(handler: StreamHandler) -> AsyncIterator[asyncio.Server]:
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    try:
        yield server
    finally:
        server.close()
        await server.wait_closed()


def server_port(server: asyncio.Server) -> int:
    sockets = server.sockets
    if not sockets:
        raise RuntimeError("server has no sockets")
    address = sockets[0].getsockname()
    if not isinstance(address, tuple) or not isinstance(address[1], int):
        raise RuntimeError("server socket has no TCP port")
    return address[1]


async def closed_local_port() -> int:
    async def ignore(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await close_writer(writer)

    server = await asyncio.start_server(ignore, "127.0.0.1", 0)
    port = server_port(server)
    server.close()
    await server.wait_closed()
    return port


def repository_root() -> Path:
    return Path(__file__).resolve().parents[1]
