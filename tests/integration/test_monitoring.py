from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest

from mc_failover.circuit_breaker import CircuitBreaker
from mc_failover.cli import probe_liveness
from mc_failover.health import HealthState
from mc_failover.models import HealthCheckResult, RejectionReason, TargetName
from mc_failover.monitoring import MonitoringServer
from mc_failover.routing import MaintenanceWatcher, Router
from mc_failover.runtime import RuntimeState
from tests.conftest import (
    close_writer,
    closed_local_port,
    make_config,
    running_server,
    server_port,
)


class MonitoringHarness:
    def __init__(self, *, main_ok: bool, fallback_ok: bool, token: str | None = None) -> None:
        config = make_config(25564, 25566, monitoring=True)
        config = replace(
            config,
            monitoring=replace(config.monitoring, bearer_token=token),
        )
        self.config = config
        self.runtime = RuntimeState()
        self.main = HealthState(TargetName.MAIN, config.healthcheck)
        self.fallback = HealthState(TargetName.FALLBACK, config.fallback_healthcheck)
        self.main.report(HealthCheckResult(main_ok, "main_initial", 1.0), initial=True)
        self.fallback.report(HealthCheckResult(fallback_ok, "fallback_initial", 2.0), initial=True)
        self.circuit = CircuitBreaker(config.circuit_breaker)
        self.maintenance = MaintenanceWatcher(config.maintenance)
        self.router = Router(config, self.main, self.fallback, self.circuit, self.maintenance)
        self.server = MonitoringServer(
            config,
            self.runtime,
            self.main,
            self.fallback,
            self.circuit,
            self.router,
        )

    async def start(self) -> int:
        await self.maintenance.refresh()
        listener = await self.server.start()
        assert listener is not None
        return server_port(listener)

    async def stop(self) -> None:
        await self.server.stop()


async def request(port: int, raw: bytes) -> tuple[int, dict[str, str], bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    response = await asyncio.wait_for(reader.read(), 2.0)
    await close_writer(writer)
    head, body = response.split(b"\r\n\r\n", 1)
    lines = head.decode("ascii").split("\r\n")
    status = int(lines[0].split(" ", 2)[1])
    headers = {
        name.lower(): value.strip() for name, value in (line.split(":", 1) for line in lines[1:])
    }
    assert int(headers["content-length"]) == len(body)
    assert headers["connection"] == "close"
    return status, headers, body


def get(path: str, *, authorization: str | None = None) -> bytes:
    auth = b"" if authorization is None else f"Authorization: {authorization}\r\n".encode()
    return b"GET " + path.encode() + b" HTTP/1.1\r\nHost: localhost\r\n" + auth + b"\r\n"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("main_ok", "fallback_ok", "expected_status", "expected_state", "target"),
    [
        (True, True, 200, "healthy", "MAIN"),
        (True, False, 200, "degraded", "MAIN"),
        (False, True, 200, "degraded", "FALLBACK"),
        (False, False, 503, "unavailable", "NONE"),
    ],
)
async def test_live_ready_and_health_status_matrix(
    main_ok: bool,
    fallback_ok: bool,
    expected_status: int,
    expected_state: str,
    target: str,
) -> None:
    harness = MonitoringHarness(main_ok=main_ok, fallback_ok=fallback_ok)
    port = await harness.start()
    live_status, _, live_body = await request(port, get("/live"))
    assert live_status == 200
    assert json.loads(live_body) == {"live": True, "service": "mc-failover"}

    ready_status, _, ready_body = await request(port, get("/ready?probe=1"))
    ready = json.loads(ready_body)
    assert ready_status == expected_status
    assert ready["ready"] is (expected_status == 200)
    assert ready["status"] == expected_state
    assert ready["active_target"] == target

    health_status, _, health_body = await request(port, get("/health"))
    health = json.loads(health_body)
    assert health_status == expected_status
    assert health["status"] == expected_state
    assert health["started_at"].endswith("Z")
    assert health["uptime_seconds"] >= 0
    assert "last_check_at" in health["main"]
    assert "circuit_breaker" in health
    assert "targets" not in health
    await harness.stop()


@pytest.mark.asyncio
async def test_health_json_exposes_precise_connection_counters_and_legacy_aliases() -> None:
    harness = MonitoringHarness(main_ok=True, fallback_ok=True)
    harness.runtime.incoming_connection_received()
    harness.runtime.incoming_connection_received()
    harness.runtime.connection_admitted()
    harness.runtime.backend_connection_started()
    harness.runtime.backend_connection_finished()
    harness.runtime.reject(RejectionReason.NO_TARGET)
    port = await harness.start()

    status, _, body = await request(port, get("/health"))
    payload = json.loads(body)

    assert status == 200
    assert payload["incoming_connections_total"] == 2
    assert payload["backend_connections_established_total"] == 1
    assert payload["connections_rejected_total"] == 1
    assert payload["active_connections"] == 0
    assert payload["total_connections"] == 1
    assert payload["rejected_connections"] == payload["connections_rejected_total"]
    await harness.stop()


@pytest.mark.asyncio
async def test_state_redacts_targets_unless_explicitly_enabled() -> None:
    harness = MonitoringHarness(main_ok=True, fallback_ok=True)
    port = await harness.start()
    status, _, body = await request(port, get("/state"))
    assert status == 200
    assert "targets" not in json.loads(body)
    await harness.stop()

    harness = MonitoringHarness(main_ok=True, fallback_ok=True)
    harness.config = replace(
        harness.config,
        monitoring=replace(harness.config.monitoring, expose_sensitive_state=True),
    )
    harness.server.config = harness.config
    harness.router.config = harness.config
    port = await harness.start()
    _, _, body = await request(port, get("/state"))
    assert json.loads(body)["targets"]["main"]["host"] == "127.0.0.1"
    await harness.stop()


@pytest.mark.asyncio
async def test_bearer_auth_protects_every_endpoint_and_is_not_reflected() -> None:
    harness = MonitoringHarness(main_ok=True, fallback_ok=True, token="correct-secret")
    port = await harness.start()
    for path in ("/live", "/ready", "/health", "/state", "/metrics"):
        status, headers, body = await request(port, get(path))
        assert status == 401
        assert headers["www-authenticate"].startswith("Bearer")
        assert b"correct-secret" not in body
    status, _, body = await request(port, get("/ready", authorization="Bearer wrong"))
    assert status == 401
    assert b"correct-secret" not in body
    status, _, body = await request(port, get("/ready", authorization="Bearer correct-secret"))
    assert status == 200
    assert b"correct-secret" not in body
    await harness.stop()


@pytest.mark.asyncio
async def test_cli_liveness_probe_uses_loopback_and_configured_bearer_token() -> None:
    harness = MonitoringHarness(main_ok=False, fallback_ok=False, token="correct-secret")
    port = await harness.start()
    probe_config = replace(
        harness.config,
        monitoring=replace(
            harness.config.monitoring,
            listen_host="0.0.0.0",
            listen_port=port,
        ),
    )

    assert await probe_liveness(probe_config)
    assert not await probe_liveness(
        replace(
            probe_config,
            monitoring=replace(
                probe_config.monitoring,
                bearer_token="wrong-secret",
            ),
        )
    )
    assert not await probe_liveness(
        replace(
            probe_config,
            monitoring=replace(probe_config.monitoring, enabled=False),
        )
    )
    await harness.stop()


@pytest.mark.asyncio
async def test_cli_liveness_probe_rejects_wrong_identity_timeout_and_closed_port() -> None:
    config = make_config(25564, 25566, monitoring=True)

    async def impostor(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        body = b'{"live":false,"service":"other"}'
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        await close_writer(writer)

    async with running_server(impostor) as server:
        wrong_identity = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=server_port(server)),
        )
        assert not await probe_liveness(wrong_identity)

    async def stalled(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()
        await close_writer(writer)

    async with running_server(stalled) as server:
        timeout = replace(
            config,
            monitoring=replace(config.monitoring, listen_port=server_port(server)),
        )
        assert not await probe_liveness(timeout, timeout_seconds=0.01)

    closed_port = await closed_local_port()
    unavailable = replace(
        config,
        monitoring=replace(config.monitoring, listen_port=closed_port),
    )
    assert not await probe_liveness(unavailable)


@pytest.mark.asyncio
async def test_metrics_have_descriptors_and_required_circuit_and_connect_names() -> None:
    harness = MonitoringHarness(main_ok=True, fallback_ok=True)
    harness.runtime.main_connect_failures = 3
    port = await harness.start()
    status, headers, body = await request(port, get("/metrics"))
    text = body.decode()
    assert status == 200
    assert headers["content-type"].startswith("text/plain; version=0.0.4")
    for name in (
        "mc_failover_incoming_connections_total",
        "mc_failover_backend_connections_established_total",
        "mc_failover_connections_rejected_total",
        "mc_failover_main_connect_failures_total",
        "mc_failover_fallback_connect_failures_total",
        "mc_failover_main_connect_successes_total",
        "mc_failover_fallback_connect_successes_total",
        "mc_failover_circuit_breaker_state",
        "mc_failover_circuit_breaker_open_total",
    ):
        assert f"# HELP {name} " in text
        assert f"# TYPE {name} " in text
    assert "mc_failover_main_connect_failures_total 3" in text
    await harness.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"POST /live HTTP/1.1\r\nHost: localhost\r\n\r\n", 405),
        (b"GET /live HTTP/2\r\nHost: localhost\r\n\r\n", 400),
        (b"GET /live HTTP/1.1\nHost: localhost\n\n", 400),
        (b"GET /../state HTTP/1.1\r\nHost: localhost\r\n\r\n", 400),
        (b"GET /%2e%2e/state HTTP/1.1\r\nHost: localhost\r\n\r\n", 400),
        (b"GET /live HTTP/1.1\r\n\r\n", 400),
        (b"GET /live HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\n", 400),
        (b"GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n", 404),
    ],
)
async def test_http_parser_rejects_invalid_framing(raw: bytes, expected: int) -> None:
    harness = MonitoringHarness(main_ok=True, fallback_ok=True)
    port = await harness.start()
    status, _, _ = await request(port, raw)
    assert status == expected
    await harness.stop()


@pytest.mark.asyncio
async def test_request_deadline_and_monitoring_connection_limit_cleanup() -> None:
    harness = MonitoringHarness(main_ok=True, fallback_ok=True)
    harness.config = replace(
        harness.config,
        monitoring=replace(
            harness.config.monitoring,
            max_connections=1,
            request_timeout_seconds=0.05,
        ),
    )
    harness.server.config = harness.config
    port = await harness.start()
    slow_reader, slow_writer = await asyncio.open_connection("127.0.0.1", port)
    slow_writer.write(b"GET /live HTTP/1.1\r\n")
    await slow_writer.drain()

    rejected_reader, rejected_writer = await asyncio.open_connection("127.0.0.1", port)
    rejected_writer.write(get("/live"))
    await rejected_writer.drain()
    assert await asyncio.wait_for(rejected_reader.read(), 1.0) == b""
    await close_writer(rejected_writer)
    assert harness.runtime.monitoring_rejected_connections == 1

    response = await asyncio.wait_for(slow_reader.read(), 1.0)
    assert b"408 Request Timeout" in response
    await close_writer(slow_writer)
    await harness.stop()
    assert harness.server.tasks.active_count == 0
