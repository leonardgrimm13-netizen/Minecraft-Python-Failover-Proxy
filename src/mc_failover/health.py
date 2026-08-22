"""Independent active health checks and hysteresis for routing targets."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from .config import HealthCheckConfig, TargetConfig
from .endpoint_safety import EndpointLoopError, EndpointLoopGuard
from .minecraft_status import (
    make_minecraft_status_packet,
    parse_status_payload,
    read_status_packet,
    sanitize_log_value,
)
from .models import HealthCheckResult, HealthStatus, TargetName
from .time_utils import SYSTEM_CLOCK, Clock, format_utc, non_negative_elapsed

log = logging.getLogger("mc-failover.health")


async def close_writer(writer: asyncio.StreamWriter | None, timeout: float = 0.25) -> None:
    if writer is None:
        return
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=timeout)
    except (asyncio.TimeoutError, ConnectionError, OSError):
        transport = writer.transport
        transport.abort()


def healthcheck_target(target: TargetConfig, check: HealthCheckConfig) -> TargetConfig:
    return TargetConfig(check.target_host or target.host, check.target_port or target.port)


async def _tcp_check(
    target: TargetConfig,
    check: HealthCheckConfig,
    clock: Clock,
    *,
    endpoint_guard: EndpointLoopGuard | None = None,
) -> HealthCheckResult:
    writer: asyncio.StreamWriter | None = None
    started = clock.monotonic()
    try:
        if endpoint_guard is None:
            _reader, writer = await asyncio.open_connection(target.host, target.port)
        else:
            _reader, writer = await endpoint_guard.open_connection(
                target.host,
                target.port,
                timeout_seconds=check.timeout_seconds,
            )
        latency = max(0.0, clock.monotonic() - started) * 1000.0
        return HealthCheckResult(True, "tcp_connect_ok", latency_ms=latency)
    finally:
        await close_writer(writer)


async def _minecraft_check(
    target: TargetConfig,
    check: HealthCheckConfig,
    clock: Clock,
    *,
    endpoint_guard: EndpointLoopGuard | None = None,
) -> HealthCheckResult:
    writer: asyncio.StreamWriter | None = None
    started = clock.monotonic()
    try:
        if endpoint_guard is None:
            reader, writer = await asyncio.open_connection(target.host, target.port)
        else:
            reader, writer = await endpoint_guard.open_connection(
                target.host,
                target.port,
                timeout_seconds=check.timeout_seconds,
            )
        writer.write(
            make_minecraft_status_packet(
                check.status_hostname or target.host, target.port, check.protocol_version
            )
        )
        await writer.drain()
        payload = await read_status_packet(reader)
        latency = max(0.0, clock.monotonic() - started) * 1000.0
        return parse_status_payload(
            payload,
            latency_ms=latency,
            require_valid_json=check.require_valid_json,
            reject_uninitialized_protocol=check.reject_uninitialized_protocol,
            max_latency_ms=check.max_latency_ms,
            expected_version_contains=check.expected_version_contains,
            motd_must_contain=check.motd_must_contain,
            motd_must_not_contain=check.motd_must_not_contain,
            min_players_max=check.min_players_max,
        )
    finally:
        await close_writer(writer)


async def perform_health_check(
    target: TargetConfig,
    check: HealthCheckConfig,
    *,
    clock: Clock = SYSTEM_CLOCK,
    endpoint_guard: EndpointLoopGuard | None = None,
) -> HealthCheckResult:
    """Run one check under a single deadline covering every I/O operation."""

    if not check.enabled:
        return HealthCheckResult(True, "healthcheck_disabled")
    effective = healthcheck_target(target, check)

    async def operation() -> HealthCheckResult:
        if check.mode == "tcp":
            if endpoint_guard is None:
                return await _tcp_check(effective, check, clock)
            return await _tcp_check(
                effective,
                check,
                clock,
                endpoint_guard=endpoint_guard,
            )
        if endpoint_guard is None:
            return await _minecraft_check(effective, check, clock)
        return await _minecraft_check(
            effective,
            check,
            clock,
            endpoint_guard=endpoint_guard,
        )

    try:
        return await asyncio.wait_for(operation(), timeout=check.timeout_seconds)
    except asyncio.TimeoutError:
        return HealthCheckResult(False, "healthcheck_timeout")
    except asyncio.CancelledError:
        raise
    except EndpointLoopError:
        log.error(
            "Healthcheck endpoint rejected because it resolves to a local listener target=%s:%s",
            sanitize_log_value(effective.host),
            effective.port,
        )
        return HealthCheckResult(False, "unsafe_listener_loop")
    except (ConnectionError, OSError, asyncio.IncompleteReadError, UnicodeError, ValueError) as exc:
        log.debug(
            "Healthcheck failed target=%s:%s error=%s",
            sanitize_log_value(effective.host),
            effective.port,
            type(exc).__name__,
        )
        return HealthCheckResult(False, f"healthcheck_{type(exc).__name__.lower()}")
    except Exception:
        log.exception("Unexpected internal healthcheck failure")
        return HealthCheckResult(False, "healthcheck_internal_error")


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    target: TargetName
    status: HealthStatus
    healthy: bool | None
    routable: bool
    successes: int
    failures: int
    total_successes: int
    total_failures: int
    last_result: HealthCheckResult | None
    last_check_at: datetime | None
    last_check_at_iso: str | None
    seconds_since_last_check: float | None
    status_changed_at: datetime


class HealthState:
    """Hysteresis state for one target, based only on monotonic durations."""

    def __init__(
        self,
        target: TargetName,
        config: HealthCheckConfig,
        *,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        self.target = target
        self.config = config
        self.clock = clock
        self.status = HealthStatus.UNKNOWN if config.enabled else HealthStatus.DISABLED
        self.successes = 0
        self.failures = 0
        self.total_successes = 0
        self.total_failures = 0
        self.last_result: HealthCheckResult | None = None
        self.last_check_at: datetime | None = None
        self.last_check_monotonic: float | None = None
        self.status_changed_at = clock.utc_now()
        self._recovery_started_monotonic: float | None = None

    @property
    def healthy(self) -> bool | None:
        if self.status is HealthStatus.HEALTHY:
            return True
        if self.status is HealthStatus.UNHEALTHY:
            return False
        return None

    @property
    def routable(self) -> bool:
        # Disabling an active check is an explicit administrative opt-out. The
        # target remains usable, but monitoring exposes the unknown health.
        return self.status in {HealthStatus.HEALTHY, HealthStatus.DISABLED}

    @property
    def recovering(self) -> bool:
        if self._recovery_started_monotonic is None:
            return False
        elapsed = max(0.0, self.clock.monotonic() - self._recovery_started_monotonic)
        return self.status is HealthStatus.UNHEALTHY and elapsed < self.config.min_recovery_seconds

    def report(self, result: HealthCheckResult, *, initial: bool = False) -> bool:
        if not self.config.enabled:
            return False
        now_mono = self.clock.monotonic()
        now_utc = self.clock.utc_now()
        self.last_result = result
        self.last_check_monotonic = now_mono
        self.last_check_at = now_utc
        previous = self.status
        if initial or self.status is HealthStatus.UNKNOWN:
            self.successes = 1 if result.ok else 0
            self.failures = 0 if result.ok else 1
            self.total_successes += int(result.ok)
            self.total_failures += int(not result.ok)
            self.status = HealthStatus.HEALTHY if result.ok else HealthStatus.UNHEALTHY
            self._recovery_started_monotonic = None
        elif result.ok:
            self.total_successes += 1
            self.successes += 1
            self.failures = 0
            if self.status is HealthStatus.UNHEALTHY:
                if self._recovery_started_monotonic is None:
                    self._recovery_started_monotonic = now_mono
                recovery_elapsed = max(0.0, now_mono - self._recovery_started_monotonic)
                if (
                    self.successes >= self.config.recover_after
                    and recovery_elapsed >= self.config.min_recovery_seconds
                ):
                    self.status = HealthStatus.HEALTHY
                    self._recovery_started_monotonic = None
        else:
            self.total_failures += 1
            self.failures += 1
            self.successes = 0
            self._recovery_started_monotonic = None
            if self.status is HealthStatus.HEALTHY and self.failures >= self.config.fail_after:
                self.status = HealthStatus.UNHEALTHY
        changed = previous is not self.status
        if changed:
            self.status_changed_at = now_utc
        return changed

    def snapshot(self) -> HealthSnapshot:
        age = non_negative_elapsed(self.clock.monotonic(), self.last_check_monotonic)
        return HealthSnapshot(
            target=self.target,
            status=self.status,
            healthy=self.healthy,
            routable=self.routable,
            successes=self.successes,
            failures=self.failures,
            total_successes=self.total_successes,
            total_failures=self.total_failures,
            last_result=self.last_result,
            last_check_at=self.last_check_at,
            last_check_at_iso=format_utc(self.last_check_at),
            seconds_since_last_check=age,
            status_changed_at=self.status_changed_at,
        )


ResultCallback = Callable[[HealthCheckResult], Awaitable[None]]


class HealthMonitor:
    def __init__(
        self,
        target: TargetConfig,
        state: HealthState,
        *,
        on_result: ResultCallback | None = None,
        endpoint_guard: EndpointLoopGuard | None = None,
        random_uniform: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self.target = target
        self.state = state
        self.on_result = on_result
        self.endpoint_guard = endpoint_guard
        self.random_uniform = random_uniform

    async def check_once(self, *, initial: bool = False) -> HealthCheckResult:
        if self.endpoint_guard is None:
            result = await perform_health_check(
                self.target,
                self.state.config,
                clock=self.state.clock,
            )
        else:
            result = await perform_health_check(
                self.target,
                self.state.config,
                clock=self.state.clock,
                endpoint_guard=self.endpoint_guard,
            )
        previous_status = self.state.status
        changed = self.state.report(result, initial=initial)
        if self.on_result is not None:
            await self.on_result(result)
        if changed:
            message = "Health state changed target=%s previous=%s status=%s reason=%s"
            arguments = (
                self.state.target.value,
                previous_status.value,
                self.state.status.value,
                result.reason,
            )
            if self.state.status is HealthStatus.HEALTHY:
                log.info(message, *arguments)
            else:
                log.warning(message, *arguments)
        elif not result.ok:
            log.debug(
                "Healthcheck unsuccessful target=%s status=%s reason=%s "
                "consecutive_failures=%s fail_after=%s",
                self.state.target.value,
                self.state.status.value,
                result.reason,
                self.state.failures,
                self.state.config.fail_after,
            )
        elif self.state.config.log_status_details and result.ok:
            log.info(
                "Healthcheck ok target=%s latency_ms=%s version=%s players=%s/%s motd=%s",
                self.state.target.value,
                f"{result.latency_ms:.1f}" if result.latency_ms is not None else "n/a",
                sanitize_log_value(result.version_name),
                result.players_online if result.players_online is not None else "n/a",
                result.players_max if result.players_max is not None else "n/a",
                sanitize_log_value(result.motd_text),
            )
        return result

    async def run(self, stop_event: asyncio.Event, *, check_immediately: bool = False) -> None:
        if not self.state.config.enabled:
            return
        first = True
        while not stop_event.is_set():
            if check_immediately or not first:
                try:
                    await self.check_once(initial=self.state.status is HealthStatus.UNKNOWN)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception(
                        "Unexpected health-monitor iteration failure target=%s",
                        self.state.target.value,
                    )
            first = False
            jitter = (
                self.random_uniform(0.0, self.state.config.jitter_seconds)
                if self.state.config.jitter_seconds > 0
                else 0.0
            )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.state.config.interval_seconds + jitter,
                )
            except asyncio.TimeoutError:
                continue
