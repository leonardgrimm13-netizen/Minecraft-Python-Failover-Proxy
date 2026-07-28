from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Coroutine
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import mc_failover.health as health_module
from mc_failover.config import HealthCheckConfig, TargetConfig
from mc_failover.health import HealthMonitor, HealthState, healthcheck_target, perform_health_check
from mc_failover.minecraft_status import write_varint
from mc_failover.models import HealthCheckResult, HealthStatus, TargetName

UTC = timezone.utc


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

    def advance_monotonic(self, seconds: float) -> None:
        self.monotonic_value += seconds

    def jump_wall_clock(self, seconds: float) -> None:
        self.utc_value += timedelta(seconds=seconds)


def make_health_config(**overrides: object) -> HealthCheckConfig:
    values: dict[str, object] = {
        "enabled": True,
        "mode": "tcp",
        "interval_seconds": 3.0,
        "timeout_seconds": 2.0,
        "fail_after": 2,
        "recover_after": 2,
        "min_recovery_seconds": 0.0,
        "target_host": None,
        "target_port": None,
        "protocol_version": 767,
        "status_hostname": None,
        "require_valid_json": True,
        "reject_uninitialized_protocol": True,
        "log_status_details": False,
        "jitter_seconds": 0.0,
        "max_latency_ms": 0.0,
        "expected_version_contains": "",
        "motd_must_contain": "",
        "motd_must_not_contain": "",
        "min_players_max": 0,
    }
    values.update(overrides)
    return HealthCheckConfig(**values)  # type: ignore[arg-type]


def ok(reason: str = "ok") -> HealthCheckResult:
    return HealthCheckResult(True, reason, latency_ms=1.0)


def failed(reason: str = "failed") -> HealthCheckResult:
    return HealthCheckResult(False, reason, latency_ms=1.0)


def assert_status(state: HealthState, expected: HealthStatus) -> None:
    assert state.status is expected


def assert_health_view(
    state: HealthState,
    *,
    status: HealthStatus,
    healthy: bool | None,
    routable: bool,
) -> None:
    assert state.status is status
    assert state.healthy is healthy
    assert state.routable is routable


class FakeTransport:
    def __init__(self) -> None:
        self.abort_calls = 0

    def abort(self) -> None:
        self.abort_calls += 1


class FakeWriter:
    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.drain_calls = 0
        self.close_calls = 0
        self.wait_closed_calls = 0
        self.transport = FakeTransport()

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        self.drain_calls += 1

    def close(self) -> None:
        self.close_calls += 1

    async def wait_closed(self) -> None:
        self.wait_closed_calls += 1


class MemoryReader:
    def __init__(self, data: bytes) -> None:
        self.data = bytearray(data)

    async def readexactly(self, size: int) -> bytes:
        if len(self.data) < size:
            partial = bytes(self.data)
            self.data.clear()
            raise asyncio.IncompleteReadError(partial=partial, expected=size)
        result = bytes(self.data[:size])
        del self.data[:size]
        return result


def minecraft_status_packet(*, protocol: int = 767) -> bytes:
    payload_json = (
        f'{{"version":{{"name":"1.21.1","protocol":{protocol}}},'
        '"players":{"online":1,"max":20},"description":{"text":"READY"}}'
    ).encode()
    payload = write_varint(0) + write_varint(len(payload_json)) + payload_json
    return write_varint(len(payload)) + payload


def test_main_and_fallback_health_states_are_fully_independent() -> None:
    clock = FakeClock()
    config = make_health_config(fail_after=2, recover_after=2)
    main = HealthState(TargetName.MAIN, config, clock=clock)
    fallback = HealthState(TargetName.FALLBACK, config, clock=clock)

    assert main.report(ok("main_up"), initial=True)
    assert fallback.report(failed("fallback_down"), initial=True)
    assert_status(main, HealthStatus.HEALTHY)
    assert_status(fallback, HealthStatus.UNHEALTHY)
    assert main.total_successes == 1 and main.total_failures == 0
    assert fallback.total_successes == 0 and fallback.total_failures == 1
    assert main.snapshot().target is TargetName.MAIN
    assert fallback.snapshot().target is TargetName.FALLBACK


@pytest.mark.parametrize(
    ("initial_result", "status", "healthy", "routable"),
    [
        (ok(), HealthStatus.HEALTHY, True, True),
        (failed(), HealthStatus.UNHEALTHY, False, False),
    ],
)
def test_first_report_establishes_an_initial_state_without_waiting_for_thresholds(
    initial_result: HealthCheckResult,
    status: HealthStatus,
    healthy: bool,
    routable: bool,
) -> None:
    state = HealthState(
        TargetName.MAIN,
        make_health_config(fail_after=5, recover_after=5),
        clock=FakeClock(),
    )
    assert_health_view(
        state,
        status=HealthStatus.UNKNOWN,
        healthy=None,
        routable=False,
    )
    assert state.report(initial_result)
    assert_health_view(state, status=status, healthy=healthy, routable=routable)


def test_failure_threshold_and_counters_change_only_at_the_boundary() -> None:
    state = HealthState(
        TargetName.MAIN,
        make_health_config(fail_after=3),
        clock=FakeClock(),
    )
    state.report(ok(), initial=True)

    assert not state.report(failed("first"))
    assert not state.report(failed("second"))
    assert_status(state, HealthStatus.HEALTHY)
    assert state.failures == 2
    assert state.successes == 0

    assert state.report(failed("third"))
    assert_status(state, HealthStatus.UNHEALTHY)
    assert state.failures == 3
    assert state.total_successes == 1
    assert state.total_failures == 3


def test_recovery_requires_both_success_threshold_and_monotonic_stability_time() -> None:
    clock = FakeClock()
    state = HealthState(
        TargetName.MAIN,
        make_health_config(recover_after=2, min_recovery_seconds=10.0),
        clock=clock,
    )
    state.report(failed(), initial=True)

    assert not state.report(ok("first_recovery"))
    assert_status(state, HealthStatus.UNHEALTHY)
    assert state.recovering
    clock.advance_monotonic(9.999)
    assert not state.report(ok("threshold_met_but_too_early"))
    assert_status(state, HealthStatus.UNHEALTHY)

    clock.advance_monotonic(0.001)
    assert state.report(ok("stable"))
    assert_status(state, HealthStatus.HEALTHY)
    assert state.successes == 3
    assert not state.recovering


def test_failed_recovery_resets_success_count_and_stability_timer() -> None:
    clock = FakeClock()
    state = HealthState(
        TargetName.FALLBACK,
        make_health_config(recover_after=2, min_recovery_seconds=5.0),
        clock=clock,
    )
    state.report(failed(), initial=True)
    state.report(ok())
    clock.advance_monotonic(5.0)
    state.report(failed("flap"))
    assert state.successes == 0
    assert not state.recovering

    state.report(ok())
    assert_status(state, HealthStatus.UNHEALTHY)
    clock.advance_monotonic(4.999)
    state.report(ok())
    assert_status(state, HealthStatus.UNHEALTHY)
    clock.advance_monotonic(0.001)
    assert state.report(ok())
    assert_status(state, HealthStatus.HEALTHY)


def test_zero_recovery_duration_preserves_success_hysteresis() -> None:
    state = HealthState(
        TargetName.MAIN,
        make_health_config(recover_after=2, min_recovery_seconds=0.0),
        clock=FakeClock(),
    )
    state.report(failed(), initial=True)
    assert not state.report(ok())
    assert_status(state, HealthStatus.UNHEALTHY)
    assert state.report(ok())
    assert_status(state, HealthStatus.HEALTHY)


def test_disabled_healthcheck_is_routable_but_has_unknown_health_and_ignores_reports() -> None:
    clock = FakeClock()
    state = HealthState(
        TargetName.FALLBACK,
        make_health_config(enabled=False),
        clock=clock,
    )
    assert_status(state, HealthStatus.DISABLED)
    assert state.healthy is None
    assert state.routable
    assert not state.report(failed())
    snapshot = state.snapshot()
    assert snapshot.last_result is None
    assert snapshot.last_check_at is None
    assert snapshot.seconds_since_last_check is None
    assert snapshot.total_failures == 0


def test_snapshot_uses_utc_for_timestamps_and_monotonic_time_for_age() -> None:
    clock = FakeClock()
    state = HealthState(TargetName.MAIN, make_health_config(), clock=clock)
    initial_wall = clock.utc_now()
    state.report(ok(), initial=True)

    clock.advance_monotonic(7.25)
    clock.jump_wall_clock(-86_400.0)
    snapshot = state.snapshot()
    assert snapshot.last_check_at == initial_wall
    assert snapshot.last_check_at_iso == "2026-07-19T12:00:00.000Z"
    assert snapshot.seconds_since_last_check == pytest.approx(7.25)
    assert snapshot.status_changed_at == initial_wall

    clock.monotonic_value -= 1000.0
    assert state.snapshot().seconds_since_last_check == 0.0


def test_wall_clock_jumps_do_not_complete_recovery() -> None:
    clock = FakeClock()
    state = HealthState(
        TargetName.MAIN,
        make_health_config(recover_after=1, min_recovery_seconds=30.0),
        clock=clock,
    )
    state.report(failed(), initial=True)
    state.report(ok())
    clock.jump_wall_clock(365 * 24 * 3600.0)
    assert not state.report(ok())
    assert_status(state, HealthStatus.UNHEALTHY)
    clock.advance_monotonic(30.0)
    assert state.report(ok())
    assert_status(state, HealthStatus.HEALTHY)


def test_healthcheck_target_uses_overrides_without_mutating_config() -> None:
    target = TargetConfig("main.internal", 25_565)
    check = make_health_config(target_host="check.internal", target_port=25_566)
    assert healthcheck_target(target, check) == TargetConfig("check.internal", 25_566)
    assert target == TargetConfig("main.internal", 25_565)
    assert healthcheck_target(target, make_health_config()) == target


@pytest.mark.asyncio
async def test_disabled_perform_check_does_not_open_a_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unexpected_open(*_args: object, **_kwargs: object) -> None:
        pytest.fail("disabled healthcheck attempted network I/O")

    monkeypatch.setattr("mc_failover.health.asyncio.open_connection", unexpected_open)
    result = await perform_health_check(
        TargetConfig("main.internal", 25_565),
        make_health_config(enabled=False),
        clock=FakeClock(),
    )
    assert result == HealthCheckResult(True, "healthcheck_disabled")


@pytest.mark.asyncio
async def test_tcp_healthcheck_uses_effective_target_measures_monotonic_latency_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    writer = FakeWriter()
    calls: list[tuple[str, int]] = []

    async def fake_open(host: str, port: int) -> tuple[MemoryReader, FakeWriter]:
        calls.append((host, port))
        clock.advance_monotonic(0.025)
        return MemoryReader(b""), writer

    monkeypatch.setattr("mc_failover.health.asyncio.open_connection", fake_open)
    result = await perform_health_check(
        TargetConfig("main.internal", 25_565),
        make_health_config(target_host="probe.internal", target_port=25_566),
        clock=clock,
    )
    assert result.ok and result.reason == "tcp_connect_ok"
    assert result.latency_ms == pytest.approx(25.0)
    assert calls == [("probe.internal", 25_566)]
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1


@pytest.mark.asyncio
async def test_minecraft_healthcheck_writes_handshake_drains_validates_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    writer = FakeWriter()
    reader = MemoryReader(minecraft_status_packet())
    calls: list[tuple[str, int]] = []

    async def fake_open(host: str, port: int) -> tuple[MemoryReader, FakeWriter]:
        calls.append((host, port))
        clock.advance_monotonic(0.012)
        return reader, writer

    monkeypatch.setattr("mc_failover.health.asyncio.open_connection", fake_open)
    result = await perform_health_check(
        TargetConfig("fallback.internal", 25_566),
        make_health_config(
            mode="minecraft_status",
            status_hostname="status.example.test",
            expected_version_contains="1.21",
            motd_must_contain="READY",
            min_players_max=20,
        ),
        clock=clock,
    )
    assert result.ok and result.reason == "status_json_ok"
    assert result.latency_ms == pytest.approx(12.0)
    assert result.version_name == "1.21.1"
    assert result.motd_text == "READY"
    assert calls == [("fallback.internal", 25_566)]
    assert len(writer.writes) == 1 and writer.writes[0]
    assert writer.drain_calls == 1
    assert writer.close_calls == writer.wait_closed_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reject_uninitialized_protocol", "expected_ok", "expected_reason"),
    [
        (True, False, "status_server_not_initialized"),
        (False, True, "status_json_ok"),
    ],
)
async def test_minecraft_healthcheck_applies_uninitialized_protocol_filter(
    monkeypatch: pytest.MonkeyPatch,
    reject_uninitialized_protocol: bool,
    expected_ok: bool,
    expected_reason: str,
) -> None:
    writer = FakeWriter()

    async def fake_open(_host: str, _port: int) -> tuple[MemoryReader, FakeWriter]:
        return MemoryReader(minecraft_status_packet(protocol=-1)), writer

    monkeypatch.setattr("mc_failover.health.asyncio.open_connection", fake_open)
    result = await perform_health_check(
        TargetConfig("main.internal", 25_565),
        make_health_config(
            mode="minecraft_status",
            reject_uninitialized_protocol=reject_uninitialized_protocol,
        ),
        clock=FakeClock(),
    )

    assert result.ok is expected_ok
    assert result.reason == expected_reason
    assert writer.close_calls == writer.wait_closed_calls == 1


@pytest.mark.asyncio
async def test_perform_healthcheck_has_one_absolute_overall_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_timeouts: list[float] = []
    operation_started = False

    async def blocked_check(
        _target: TargetConfig, _check: HealthCheckConfig, _clock: FakeClock
    ) -> HealthCheckResult:
        nonlocal operation_started
        operation_started = True
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def immediate_timeout(
        operation: Coroutine[Any, Any, HealthCheckResult], timeout: float
    ) -> Any:
        observed_timeouts.append(timeout)
        operation.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(health_module, "_tcp_check", blocked_check)
    monkeypatch.setattr("mc_failover.health.asyncio.wait_for", immediate_timeout)
    result = await perform_health_check(
        TargetConfig("main.internal", 25_565),
        make_health_config(timeout_seconds=1.75),
        clock=FakeClock(),
    )
    assert not result.ok and result.reason == "healthcheck_timeout"
    assert observed_timeouts == [1.75]
    assert not operation_started


@pytest.mark.asyncio
async def test_perform_healthcheck_wraps_the_complete_operation_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = ok("complete_operation")
    observed_timeouts: list[float] = []

    async def complete_check(
        _target: TargetConfig, _check: HealthCheckConfig, _clock: FakeClock
    ) -> HealthCheckResult:
        return expected

    async def recording_wait_for(
        operation: Awaitable[HealthCheckResult], timeout: float
    ) -> HealthCheckResult:
        observed_timeouts.append(timeout)
        return await operation

    monkeypatch.setattr(health_module, "_minecraft_check", complete_check)
    monkeypatch.setattr("mc_failover.health.asyncio.wait_for", recording_wait_for)
    result = await perform_health_check(
        TargetConfig("main.internal", 25_565),
        make_health_config(mode="minecraft_status", timeout_seconds=2.25),
        clock=FakeClock(),
    )
    assert result is expected
    assert observed_timeouts == [2.25]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exception", "reason"),
    [
        (ConnectionRefusedError(), "healthcheck_connectionrefusederror"),
        (OSError(), "healthcheck_oserror"),
        (asyncio.IncompleteReadError(partial=b"", expected=1), "healthcheck_incompletereaderror"),
        (UnicodeError(), "healthcheck_unicodeerror"),
        (ValueError(), "healthcheck_valueerror"),
    ],
)
async def test_perform_healthcheck_maps_expected_failures_to_bounded_reasons(
    monkeypatch: pytest.MonkeyPatch,
    exception: Exception,
    reason: str,
) -> None:
    async def failing_check(
        _target: TargetConfig, _check: HealthCheckConfig, _clock: FakeClock
    ) -> HealthCheckResult:
        raise exception

    monkeypatch.setattr(health_module, "_tcp_check", failing_check)
    result = await perform_health_check(
        TargetConfig("main\nforged", 25_565), make_health_config(), clock=FakeClock()
    )
    assert not result.ok
    assert result.reason == reason
    assert "\n" not in result.reason


@pytest.mark.asyncio
async def test_perform_healthcheck_logs_and_contains_unexpected_internal_exceptions(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def failing_check(
        _target: TargetConfig, _check: HealthCheckConfig, _clock: FakeClock
    ) -> HealthCheckResult:
        raise RuntimeError("secret external detail\nforged")

    monkeypatch.setattr(health_module, "_tcp_check", failing_check)
    with caplog.at_level(logging.ERROR, logger="mc-failover.health"):
        result = await perform_health_check(
            TargetConfig("main.internal", 25_565), make_health_config(), clock=FakeClock()
        )
    assert result == HealthCheckResult(False, "healthcheck_internal_error")
    internal_records = [
        record
        for record in caplog.records
        if "Unexpected internal healthcheck failure" in record.getMessage()
    ]
    assert len(internal_records) == 1
    assert internal_records[0].levelno == logging.ERROR
    assert internal_records[0].exc_info is not None


@pytest.mark.asyncio
async def test_perform_healthcheck_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def cancelled_check(
        _target: TargetConfig, _check: HealthCheckConfig, _clock: FakeClock
    ) -> HealthCheckResult:
        raise asyncio.CancelledError

    monkeypatch.setattr(health_module, "_tcp_check", cancelled_check)
    with pytest.raises(asyncio.CancelledError):
        await perform_health_check(
            TargetConfig("main.internal", 25_565), make_health_config(), clock=FakeClock()
        )


@pytest.mark.asyncio
async def test_monitor_check_once_reports_result_and_awaits_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    state = HealthState(TargetName.MAIN, make_health_config(), clock=clock)
    expected = ok("probe_ok")
    callbacks: list[HealthCheckResult] = []

    async def fake_perform(
        _target: TargetConfig, _check: HealthCheckConfig, *, clock: FakeClock
    ) -> HealthCheckResult:
        return expected

    async def on_result(result: HealthCheckResult) -> None:
        callbacks.append(result)

    monkeypatch.setattr(health_module, "perform_health_check", fake_perform)
    monitor = HealthMonitor(TargetConfig("main.internal", 25_565), state, on_result=on_result)
    assert await monitor.check_once(initial=True) is expected
    assert callbacks == [expected]
    assert_status(state, HealthStatus.HEALTHY)
    assert state.last_result is expected


@pytest.mark.asyncio
async def test_initial_healthy_transition_is_logged_at_info_not_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    state = HealthState(TargetName.MAIN, make_health_config(), clock=FakeClock())

    async def fake_perform(
        _target: TargetConfig, _check: HealthCheckConfig, *, clock: FakeClock
    ) -> HealthCheckResult:
        return ok("initial_ready")

    monkeypatch.setattr(health_module, "perform_health_check", fake_perform)
    monitor = HealthMonitor(TargetConfig("main", 25_565), state)

    with caplog.at_level(logging.DEBUG, logger="mc-failover.health"):
        await monitor.check_once(initial=True)

    transition_records = [
        record
        for record in caplog.records
        if record.getMessage().startswith("Health state changed")
    ]
    assert len(transition_records) == 1
    assert transition_records[0].levelno == logging.INFO
    assert all(record.levelno < logging.WARNING for record in caplog.records)


@pytest.mark.asyncio
async def test_health_transition_and_threshold_logging_levels(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    state = HealthState(
        TargetName.MAIN,
        make_health_config(fail_after=2, recover_after=2),
        clock=FakeClock(),
    )
    state.report(ok("already_ready"), initial=True)
    results = iter(
        (
            failed("first_failure"),
            failed("failure_threshold"),
            ok("first_recovery"),
            ok("recovered"),
        )
    )

    async def fake_perform(
        _target: TargetConfig, _check: HealthCheckConfig, *, clock: FakeClock
    ) -> HealthCheckResult:
        return next(results)

    monkeypatch.setattr(health_module, "perform_health_check", fake_perform)
    monitor = HealthMonitor(TargetConfig("main", 25_565), state)

    with caplog.at_level(logging.DEBUG, logger="mc-failover.health"):
        await monitor.check_once()
        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.DEBUG
        assert caplog.records[0].getMessage().startswith("Healthcheck unsuccessful")

        caplog.clear()
        await monitor.check_once()
        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.WARNING
        assert "status=unhealthy" in caplog.records[0].getMessage()

        caplog.clear()
        await monitor.check_once()
        assert caplog.records == []

        await monitor.check_once()
        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.INFO
        assert "previous=unhealthy status=healthy" in caplog.records[0].getMessage()


@pytest.mark.asyncio
async def test_main_and_fallback_monitors_do_not_share_results_or_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    config = make_health_config()
    main_state = HealthState(TargetName.MAIN, config, clock=clock)
    fallback_state = HealthState(TargetName.FALLBACK, config, clock=clock)

    async def fake_perform(
        target: TargetConfig, _check: HealthCheckConfig, *, clock: FakeClock
    ) -> HealthCheckResult:
        return failed("healthcheck_internal_error") if target.host == "main" else ok("fallback_ok")

    monkeypatch.setattr(health_module, "perform_health_check", fake_perform)
    main = HealthMonitor(TargetConfig("main", 25_565), main_state)
    fallback = HealthMonitor(TargetConfig("fallback", 25_566), fallback_state)
    main_result, fallback_result = await asyncio.gather(
        main.check_once(initial=True), fallback.check_once(initial=True)
    )
    assert not main_result.ok and fallback_result.ok
    assert_status(main_state, HealthStatus.UNHEALTHY)
    assert_status(fallback_state, HealthStatus.HEALTHY)
    assert main_state.total_failures == 1 and main_state.total_successes == 0
    assert fallback_state.total_successes == 1 and fallback_state.total_failures == 0


@pytest.mark.asyncio
async def test_monitor_run_checks_immediately_then_stops_without_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = HealthState(TargetName.MAIN, make_health_config(), clock=FakeClock())
    monitor = HealthMonitor(TargetConfig("main", 25_565), state)
    stop_event = asyncio.Event()
    calls: list[bool] = []

    async def fake_check_once(*, initial: bool = False) -> HealthCheckResult:
        calls.append(initial)
        stop_event.set()
        return ok()

    monkeypatch.setattr(monitor, "check_once", fake_check_once)
    await monitor.run(stop_event, check_immediately=True)
    assert calls == [True]


@pytest.mark.asyncio
async def test_monitor_run_logs_iteration_failure_and_keeps_running(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    state = HealthState(
        TargetName.MAIN,
        make_health_config(interval_seconds=0.001),
        clock=FakeClock(),
    )
    monitor = HealthMonitor(TargetConfig("main", 25_565), state)
    stop_event = asyncio.Event()
    calls = 0

    async def flaky_check(*, initial: bool = False) -> HealthCheckResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("callback failed")
        stop_event.set()
        return ok()

    monkeypatch.setattr(monitor, "check_once", flaky_check)
    with caplog.at_level("ERROR", logger="mc-failover.health"):
        await monitor.run(stop_event, check_immediately=True)
    assert calls == 2
    iteration_records = [
        record
        for record in caplog.records
        if "health-monitor iteration failure target=MAIN" in record.getMessage()
    ]
    assert len(iteration_records) == 1
    assert iteration_records[0].levelno == logging.ERROR
    assert iteration_records[0].exc_info is not None


@pytest.mark.asyncio
async def test_monitor_run_applies_deterministic_jitter_and_stops_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_health_config(interval_seconds=3.0, jitter_seconds=0.5)
    state = HealthState(TargetName.FALLBACK, config, clock=FakeClock())
    random_calls: list[tuple[float, float]] = []

    def random_uniform(start: float, end: float) -> float:
        random_calls.append((start, end))
        return 0.25

    monitor = HealthMonitor(TargetConfig("fallback", 25_566), state, random_uniform=random_uniform)
    stop_event = asyncio.Event()
    observed_timeouts: list[float] = []

    async def fake_check_once(*, initial: bool = False) -> HealthCheckResult:
        stop_event.set()
        return ok()

    async def recording_wait_for(operation: Awaitable[bool], timeout: float) -> bool:
        observed_timeouts.append(timeout)
        return await operation

    monkeypatch.setattr(monitor, "check_once", fake_check_once)
    monkeypatch.setattr("mc_failover.health.asyncio.wait_for", recording_wait_for)
    await monitor.run(stop_event, check_immediately=True)
    assert random_calls == [(0.0, 0.5)]
    assert observed_timeouts == [3.25]


@pytest.mark.asyncio
async def test_disabled_monitor_returns_without_check_or_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = HealthState(TargetName.FALLBACK, make_health_config(enabled=False), clock=FakeClock())
    monitor = HealthMonitor(TargetConfig("fallback", 25_566), state)

    async def unexpected_check(*, initial: bool = False) -> HealthCheckResult:
        pytest.fail("disabled monitor performed a check")

    monkeypatch.setattr(monitor, "check_once", unexpected_check)
    await monitor.run(asyncio.Event(), check_immediately=True)


@pytest.mark.asyncio
async def test_monitor_run_propagates_cancellation_and_cancels_active_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = HealthState(TargetName.MAIN, make_health_config(), clock=FakeClock())
    monitor = HealthMonitor(TargetConfig("main", 25_565), state)
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def blocking_check(*, initial: bool = False) -> HealthCheckResult:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        raise AssertionError("unreachable")

    monkeypatch.setattr(monitor, "check_once", blocking_check)
    task = asyncio.create_task(monitor.run(asyncio.Event(), check_immediately=True))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
