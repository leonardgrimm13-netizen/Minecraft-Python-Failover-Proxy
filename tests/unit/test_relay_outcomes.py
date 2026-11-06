from __future__ import annotations

import asyncio
import errno
from dataclasses import replace

import pytest

from mc_failover.relay import (
    PipeEnd,
    PipeResult,
    RelayEndpoint,
    RelayErrorKind,
    RelayOutcomeKind,
    _error_result,
    classify_relay_result,
    pipe,
)
from tests.unit.test_relay_edges import (
    FakeWriter,
    ScriptedReader,
    activity,
    stream_reader,
    stream_writer,
)


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (ConnectionResetError(errno.ECONNRESET, "arbitrary text"), RelayErrorKind.RESET),
        (BrokenPipeError(errno.EPIPE, "arbitrary text"), RelayErrorKind.BROKEN_PIPE),
        (ConnectionAbortedError(errno.ECONNABORTED, "arbitrary text"), RelayErrorKind.NETWORK),
        (OSError(errno.EHOSTUNREACH, "arbitrary text"), RelayErrorKind.NETWORK),
        (OSError(errno.EACCES, "connection reset by peer"), RelayErrorKind.OTHER_IO),
        (TimeoutError(), RelayErrorKind.TIMEOUT),
    ],
)
def test_socket_evidence_uses_exception_types_and_errno(
    error: OSError, kind: RelayErrorKind
) -> None:
    result = _error_result("server_to_client", PipeEnd.READ_ERROR, 3, error)
    assert result.error_kind is kind
    assert result.error_errno == error.errno
    assert result.error_type == type(error).__name__
    assert result.bytes_forwarded == 3


def test_pipe_result_exposes_endpoint_without_guessing_unknown_directions() -> None:
    assert PipeResult("client_to_server", PipeEnd.EOF, 0).error_endpoint is RelayEndpoint.UNKNOWN
    assert PipeResult("other", PipeEnd.READ_ERROR, 0).error_endpoint is RelayEndpoint.UNKNOWN
    assert (
        PipeResult("client_to_server", PipeEnd.READ_ERROR, 0).error_endpoint is RelayEndpoint.CLIENT
    )
    assert (
        PipeResult("client_to_server", PipeEnd.WRITE_ERROR, 0).error_endpoint
        is RelayEndpoint.BACKEND
    )
    assert (
        PipeResult("server_to_client", PipeEnd.READ_ERROR, 0).error_endpoint
        is RelayEndpoint.BACKEND
    )
    assert (
        PipeResult("server_to_client", PipeEnd.WRITE_ERROR, 0).error_endpoint
        is RelayEndpoint.CLIENT
    )


@pytest.mark.parametrize(
    ("direction", "end", "kind", "expected", "failed", "confirm"),
    [
        (
            "server_to_client",
            PipeEnd.READ_ERROR,
            RelayErrorKind.RESET,
            RelayOutcomeKind.BACKEND_RESET,
            True,
            False,
        ),
        (
            "client_to_server",
            PipeEnd.WRITE_ERROR,
            RelayErrorKind.BROKEN_PIPE,
            RelayOutcomeKind.BACKEND_IO_ERROR,
            True,
            False,
        ),
        (
            "server_to_client",
            PipeEnd.READ_ERROR,
            RelayErrorKind.NETWORK,
            RelayOutcomeKind.BACKEND_IO_ERROR,
            True,
            False,
        ),
        (
            "client_to_server",
            PipeEnd.READ_ERROR,
            RelayErrorKind.RESET,
            RelayOutcomeKind.CLIENT_DISCONNECT,
            False,
            False,
        ),
        (
            "server_to_client",
            PipeEnd.WRITE_ERROR,
            RelayErrorKind.BROKEN_PIPE,
            RelayOutcomeKind.CLIENT_DISCONNECT,
            False,
            False,
        ),
        (
            "client_to_server",
            PipeEnd.WRITE_ERROR,
            RelayErrorKind.TIMEOUT,
            RelayOutcomeKind.AMBIGUOUS,
            False,
            True,
        ),
        (
            "server_to_client",
            PipeEnd.READ_ERROR,
            RelayErrorKind.OTHER_IO,
            RelayOutcomeKind.AMBIGUOUS,
            False,
            True,
        ),
        (
            "server_to_client",
            PipeEnd.READ_ERROR,
            None,
            RelayOutcomeKind.AMBIGUOUS,
            False,
            True,
        ),
    ],
)
def test_error_attribution_by_socket_operation(
    direction: str,
    end: PipeEnd,
    kind: RelayErrorKind | None,
    expected: RelayOutcomeKind,
    failed: bool,
    confirm: bool,
) -> None:
    outcome = classify_relay_result(
        (PipeResult(direction, end, 0, "arbitrary", kind),), duration_seconds=1.0
    )
    assert outcome.kind is expected
    assert outcome.backend_failure is failed
    assert outcome.confirm_health is confirm


def test_normal_eof_idle_cancellation_and_shutdown_do_not_poison_backend() -> None:
    client_eof = PipeResult("client_to_server", PipeEnd.EOF, 10)
    backend_eof = PipeResult("server_to_client", PipeEnd.EOF, 0)
    for results in ((client_eof,), (backend_eof,), (client_eof, backend_eof), ()):
        outcome = classify_relay_result(results, duration_seconds=0.01)
        assert outcome.kind is RelayOutcomeKind.EOF
        assert not outcome.backend_failure
        assert not outcome.confirm_health
    idle = classify_relay_result(
        (PipeResult("client_to_server", PipeEnd.IDLE_TIMEOUT, 0),), duration_seconds=5
    )
    assert idle.kind is RelayOutcomeKind.IDLE_TIMEOUT
    failed = _error_result("server_to_client", PipeEnd.READ_ERROR, 0, ConnectionResetError())
    shutdown = classify_relay_result((failed,), duration_seconds=0, shutting_down=True)
    assert shutdown.kind is RelayOutcomeKind.SHUTDOWN
    assert not shutdown.backend_failure
    cancelled = classify_relay_result((failed,), duration_seconds=0, cancelled=True)
    assert cancelled.kind is RelayOutcomeKind.CANCELLED
    assert not cancelled.backend_failure


def test_early_eof_only_requests_confirmation_without_client_eof_or_backend_response() -> None:
    backend_eof = PipeResult("server_to_client", PipeEnd.EOF, 0)
    outcome = classify_relay_result((backend_eof,), duration_seconds=0.01, early_session_seconds=1)
    assert outcome.kind is RelayOutcomeKind.EARLY_BACKEND_END
    assert outcome.confirm_health
    assert not outcome.backend_failure
    for results in (
        (replace(backend_eof, bytes_forwarded=40),),
        (backend_eof, PipeResult("client_to_server", PipeEnd.EOF, 0)),
    ):
        normal = classify_relay_result(results, duration_seconds=0.01, early_session_seconds=1)
        assert normal.kind is RelayOutcomeKind.EOF
        assert not normal.confirm_health
    assert (
        classify_relay_result((backend_eof,), duration_seconds=2, early_session_seconds=1).kind
        is RelayOutcomeKind.EOF
    )


def test_eof_propagation_failure_is_conservative_and_client_abort_takes_precedence() -> None:
    propagation = _error_result(
        "client_to_server",
        PipeEnd.WRITE_ERROR,
        0,
        BrokenPipeError(),
        eof_propagation=True,
    )
    ambiguous = classify_relay_result((propagation,), duration_seconds=1)
    assert ambiguous.confirm_health
    assert not ambiguous.backend_failure
    client_abort = _error_result("client_to_server", PipeEnd.READ_ERROR, 0, ConnectionResetError())
    normal = classify_relay_result((propagation, client_abort), duration_seconds=1)
    assert normal.kind is RelayOutcomeKind.CLIENT_DISCONNECT
    assert not normal.confirm_health


@pytest.mark.asyncio
async def test_pipe_captures_typed_reset_read_write_timeout_and_half_close_errors() -> None:
    reset = await pipe(
        stream_reader(ScriptedReader(ConnectionResetError(errno.ECONNRESET, "not parsed"))),
        stream_writer(FakeWriter()),
        direction="server_to_client",
        buffer_size=4096,
        idle_timeout_seconds=0,
        write_timeout_seconds=1,
        activity=activity(),
    )
    assert reset.error_kind is RelayErrorKind.RESET
    assert classify_relay_result((reset,), duration_seconds=1).backend_failure
    for data in (b"", b"data"):
        result = await pipe(
            stream_reader(ScriptedReader(data)),
            stream_writer(FakeWriter(drain_error=asyncio.TimeoutError())),
            direction="client_to_server",
            buffer_size=4096,
            idle_timeout_seconds=0,
            write_timeout_seconds=1,
            activity=activity(),
        )
        assert result.error_kind is RelayErrorKind.TIMEOUT
        assert result.eof_propagation is (data == b"")
        assert classify_relay_result((result,), duration_seconds=1).confirm_health
