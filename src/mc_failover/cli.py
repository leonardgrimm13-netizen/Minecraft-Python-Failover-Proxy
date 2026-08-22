"""Command-line interface for the proxy and offline diagnostics."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from collections.abc import Sequence
from dataclasses import asdict, is_dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any, cast

from . import __version__
from .app import FailoverApplication
from .config import AppConfig, ConfigError, TargetConfig, load_config
from .endpoint_safety import EndpointLoopError, EndpointLoopGuard
from .health import close_writer, perform_health_check

DEFAULT_CONFIG_PATH = Path("config.toml")
LIVENESS_PROBE_TIMEOUT_SECONDS = 2.0
LIVENESS_PROBE_STREAM_LIMIT = 4096
LIVENESS_PROBE_MAX_HEADER_BYTES = 4096
LIVENESS_PROBE_MAX_BODY_BYTES = 1024


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mc-failover",
        description="Secure asynchronous Minecraft TCP failover proxy",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to the TOML configuration (default: ./config.toml)",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Validate the configuration and exit.",
    )
    parser.add_argument(
        "--print-effective-config",
        action="store_true",
        help="Print the effective configuration with secrets redacted and exit.",
    )
    parser.add_argument(
        "--probe-live",
        action="store_true",
        help="Probe the configured monitoring /live endpoint and exit.",
    )
    parser.add_argument("--test-main", action="store_true", help="Test MAIN over TCP and exit.")
    parser.add_argument(
        "--test-fallback", action="store_true", help="Test FALLBACK over TCP and exit."
    )
    parser.add_argument(
        "--test-healthcheck",
        action="store_true",
        help="Run the configured MAIN healthcheck and exit.",
    )
    parser.add_argument(
        "--test-fallback-healthcheck",
        action="store_true",
        help="Run the configured FALLBACK healthcheck and exit.",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def setup_logging(config: AppConfig) -> None:
    logging.basicConfig(
        level=getattr(logging, config.logging.level.upper()),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )


def config_to_dict(config: AppConfig) -> dict[str, Any]:
    def convert(value: Any) -> Any:
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, Enum):
            return value.value
        if is_dataclass(value) and not isinstance(value, type):
            return {key: convert(item) for key, item in asdict(cast(Any, value)).items()}
        if isinstance(value, dict):
            return {str(key): convert(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [convert(item) for item in value]
        return value

    converted = convert(config)
    if not isinstance(converted, dict):
        raise TypeError("AppConfig conversion did not produce a mapping")
    monitoring = converted.get("monitoring")
    if isinstance(monitoring, dict) and monitoring.get("bearer_token") is not None:
        monitoring["bearer_token"] = "<redacted>"  # noqa: S105 -- deliberate secret removal
    return converted


def _liveness_probe_host(listen_host: str) -> str:
    if listen_host == "0.0.0.0":  # noqa: S104 -- map a wildcard listener to local probe traffic
        return "127.0.0.1"
    if listen_host == "::":
        return "::1"
    return listen_host


async def _read_liveness_response(reader: asyncio.StreamReader) -> bool:
    status_line = await reader.readline()
    header_bytes = len(status_line)
    parts = status_line.decode("ascii").rstrip("\r\n").split(" ", 2)
    status_ok = len(parts) >= 2 and parts[0] in {"HTTP/1.0", "HTTP/1.1"} and parts[1] == "200"
    content_lengths: list[int] = []
    while True:
        line = await reader.readline()
        header_bytes += len(line)
        if not line or header_bytes > LIVENESS_PROBE_MAX_HEADER_BYTES:
            return False
        if line == b"\r\n":
            break
        if not line.endswith(b"\r\n"):
            return False
        name, separator, value = line[:-2].partition(b":")
        if not separator:
            return False
        if name.strip().lower() == b"content-length":
            content_lengths.append(int(value.strip()))
    if (
        not status_ok
        or len(content_lengths) != 1
        or not 0 <= content_lengths[0] <= LIVENESS_PROBE_MAX_BODY_BYTES
    ):
        return False
    body = await reader.readexactly(content_lengths[0])
    parsed: object = json.loads(body.decode("utf-8"))
    return parsed == {"live": True, "service": "mc-failover"}


async def probe_liveness(
    config: AppConfig,
    *,
    timeout_seconds: float = LIVENESS_PROBE_TIMEOUT_SECONDS,
) -> bool:
    """Return whether the configured monitoring server answers ``/live``."""

    monitoring = config.monitoring
    if not monitoring.enabled:
        return False
    host = _liveness_probe_host(monitoring.listen_host)
    headers = [
        "GET /live HTTP/1.1",
        "Host: localhost",
        "Accept: application/json",
        "Connection: close",
    ]
    if monitoring.bearer_token is not None:
        headers.append(f"Authorization: Bearer {monitoring.bearer_token}")
    request = ("\r\n".join(headers) + "\r\n\r\n").encode("ascii")

    async def request_live() -> bool:
        writer: asyncio.StreamWriter | None = None
        try:
            reader, writer = await asyncio.open_connection(
                host,
                monitoring.listen_port,
                limit=LIVENESS_PROBE_STREAM_LIMIT,
            )
            writer.write(request)
            await writer.drain()
            return await _read_liveness_response(reader)
        finally:
            await close_writer(writer)

    try:
        return await asyncio.wait_for(request_live(), timeout=timeout_seconds)
    except (
        asyncio.TimeoutError,
        TimeoutError,
        asyncio.IncompleteReadError,
        ConnectionError,
        OSError,
        json.JSONDecodeError,
        UnicodeError,
        ValueError,
    ):
        return False


async def _run_check(
    label: str,
    target: TargetConfig,
    config: AppConfig,
    *,
    configured: bool,
) -> bool:
    health_config = config.healthcheck if label == "MAIN" else config.fallback_healthcheck
    if not configured:
        health_config = replace(
            health_config,
            enabled=True,
            mode="tcp",
            target_host=None,
            target_port=None,
            require_valid_json=False,
            expected_version_contains="",
            motd_must_contain="",
            motd_must_not_contain="",
            min_players_max=0,
        )
    result = await perform_health_check(target, health_config)
    stream = sys.stdout if result.ok else sys.stderr
    print(
        f"{'OK' if result.ok else 'ERROR'}: {label} reason={result.reason} "
        f"latency_ms={result.latency_ms if result.latency_ms is not None else 'n/a'}",
        file=stream,
    )
    return result.ok


async def run_cli_checks(args: argparse.Namespace, config: AppConfig) -> int | None:
    requested = any(
        (
            args.check_config,
            args.print_effective_config,
            args.test_main,
            args.test_fallback,
            args.test_healthcheck,
            args.test_fallback_healthcheck,
        )
    )
    if not requested:
        return None
    successful = True
    if args.check_config:
        print(f"OK: configuration is valid: {args.config}")
    if args.print_effective_config:
        print(json.dumps(config_to_dict(config), indent=2, sort_keys=True))
    if args.test_main:
        successful = await _run_check("MAIN", config.main, config, configured=False) and successful
    if args.test_fallback:
        successful = (
            await _run_check("FALLBACK", config.fallback, config, configured=False) and successful
        )
    if args.test_healthcheck:
        successful = await _run_check("MAIN", config.main, config, configured=True) and successful
    if args.test_fallback_healthcheck:
        successful = (
            await _run_check("FALLBACK", config.fallback, config, configured=True) and successful
        )
    return 0 if successful else 1


async def async_main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.probe_live and any(
        (
            args.check_config,
            args.print_effective_config,
            args.test_main,
            args.test_fallback,
            args.test_healthcheck,
            args.test_fallback_healthcheck,
        )
    ):
        print("--probe-live cannot be combined with other diagnostic actions", file=sys.stderr)
        return 2
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    if args.probe_live:
        if not config.monitoring.enabled:
            print(
                "ERROR: liveness probe requires [monitoring].enabled = true",
                file=sys.stderr,
            )
            return 1
        if await probe_liveness(config):
            print("OK: monitoring /live responded")
            return 0
        print("ERROR: monitoring /live did not respond successfully", file=sys.stderr)
        return 1

    try:
        await EndpointLoopGuard(config).validate_all()
    except EndpointLoopError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    check_result = await run_cli_checks(args, config)
    if check_result is not None:
        return check_result

    setup_logging(config)
    logger = logging.getLogger("mc-failover.cli")
    if config.proxy_protocol.accept and config.proxy_protocol.trust_all_proxies:
        logger.critical(
            "DANGEROUS CONFIGURATION: trust_all_proxies=true permits every direct peer "
            "to assert arbitrary client addresses"
        )
    if (
        config.monitoring.enabled
        and config.monitoring.allow_remote
        and config.monitoring.allow_unauthenticated_remote
    ):
        logger.critical("DANGEROUS CONFIGURATION: remote monitoring has no authentication")

    application = FailoverApplication(config)
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for caught in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(caught, application.request_shutdown)
            installed_signals.append(caught)
        except (NotImplementedError, RuntimeError):
            logger.warning("Signal handlers are unavailable for %s", caught.name)
    try:
        await application.run_until_stopped()
    except OSError as exc:
        logger.error("Listener startup failed error=%s", type(exc).__name__)
        await application.shutdown()
        return 1
    finally:
        for caught in installed_signals:
            loop.remove_signal_handler(caught)
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))
