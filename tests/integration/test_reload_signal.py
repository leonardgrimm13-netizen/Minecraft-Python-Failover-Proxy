"""Exercise the shipped CLI with real POSIX reload/shutdown signals."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import close_writer, closed_local_port, running_server, server_port
from tests.integration.test_hot_reload import echo, exchange

_SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src"


@pytest.mark.skipif(not hasattr(signal, "SIGHUP"), reason="SIGHUP requires a POSIX host")
@pytest.mark.asyncio
async def test_cli_sighup_keeps_session_and_rejects_broken_file(tmp_path: Path) -> None:
    async with running_server(echo) as main, running_server(echo) as fallback:
        proxy_port, monitor_port = await closed_local_port(), await closed_local_port()
        config_file = tmp_path / "config.toml"
        text = f"""[proxy]
listen_host = "127.0.0.1"
listen_port = {proxy_port}
[main]
host = "127.0.0.1"
port = {server_port(main)}
[fallback]
host = "127.0.0.1"
port = {server_port(fallback)}
[healthcheck]
mode = "tcp"
interval_seconds = 0.05
timeout_seconds = 0.5
fail_after = 1
recover_after = 1
[connection]
timeout_seconds = 0.5
buffer_size = 4096
shutdown_grace_seconds = 0.1
shutdown_cancel_timeout_seconds = 0.5
[monitoring]
enabled = true
listen_host = "127.0.0.1"
listen_port = {monitor_port}
[maintenance]
mode = "auto"
[logging]
level = "WARNING"
"""
        config_file.write_text(text, encoding="utf-8")
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(_SOURCE_ROOT)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "mc_failover",
            "--config",
            str(config_file),
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        writer: asyncio.StreamWriter | None = None

        async def state() -> dict[str, Any]:
            reader, request_writer = await asyncio.open_connection("127.0.0.1", monitor_port)
            try:
                request_writer.write(b"GET /state HTTP/1.1\r\nHost: localhost\r\n\r\n")
                await request_writer.drain()
                response = await reader.read()
                value: dict[str, Any] = json.loads(response.split(b"\r\n\r\n", 1)[1])
                return value
            finally:
                await close_writer(request_writer)

        async def wait_state(generation: int, failures: int) -> dict[str, Any]:
            while True:
                try:
                    value = await state()
                except ConnectionRefusedError:
                    pass
                else:
                    diagnostic = value["configuration"]
                    if (
                        diagnostic["generation"] == generation
                        and diagnostic["reload_failures"] == failures
                    ):
                        return value
                await asyncio.sleep(0.01)

        try:
            await asyncio.wait_for(wait_state(1, 0), 5)
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            await exchange(reader, writer, b"before real SIGHUP")
            config_file.write_text(
                text.replace('mode = "auto"', 'mode = "force_fallback"'), encoding="utf-8"
            )
            process.send_signal(signal.SIGHUP)
            value = await asyncio.wait_for(wait_state(2, 0), 5)
            assert value["selected_backend"] == "FALLBACK"
            await exchange(reader, writer, b"after real SIGHUP")
            config_file.write_text("[unterminated", encoding="utf-8")
            process.send_signal(signal.SIGHUP)
            value = await asyncio.wait_for(wait_state(2, 1), 5)
            assert value["configuration"]["last_reload_result"] == "invalid_toml"
            await exchange(reader, writer, b"after invalid real SIGHUP")
            process.send_signal(signal.SIGTERM)
            assert await asyncio.wait_for(reader.read(), 2) == b""
            assert await asyncio.wait_for(process.wait(), 2) == 0
        finally:
            await close_writer(writer)
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 2)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
            await process.communicate()
