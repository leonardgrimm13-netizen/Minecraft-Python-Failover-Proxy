#!/usr/bin/env python3
"""Loopback-only multi-backend/status/reload load test (Linux, installed checkout).

Run in separate processes, for example:
  python tools/benchmark_features.py --clients 100 --output /tmp/features-100.json
  python tools/benchmark_features.py --clients 500 --output /tmp/features-500.json
  python tools/benchmark_features.py --clients 2000 --output /tmp/features-2000.json

The process includes proxy, fake backends and clients. These comparative figures
are not a production capacity or latency guarantee. No configured remote target
is contacted. Open-file limits must accommodate roughly six sockets per client during simultaneous status queries.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import platform
import resource
import socket
import tempfile
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from mc_failover.app import FailoverApplication
from mc_failover.config import load_config
from mc_failover.minecraft_status import (
    decode_varint,
    make_minecraft_status_packet,
    read_status_packet,
    write_varint,
)
from mc_failover.relay import close_stream_writer


def resources() -> dict[str, int]:
    descriptors = list(Path("/proc/self/fd").iterdir())
    sockets = 0
    for descriptor in descriptors:
        try:
            sockets += int(os.readlink(descriptor).startswith("socket:"))
        except FileNotFoundError:
            continue
    return {
        "tasks": len(asyncio.all_tasks()),
        "fds": len(descriptors),
        "sockets": sockets,
        "rss_kib": int(Path("/proc/self/statm").read_text().split()[1])
        * os.sysconf("SC_PAGE_SIZE")
        // 1024,
    }


def percentiles(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        f"p{quantile}_ms": ordered[int((len(ordered) - 1) * quantile / 100)] * 1000
        for quantile in (50, 95, 99)
    }


async def wait_until(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    async def wait() -> None:
        while not predicate():  # noqa: ASYNC110 -- bounded external-state observation
            await asyncio.sleep(0.005)

    await asyncio.wait_for(wait(), timeout)


def unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    tasks: set[asyncio.Task[None]] = set()
    backend_writers: set[asyncio.StreamWriter] = set()
    clients: set[asyncio.StreamWriter] = set()
    servers: list[asyncio.Server] = []
    app: FailoverApplication | None = None
    lag_task: asyncio.Task[None] | None = None
    stop_lag = asyncio.Event()
    lag: list[float] = []
    report: dict[str, Any] = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "parameters": vars(args).copy(),
        "errors": 0,
    }

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        backend_writers.add(writer)
        try:
            while data := await reader.read(65_536):
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            await close_stream_writer(writer)
            backend_writers.discard(writer)

    def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(echo(reader, writer))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    async def sample_lag() -> None:
        loop = asyncio.get_running_loop()
        while not stop_lag.is_set():
            started = loop.time()
            await asyncio.sleep(0.01)
            lag.append(max(0.0, loop.time() - started - 0.01))

    async def close(writer: asyncio.StreamWriter) -> None:
        await close_stream_writer(writer)
        clients.discard(writer)

    proxy_port = unused_port()
    login_body = (
        b"\x00" + write_varint(767) + b"\x09localhost" + proxy_port.to_bytes(2, "big") + b"\x02"
    )
    login = write_varint(len(login_body)) + login_body

    async def connect() -> tuple[asyncio.StreamReader, asyncio.StreamWriter, float]:
        started = time.perf_counter()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        clients.add(writer)
        writer.write(login)
        await writer.drain()
        if await reader.readexactly(len(login)) != login:
            raise RuntimeError("initial login bytes changed")
        return reader, writer, time.perf_counter() - started

    async def roundtrip(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> float:
        payload = b"x" * args.payload_bytes
        started = time.perf_counter()
        writer.write(payload)
        await writer.drain()
        if await reader.readexactly(len(payload)) != payload:
            raise RuntimeError("relay bytes changed")
        return time.perf_counter() - started

    async def status() -> float:
        started = time.perf_counter()
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        clients.add(writer)
        try:
            writer.write(make_minecraft_status_packet("localhost", proxy_port, 767))
            await writer.drain()
            packet = await read_status_packet(reader)
            packet_id, offset = decode_varint(packet)
            size, offset = decode_varint(packet, offset)
            parsed = json.loads(packet[offset:].decode())
            if packet_id or size != len(packet) - offset or "description" not in parsed:
                raise RuntimeError("invalid local status JSON")
            ping = b"\x09\x01" + b"12345678"
            writer.write(ping)
            await writer.drain()
            if await reader.readexactly(len(ping)) != ping:
                raise RuntimeError("invalid local status pong")
            return time.perf_counter() - started
        finally:
            await close(writer)

    try:
        for _ in range(4):
            servers.append(await asyncio.start_server(accept, "127.0.0.1", 0, backlog=4096))
        ports = [int(server.sockets[0].getsockname()[1]) for server in servers]
        with tempfile.TemporaryDirectory(prefix="mc-failover-load-") as temporary:
            config_path = Path(temporary) / "config.toml"
            base_text = f"""[proxy]
listen_host = "127.0.0.1"
listen_port = {proxy_port}
backlog = 4096
[connection]
timeout_seconds = 2
buffer_size = 65536
max_connections = {max(256, args.clients * 3)}
shutdown_grace_seconds = 0
shutdown_cancel_timeout_seconds = 2
[logging]
level = "ERROR"
[status]
enabled = true
always = true
max_concurrent = {max(128, args.clients * 2)}
session_timeout_seconds = 30
handshake_timeout_seconds = 30
[routing]
connect_budget_seconds = 5
max_connect_attempts = 4
[circuit_breaker]
enabled = true
failure_threshold = 3
failure_window_seconds = 10
open_seconds = 0.1
half_open_max_attempts = 1
"""
            backend_host = "localhost" if args.backend_hostname else "127.0.0.1"
            backend_text = "".join(
                f'''[[backends]]
id = "{name}"
host = "{backend_host}"
port = {backend_port}
priority = {index * 100}
[backends.healthcheck]
mode = "tcp"
interval_seconds = 0.05
timeout_seconds = 0.5
fail_after = 1
recover_after = 1
'''
                for index, (name, backend_port) in enumerate(
                    zip(("MAIN", "F1", "F2", "F3"), ports, strict=True)
                )
            )
            config_path.write_text(base_text + backend_text)
            app = FailoverApplication(load_config(config_path), config_path=config_path)
            await app.start()
            report["baseline"] = resources()
            lag_task = asyncio.create_task(sample_lag())
            started, cpu = time.perf_counter(), time.process_time()
            held = await asyncio.gather(*(connect() for _ in range(args.clients)))
            report["connect"] = {
                **percentiles([elapsed for _, _, elapsed in held]),
                "wall_seconds": time.perf_counter() - started,
                "cpu_seconds": time.process_time() - cpu,
                **resources(),
            }
            started, cpu = time.perf_counter(), time.process_time()
            rtts = [
                elapsed
                for _ in range(args.rounds)
                for elapsed in await asyncio.gather(
                    *(roundtrip(reader, writer) for reader, writer, _ in held)
                )
            ]
            wall = time.perf_counter() - started
            report["relay"] = {
                **percentiles(rtts),
                "roundtrips": len(rtts),
                "wall_seconds": wall,
                "cpu_seconds": time.process_time() - cpu,
                "duplex_mib_per_second": 2 * args.payload_bytes * len(rtts) / wall / 2**20,
            }
            started, cpu = time.perf_counter(), time.process_time()
            statuses = await asyncio.gather(*(status() for _ in range(args.clients)))
            report["status"] = {
                **percentiles(statuses),
                "count": len(statuses),
                "wall_seconds": time.perf_counter() - started,
                "cpu_seconds": time.process_time() - cpu,
                **resources(),
            }
            before_reload = resources()
            reload_times = []
            reload_started, reload_cpu = time.perf_counter(), time.process_time()
            for index in range(args.reloads):
                tick = time.perf_counter()
                candidate = replace(
                    app.config, status=replace(app.config.status, motd_online=f"reload {index}")
                )
                traffic = asyncio.gather(*(roundtrip(reader, writer) for reader, writer, _ in held))
                activated = await app.reload_config(candidate)
                await traffic
                if not activated:
                    raise RuntimeError("reload rejected under load")
                reload_times.append(time.perf_counter() - tick)
            await asyncio.sleep(0.05)
            report["reload_under_load"] = {
                **percentiles(reload_times),
                "count": args.reloads,
                "generation": app.runtime.config_generation,
                "includes_held_roundtrip": True,
                "traffic_overlaps_reload": True,
                "wall_seconds": time.perf_counter() - reload_started,
                "cpu_seconds": time.process_time() - reload_cpu,
                "before": before_reload,
                "after": resources(),
            }
            started = time.perf_counter()
            servers[0].close()
            servers[1].close()
            await wait_until(lambda: app is not None and app.router.snapshot().backend_id == "F2")
            report["two_backend_failures_detection_ms"] = (time.perf_counter() - started) * 1000
            await asyncio.gather(*(roundtrip(reader, writer) for reader, writer, _ in held))
            reader, writer, _ = await connect()
            await roundtrip(reader, writer)
            await close(writer)
            started = time.perf_counter()
            recovered = await asyncio.start_server(accept, "127.0.0.1", ports[0], backlog=4096)
            servers.append(recovered)
            await wait_until(lambda: app is not None and app.router.snapshot().backend_id == "MAIN")
            report["recovery_detection_ms"] = (time.perf_counter() - started) * 1000
            await asyncio.gather(*(close(writer) for _, writer, _ in held))
            await wait_until(lambda: app is not None and app.runtime.active_connections == 0)
            report["after_clients"] = resources()
            await app.shutdown()
            report["shutdown"] = {
                "proxy_tasks": app.proxy.tasks.active_count,
                "background_tasks": app.background.active_count,
                "active_sessions": app.runtime.active_connections,
                "limiter_active": app.limiter.active,
            }
            report["eventloop_lag"] = percentiles(lag)
    finally:
        stop_lag.set()
        if lag_task is not None:
            await lag_task
        if app is not None:
            await app.shutdown()
        await asyncio.gather(*(close_stream_writer(writer) for writer in clients))
        for server in servers:
            server.close()
        await asyncio.gather(*(close_stream_writer(writer) for writer in tuple(backend_writers)))
        if tasks:
            await asyncio.gather(*tasks)
        await asyncio.gather(*(server.wait_closed() for server in servers))
        # Let queued connection_lost callbacks release their transport FDs before
        # comparing cleanup resources; close()/wait_closed() may finish inline.
        await asyncio.sleep(0.05)
    report["final"] = resources()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--reloads", type=int, default=10)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--output")
    parser.add_argument(
        "--backend-hostname",
        action="store_true",
        help="Exercise dual-stack localhost DNS backend connections.",
    )
    args = parser.parse_args()
    if not (
        1 <= args.clients <= 2000
        and 1 <= args.rounds <= 1000
        and 1 <= args.reloads <= 100
        and 1 <= args.payload_bytes <= 65536
    ):
        parser.error("parameters exceed local safety bounds")
    descriptor_budget = args.clients * 6 + 256
    soft_limit, _hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit != resource.RLIM_INFINITY and soft_limit < descriptor_budget:
        parser.error(
            f"open-file limit {soft_limit} is too low; run ulimit -n {descriptor_budget} or higher"
        )
    logging.basicConfig(level=logging.ERROR)
    report = asyncio.run(asyncio.wait_for(benchmark(args), timeout=180))
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        Path(args.output).write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
