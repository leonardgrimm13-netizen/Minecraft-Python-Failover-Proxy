#!/usr/bin/env python3
"""Bounded loopback-only proxy benchmark; run against an installed checkout.

Example: python tools/benchmark_proxy.py --output /tmp/proxy-benchmark.json
Run each revision in a fresh process on the same idle machine. RSS includes the
local clients/backends; this is a comparative harness, not a capacity claim.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import platform
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from mc_failover.app import FailoverApplication
from mc_failover.config import load_config
from mc_failover.models import TargetName
from mc_failover.relay import close_stream_writer


def percentiles(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    return {
        f"p{p}_ms": ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p / 100))] * 1000
        for p in (50, 95, 99)
    }


def port(server: asyncio.Server) -> int:
    return int(server.sockets[0].getsockname()[1])


def resources() -> dict[str, int]:
    return {
        "tasks": len(asyncio.all_tasks()),
        "fds": len(os.listdir("/proc/self/fd")),
        "rss_kib": int(Path("/proc/self/statm").read_text().split()[1])
        * os.sysconf("SC_PAGE_SIZE")
        // 1024,
    }


async def wait_until(predicate: Callable[[], bool]) -> None:
    async def wait() -> None:
        while not predicate():  # noqa: ASYNC110 -- observes external state, bounded by wait_for
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), 5.0)


async def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    backend_tasks: set[asyncio.Task[None]] = set()
    backend_writers: set[asyncio.StreamWriter] = set()

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
        backend_tasks.add(task)
        task.add_done_callback(backend_tasks.discard)

    main = await asyncio.start_server(accept, "127.0.0.1", 0)
    fallback = await asyncio.start_server(accept, "127.0.0.1", 0)
    all_servers = [main, fallback]
    base = load_config(Path(args.config))
    check = replace(
        base.healthcheck,
        mode="tcp",
        interval_seconds=0.02,
        timeout_seconds=0.1,
        fail_after=1,
        recover_after=1,
        min_recovery_seconds=0.0,
        jitter_seconds=0.0,
        target_host=None,
        target_port=None,
    )
    config = replace(
        base,
        proxy=replace(base.proxy, listen_host="127.0.0.1", listen_port=0),
        main=replace(base.main, host="127.0.0.1", port=port(main)),
        fallback=replace(base.fallback, host="127.0.0.1", port=port(fallback)),
        healthcheck=check,
        fallback_healthcheck=check,
        monitoring=replace(base.monitoring, enabled=True, listen_host="127.0.0.1", listen_port=0),
        proxy_protocol=replace(base.proxy_protocol, accept=False, send=False),
        maintenance=replace(base.maintenance, force_main_file=None, force_fallback_file=None),
        connection=replace(
            base.connection,
            max_connections=max(128, args.clients * 2),
            max_connections_per_ip=0,
            new_connections_per_second=0.0,
            new_connections_burst=0,
            idle_timeout_seconds=10.0,
            shutdown_grace_seconds=0.05,
            shutdown_cancel_timeout_seconds=0.5,
        ),
        logging=replace(base.logging, access_log=False),
    )
    app = FailoverApplication(config)
    writers: set[asyncio.StreamWriter] = set()
    report: dict[str, Any] = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "parameters": vars(args).copy(),
    }
    lag: list[float] = []
    stop_lag = asyncio.Event()

    async def sample_lag() -> None:
        loop = asyncio.get_running_loop()
        while not stop_lag.is_set():
            started = loop.time()
            await asyncio.sleep(0.01)
            lag.append(max(0.0, loop.time() - started - 0.01))

    async def connect() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writers.add(writer)
        return reader, writer

    async def close(writer: asyncio.StreamWriter) -> None:
        await close_stream_writer(writer)
        writers.discard(writer)

    async def roundtrip(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter, data: bytes
    ) -> float:
        started = time.perf_counter()
        writer.write(data)
        await writer.drain()
        received = await reader.readexactly(len(data))
        if received != data:
            raise RuntimeError("relay corrupted bytes")
        return time.perf_counter() - started

    lag_task: asyncio.Task[None] | None = None
    try:
        await app.start()
        if app.proxy.server is None:
            raise RuntimeError("no proxy listener")
        proxy_port = port(app.proxy.server)
        before = resources()
        started, cpu = time.perf_counter(), time.process_time()
        await asyncio.sleep(args.idle_seconds)
        report["idle"] = {
            "wall_seconds": time.perf_counter() - started,
            "cpu_seconds": time.process_time() - cpu,
            **resources(),
        }
        lag_task = asyncio.create_task(sample_lag())

        async def client() -> list[float]:
            reader, writer = await connect()
            samples = [await roundtrip(reader, writer, b"x" * 64) for _ in range(args.rounds)]
            await close(writer)
            return samples

        for count in (1, args.clients):
            started, cpu = time.perf_counter(), time.process_time()
            samples = [
                sample
                for values in await asyncio.gather(*(client() for _ in range(count)))
                for sample in values
            ]
            report[f"small_packets_{count}_clients"] = {
                **percentiles(samples),
                "roundtrips": len(samples),
                "wall_seconds": time.perf_counter() - started,
                "cpu_seconds": time.process_time() - cpu,
                **resources(),
            }

        reader, writer = await connect()
        payload = bytes(range(256)) * (args.bulk_bytes // 256)
        started, cpu = time.perf_counter(), time.process_time()

        # read concurrently with writes so both endpoint buffers stay bounded.
        async def send() -> None:
            for offset in range(0, len(payload), 65_536):
                writer.write(payload[offset : offset + 65_536])
                await writer.drain()

        sender = asyncio.create_task(send())
        received = await reader.readexactly(len(payload))
        await sender
        if received != payload:
            raise RuntimeError("bulk relay corrupted bytes")
        elapsed = time.perf_counter() - started
        report["bulk"] = {
            "bytes_one_way": len(payload),
            "wall_seconds": elapsed,
            "cpu_seconds": time.process_time() - cpu,
            "duplex_mib_per_second": 2 * len(payload) / elapsed / 2**20,
        }
        await close(writer)
        received = payload = b""

        # Pause the receiver while an echo is in flight, then verify every byte.
        reader, writer = await connect()
        slow_payload = b"s" * (4 * 2**20)
        writer.write(slow_payload)
        draining = asyncio.create_task(writer.drain())
        await asyncio.sleep(0.05)
        report["slow_reader"] = resources()
        if await reader.readexactly(len(slow_payload)) != slow_payload:
            raise RuntimeError("slow receiver corrupted bytes")
        await draining
        await close(writer)
        slow_payload = b""
        for _ in range(args.clients):
            _reader, writer = await connect()
            writer.write(b"abort" * 1024)
            writer.transport.abort()
            await close(writer)
        await wait_until(lambda: app.proxy.tasks.active_count == 0)
        report["abrupt_disconnects"] = {"count": args.clients, **resources()}

        started, cpu = time.perf_counter(), time.process_time()
        samples = []
        for _ in range(args.cycles):
            tick = time.perf_counter()
            reader, writer = await connect()
            await roundtrip(reader, writer, b"churn")
            samples.append(time.perf_counter() - tick)
            await close(writer)
        report["churn"] = {
            **percentiles(samples),
            "cycles": args.cycles,
            "wall_seconds": time.perf_counter() - started,
            "cpu_seconds": time.process_time() - cpu,
        }

        held = [await connect() for _ in range(args.clients)]
        await asyncio.gather(*(roundtrip(r, w, b"held") for r, w in held))
        report["held_connections"] = resources()
        # Stop accepting MAIN while existing TCP sessions continue transparently.
        started = time.perf_counter()
        main_port = port(main)
        main.close()
        await wait_until(lambda: app.router.snapshot().active_target is TargetName.FALLBACK)
        report["failover_detection_ms"] = (time.perf_counter() - started) * 1000
        await asyncio.gather(*(roundtrip(r, w, b"existing") for r, w in held))
        reader, writer = await connect()
        await roundtrip(reader, writer, b"fallback")
        await close(writer)
        started = time.perf_counter()
        main = await asyncio.start_server(accept, "127.0.0.1", main_port)
        all_servers.append(main)
        await wait_until(lambda: app.router.snapshot().active_target is TargetName.MAIN)
        report["recovery_detection_ms"] = (time.perf_counter() - started) * 1000
        fallback.close()
        await wait_until(lambda: app.fallback_health.healthy is False)
        reader, writer = await connect()
        await roundtrip(reader, writer, b"main-with-fallback-down")
        await close(writer)
        main.close()
        await wait_until(lambda: not app.router.snapshot().ready)
        reader, writer = await connect()
        if await reader.read() != b"":
            raise RuntimeError("both-down connection was not rejected")
        await close(writer)
        # Idle clients and abrupt disconnects exercise cleanup while sessions exist.
        for _ in range(args.clients):
            _reader, writer = await connect()
            writer.transport.abort()
            await close(writer)
        await asyncio.gather(*(close(w) for _, w in held))
        await wait_until(lambda: app.proxy.tasks.active_count == 0)
        report["after_churn"] = {
            **resources(),
            "limiter_active": app.limiter.active,
            "tracked_ips": app.limiter.tracked_active_ips,
            "baseline_fds": before["fds"],
        }
        # Reopen MAIN and force shutdown of active, idle sessions.
        main = await asyncio.start_server(accept, "127.0.0.1", main_port)
        all_servers.append(main)
        await wait_until(lambda: app.main_health.healthy is True)
        held = [await connect() for _ in range(args.clients)]
        await asyncio.gather(*(roundtrip(r, w, b"shutdown") for r, w in held))
        started = time.perf_counter()
        await app.shutdown()
        report["shutdown"] = {
            "wall_seconds": time.perf_counter() - started,
            "proxy_tasks": app.proxy.tasks.active_count,
            "background_tasks": app.background.active_count,
            "limiter_active": app.limiter.active,
            "active_sessions": app.runtime.active_connections,
        }
        report["eventloop_lag"] = percentiles(lag)
        report["runtime"] = {
            "incoming": app.runtime.incoming_connections_total,
            "established": app.runtime.backend_connections_established_total,
            "rejected": app.runtime.connections_rejected_total,
        }
    finally:
        stop_lag.set()
        if lag_task is not None:
            await lag_task
        await app.shutdown()
        await asyncio.gather(*(close_stream_writer(w) for w in writers))
        for server in all_servers:
            server.close()
        await asyncio.gather(*(close_stream_writer(w) for w in tuple(backend_writers)))
        if backend_tasks:
            await asyncio.gather(*backend_tasks)
        await asyncio.gather(*(server.wait_closed() for server in all_servers))
    report["final"] = resources()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.example.toml")
    parser.add_argument("--output", help="Optional JSON result path")
    parser.add_argument("--clients", type=int, default=32)
    parser.add_argument("--cycles", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=1000)
    parser.add_argument("--bulk-bytes", type=int, default=16 * 2**20)
    parser.add_argument("--idle-seconds", type=float, default=0.25)
    args = parser.parse_args()
    if not (
        1 <= args.clients <= 256
        and 1 <= args.cycles <= 10_000
        and 1 <= args.rounds <= 10_000
        and 256 <= args.bulk_bytes <= 64 * 2**20
        and 0.01 <= args.idle_seconds <= 10
    ):
        parser.error("parameters exceed local benchmark safety bounds")
    logging.basicConfig(level=logging.ERROR)
    result = asyncio.run(asyncio.wait_for(benchmark(args), timeout=60.0))
    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
