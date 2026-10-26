# Multi-backend, passive failover, local status and reload validation

This record accompanies the implementation and the matching English/German READMEs.
The complete runnable four-backend example is
[config.multi.example.toml](../config.multi.example.toml); the compatible legacy example is
[config.example.toml](../config.example.toml).

## Implemented architecture

`AppConfig.effective_backends` adapts the original MAIN/FALLBACK schema into the same
backend model used by explicit `[[backends]]`. These schemas cannot be mixed. Backend IDs
are unique bounded identifiers; priority is ascending and ties preserve declaration order.
`BackendRegistry` owns per-backend active health, connect/session statistics, passive
failure windows, quarantine and circuits. Independent workers share a semaphore limiting
network checks. Routing, monitoring and reload consume that registry rather than keeping
another backend-health table.

Incoming connections acquire the existing bounded admission lease, validate trusted
PROXY v1/v2 metadata, and apply the source policy. Opt-in local status inspects only a
bounded initial Minecraft Java handshake. All inspected bytes are preserved when a login
is forwarded. Backend connection attempts use distinct candidates within an absolute
budget; forced policy only permits its configured target. Once connected, the existing
opaque bidirectional relay and TCP half-close behavior continue.

Structured relay outcomes attach error type and direction to socket operations. Proven
backend resets/read/write failures can affect future routes; bare EOF, client resets,
idle timeout, shutdown and cancellation never establish backend failure. Optional early
EOF verification coalesces checks into existing backend workers. Quarantine, active
health recovery thresholds, circuit open durations and bounded half-open probes restrain
reconnection storms. An established Minecraft session is never migrated to another server.

`RuntimeConfiguration` records a generation and a complete prepared configuration,
registry, router, maintenance watcher, endpoint guard and status service. SIGHUP requests
coalesce in a single reload worker. Parsing and validation precede DNS/listener safety
checks, preparation of changed backends and a synchronous activation without `await`.
Old sessions retain their captured generation and sockets; removed backends stop receiving
new connections. Preparation errors preserve the old listener, worker and configuration
set. Retired generation workers remain owned until cancellation cleanup completes.

## Compatibility and operating boundaries

Legacy `[main]`, `[fallback]`, `[healthcheck]` and `[fallback_healthcheck]` remain supported.
MAIN retains its original circuit and the legacy FALLBACK circuit remains disabled.
`connect_fallback_on_main_connect_failure` keeps its legacy meaning. Forced MAIN/FALLBACK
and file overrides use the exact IDs `MAIN` and `FALLBACK`; named multi-backend targets
use validated `force_target`. No forced route silently substitutes a different target.

Local Java status defaults to disabled. When enabled, its response JSON omits unknown
player counts rather than inventing them; configured counts are administrator supplied.
The responder supports the modern Java handshake/status/ping/pong exchange, not Bedrock,
legacy pre-1.7 pings or Minecraft login termination. Forced maintenance MOTD takes
precedence whenever local handling applies. Otherwise all-down status uses the offline
MOTD; `always` enables online/fallback local MOTDs.

Reload supports backend lists, priorities, health/circuit/passive policy, maintenance,
status/MOTDs, logging, connection deadlines, and monitoring authentication/timeouts.
Proxy listener/backlog, PROXY trust/framing, admission/rate limits and monitoring
enabled/bind/port/cap changes require restart and reject the whole candidate. Existing
sessions keep their captured transport options. There is no remote reload endpoint or
automatic file watcher. A delivered SIGHUP is not an activation acknowledgment: inspect
the journal and `/state.configuration`.

Compose preserves the legacy single-file bind by default. Atomic file replacement
requires the documented opt-in directory bind: set `MC_FAILOVER_CONFIG_SOURCE=./config`
and `MC_FAILOVER_CONFIG_TARGET=/config`. Existing deployments need no migration unless
they choose atomic host-side replacements. systemd reload sends HUP;
file owner/group and mode must survive replacement. A rejected file with changed
monitoring credentials should be restored because the Docker liveness CLI reads disk.

## Security analysis

Network inputs remain bounded by global/source/rate limits. Local status adds a parallel
inspection cap, handshake/exchange deadlines, five-byte canonical VarInts, a bounded
handshake buffer and a 64 KiB response limit. MOTD strings are escaped JSON text rather
than arbitrary serialized components. Icons accept no path or URL and undergo strict
64×64 PNG framing/checksum and bounded decompression validation at configuration load.
Gameplay chunks receive no packet parsing or per-player polling.

Reload repeats validation and DNS/listener loop checks; player connects use validated
numeric destinations when loop checks require them, preventing another DNS lookup from
reintroducing a self-route. Trusted inbound PROXY policy is unchanged. Monitoring still
requires its configured authorization, and backend host/port only appear on the explicit
sensitive `/state` option. New metrics use configured backend IDs and bounded enums;
client addresses, player identities and exception strings are never labels.

Strong passive evidence is conservative but TCP alone cannot distinguish every network
failure from an application crash. A TCP healthcheck only proves acceptance, whereas
Minecraft status checks can additionally apply application filters. Operator-disabled
healthchecks remain an optimistic opt-out. Reliable local DNS/NSS and filesystem paths
are required because Python cannot terminate an already blocked operating-system worker
thread. No production capacity guarantee follows from a local single-process harness.

## Reproduction

```bash
ruff check .
ruff format --check .
mypy src tests tools
python -m compileall -q src tests tools mc_failover_proxy.py
pytest
pytest --cov=mc_failover --cov-branch --cov-report=term-missing
python -m build
python tools/check_release_archives.py dist
python tools/benchmark_proxy.py --output /tmp/proxy-benchmark.json
python tools/benchmark_features.py --clients 100 --rounds 5 --reloads 10 --output /tmp/features-100.json
python tools/benchmark_features.py --clients 500 --rounds 5 --reloads 10 --output /tmp/features-500.json
python tools/benchmark_features.py --clients 2000 --rounds 5 --reloads 10 --output /tmp/features-2000.json
```

The feature benchmark creates four local echo backends and a private configuration. It
validates login byte transparency, held relay sessions, concurrent local status/pong,
reload while sessions exchange bytes, two failed backends, recovery and cleanup. Timing
includes local clients and fake backends. Connect timing includes initial handshake echo;
reload timing includes one concurrent byte roundtrip on every held connection. A separate
loop-lag sampler records scheduling delay. CPU is process time; Linux `/proc` supplies
RSS, descriptors and sockets. Use fresh processes on the same idle machine and allow
approximately four descriptors per held client. RSS includes retained Python allocator
arenas and does not by itself establish a memory leak.

## Environment and available verification

Initial repository baseline: 715 passing tests and 96.43% branch coverage, before any
feature edits. The focused monitoring/metrics/runtime regression set has 75 passing
tests. Its new cases verify reload diagnostics/counters, five dynamic backend IDs,
legacy metric families, secret/address redaction and synchronous monitoring policy swap.
The existing monitoring integration tests exercise real local HTTP/TCP connections.
Both shipped configuration examples parse and validate.

Docker is not installed in this environment, so image/Compose execution cannot be
performed locally. The existing CI retains image build, Compose validation and container
liveness/SIGTERM smoke checks. `systemd-analyze verify` passes on a temporary copy with
only the deployment executable path replaced by the installed development CLI; the real
unit references `/opt/mc-failover/.venv/bin/mc-failover`, which is intentionally absent
on the development host. Network tests and systemd verification require an execution
context that permits local socket creation; the restricted shell sandbox denies even
AsyncIO's socketpair.

## Intermediate feature load results (before final review)

Measured locally with CPython 3.13.15 on Linux 7.0.0-38-generic x86_64, sequential fresh
processes, after the numeric socket cancellation fix. Each run used 64-byte roundtrips,
five relay rounds, ten reloads overlapping one roundtrip on every held session, and the
same number of concurrent local Java status/pong exchanges as held sessions. All three
runs used `-Werror::ResourceWarning`, completed without reported errors, reached
configuration generation 11 and preserved every held session's bytes across the reloads
and backend listener failures. Raw results:
[100 clients](feature-validation/clients-100.json),
[500 clients](feature-validation/clients-500.json),
[2000 clients](feature-validation/clients-2000.json).

| Held clients | Connect p95 ms | Relay RTT p95 ms | Status+pong p95 ms | Reload + all-client RTT p95 ms | Loop lag p95 ms | Peak sampled RSS KiB |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 63.55 | 9.48 | 44.56 | 11.48 | 10.68 | 33660 |
| 500 | 282.20 | 56.27 | 186.87 | 61.41 | 69.37 | 53244 |
| 2000 | 1415.53 | 258.38 | 1022.82 | 421.64 | 229.22 | 125684 |

The 2000-client run performed 10,000 measured relay roundtrips plus 20,000 across reloads
and 2000 complete status/pong exchanges. Its connect phase consumed 1.399 CPU seconds,
relay phase 1.365, status phase 1.189 and reload phase 3.056. Two-backend failure detection
was 5.62 ms and recovery 52.93 ms with deliberately short benchmark health intervals.
Those values depend on healthcheck phase and are not production detection guarantees.
The small-packet relay figure was 0.894 duplex MiB/s; this is a burst scheduling workload,
not a bulk-throughput capacity measurement.

Sampled RSS before/after ten reloads was 33652/33660 KiB at 100 clients,
53244/53068 KiB at 500, and 125684/119624 KiB at 2000. The measurements show no increasing
retained-resource count in those ten-reload runs; they do not establish an indefinite
soak-test memory bound. After shutdown, all runs reported zero proxy/background tasks,
zero active sessions and zero limiter leases. Once fake peers/listeners were closed,
all returned to one harness task, seven descriptors and the event loop's two local
sockets. Cleanup snapshots taken earlier can include callbacks still unwinding.

The benchmark exposed a CPython happy-eyeballs cancellation path retaining a winning raw
socket before transport ownership. Tracemalloc attributed allocations to
`asyncio.staggered` / `_connect_sock`; a deterministic real-socket regression was added.
Literal address connects now avoid the unnecessary happy-eyeballs race. The retained
socket count under 500 clients fell from five sockets after cleanup to the expected two,
without forced garbage collection in the measured harness.

There is no isolated same-command pre-feature capacity comparison in this record. These
measurements exercise the integrated implementation and are not evidence of unchanged
production throughput. Existing historical audit results remain separate.


## Final verification after cleanup corrections — 2026-10-26

The branch incorporates `origin/main` / PR #33 (`b33a802`) through merge `e7009e0`;
no pre-existing commit or working file was discarded. The final source passed 954 tests
on each of Python 3.10.21, 3.11.16, 3.12.13, 3.13.15 and 3.14.7. Branch-inclusive
coverage on 3.13 was **95.35%**, retaining the 90% minimum. `ruff check .`,
`ruff format --check .`, `mypy src tests tools`, and compileall passed. Both isolated
and existing-environment builds produced the wheel and source distribution; archive
hygiene passed. The isolated installed wheel matched every source module byte-for-byte,
passed nine console/module/compatibility/config checks outside the checkout, and passed
real TCP relay, offline status/pong and shutdown cleanup checks. Dependency audits of the
3.13 development environment and the 3.10 environment (including tomli) reported no known
vulnerabilities. These checks do not certify absence of undiscovered vulnerabilities.

Deterministic regressions cover cancellation at raw TCP completion, cancellation during
circuit success, lost permit ownership, stream construction failure, and DNS timeouts.
DNS connects now retain parent ownership of all raw descriptors and continue staggered
attempts across at most 32 interleaved IPv4/IPv6 candidates. Two blackholed addresses cannot
starve a later reachable one. Local `EMFILE`, `ENFILE`, `ENOMEM`, `ENOBUFS` and
`EADDRNOTAVAIL` errors stop retries without quarantining healthy backends. Real sequential
backend RSTs still open the circuit despite preceding successful TCP acceptance.

An application-level weak-reference regression confirms removing MAIN releases the old
monitor and backend once sessions finish. Candidate health callbacks do not mutate live
quarantine/circuit policy before activation. A real CLI subprocess test sends SIGHUP,
checks generation/forced routing, preserves a held stream through a rejected malformed
file, and exits cleanly on SIGTERM. PNG compressed metadata chunks iCCP/zTXt/iTXt are
rejected in addition to bounded image decompression. A read-only independent review's two
confirmed findings were reproduced and fixed; its focused re-review passed 35 tests and
reported no additional finding in those fixes.

Final load runs were sequential fresh processes with 64-byte data, five relay rounds,
ten reloads, all-client traffic overlapping activation, and 100/500/2000 local status
exchanges. The inherited soft descriptor limit of 4096 first caused the 2000-client harness
to hit EMFILE; that run failed and is not reported as a successful measurement. The successful
reruns inherited **32768** descriptors (hard limit 524288), changed only for the test process.
The harness now rejects an insufficient limit before opening clients. Reproduce with
`ulimit -n 32768` and the commands above; add `--backend-hostname` to exercise localhost
DNS resolution instead of numeric backends. Roughly six descriptors per held client are
needed while status clients are also connected. No forced GC is used for socket cleanup.

| Backend address / clients | Connect p95 ms | Relay p95 ms | Status p95 ms | Reload + all-client RTT p95 ms | Peak sampled RSS KiB |
|---|---:|---:|---:|---:|---:|
| [IP / 100](feature-validation/final-clients-100.json) | 74.20 | 12.29 | 50.72 | 13.42 | 33928 |
| [IP / 500](feature-validation/final-clients-500.json) | 338.01 | 59.91 | 189.49 | 71.73 | 53180 |
| [IP / 2000](feature-validation/final-clients-2000.json) | 1609.68 | 277.75 | 1249.26 | 516.90 | 125060 |
| [DNS / 500](feature-validation/final-dns-clients-500.json) | 358.68 | 57.39 | 206.39 | 64.97 | 54664 |
| [DNS / 2000](feature-validation/final-dns-clients-2000.json) | 1642.18 | 259.91 | 916.09 | 314.52 | 127516 |

All five final feature runs completed with **zero errors**, generation 11, zero proxy and
background tasks, zero sessions and zero limiter leases after shutdown. After closing fake
peers, each returned to one harness task, seven sampled descriptors and the event loop's
two sockets. The separate [16 MiB bulk run](feature-validation/final-bulk-100.json) measured
**296.24 duplex MiB/s**, including local clients and echo peers.
CPU, loop lag, failure/recovery times, descriptor/task snapshots and per-phase RSS are in
the linked raw JSON. The IP 2000 run used 1.595/1.507/1.448/3.580 CPU seconds for
connect/relay/status/ten reloads, respectively; before/after reload RSS was
125060/117972 KiB. Its two-listener failure and recovery detection were 5.59/52.81 ms
with 50 ms benchmark health intervals. These are local measurements, not an external-server
capacity guarantee or a statistically controlled pre-feature throughput comparison.

Docker is absent locally. Existing CI retains container build/Compose/liveness/shutdown
checks; no local Docker success is claimed. systemd validation passed with only the
installation-specific executable path substituted in a temporary unit copy.
