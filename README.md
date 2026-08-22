# Minecraft Python Failover Proxy

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/leonardgrimm13-netizen/Minecraft-Python-Failover-Proxy/actions/workflows/tests.yml/badge.svg)](https://github.com/leonardgrimm13-netizen/Minecraft-Python-Failover-Proxy/actions/workflows/tests.yml)

[Deutsch](README.de.md)

A dependency-light, AsyncIO-based TCP failover proxy for Minecraft on Linux. It routes each
**new TCP connection** to MAIN or FALLBACK using independent active health state, maintenance
policy, and passive MAIN connection failures.

The fundamental limit is intentional: existing player sessions remain attached to their chosen
backend. The proxy cannot live-migrate an already connected player after a backend fails.

## Architecture and routing

```text
Minecraft clients
       |
       v
mc-failover listener
       |---- MAIN      (active health + passive circuit breaker)
       `---- FALLBACK  (independent active health)

monitoring listener (optional): /live /ready /health /state /metrics
```

MAIN and FALLBACK checks start concurrently before the listeners are exposed. Each target owns
its own status, consecutive success/failure counts, totals, timestamps, and hysteresis.

In `maintenance.mode = "auto"`:

| MAIN | FALLBACK | Circuit | Result for a new connection |
|---|---|---|---|
| routable | any | closed | MAIN; healthy only when both targets are confirmed healthy, otherwise degraded |
| unavailable | routable | any | FALLBACK, degraded |
| routable | routable | open/half-open | FALLBACK, except a bounded half-open MAIN probe |
| unavailable | unavailable | any | no target; connection is closed and readiness is 503 |

`force_main` and `force_fallback` are fail-closed policies, not preferences. If the forced target
is unhealthy, or MAIN is blocked by its circuit, the proxy does **not** route to the other target:
the active target is `NONE`, the reason is `forced_main_unavailable` or
`forced_fallback_unavailable`, and `/ready` and `/health` return 503. `force_main` can only use a
controlled half-open probe after the circuit's open interval. A usable forced target is still
reported as degraded because the operator is overriding automatic policy.

Static maintenance mode has priority over files. In `auto`, the files are polled outside the
connection path; `force_fallback_file` wins if both files exist.

### Active healthchecks

- `tcp` opens and closes a TCP connection. It proves that a listener accepted a connection, not
  that a Minecraft server finished loading worlds, plugins, or databases.
- `minecraft_status` performs a bounded status handshake and validates the response packet. It
  can require valid JSON, version/MOTD filters, a latency ceiling, and a minimum `players.max`.
  JSON-dependent filters require `require_valid_json = true`.
- A response `version.protocol` is validated as a signed 32-bit integer. Paper may legitimately
  report `-1` while startup is incomplete; this remains structurally valid JSON, but the secure
  default `reject_uninitialized_protocol = true` marks the check unsuccessful with
  `status_server_not_initialized`. Set the option to `false` per target only when that startup
  state should count as ready. In `minecraft_status` mode the filter requires valid-JSON checking.
  For migration compatibility, an older configuration that explicitly has
  `require_valid_json = false` and omits this new option keeps the filter disabled; explicitly
  combining `reject_uninitialized_protocol = true` with disabled JSON validation is rejected.
- One absolute `timeout_seconds` deadline covers the complete check, including DNS, connect,
  write, and read. A stuck check does not stop the other target's task.
- `target_host`/`target_port` may check a backend behind a routing proxy while player traffic is
  still sent to the configured target.

Disabling a target's active check is an explicit optimistic opt-out. Its health becomes unknown
but it remains routable. Readiness can therefore be true while the overall state is degraded; a
real connect can still fail. MAIN remains ready when FALLBACK alone is unhealthy or unverified,
but loss of that failover capacity makes the overall status degraded. Keep both checks enabled
when readiness must prove current reachability.

### Passive failures and circuit breaker

Failed real MAIN connects enter a monotonic sliding window. At `failure_threshold`, the circuit
opens for `open_seconds`; new automatic connections bypass the MAIN timeout and use routable
FALLBACK. After the interval, at most `half_open_max_attempts` concurrent probes may try MAIN.
A successful real connection closes and resets the circuit; a failed probe reopens it. A
successful active MAIN check may close it only after the full open interval has elapsed.

FALLBACK connect failures have separate metrics but do not recursively trigger another route, so
there is no MAIN/FALLBACK retry loop. With
`connect_fallback_on_main_connect_failure = true`, a failed MAIN connect may make one immediate
FALLBACK attempt only in `auto` and only while FALLBACK is routable.

## TCP relay and shutdown

The relay is raw TCP; it does not terminate TLS or rewrite Minecraft login packets. EOF is
propagated with `write_eof()` where the platform supports it. After a clean half-close, the other
direction may continue for `relay_drain_timeout_seconds`, allowing a final response without
leaving an unbounded task. Reads share a session-wide idle deadline, writes have bounded `drain()`
deadlines, and all relay tasks are retrieved on normal completion, error, timeout, or cancellation.

SIGINT and SIGTERM trigger graceful shutdown:

1. mark the process unready and close Minecraft and monitoring listeners;
2. stop and collect health, maintenance, and monitoring tasks;
3. let existing player sessions finish for `shutdown_grace_seconds`;
4. cancel remaining sessions, bound writer cleanup by `shutdown_cancel_timeout_seconds`, and
   collect every task.

A normal signal-driven shutdown exits with status 0. Configure the service manager's stop timeout
above the sum of grace, cancellation, and cleanup margin. The supplied systemd and Compose files
use 60 seconds for the example defaults of 30 + 5 seconds, leaving additional cleanup margin.

Durations, intervals, age, recovery, idle deadlines, and circuit windows use a monotonic clock.
Externally visible start/check/change/open timestamps use timezone-aware UTC and ISO-8601 `Z`;
Prometheus timestamp gauges use Unix seconds. Wall-clock jumps do not change timeout durations,
and displayed ages/uptime are never negative.

## Requirements and installation

- Linux is the primary target.
- CPython 3.10 or newer (CI is configured for 3.10 through 3.14).
- MAIN and FALLBACK reachable from the proxy host.

Install from a checkout:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
cp config.example.toml config.toml
mc-failover --config config.toml --check-config
mc-failover --config config.toml
```

`config.toml` is intentionally ignored by Git. Keep deployment configuration and any monitoring
token outside source control.

### Migration from the single-file release

- The implementation now lives in `src/mc_failover`; the installed command is `mc-failover`.
- `python mc_failover_proxy.py --config config.toml` remains a compatibility entrypoint and calls
  the package CLI.
- Existing core sections remain valid. New sections have documented defaults, and the independent
  FALLBACK check defaults to enabled TCP using FALLBACK plus the MAIN timing/hysteresis values.
- Unknown keys now fail by default (`[config].strict_unknown_keys = true`). This catches typos;
  fix the key instead of disabling strict mode unless a staged migration requires it.
- The former unsafe combination `proxy_protocol.accept = true` with an empty trusted list now
  fails configuration validation. Add trusted IP/CIDR entries or the explicit dangerous
  `trust_all_proxies = true` opt-in.
- Monitoring readiness is no longer unconditionally positive: `/ready` and `/health` return 503
  when routing has no usable target.

Always run `--check-config` before restarting an upgraded service.

## Configuration and CLI

[config.example.toml](config.example.toml) is the complete commented reference and contains every
current section:

- strict parsing, listener/backlog, MAIN and FALLBACK;
- both healthchecks and Minecraft status filters;
- connect/write/idle/drain/shutdown deadlines, global/per-IP limits, and token-bucket rate limit;
- maintenance mode and asynchronously polled override files;
- circuit breaker;
- PROXY Protocol trust, versions, deadline, and size bound;
- protected monitoring and access logging.

Numeric values are bounded and booleans are never accepted as integers. Targets may not point
back to overlapping proxy/monitoring listeners. In addition to textual validation, same-port DNS
targets are resolved under the absolute connect deadline and compared with the sockets that were
actually bound. DNS failure fails closed for this check; wildcard listeners also reject every
address the kernel identifies as local. The validated numeric addresses (including IPv6 scope
IDs) are used for the connection, preventing a second DNS lookup from reintroducing the loop. The
two rate-limit values must be both zero or both positive; zero disables rate/per-IP/idle features
where the example says so.

Offline CLI operations do not start a listener:

```bash
mc-failover --version
mc-failover --config config.toml --check-config
mc-failover --config config.toml --print-effective-config
mc-failover --config config.toml --test-main
mc-failover --config config.toml --test-fallback
mc-failover --config config.toml --test-healthcheck
mc-failover --config config.toml --test-fallback-healthcheck
mc-failover --config config.toml --probe-live
```

`--test-main` and `--test-fallback` are plain TCP tests. The two healthcheck commands use the
configured mode and filters. `--probe-live` checks the already-running monitoring `/live`
endpoint, supplies the configured bearer token internally when needed, and never prints it.
It is a liveness check, not a backend-readiness check. Effective configuration output redacts the
bearer token.

## Monitoring and Prometheus

Monitoring defaults to disabled when the section is absent, preserving older configurations.
The shipped example enables it only on `127.0.0.1:8080` so the container can probe its running
event loop without publishing the port. A non-loopback bind is rejected unless
`allow_remote = true`; the policy is checked again against every actually bound socket before
serving requests. Remote monitoring additionally requires a bearer token, unless the operator
sets the prominently unsafe `allow_unauthenticated_remote = true` opt-in. The token is compared in
constant time, protects every endpoint including `/live`, and is never printed by
`--print-effective-config` or `--probe-live`. It is also redacted from validation diagnostics and
is never logged.

```bash
curl http://127.0.0.1:8080/live
curl -H 'Authorization: Bearer YOUR_TOKEN' http://127.0.0.1:8080/ready
```

| Path | Status and purpose |
|---|---|
| `/live` | 200 when the monitoring handler/event loop responds; backend health is irrelevant |
| `/ready` | concise routing state; 200 only with a usable route, otherwise 503 |
| `/health` | full health/routing/circuit/uptime state; 200 or 503 using the same readiness rule |
| `/state` | diagnostic state, always 200; configured target hosts/ports appear only when `expose_sensitive_state = true` |
| `/metrics` | Prometheus text format, always 200 while monitoring responds |

`/ready` reports `healthy`, `degraded`, or `unavailable` plus the active target and both health
values (`true`, `false`, or `null` when unknown/disabled). `/health` adds UTC start/change/check
times, monotonic uptime/check ages, connection and rejection counts, maintenance source, routing
reason, both complete latest check results, and the circuit state/open time/retry delay.

`/state` still exposes operational health, routing reasons, maintenance mode, counters, and
timestamps even when target addresses are hidden. Keep it local or authenticated. The HTTP server
accepts GET only, closes every response, bounds request line/header counts/sizes, uses one absolute
request deadline, and limits concurrent monitoring clients independently.

Connection lifecycle counters have distinct meanings. `incoming_connections_total` counts every
TCP accept on the Minecraft listener. `backend_connections_established_total` counts clients for
which a backend was successfully prepared for relay, while `active_connections` is the current
subset with such a backend. `connections_rejected_total` counts explicit protocol, admission,
routing, and terminal backend-connect rejections, and never counts a MAIN failure that
successfully falls back. Unexpected internal handler failures are logged rather than assigned an
unbounded or misleading rejection reason. The JSON fields
`total_connections` (global-limiter admissions) and `rejected_connections` remain as deprecated
compatibility fields.

Metrics include `# HELP` and `# TYPE`, bounded label values, and no free-form error labels:

```text
mc_failover_up
mc_failover_uptime_seconds
mc_failover_shutting_down
mc_failover_active_connections
mc_failover_incoming_connections_total
mc_failover_backend_connections_established_total
mc_failover_connections_rejected_total
mc_failover_connections_total
mc_failover_rejected_connections_total
mc_failover_monitoring_rejected_connections_total
mc_failover_main_connect_failures_total
mc_failover_fallback_connect_failures_total
mc_failover_main_connect_successes_total
mc_failover_fallback_connect_successes_total
mc_failover_target_health_status
mc_failover_healthcheck_successes_total
mc_failover_healthcheck_failures_total
mc_failover_healthcheck_latency_milliseconds
mc_failover_healthcheck_age_seconds
mc_failover_healthcheck_timestamp_seconds
mc_failover_circuit_breaker_state
mc_failover_circuit_breaker_open_total
mc_failover_circuit_breaker_retry_after_seconds
mc_failover_active_target
mc_failover_routing_reason_info
```

`mc_failover_connections_total` is retained temporarily as a deprecated counter of connections
granted a global limiter lease; such a connection can still fail protocol validation, deferred
admission, routing, or backend connection. `mc_failover_rejected_connections_total` is a deprecated
compatibility family for the same client-rejection values; it retains the historical
`reason="monitoring_limit"` zero-valued series. Monitoring saturation is counted only by
`mc_failover_monitoring_rejected_connections_total`.

## PROXY Protocol security

Inbound and outbound PROXY Protocol v1 and v2 are independent. `version` is the shared default;
`accept_version` and `send_version` optionally override one direction. Acceptance expects the
configured version and a complete valid header within `header_timeout_seconds`; malformed,
oversized, incomplete, wrong-family/length/port headers and untrusted peers are closed cleanly.
The v2 parser bounds every payload and validates TLV framing for PROXY/UNSPEC records; LOCAL
payload is ignored according to the protocol semantics. LOCAL/UNKNOWN never supplies an asserted
client IP. Parsed inbound TLVs are not copied into a newly generated outbound header.

`accept = true` is safe only when the direct TCP peer is a trusted proxy. At least one valid IPv4,
IPv6, or CIDR entry in `trusted_proxy_ips` is mandatory. An empty list fails closed.

`trust_all_proxies = true` is an explicit dangerous escape hatch. It permits any direct client to
forge an arbitrary source address, emits a CRITICAL warning, cannot be combined with a trusted
list, and must never be used on a public listener. Firewalling the listener to trusted proxy peers
is still recommended. Enable `send` only when every selected backend expects the configured
PROXY version.

## Abuse protection and logging

The Minecraft listener has a bounded backlog, global session limit, optional per-source-IP limit,
and optional token-bucket rate limit. With inbound PROXY Protocol, the global slot is acquired
before parsing. Per-IP/rate limits for a trusted proxy are deferred until its valid header is
available and then use the asserted client address (or the direct peer for UNKNOWN); an untrusted
peer is accounted by its direct address and rejected. A trusted edge proxy must therefore sanitize
client-supplied headers. Monitoring has its own connection cap. Rejection reasons use a bounded
enum in metrics.

Expected disconnects and network resets are not logged as internal errors. Unexpected failures
retain tracebacks. External log values are sanitized and bounded; `logging.access_log` is optional
because connection-level logs can be high volume.

## Docker

The image is built as a wheel in a builder stage and copied into a Python slim runtime. It runs as
UID/GID 10001, has an exec-form entrypoint and SIGTERM stop signal, exposes only the Minecraft
port, and uses `--probe-live` as its built-in healthcheck. The probe connects to the configured
monitoring listener inside the container and requires `/live` to answer, so a stopped or hung
event loop becomes unhealthy. The example enables loopback-only monitoring; it is not published
by Compose. Custom container configurations must also enable monitoring for the image healthcheck.
If monitoring has a bearer token, the probe reads it from the mounted configuration and sends it
internally without printing it. Restart the container immediately after changing the mounted
configuration, because the running process retains its startup configuration while each probe
reads the current file. Very long initial backend-check timeouts may require a corresponding
orchestrator health start grace.

```bash
cp config.example.toml config.toml
# If the file contains a token, restrict it while retaining read access for container GID 10001.
sudo chown root:10001 config.toml
sudo chmod 0640 config.toml
docker compose config
docker compose up -d --build
docker compose logs -f mc-failover
```

Compose enables an init process, read-only root filesystem, all-capability drop,
`no-new-privileges`, bounded `/tmp`, PID limit, and a 60-second stop grace. Only `25565/tcp` is
published. To publish monitoring only on host loopback, uncomment its mapping and set an
in-container non-loopback bind (`0.0.0.0`), `allow_remote = true`, and a bearer token.

The required volume is the read-only `/config/config.toml` bind. For file-based maintenance,
create a host `state/` directory, uncomment the read-only `/state` bind, and configure
`/state/force_fallback` and `/state/force_main`; the host operator creates or removes these flags.
No persistent application-written volume is required. `/tmp` is an ephemeral bounded tmpfs.

Container loopback is not host loopback. If backends run directly on a Linux Docker host, enable
the commented `host.docker.internal:host-gateway` mapping and use that hostname for targets.

## systemd

Use [the hardened unit](packaging/systemd/mc-failover.service) and
[installation guide](packaging/systemd/README.md). It runs the venv console script as
`mcfailover`, validates configuration with `ExecStartPre`, creates runtime/state directories,
allows only Unix/IPv4/IPv6 socket families, sets `LimitNOFILE=16384`, and uses a 60-second stop
timeout. Configuration is root-controlled and group-readable (`0640`) because it may contain a
token.

## Development and CI

```bash
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
python -m compileall -q src tests mc_failover_proxy.py
pytest --cov=mc_failover --cov-branch --cov-report=term-missing
python -m build
```

The release version is maintained only in `project.version` in `pyproject.toml`. Installed
executions read package metadata; an uninstalled source checkout reads that same TOML value and
falls back explicitly to `0+unknown` if it is unavailable.

Coverage configuration enables branch coverage and a 90% repository threshold. GitHub Actions is
configured for Ruff, mypy, coverage, sdist/wheel build, isolated wheel/CLI smoke checks,
dependency audit, Python 3.10-3.14 compatibility, Docker build, and Compose validation. Separate
CodeQL and Dependabot configurations are included. The container job also verifies that the image
becomes healthy, becomes unhealthy while the proxy process is suspended, recovers afterward, and
exits cleanly on SIGTERM. These statements describe configured checks, not results from a
particular machine.

## Scope and security limits

- This is a TCP failover router, not a replacement for Velocity/BungeeCord and not a live-session
  migration system.
- It does not encrypt traffic or authenticate Minecraft clients; use network controls appropriate
  to the deployment.
- Only publish the Minecraft listener. Keep monitoring local or authenticated.
- Do not run as root; port 25565 does not require it on Linux.
- When both targets are unavailable, new clients are closed rather than sent to a known-bad target.
- Python can time out DNS/NSS and maintenance-file awaits, but it cannot forcibly terminate an
  already blocked operating-system resolver or network-filesystem worker thread. Use reliable
  local NSS/resolver configuration and local maintenance paths; the supplied service-manager stop
  deadline remains the final operational bound.

Licensed under the [MIT License](LICENSE).
