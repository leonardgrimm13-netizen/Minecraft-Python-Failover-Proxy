# systemd deployment

The supplied unit runs the installed `mc-failover` console script as a dedicated,
unprivileged user. It assumes the repository is the current directory.

## Install

Install the distro package that provides `python3 -m venv`, then run:

```bash
sudo useradd --system --home-dir /var/lib/mc-failover \
  --shell /usr/sbin/nologin mcfailover
sudo install -d -o root -g root -m 0755 /opt/mc-failover
sudo python3 -m venv /opt/mc-failover/.venv
sudo /opt/mc-failover/.venv/bin/python -m pip install .

sudo install -d -o root -g mcfailover -m 0750 /etc/mc-failover
sudo install -o root -g mcfailover -m 0640 \
  config.example.toml /etc/mc-failover/config.toml
sudo editor /etc/mc-failover/config.toml
sudo chown root:mcfailover /etc/mc-failover/config.toml
sudo chmod 0640 /etc/mc-failover/config.toml
sudo -u mcfailover /opt/mc-failover/.venv/bin/mc-failover \
  --config /etc/mc-failover/config.toml --check-config

sudo install -o root -g root -m 0644 \
  packaging/systemd/mc-failover.service /etc/systemd/system/mc-failover.service
sudo systemctl daemon-reload
sudo systemctl enable --now mc-failover
```

The configuration is `root:mcfailover` mode `0640` because it may contain a
monitoring bearer token. `StateDirectory=` and `RuntimeDirectory=` create
`/var/lib/mc-failover` and `/run/mc-failover` with the service ownership.

For file-based maintenance, configure paths such as:

```toml
[maintenance]
force_fallback_file = "/var/lib/mc-failover/force_fallback"
force_main_file = "/var/lib/mc-failover/force_main"
```

Then use `sudo touch`/`sudo rm` on those files. FALLBACK has priority when both exist.

## Operations

```bash
systemctl status mc-failover
journalctl -u mc-failover -f
sudo systemctl reload-or-restart mc-failover
```

There is no live configuration reload; `reload-or-restart` performs a graceful
restart. `TimeoutStopSec=60s` leaves cleanup margin beyond the example's 30-second
connection grace and 5-second cancellation timeout. Increase it if cleanup regularly
reaches these deadlines or whenever either configuration value is increased.

`LimitNOFILE=16384` supports the example limit of 4096 sessions (normally two
sockets per session plus listeners/resolver headroom). Raise both coherently for
larger deployments.

## Upgrade and verification

From the updated repository:

```bash
sudo /opt/mc-failover/.venv/bin/python -m pip install --upgrade .
sudo -u mcfailover /opt/mc-failover/.venv/bin/mc-failover \
  --config /etc/mc-failover/config.toml --check-config
sudo systemctl restart mc-failover

systemd-analyze verify /etc/systemd/system/mc-failover.service
systemd-analyze security mc-failover.service
```

The hardening allows `AF_UNIX` because AsyncIO uses local socket pairs, plus
`AF_INET`/`AF_INET6` for listeners, DNS and upstream connections. If a local
Python build or extension conflicts with `MemoryDenyWriteExecute=true`, diagnose
that build before relaxing the option.
