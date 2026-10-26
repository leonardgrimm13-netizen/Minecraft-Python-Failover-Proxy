# Abschlussbericht: Failover-Erweiterung

Stand: 26.10.2026. Ausgangsbasis und sämtliche vorhandenen Änderungen wurden erhalten.
PR #33 (`b33a802`) ist über Merge `e7009e0` integriert. Dieser Bericht beschreibt die
implementierte und lokal geprüfte Fassung; der Pull Request wird nicht automatisch gemergt.

## A. Implementierte Funktionen

1. **Intelligentes Failover:** Strukturierte Relay-Ergebnisse unterscheiden Endpunkt,
   Operation und Fehlertyp. Nachgewiesene Backend-Resets und Netzwerk-I/O-Fehler beeinflussen
   passive Fenster, Quarantäne und Circuits. EOF, Client-Abbrüche, Idle, Shutdown und
   Cancellation allein zählen nicht als Backend-Ausfall. Optionales frühes EOF löst eine
   zusammengefasste aktive Prüfung aus. Erfolgreiche TCP-Annahme löscht die getrennte
   Sitzungsfehler-Historie nicht. Defekte Sitzungen enden kontrolliert; andere bleiben bestehen.
2. **Multi-Backend:** Beliebig viele konfigurierte IDs, aufsteigende Prioritäten und stabile
   Deklarationsreihenfolge bei Gleichstand. Jedes Backend hat Healthcheck, Recovery-Hysterese,
   optionale Circuit-Einstellungen und Connect-Timeout. Distinkte Connect-Versuche sind durch
   Anzahl und Gesamtbudget begrenzt. Forced-Modi bleiben fail-closed.
3. **Minecraft-Status/MOTD:** Optionaler nativer Java-Status mit Handshake, JSON-Antwort und
   Ping/Pong. Online-, Wartungs-, Fallback- und Offline-Texte sind getrennt konfigurierbar.
   Normale Logins erhalten alle untersuchten Bytes unverändert. Unbekannte Spielerzahlen
   werden ausgelassen. Icons sind eingebettete, begrenzt validierte 64×64-PNGs.
4. **Atomarer Reload:** SIGHUP lädt und validiert die Datei, prüft Listener-/DNS-Grenzen,
   bereitet geänderte Ressourcen vor und aktiviert einen vollständigen Laufzeitstand ohne
   await im Aktivierungsabschnitt. Ungültige Kandidaten werden verworfen. Bestehende
   Spielerverbindungen behalten ihre Sockets und ihre bisherige Generation.

## B. Wichtige Dateien

- Neue `src/mc_failover/registry.py`, `runtime_config.py` und `local_status.py`.
- Erweiterte `app.py`, `proxy.py`, `relay.py`, `routing.py`, `health.py`,
  `circuit_breaker.py`, `config.py`, `models.py`, `endpoint_safety.py`, `cli.py`,
  `monitoring.py`, `metrics.py` und `runtime.py`.
- Neue Unit-/TCP-/Prozess-Regressionen unter `tests/unit` und `tests/integration`,
  darunter `test_endpoint_connections.py`, `test_connect_evidence.py`,
  `test_feature_review.py`, `test_hot_reload.py` und `test_reload_signal.py`.
- `config.multi.example.toml`, beide READMEs, systemd-Unit/Betriebsanleitung,
  Compose-Verzeichnismount und Packaging-/CI-Smoke-Checks.
- `tools/benchmark_features.py` sowie echte JSON-Messwerte unter `docs/feature-validation`.

## C. Architektur

`AppConfig.effective_backends` adaptiert das Legacy-Schema in dieselbe Registry wie
`[[backends]]`. Es gibt keine zweite Health-/Routing-Tabelle. Backend-Zustände sind dem
Eventloop zugeordnet; Circuits verwenden weiterhin geschützte, generationsbezogene Permits.
Health-Worker laufen unabhängig und teilen ein konfigurierbares Parallelitätslimit.

Der Verbindungspfad lautet: Annahme → Limits → vertrauenswürdiges PROXY v1/v2 → optionaler
begrenzter Status-Handshake → Routing → begrenzter Connect → unveränderter TCP-Relay →
strukturierte Fehlerbewertung → Cleanup. Ein Permit wird nur vom zugehörigen Backend
freigegeben. Nicht übergebene Writer werden auch bei Cancellation während der
Circuit-Bewertung geschlossen. DNS-Verbindungen behalten Raw-Socket-Ownership bis zum
Transport-Handoff und schließen jeden unterlegenen oder abgebrochenen Kandidaten.

Neue Verbindungen erfassen `RuntimeConfiguration`. Reload erzeugt neue Monitor-/Registry-
Wrapper und übernimmt nur passende Health-, Circuit- und Statistikzustände. Entfernte
Backends erhalten keine neuen Sessions; aktive alte Sessions dürfen auslaufen. Alte
Callbacks halten keine entfernten Registries fest. Die Legacy-Monitoring-Aliase werden
beim Entfernen eines reservierten Ziels ebenfalls ersetzt.

## D. Kompatibilität

Die ursprünglichen `[main]`, `[fallback]`, `[healthcheck]` und `[fallback_healthcheck]`
funktionieren ohne Migration. Das neue Backend-Array ist eine Alternative und darf nicht
mit diesen Abschnitten gemischt werden. `force_main` und `force_fallback` verwenden die
exakten IDs MAIN/FALLBACK; für andere IDs dient validiertes `force_target`.

Status ist standardmäßig deaktiviert. Der Legacy-Fallback-Connect-Schalter, PROXY v1/v2,
TCP-Half-Close, Graceful Shutdown, CLI und bestehende Metrikfamilien bleiben erhalten.
Alle 954 Tests bestehen unter Python 3.10.21 bis 3.14.7. Der bestehende 90-%-Coverage-
Mindestwert wurde nicht gesenkt.

## E. Sicherheit

Begrenzte VarInts, Handshake-/JSON-Größen, Deadlines, Status-Slots sowie vorhandene globale,
Quell-IP- und Rate-Limits begrenzen untrusted Input. Gelesene Login-Daten werden vollständig
wiedergegeben; Gameplay erhält keine zusätzliche Paketinspektion. PNG-Checks prüfen
Framing, CRC, Größe und begrenzte Dekomprimierung; komprimierte Metadaten werden verworfen.

DNS-/Listener-Schleifen werden auch bei Reload und Connect geprüft; wo erforderlich,
werden geprüfte numerische Ziele ohne zweite Namensauflösung verwendet. Konfigurationsdateien
sind auf 2 MiB begrenzt und müssen reguläre Dateien sein; FIFOs blockieren keinen Reload.
Fehlerdiagnosen geben Kategorien/Feldnamen statt Eingabewerten aus. Neue Prometheus-Labels
verwenden konfigurierte IDs und feste Enums. Es gibt keinen Remote-Reload-Endpunkt.
Lokale Ressourcenfehler wie EMFILE führen nicht zur passiven Quarantäne gesunder Backends.

## F. Performance

Die folgenden finalen Messungen stammen aus getrennten lokalen Prozessen mit Proxy,
Fake-Backends und Clients. Zehn Reloads liefen während Datenverkehr auf allen gehaltenen
Sessions; jeder Lauf erreichte Generation 11 und beendete alle Proxy-/Health-Ressourcen.
Für den kombinierten Harness wurde das Prozesslimit auf 32768 Deskriptoren gesetzt;
der erste Versuch mit dem geerbten Limit 4096 scheiterte bei 2000 Clients und wurde nicht
als erfolgreiche Messung gewertet.

| Backend address / clients | Connect p95 ms | Relay p95 ms | Status p95 ms | Reload + all-client RTT p95 ms | Peak sampled RSS KiB |
|---|---:|---:|---:|---:|---:|
| [IP / 100](feature-validation/final-clients-100.json) | 74.20 | 12.29 | 50.72 | 13.42 | 33928 |
| [IP / 500](feature-validation/final-clients-500.json) | 338.01 | 59.91 | 189.49 | 71.73 | 53180 |
| [IP / 2000](feature-validation/final-clients-2000.json) | 1609.68 | 277.75 | 1249.26 | 516.90 | 125060 |
| [DNS / 500](feature-validation/final-dns-clients-500.json) | 358.68 | 57.39 | 206.39 | 64.97 | 54664 |
| [DNS / 2000](feature-validation/final-dns-clients-2000.json) | 1642.18 | 259.91 | 916.09 | 314.52 | 127516 |

Alle fünf Läufe: Fehlerrate 0; nach Cleanup eine Harness-Task, sieben gemessene FDs und
zwei Eventloop-Sockets, bei null Proxy-/Background-Tasks, Sessions und Leases. Im IP-Lauf
mit 2000 Clients: CPU-Sekunden Connect 1,595, Relay 1,507, Status 1,448 und zehn Reloads
3,580. RSS vor/nach Reload 125060/117972 KiB; Failover/Recovery 5,59/52,81 ms bei 50-ms-
Health-Intervallen. Der gesonderte 16-MiB-Test maß 296.24 duplex MiB/s.
Das sind keine Produktionskapazitätszusagen. Frühere Messungen bleiben als Vergleichsbasis
erhalten; ein statistisch kontrollierter Vorher-/Nachher-Kapazitätsnachweis liegt nicht vor.

## G. Testergebnisse

- `pytest -q`: **954 bestanden**, keine fehlgeschlagenen oder deaktivierten Tests.
- Python-Matrix: jeweils **954 bestanden** unter 3.10.21, 3.11.16, 3.12.13 und 3.14.7;
  Hauptumgebung 3.13.15.
- Coverage mit Branches: **95,35 %**, Mindestwert weiterhin 90 %; Relay 100 %,
  lokale Statusverarbeitung 98,1 %.
- Ruff, Formatprüfung, `mypy src tests tools` und `compileall`: bestanden.
- Isolierter und vorhandener Build, Wheel/sdist, Release-Archivhygiene: bestanden.
- Installiertes Wheel außerhalb des Checkouts: Quellmodulvergleich, neun CLI-/Modul-/
  Kompatibilitätsprüfungen sowie echtes TCP, Offline-Status/Pong und Shutdown bestanden.
- Reale SIGHUP-/SIGTERM-Prozessprüfung, TCP-Resets, Half-Close, Cancellation, Backpressure,
  PROXY v1/v2, IPv6, fehlerhafte Pakete, Reload-Rollback und Backend-Entfernung abgedeckt.
- Unabhängige Review-Feststellungen wurden reproduziert und behoben; fokussierte Nachprüfung
  mit 35 Tests bestand.
- Dependency-Audits der 3.13-Entwicklungsumgebung und der 3.10-Umgebung einschließlich tomli:
  keine bekannten Schwachstellen gemeldet.
- systemd-Validierung mit angepasstem lokalem Programmpfad: bestanden. Docker ist lokal
  nicht installiert; Image-/Compose-/Containerprüfungen bleiben in GitHub Actions erhalten.

## H. Neue Konfiguration

Die vollständige folgende Konfiguration ist der Inhalt von
[`config.multi.example.toml`](../config.multi.example.toml). Kleinere Prioritäten gewinnen.
Die einfache Zwei-Server-Fassung bleibt in [`config.example.toml`](../config.example.toml).

```toml
# Multi-backend alternative to config.example.toml. Do not combine [[backends]]
# with legacy [main], [fallback], [healthcheck] or [fallback_healthcheck].
# Lower priority is preferred; equal priorities preserve declaration order.
[config]
strict_unknown_keys = true

[proxy]
listen_host = "0.0.0.0"
listen_port = 25565
backlog = 256

[connection]
timeout_seconds = 3.0
buffer_size = 65536
idle_timeout_seconds = 300.0
write_timeout_seconds = 30.0
relay_drain_timeout_seconds = 10.0
shutdown_grace_seconds = 30.0
shutdown_cancel_timeout_seconds = 5.0
max_connections = 4096
max_connections_per_ip = 0
new_connections_per_second = 0.0
new_connections_burst = 0
tcp_keepalive = false

[routing]
max_connect_attempts = 4
connect_budget_seconds = 8.0
max_parallel_healthchecks = 4

[circuit_breaker]
enabled = true
failure_threshold = 3
failure_window_seconds = 10.0
open_seconds = 15.0
half_open_max_attempts = 1

[passive_health]
enabled = true
failure_threshold = 3
failure_window_seconds = 30.0
quarantine_seconds = 10.0
early_session_seconds = 2.0

[maintenance]
mode = "auto"
# mode = "force_target" requires force_target = "FALLBACK_2".
force_main_file = ""
force_fallback_file = ""
file_check_interval_seconds = 1.0

[status]
enabled = true
always = true
maintenance = true
handshake_timeout_seconds = 2.0
session_timeout_seconds = 5.0
max_handshake_bytes = 2048
max_concurrent = 64
motd_online = "§aMinecraft network online"
motd_maintenance = "§eMaintenance\nPlease try again later."
motd_fallback = "§eMain server offline\nA fallback server is available."
motd_offline = "§cServer temporarily offline\nPlease try again later."
version_name = "Network status"
protocol_version = -1
dynamic_hint = true
# Player counts are omitted unless both truthful values are configured.
# players_online = 0
# players_max = 100
# favicon = "data:image/png;base64,..."

[reload]
enabled = true

[proxy_protocol]
accept = false
send = false
version = 1
trust_all_proxies = false
trusted_proxy_ips = []
header_timeout_seconds = 2.0
max_header_bytes = 4096

[monitoring]
enabled = true
listen_host = "127.0.0.1"
listen_port = 8080
allow_remote = false
bearer_token = ""
allow_unauthenticated_remote = false
expose_sensitive_state = false
max_connections = 128
request_timeout_seconds = 2.0
write_timeout_seconds = 2.0

[logging]
level = "INFO"
access_log = false

[[backends]]
id = "MAIN"
host = "127.0.0.1"
port = 25564
priority = 0
enabled = true
connect_timeout_seconds = 2.0
[backends.healthcheck]
mode = "minecraft_status"
interval_seconds = 3.0
timeout_seconds = 1.5
fail_after = 2
recover_after = 3
min_recovery_seconds = 5.0
protocol_version = 767
require_valid_json = true
reject_uninitialized_protocol = true
jitter_seconds = 0.2

[[backends]]
id = "FALLBACK_1"
host = "127.0.0.1"
port = 25566
priority = 100
enabled = true
connect_timeout_seconds = 2.0
[backends.healthcheck]
mode = "tcp"
interval_seconds = 4.0
timeout_seconds = 1.0
fail_after = 2
recover_after = 2
min_recovery_seconds = 3.0

[[backends]]
id = "FALLBACK_2"
host = "127.0.0.1"
port = 25567
priority = 200
enabled = true
connect_timeout_seconds = 2.0
[backends.healthcheck]
mode = "tcp"
interval_seconds = 5.0
timeout_seconds = 1.0
fail_after = 2
recover_after = 2
min_recovery_seconds = 3.0
[backends.circuit_breaker]
enabled = true
failure_threshold = 2
failure_window_seconds = 15.0
open_seconds = 20.0
half_open_max_attempts = 1

[[backends]]
id = "FALLBACK_3"
host = "127.0.0.1"
port = 25568
priority = 300
enabled = true
connect_timeout_seconds = 2.0
[backends.healthcheck]
mode = "tcp"
interval_seconds = 8.0
timeout_seconds = 1.0
fail_after = 2
recover_after = 2
min_recovery_seconds = 3.0
```

## I. Verbleibende Grenzen

- Keine Migration bereits etablierter Minecraft-Sessions zwischen Servern.
- Status unterstützt das moderne Java-Protokoll; Bedrock, alte Pings vor 1.7 und ein
  provisorischer Login-Erfolg/Disconnect-Dialog sind nicht implementiert.
- Keine automatische Dateiüberwachung; Reload erfolgt explizit über SIGHUP.
- Listener/Backlog, PROXY-Vertrauensgrenzen, Admission-/Rate-Limits und Monitoring-Bind/
  Aktivierung/Verbindungscap benötigen Neustart; solche Kandidaten werden vollständig verworfen.
- Spielerzahlen müssen als statische verlässliche Angaben konfiguriert werden oder fehlen.
- Lokale Messungen ersetzen weder einen Langzeit-Soak noch Tests gegen echte Spielserver.
- DNS-/NSS- und reguläre Dateizugriffe besitzen Betriebssystemgrenzen; Python kann einen
  bereits blockierten Resolver-Thread nicht gewaltsam terminieren.
- Lokale Docker-Ausführung war mangels Docker nicht möglich. CI-Ergebnisse stehen am PR.
