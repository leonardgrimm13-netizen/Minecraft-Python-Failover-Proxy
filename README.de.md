# Minecraft Python Failover Proxy

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Lizenz: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/leonardgrimm13-netizen/Minecraft-Python-Failover-Proxy/actions/workflows/tests.yml/badge.svg)](https://github.com/leonardgrimm13-netizen/Minecraft-Python-Failover-Proxy/actions/workflows/tests.yml)

[English](README.md)

Ein schlanker, AsyncIO-basierter TCP-Failover-Proxy für Minecraft unter Linux. Er leitet jede
**neue TCP-Verbindung** anhand unabhängiger aktiver Health-Zustände, der Wartungsrichtlinie und
passiver MAIN-Verbindungsfehler an MAIN oder FALLBACK weiter.

Die grundlegende Grenze ist beabsichtigt: Bestehende Spielersitzungen bleiben mit dem beim
Verbindungsaufbau gewählten Backend verbunden. Der Proxy kann einen bereits verbundenen Spieler
nach einem Backend-Ausfall nicht live migrieren.

## Architektur und Routing

```text
Minecraft-Clients
       |
       v
mc-failover Listener
       |---- MAIN      (aktiver Healthcheck + passiver Circuit Breaker)
       `---- FALLBACK  (unabhängiger aktiver Healthcheck)

Monitoring-Listener (optional): /live /ready /health /state /metrics
```

MAIN- und FALLBACK-Prüfung starten parallel, bevor die Listener freigegeben werden. Jedes Ziel
besitzt einen eigenen Status, aufeinanderfolgende Erfolgs-/Fehlerzähler, Gesamtzähler,
Zeitstempel und Hysterese.

Bei `maintenance.mode = "auto"` gilt:

| MAIN | FALLBACK | Circuit | Ergebnis für eine neue Verbindung |
|---|---|---|---|
| routable | beliebig | closed | MAIN; Gesamtstatus nur bei beiden bestätigten healthy Zielen healthy, sonst degraded |
| nicht verfügbar | routable | beliebig | FALLBACK, degraded |
| routable | routable | open/half-open | FALLBACK, außer einem begrenzten Half-Open-MAIN-Test |
| nicht verfügbar | nicht verfügbar | beliebig | kein Ziel; Verbindung wird geschlossen und Readiness ist 503 |

`force_main` und `force_fallback` sind Fail-Closed-Richtlinien, keine Präferenzen. Ist das
erzwungene Ziel unhealthy oder wird MAIN von seinem Circuit blockiert, leitet der Proxy
**nicht** zum anderen Ziel: Das aktive Ziel ist `NONE`, der Grund lautet
`forced_main_unavailable` oder `forced_fallback_unavailable`, und `/ready` sowie `/health`
antworten mit 503. `force_main` kann nach Ablauf der Open-Zeit nur einen kontrollierten
Half-Open-Test verwenden. Auch ein verwendbares erzwungenes Ziel wird als degraded gemeldet, weil
der Operator die automatische Richtlinie überschreibt.

Ein statischer Wartungsmodus hat Vorrang vor Dateien. In `auto` werden die Dateien außerhalb des
Verbindungs-Hot-Paths abgefragt; `force_fallback_file` gewinnt, wenn beide Dateien existieren.

### Aktive Healthchecks

- `tcp` öffnet und schließt eine TCP-Verbindung. Das beweist, dass ein Listener eine Verbindung
  angenommen hat, nicht dass ein Minecraft-Server Welten, Plugins oder Datenbanken vollständig
  geladen hat.
- `minecraft_status` führt einen begrenzten Status-Handshake aus und validiert das Antwortpaket.
  Optional werden gültiges JSON, Versions-/MOTD-Filter, eine Latenzobergrenze und ein minimales
  `players.max` verlangt. JSON-abhängige Filter benötigen `require_valid_json = true`.
- `version.protocol` aus der Antwort wird als vorzeichenbehafteter 32-Bit-Integer validiert. Paper
  kann während des Starts legitim `-1` melden; das JSON bleibt strukturell gültig, aber der sichere
  Default `reject_uninitialized_protocol = true` lässt den Check mit
  `status_server_not_initialized` fehlschlagen. Setze die Option je Ziel nur dann auf `false`, wenn
  dieser Startzustand bereits als ready gelten soll. Im Modus `minecraft_status` benötigt der
  Filter die Validierung des JSON. Zur rückwärtskompatiblen Migration bleibt der Filter
  deaktiviert, wenn eine ältere Konfiguration ausdrücklich `require_valid_json = false` setzt und
  die neue Option weglässt; eine explizite Kombination aus
  `reject_uninitialized_protocol = true` und deaktivierter JSON-Validierung wird abgelehnt.
- Eine absolute `timeout_seconds`-Deadline umfasst die gesamte Prüfung einschließlich DNS,
  Connect, Schreiben und Lesen. Eine hängende Prüfung stoppt die Task des anderen Ziels nicht.
- Mit `target_host`/`target_port` kann ein Backend hinter einem Routing-Proxy geprüft werden,
  während Spielertraffic weiterhin zum konfigurierten Routing-Ziel fließt.

Das Deaktivieren eines aktiven Checks ist ein ausdrücklich optimistisches Opt-out. Der Zustand
wird unbekannt, das Ziel bleibt jedoch routable. Readiness kann deshalb true sein, während der
Gesamtstatus degraded lautet; ein realer Connect kann trotzdem fehlschlagen. Ist allein FALLBACK
unhealthy oder ungeprüft, bleibt MAIN ready, der Verlust der Failover-Kapazität macht den
Gesamtstatus jedoch degraded. Lasse beide Checks aktiviert, wenn Readiness aktuelle
Erreichbarkeit nachweisen soll.

### Passive Fehler und Circuit Breaker

Fehlgeschlagene reale MAIN-Connects werden in einem monotonen gleitenden Zeitfenster gezählt. Bei
`failure_threshold` öffnet der Circuit für `open_seconds`; neue automatische Verbindungen umgehen
den MAIN-Timeout und verwenden den routable FALLBACK. Danach dürfen höchstens
`half_open_max_attempts` Tests gleichzeitig MAIN versuchen. Ein erfolgreicher echter Connect
schließt und leert den Circuit, ein fehlgeschlagener Test öffnet ihn erneut. Ein erfolgreicher
aktiver MAIN-Check darf ihn erst nach Ablauf der vollständigen Open-Zeit schließen.

FALLBACK-Connect-Fehler haben eigene Metriken, lösen aber keine rekursive weitere Route aus; es
entsteht keine MAIN/FALLBACK-Endlosschleife. Mit
`connect_fallback_on_main_connect_failure = true` darf ein fehlgeschlagener MAIN-Connect genau
einen unmittelbaren FALLBACK-Versuch durchführen, nur in `auto` und nur bei routable FALLBACK.

## TCP-Relay und Herunterfahren

Das Relay transportiert reines TCP; es terminiert kein TLS und schreibt keine Minecraft-
Loginpakete um. EOF wird, soweit von der Plattform unterstützt, mit `write_eof()` weitergegeben.
Nach einem sauberen Half-Close darf die Gegenrichtung noch
`relay_drain_timeout_seconds` weiterlaufen. So wird eine letzte Antwort nicht abgeschnitten, ohne
eine unbegrenzte Task zu hinterlassen. Reads teilen eine sitzungsweite Idle-Deadline, Writes haben
begrenzte `drain()`-Deadlines, und alle Relay-Tasks werden bei Erfolg, Fehler, Timeout oder
Cancellation eingesammelt.

SIGINT und SIGTERM starten Graceful Shutdown:

1. Prozess als nicht ready markieren und Minecraft-/Monitoring-Listener schließen;
2. Health-, Wartungs- und Monitoring-Tasks stoppen und einsammeln;
3. bestehende Spielersitzungen bis `shutdown_grace_seconds` weiterlaufen lassen;
4. verbleibende Sitzungen abbrechen, Writer-Cleanup mit
   `shutdown_cancel_timeout_seconds` begrenzen und jede Task einsammeln.

Ein regulärer signalgesteuerter Shutdown endet mit Status 0. Der Stop-Timeout des Service-Managers
muss oberhalb der Summe aus Grace, Cancellation und Cleanup-Reserve liegen. Die mitgelieferten
systemd-/Compose-Dateien verwenden für die Beispielwerte 30 + 5 Sekunden insgesamt 60 Sekunden
und lassen damit zusätzliche Cleanup-Reserve.

Dauern, Intervalle, Alter, Recovery, Idle-Deadlines und Circuit-Fenster verwenden eine monotone
Uhr. Extern sichtbare Start-/Check-/Wechsel-/Open-Zeitstempel sind UTC mit Zeitzone und
ISO-8601-`Z`; Prometheus-Zeitstempel-Gauges verwenden Unix-Sekunden. Sprünge der Systemuhr ändern
keine Timeout-Dauern, und angezeigte Alter/Uptime werden nie negativ.

## Voraussetzungen und Installation

- Linux ist die primäre Zielplattform.
- CPython 3.10 oder neuer (CI ist für 3.10 bis 3.14 konfiguriert).
- MAIN und FALLBACK sind vom Proxy-Host erreichbar.

Installation aus einem Checkout:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
cp config.example.toml config.toml
mc-failover --config config.toml --check-config
mc-failover --config config.toml
```

`config.toml` wird absichtlich von Git ignoriert. Deployment-Konfiguration und ein eventuelles
Monitoring-Token gehören nicht in die Versionsverwaltung.

### Migration von der Einzeldatei-Version

- Die Implementierung liegt jetzt in `src/mc_failover`; der installierte Befehl heißt
  `mc-failover`.
- `python mc_failover_proxy.py --config config.toml` bleibt als Kompatibilitäts-Entrypoint erhalten
  und ruft die Paket-CLI auf.
- Vorhandene Kernsektionen bleiben gültig. Neue Sektionen besitzen dokumentierte Defaults; der
  unabhängige FALLBACK-Check ist standardmäßig als TCP-Check auf FALLBACK aktiv und übernimmt
  Timing/Hysterese von MAIN.
- Unbekannte Schlüssel sind standardmäßig Fehler (`[config].strict_unknown_keys = true`). Das
  findet Tippfehler; korrigiere den Schlüssel, statt Strict Mode außer für eine gestufte Migration
  zu deaktivieren.
- Die früher unsichere Kombination `proxy_protocol.accept = true` mit leerer Vertrauensliste
  schlägt jetzt bei der Validierung fehl. Ergänze vertrauenswürdige IP/CIDR-Einträge oder das
  ausdrücklich gefährliche Opt-in `trust_all_proxies = true`.
- Monitoring-Readiness ist nicht mehr bedingungslos positiv: `/ready` und `/health` liefern 503,
  wenn keine verwendbare Route existiert.

Führe vor dem Neustart eines aktualisierten Dienstes immer `--check-config` aus.

## Konfiguration und CLI

[config.example.toml](config.example.toml) ist die vollständige kommentierte Referenz mit allen
aktuellen Bereichen:

- striktes Parsing, Listener/Backlog, MAIN und FALLBACK;
- beide Healthchecks und Minecraft-Statusfilter;
- Connect-/Write-/Idle-/Drain-/Shutdown-Deadlines, globale/Per-IP-Limits und Token-Bucket-Rate-Limit;
- Wartungsmodus und asynchron abgefragte Override-Dateien;
- Circuit Breaker;
- PROXY-Protocol-Vertrauen, Versionen, Deadline und Größenlimit;
- geschütztes Monitoring und Access-Logging.

Zahlen besitzen Grenzen; Booleans werden nie als Integer akzeptiert. Ziele dürfen nicht auf
überlappende Proxy-/Monitoring-Listener zurückzeigen. Zusätzlich zur textuellen Prüfung werden
DNS-Ziele mit gleichem Port innerhalb der absoluten Connect-Deadline aufgelöst und mit den
tatsächlich gebundenen Sockets verglichen. Ein DNS-Fehler schlägt dabei Fail-Closed fehl;
Wildcard-Listener sperren außerdem jede vom Kernel als lokal erkannte Adresse. Für den Connect
werden die bereits geprüften numerischen Adressen einschließlich IPv6-Scope-ID verwendet, sodass
keine zweite DNS-Auflösung die Schleife erneut einführen kann. Die beiden Rate-Limit-Werte müssen
entweder beide null oder beide positiv sein; null deaktiviert Rate-/Per-IP-/Idle-Funktionen an den
im Beispiel bezeichneten Stellen.

Offline-CLI-Operationen starten keinen Listener:

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

`--test-main` und `--test-fallback` sind reine TCP-Tests. Die beiden Healthcheck-Befehle verwenden
den konfigurierten Modus samt Filtern. `--probe-live` prüft den `/live`-Endpoint des bereits
laufenden Monitoring-Listeners, sendet ein konfiguriertes Bearer-Token bei Bedarf intern und gibt
es nie aus. Das ist eine Liveness-, keine Backend-Readiness-Prüfung. Die Ausgabe der effektiven
Konfiguration schwärzt das Bearer-Token.

## Monitoring und Prometheus

Fehlt die Sektion, bleibt Monitoring zur Kompatibilität mit älteren Konfigurationen deaktiviert.
Die mitgelieferte Beispielkonfiguration aktiviert es ausschließlich auf `127.0.0.1:8080`, damit
der Container seinen laufenden Eventloop prüfen kann, ohne den Port zu veröffentlichen. Ein
nichtlokaler Bind wird ohne `allow_remote = true` abgelehnt; diese Regel wird vor Annahme von
Requests nochmals gegen jeden tatsächlich gebundenen Socket geprüft. Remote-Monitoring benötigt
zusätzlich ein Bearer-Token, außer der Operator setzt das deutlich unsichere Opt-in
`allow_unauthenticated_remote = true`. Das Token wird zeitkonstant verglichen, schützt jeden
Endpoint einschließlich `/live`, wird in Validierungsdiagnosen geschwärzt, erscheint nie in
`--print-effective-config` oder `--probe-live` und wird nicht geloggt.

```bash
curl http://127.0.0.1:8080/live
curl -H 'Authorization: Bearer DEIN_TOKEN' http://127.0.0.1:8080/ready
```

| Pfad | Status und Zweck |
|---|---|
| `/live` | 200, wenn Monitoring-Handler/Eventloop antwortet; Backend-Health ist irrelevant |
| `/ready` | kompakter Routing-Zustand; 200 nur mit verwendbarer Route, sonst 503 |
| `/health` | vollständiger Health-/Routing-/Circuit-/Uptime-Zustand; nach derselben Regel 200 oder 503 |
| `/state` | Diagnosezustand, immer 200; konfigurierte Zielhosts/-ports nur mit `expose_sensitive_state = true` |
| `/metrics` | Prometheus-Textformat, immer 200, solange Monitoring antwortet |

`/ready` meldet `healthy`, `degraded` oder `unavailable` sowie das aktive Ziel und beide
Health-Werte (`true`, `false` oder `null` bei unbekannt/deaktiviert). `/health` ergänzt
UTC-Start-/Wechsel-/Check-Zeiten, monotone Uptime-/Check-Alter,
Verbindungs- und Ablehnungszähler, Wartungsquelle, Routing-Grund, beide vollständigen letzten
Check-Ergebnisse sowie Circuit-Zustand, Open-Zeit und Retry-Verzögerung.

Auch ohne Zieladressen zeigt `/state` operative Health-Daten, Routing-Gründe, Wartungsmodus,
Zähler und Zeitstempel. Halte den Endpoint lokal oder authentifiziert. Der HTTP-Server akzeptiert
nur GET, schließt jede Antwort, begrenzt Request-Zeile/Headeranzahl/-größe, verwendet eine absolute
Request-Deadline und limitiert Monitoring-Clients unabhängig.

Die Zähler des Verbindungslebenszyklus haben getrennte Bedeutungen.
`incoming_connections_total` zählt jeden TCP-Accept des Minecraft-Listeners.
`backend_connections_established_total` zählt Clients, für die ein Backend erfolgreich für das
Relay vorbereitet wurde; `active_connections` ist die aktuell aktive Teilmenge davon.
`connections_rejected_total` zählt explizite Ablehnungen durch Protokoll-, Zulassungs- und
Routingprüfungen sowie endgültig fehlgeschlagene Backend-Connects, aber keinen MAIN-Fehler mit
erfolgreichem FALLBACK. Unerwartete interne Handlerfehler werden geloggt, statt ihnen einen
unbegrenzten oder irreführenden Ablehnungsgrund zuzuordnen. Die JSON-Felder `total_connections`
(Zuteilungen des globalen Limiters) und `rejected_connections` bleiben als veraltete
Kompatibilitätsfelder erhalten.

Die Metriken enthalten `# HELP` und `# TYPE`, begrenzte Label-Werte und keine frei formulierten
Fehlertexte als Label:

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

`mc_failover_connections_total` bleibt vorübergehend als veralteter Zähler der Verbindungen
erhalten, denen der globale Limiter einen Platz zugeteilt hat. Eine solche Verbindung kann danach
noch an Protokollvalidierung, verzögerter Zulassung, Routing oder Backend-Connect scheitern.
`mc_failover_rejected_connections_total` ist eine veraltete Kompatibilitätsfamilie mit denselben
Client-Ablehnungswerten; sie behält die historische, stets nullwertige Serie
`reason="monitoring_limit"`. Monitoring-Überlastung zählt ausschließlich
`mc_failover_monitoring_rejected_connections_total`.

## Sicherheit des PROXY Protocol

Ein- und ausgehendes PROXY Protocol v1/v2 sind unabhängig. `version` ist der gemeinsame Default;
`accept_version` und `send_version` überschreiben optional eine Richtung. Beim Empfang wird die
konfigurierte Version sowie ein vollständiger gültiger Header innerhalb
`header_timeout_seconds` erwartet. Fehlerhafte, übergroße, unvollständige Header, falsche
Familien/Längen/Ports und nicht vertrauenswürdige Peers werden sauber geschlossen. Der v2-Parser
begrenzt jede Nutzlast und validiert das TLV-Framing bei PROXY/UNSPEC; eine LOCAL-Nutzlast wird
gemäß den Protokollregeln ignoriert. LOCAL/UNKNOWN liefert nie eine behauptete Client-IP.
Geparste eingehende TLVs werden nicht in einen neu erzeugten ausgehenden Header kopiert.

`accept = true` ist nur sicher, wenn der direkte TCP-Peer ein vertrauenswürdiger Proxy ist.
Mindestens eine gültige IPv4-/IPv6-Adresse oder ein CIDR-Netz in `trusted_proxy_ips` ist zwingend.
Eine leere Liste schlägt Fail-Closed fehl.

`trust_all_proxies = true` ist ein ausdrücklich gefährlicher Notausgang. Jeder direkte Client kann
damit eine beliebige Quelladresse fälschen; die Anwendung loggt eine CRITICAL-Warnung, die Option
darf nicht mit einer Vertrauensliste kombiniert werden und darf niemals auf einem öffentlichen
Listener verwendet werden. Beschränke den Listener zusätzlich per Firewall auf vertrauenswürdige
Proxy-Peers. Aktiviere `send` nur, wenn jedes ausgewählte Backend die konfigurierte PROXY-Version
erwartet.

## Missbrauchsschutz und Logging

Der Minecraft-Listener besitzt begrenzten Backlog, globales Sitzungsmaximum, optionales
Per-Quell-IP-Limit und optionales Token-Bucket-Rate-Limit. Bei eingehendem PROXY Protocol wird der
globale Platz vor dem Parsen reserviert. Per-IP-/Rate-Limits eines vertrauenswürdigen Proxys werden
bis zum gültigen Header aufgeschoben und verwenden dann die behauptete Clientadresse (bei UNKNOWN
den direkten Peer); ein nicht vertrauenswürdiger Peer wird unter seiner direkten Adresse erfasst
und abgewiesen. Ein vertrauenswürdiger Edge-Proxy muss vom Client gelieferte Header daher
bereinigen. Monitoring besitzt ein eigenes Verbindungslimit. Ablehnungsgründe verwenden in
Metriken ein begrenztes Enum.

Erwartete Disconnects und Network Resets werden nicht als interne Fehler geloggt. Unerwartete
Fehler behalten Tracebacks. Externe Logwerte sind bereinigt und begrenzt;
`logging.access_log` ist optional, weil verbindungsbezogene Logs großes Volumen erzeugen können.

## Docker

Das Image baut in einer Builder-Stufe ein Wheel und kopiert die installierte Umgebung in ein
Python-Slim-Runtime-Image. Es läuft als UID/GID 10001, verwendet Exec-Entrypoint und SIGTERM,
exponiert nur den Minecraft-Port und nutzt `--probe-live` als eingebauten Healthcheck. Die Probe
verbindet sich im Container mit dem konfigurierten Monitoring-Listener und verlangt eine Antwort
von `/live`; ein gestoppter oder hängender Eventloop wird deshalb unhealthy. Das Beispiel
aktiviert Monitoring nur auf Container-Loopback, Compose veröffentlicht den Port nicht. Eigene
Container-Konfigurationen müssen Monitoring ebenfalls für den Image-Healthcheck aktivieren. Ist
ein Bearer-Token konfiguriert, liest die Probe es intern aus der gemounteten Konfiguration, ohne es
auszugeben. Starte den Container unmittelbar nach Änderungen an der gemounteten Konfiguration neu:
Der laufende Prozess behält seine Startkonfiguration, während jede Probe die aktuelle Datei liest.
Bei sehr langen initialen Backend-Check-Timeouts muss die Health-Start-Gnadenfrist des
Orchestrators entsprechend erhöht werden.

```bash
cp config.example.toml config.toml
# Enthält die Datei ein Token, beschränke sie und erhalte Leserechte für Container-GID 10001.
sudo chown root:10001 config.toml
sudo chmod 0640 config.toml
docker compose config
docker compose up -d --build
docker compose logs -f mc-failover
```

Compose aktiviert Init-Prozess, Read-only-Root-Dateisystem, Drop aller Capabilities,
`no-new-privileges`, begrenztes `/tmp`, PID-Limit und 60 Sekunden Stop-Grace. Nur `25565/tcp` wird
veröffentlicht. Um Monitoring ausschließlich an Host-Loopback zu veröffentlichen, kommentiere das
Mapping ein und konfiguriere im Container einen nichtlokalen Bind (`0.0.0.0`),
`allow_remote = true` und ein Bearer-Token.

Das notwendige Volume ist der schreibgeschützte Bind von `/config/config.toml`. Erstelle für
dateibasierte Wartung auf dem Host ein Verzeichnis `state/`, kommentiere den schreibgeschützten
`/state`-Bind ein und konfiguriere `/state/force_fallback` sowie `/state/force_main`; der
Host-Operator erstellt oder entfernt diese Marker. Ein dauerhaftes, von der Anwendung
beschriebenes Volume ist nicht nötig. `/tmp` ist ein flüchtiges, begrenztes tmpfs.

Container-Loopback ist nicht Host-Loopback. Laufen Backends direkt auf einem Linux-Docker-Host,
aktiviere das auskommentierte `host.docker.internal:host-gateway`-Mapping und nutze diesen Hostnamen
für die Ziele.

## systemd

Nutze die [gehärtete Unit](packaging/systemd/mc-failover.service) und die
[Installationsanleitung](packaging/systemd/README.md). Sie startet das Venv-Console-Script als
`mcfailover`, validiert die Konfiguration per `ExecStartPre`, erzeugt Runtime-/State-Verzeichnisse,
erlaubt nur Unix-/IPv4-/IPv6-Socketfamilien, setzt `LimitNOFILE=16384` und nutzt 60 Sekunden
Stop-Timeout. Die Konfiguration bleibt root-kontrolliert und gruppenlesbar (`0640`), weil sie ein
Token enthalten kann.

## Entwicklung und CI

```bash
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
python -m compileall -q src tests mc_failover_proxy.py
pytest --cov=mc_failover --cov-branch --cov-report=term-missing
python -m build
```

Die Release-Version wird ausschließlich als `project.version` in `pyproject.toml` gepflegt.
Installierte Aufrufe lesen die Paketmetadaten; ein nicht installierter Source-Checkout liest
denselben TOML-Wert und verwendet bei dessen Fehlen ausdrücklich `0+unknown`.

Die Coverage-Konfiguration aktiviert Branch Coverage und einen repositoryweiten Schwellwert von
90 %. GitHub Actions ist für Ruff, mypy, Coverage, sdist-/Wheel-Build, isolierte Wheel-/CLI-
Smoke-Checks, Dependency-Audit, Python-3.10-bis-3.14-Kompatibilität, Docker-Build und Compose-
Validierung konfiguriert. Der Container-Job prüft zusätzlich, dass das Image healthy wird, bei
angehaltenem Proxyprozess unhealthy wird, sich danach erholt und per SIGTERM sauber endet.
Separate CodeQL- und Dependabot-Konfigurationen sind enthalten. Diese Aussagen beschreiben
konfigurierte Prüfungen, nicht Ergebnisse eines bestimmten Rechners.

## Umfang und Sicherheitsgrenzen

- Dies ist ein TCP-Failover-Router, kein Ersatz für Velocity/BungeeCord und kein System zur
  Live-Migration bestehender Sitzungen.
- Er verschlüsselt Traffic nicht und authentifiziert keine Minecraft-Clients; nutze passende
  Netzwerkkontrollen.
- Veröffentliche nur den Minecraft-Listener. Monitoring bleibt lokal oder authentifiziert.
- Starte den Dienst nicht als root; Port 25565 benötigt das unter Linux nicht.
- Sind beide Ziele nicht verfügbar, werden neue Clients geschlossen, statt zu einem bekanntermaßen
  defekten Ziel gesendet.
- Python kann DNS-/NSS- und Wartungsdatei-Awaits zeitlich begrenzen, einen bereits im
  Betriebssystem blockierten Resolver- oder Netzwerkdateisystem-Workerthread aber nicht gewaltsam
  beenden. Nutze zuverlässige lokale NSS-/Resolver-Konfiguration und lokale Wartungspfade; die
  Stop-Deadline des Service-Managers bleibt die letzte betriebliche Grenze.

Lizenziert unter der [MIT-Lizenz](LICENSE).
