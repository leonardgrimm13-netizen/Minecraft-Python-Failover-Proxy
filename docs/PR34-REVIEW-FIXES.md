# PR #34 – unabhängige Review: Korrekturen und Nachweise

Ausgangspunkt: `e3a3cce`, bestehender Branch `feature/multi-backend-status-reload`.
Die bestehenden Feature-Änderungen und die Einbindung von PR #33 bleiben erhalten.

## Bestätigte Fehler

| Finding | Ursache und Korrektur | Regressionen |
|---|---|---|
| 1: SIGHUP bei deaktiviertem Reload | Handler wird unabhängig vom Startwert registriert; die aktuelle Runtime-Policy ignoriert deaktivierte Reloads. Bei fehlender AsyncIO-Unterstützung sicherer SIG_IGN-Fallback; scheitert auch dieser, kein unsicherer Start. | Echte Subprozesse: Reload an/aus, wiederholtes HUP mit aktiver TCP-Session, anschließend TERM/INT; Registrierungsfehler und Cleanup. |
| 2: Healthcheck-Reload schließt Circuit | Separate Wiederverwendungsregeln für Health, Circuit und Statistik. Gleiche ID/Host/Port/Circuit-Konfiguration erhalten Circuit, Sperrzeit und aktive Permits unabhängig von Health-Änderungen. | OPEN/HALF_OPEN, mehrere Backends, atomarer Reload, Recovery und geänderte Endpunkte/Settings. |
| 3: Alte Sessions wecken alten Worker | Proxy dispatcht späte Meldungen über die aktuelle Registry. Identität gemeinsam übernommener Statistiken kennzeichnet die ununterbrochene Backend-Instanz; keine Registry-Kette. | Acht gehaltene echte TCP-Sessions, Reset/frühes EOF nach drei Reloads, gebündelte aktuelle Prüfung; Entfernen, Ersetzen und erneutes Hinzufügen ohne alte Meldungen. |
| 4: Reihenfolge gemischter Fehler | Gemeinsame Schwellenbewertung beider Fehlerhistorien. Kleine private Permit-Verdikte verhindern doppelte Fehlerzählung und verspätete Success-Resets ohne globalen Session-Speicher. | Alle Connect-/Session-Permutationen, Schwellen, Zeitfenster, Konkurrenz, wiederholte/freigegebene Permits und parallele Half-Open-Probes. |
| 5: Status-only verhindert Recovery | Nur gültige lokale Statusrequests dürfen einen prioritäts-/maintenance-konformen Probe bei deaktivierten aktiven Checks und abgelaufenem Circuit ausführen. Bestehende Half-Open-Kapazität, feste Timeouts und Statuslimit gelten; keine losgelösten Tasks. | Online/offline, Cooldown, nur Statusclients, mehrere Backends, parallele Probes über Reloads, fehlerhafte Pakete, lokale Ressourcenfehler und Shutdown-Cancellation. |

Alle fünf ursprünglichen Fehler wurden vor der Korrektur durch fehlschlagende Regressionen bestätigt.

## Zusätzliche Risiken und abschließende Review

- A bestätigt/behoben: Späte Session-Evidenz verwendet die **aktuelle** passive und frühe Fehler-Policy. Transportparameter bestehender Sessions bleiben erhalten. Bei geänderten Circuit-Einstellungen wird kein Permit des alten Circuits an den neuen übergeben.
- B bestätigt/behoben: Alle Statusgenerationen teilen ein Admission-Budget. Neue Limits gelten erst bei atomarer Aktivierung; abgelehnte Kandidaten verändern es nicht. Eine Absenkung erhält aktive Anfragen und blockiert neue bis unter die Grenze. Instanzen besitzen ihre eigenen Freigaben.
- C bestätigt/behoben: Ohne explizites Gesamtbudget erhalten Legacy-Konfigurationen die Summe zulässiger Connect-Timeouts und optionaler PROXY-Schreibzeit. Multi-Backend behält den Standard von zehn Sekunden; explizite Budgets gelten für beide Schemata. Das abgeleitete Gesamtbudget umfasst auch Scheduling/Cleanup und garantiert keine exakte volle Laufzeit jedes einzelnen Versuchs.
- D bestätigt/behoben: Programmatischer Reload vor erfolgreichem Start wird als `not_started` ohne neue Ressourcen/Generation abgewiesen. Tests prüfen Wiederholung und gleichzeitige Startvorbereitung.
- Zusätzlich bei unabhängiger Nachprüfung gefunden/behoben: Identische Probe-/Statusdeadlines konnten die Offline-Antwort abbrechen und den Circuit ohne Cooldown HALF_OPEN lassen. Ein Probe verwendet höchstens die Hälfte des statischen Statuszeitbudgets; bei zu wenig Restzeit wird er übersprungen. Langsame Clients können seine Fehlerschwelle dadurch nicht verkürzen. Regression mit echtem Statusclient und kontrolliertem Netzwerk-Timeout zunächst fehlgeschlagen, danach erfolgreich.
- Unabhängige abschließende Prüfung des Source-Diffs: keine weiteren nachgewiesenen Bugs; 16 Status-/Generations-Tests bestanden. Ein verspäteter gültiger Handshake erhielt eine Offline-Antwort ohne Probe oder Permit-Leak.

## Finale lokale Verifikation

- `pytest -q`: **1040 bestanden** (86 zusätzliche Fälle gegenüber 954 am Ausgangscommit).
- Gesamte Suite mit Branch-Coverage: **1040 bestanden, 95,41 %**, Mindestgrenze 90 % unverändert.
- Ruff, Ruff-Format (78 Dateien), `mypy src tests tools` (71 Dateien) und Compileall erfolgreich.
- Wheel/Quelldistribution mit `build --no-isolation` und Release-Archiv-Hygiene erfolgreich.
- Python-Matrix und GitHub-CI: siehe aktuellen PR-Abschnitt „Independent review findings – resolved“; dort werden die abschließend geprüften Läufe verlinkt.
- Docker ist lokal nicht installiert. Container-/Compose-/Signal-Smokes werden durch die vorhandene GitHub-CI ausgeführt.

## Erhaltene reale Lastmessungen

Die neuen Rohdaten stehen als `feature-validation/review-fixes-*.json` getrennt von den bisherigen `final-*.json` zur Verfügung. Fünf Relay-Runden mit 64 Bytes, zehn Reloads mit laufendem Verkehr, parallele Status-/Pong-Abfragen; frische Prozesse, Soft-FD-Limit 32768. Nach der letzten Deadline-Korrektur wurden sie nicht unnötig wiederholt: Diese Änderung betrifft ausschließlich Status-Recovery bei deaktivierten aktiven Checks; die Lastläufe verwenden aktive Checks.

| Backend / Clients | Connect p95 ms | Relay p95 ms | Status p95 ms | Reload + RTT p95 ms |
|---|---:|---:|---:|---:|
| IP / 100 | 91,38 | 8,18 | 42,51 | 10,31 |
| IP / 500 | 312,37 | 52,34 | 192,89 | 53,26 |
| IP / 2000 | 1364,23 | 246,97 | 873,50 | 411,32 |
| DNS / 500 | 343,03 | 50,18 | 203,95 | 57,03 |
| DNS / 2000 | 1561,12 | 262,33 | 899,80 | 398,62 |

Alle fünf Läufe: **null Fehler**, Generation 11, anschließend null Proxy-/Background-Tasks, Sessions und Limiter-Leases. Nach vollständigem Fake-Peer-Cleanup jeweils ein Harness-Task, sieben FDs und zwei Event-Loop-Sockets; keine erzwungene Garbage Collection. CPU-Zeit, RSS, Task-/Socketzahlen, Loop-Lag und Ausfall-/Recoveryzeiten je Phase stehen in den JSON-Dateien. RSS nach vollständigem Cleanup: IP/100 34032 KiB, IP/500 53296 KiB, IP/2000 116780 KiB, DNS/500 55080 KiB, DNS/2000 122728 KiB. Retained Python-Allocator-Speicher allein belegt keinen Leak.

Separater 16-MiB-Bulk-Lauf: **229,05 duplex MiB/s**, 0,1397 s Wall/0,1349 s CPU, gegenüber früher gemessenen 296,24 MiB/s. Dies ist ein kurzer Einzelvergleich, kein kontrollierter Performance-Regressionsnachweis und kein Beleg einer Verbesserung. Die normalen Relay-Chunks wurden durch diese Fixes nicht verändert. Die Latenzen sind kombinierte lokale Client-/Proxy-/Backend-Messungen, keine Produktionsgarantie.

## Dateien und Sicherheitsgrenzen

Produktionsänderungen: `app.py`, `cli.py`, `config.py`, `circuit_breaker.py`, `registry.py`, `routing.py`, `proxy.py`, `local_status.py`. Neue Testdateien: `test_review_reload.py`, `test_status_recovery.py`, `test_status_generation_limit.py`, `test_legacy_connect_budget.py`, `test_reload_lifecycle.py`; bestehende Circuit-, Registry-, CLI-, Signal- und Reloadtests erweitert. Beide READMEs, Legacy-Beispiel und systemd-Anleitung aktualisiert.

Keine neue Dependency, kein Remote-Admin-Endpunkt, keine vom Statusclient bestimmten Zieladressen. PROXY-Vertrauensgrenzen, DNS-/Listener-Schleifenprüfung, unveränderte Login-Bytefolge und Session-Ownership bleiben bestehen und werden von der Gesamtsuite geprüft. TCP-Recovery beweist Listener-Erreichbarkeit; für Minecraft-Bereitschaft sind aktive Minecraft-Statuschecks nötig. Keine Migration bestehender Minecraft-Sessions. Unbegrenzte Langzeitstabilität oder Produktionskapazität wird aus den lokalen Tests nicht abgeleitet.
