# V2 Trading System

V2 ist ein mehrprozessiges Trading-System für Interactive Brokers (IB), ausgelegt für stabilen Dauerbetrieb.

Die drei Kernprozesse:
- `monitor/broker_monitor.py`: läuft 24/7, hält IB-Verbindung und startet/stoppt Scanner+Trader nach Zeitfenster.
- `scanner/scanner_edge.py`: bewertet das Symboluniversum mit einer mehrstufigen Edge-Pipeline.
- `trader/trader_live.py`: verarbeitet Signale, platziert Orders und verwaltet Positionen/Schutzorders.

## Kernprinzipien

- Eine zentrale Konfiguration: `config.py`
- Startvalidierung aller Config-Werte: `utils/config_validator.py`
- Zustandsdateien mit Locking + atomischem Schreiben: `utils/state_utils.py`
- IB-Reconnect und Health-Checks in allen relevanten Loops
- Dauerbetrieb-Features:
  - tägliche Log-Rotation über Mitternacht
  - Queue-Kompaktierung mit Retention
  - State-Retention für alte Trades
  - dynamischer NYSE-Feiertagskalender über `holidays`

## Projektstruktur

```text
V2/
├── config.py
├── monitor/
│   ├── broker_monitor.py
│   ├── position_tracker.py
│   └── process_manager.py
├── scanner/
│   ├── scanner_edge.py
│   ├── edge_filters.py
│   ├── edge_signals.py
│   └── historical_signals.py
├── trader/
│   ├── trader_live.py
│   └── order_verification.py
├── utils/
│   ├── ib_connection.py
│   ├── market_schedule.py
│   ├── rate_limiter.py
│   ├── state_utils.py
│   ├── state_retry.py
│   ├── config_validator.py
│   └── ...
├── data/
├── output/
├── logs/
└── state/
```

## Laufzeitverhalten

Der Monitor läuft dauerhaft. Scanner und Trader laufen nur im konfigurierten Aktivitätsfenster:

- Start: `market_open - pre_market_start_minutes`
- Stop: `market_close + post_market_stop_minutes`

Beispiel mit `pre_market_start_minutes=150`:
- NYSE Open 09:30 ET
- Scanner/Trader starten um 07:00 ET

## Scanner-Pipeline (Edge)

Stufen:
- Ebene 0: Price Range
- Ebene 1: Movement Capability
- Ebene 2: Volume Activity
- Ebene 3: Directional Edge
- Ebene 4: Catalyst (optional)
- Ebene 5: Risk Control (optional)

Parameter liegen in `EdgeScannerConfig` und Sub-Configs in `config.py`.

## Trader-Logik (Kurz)

- Liest neue Signale aus `output/signals.jsonl` inkrementell per Offset
- Verhindert Doppel-Entries pro Symbol
- Entry mit Slippage-Schutz (Limit bevorzugt)
- TP/SL-Schutzorders + Verifikation
- Reconciliation mit IB, inkl. Recovery-Pfaden
- Daily-Loss-Guard, Cooldowns, Queue-/State-Housekeeping

## Dauerbetrieb & Retention

### Logs
- Tägliche Rotation automatisch über Mitternacht (`utils/logging_utils.py`).

### Signal-Queue
- Warnung ab `trading.signal_queue_warning_bytes`
- Sichere Kompaktierung ab `trading.signal_queue_rotate_bytes`
- Rotierte Queue-Dateien begrenzt über `trading.signal_queue_retention_files`

### Processed State
- Alte abgeschlossene/abgelehnte/manual-closed Einträge werden entfernt
- Aufbewahrung über `trading.processed_state_retention_days`
- Cleanup-Intervall über `trading.processed_state_cleanup_interval_seconds`

## Konfiguration (wichtigste Blöcke)

- `IBConfig`: TWS/Gateway-Verbindung, Client-IDs, RTH/EH-Daten
- `MonitorConfig`: Herzschlag + Aktivitätsfenster
- `StrategyConfig`: Regelkombination + historische Bars
- `TradingConfig`: Sizing, Risk, Execution, Retention
- `EdgeScannerConfig`: Filter- und Scannerparameter

Alle Felder sind direkt in `config.py` kommentiert (Einheit, Wirkung, typische Bedeutung).

## Start

Aus `V2/` starten.

Empfohlen (Produktivbetrieb):

```bash
python3 -m monitor.broker_monitor
```

Manuell (Debug):

```bash
python3 -m scanner.scanner_edge
python3 -m trader.trader_live
```

## Dependencies

`requirements.txt` enthält aktuell:
- `holidays>=0.67`

Installieren:

```bash
pip install -r requirements.txt
```

## Wichtige Dateien zur Beobachtung

- Signal-Queue: `output/signals.jsonl`
- Signal-Archiv: `output/signals_archive.jsonl`
- Trade-State: `state/processed_signals.json`
- Daily Loss Counter: `state/daily_losses.json`
- Monitor-State: `state/monitor_state.json`

## Troubleshooting

Keine Signale:
- Marktdaten/IB-Verbindung prüfen
- Filter in `EdgeScannerConfig` ggf. zu streng
- `use_rth` vs. Pre-/Post-Market beachten

Keine Entries trotz Signalen:
- Daily-Loss-Limit erreicht
- Symbol im Cooldown
- Offene Position/Order bereits vorhanden
- Account/Buying-Power-Checks blocken

Queue wächst stark:
- Trader-Loop läuft nicht stabil
- Scanner produziert mehr als verarbeitet wird
- Warn-/Rotate-Schwellen in `TradingConfig` anpassen

## Hinweis

`main.py` ist kein aktiver Einstiegspunkt. Verwende die Modulstarts oben.
