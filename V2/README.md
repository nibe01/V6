# V2 Trading System

V2 ist ein automatisiertes Trading-System mit drei Prozessen:

- `monitor/broker_monitor.py` läuft 24/7, hält die IB-Verbindung und steuert Scanner/Trader.
- `scanner/scanner_edge.py` erzeugt Signale aus einem mehrstufigen Edge-Filter.
- `trader/trader_live.py` verarbeitet diese Signale und führt Trades mit TP/SL-Risikologik aus.

## Architektur

```text
V2/
├── config.py                     # Zentrale Dataclass-Konfiguration
├── monitor/
│   ├── broker_monitor.py         # 24/7 Broker-Monitor (Haupt-Einstiegspunkt)
│   ├── position_tracker.py       # Positions- und P&L-Tracking
│   └── process_manager.py        # Subprocess-Verwaltung für Scanner+Trader
├── scanner/
│   ├── scanner_edge.py           # Signal-Pipeline (Edge Scanner)
│   ├── edge_filters.py           # Ebenen 0-5 Filterlogik
│   ├── edge_signals.py           # Signal-Datenstruktur
│   └── historical_signals.py     # Historische Datenhilfen
├── trader/
│   ├── trader_live.py            # Live-Trading Loop
│   └── order_verification.py     # Order-/Bracket-Checks
├── utils/
│   ├── ib_connection.py          # Robustes Connect/Reconnect zu IB
│   ├── market_schedule.py        # NYSE Marktzeiten (neu)
│   ├── rate_limiter.py           # API-Limitschutz
│   ├── account_checker.py        # Kontostand/Buying-Power Checks
│   ├── position_reconciliation.py# State vs. IB Reconciliation
│   ├── trade_status.py           # Gemeinsame Status-Quelle
│   └── ...
├── data/                         # Symboluniversum
├── output/                       # Signal-Queue + Archiv
└── state/                        # Laufzeit-State (Positions/Verluste/Cooldowns)
```

## Laufzeitfluss

```text
00:00–09:28 ET  broker_monitor läuft, Scanner+Trader pausiert
09:30 ET        broker_monitor startet scanner_edge + trader_live
09:30–16:00 ET  Normaler Handelsbetrieb
16:00 ET        broker_monitor stoppt scanner_edge + trader_live
16:00–16:05 ET  End-of-Day Report wird erstellt
16:05–23:59 ET  broker_monitor läuft, überwacht offene Positionen
```

## Scanner-Logik (Edge Pipeline)

Der Scanner nutzt folgende Ebenen:

- Ebene 0: Price Range
- Ebene 1: Movement Capability
- Ebene 2: Volume Activity
- Ebene 3: Directional Edge
- Ebene 4: Catalyst (optional)
- Ebene 5: Risk Control (optional)

Die Schwellenwerte werden in `config.py` über `EdgeScannerConfig` und Sub-Configs (`PriceRangeConfig`, `MovementConfig`, `VolumeConfig`, `DirectionConfig`, `CatalystConfig`, `RiskConfig`) gesteuert.

## Trading- und Risiko-Logik

Wichtige Punkte im Live-Trader:

- Einheitliches Position Sizing (`position_size_pct`, auto/manual max trades)
- Entry mit Slippage-Schutz (`use_limit_entry`, `max_entry_slippage_pct`)
- Fill-first Ablauf: TP/SL werden vom tatsächlichen Fill-Preis berechnet
- Schutz bei fehlenden Exit-Orders (Recovery + optional Emergency Exit)
- Daily-Loss-Limit (`max_daily_stop_losses`)
- Symbol-Cooldown nach Events
- Regelmäßige Reconciliation zwischen IB und internem State

Statuswerte sind zentralisiert in `utils/trade_status.py`, damit Trader und Reconciliator dieselbe Semantik verwenden.

## Signal Queue Handling

`output/signals.jsonl` wird bewusst als append-only Queue behandelt:

- Scanner hängt Zeilen an.
- Trader liest nur neue Zeilen über Dateioffset.
- Trader archiviert verarbeitete Zeilen zusätzlich in `output/signals_archive.jsonl`.
- Keine automatische Trunkierung der Queue-Datei (Race-Condition-Vermeidung).
- Warnungen bei zu großer Queue über:
  - `trading.signal_queue_warning_bytes`
  - `trading.signal_queue_warning_interval_seconds`

## Konfiguration

Alle relevanten Einstellungen liegen in `config.py` (Dataclasses) und werden beim Start validiert (`utils/config_validator.py`).

Wichtige Bereiche:

- `IBConfig`: Host, Port, Client IDs, Connection-Check Intervall
- `StrategyConfig`: Rule-Operator + historische Bar-Settings
- `TradingConfig`: Risiko, Positionierung, Execution, Queue-Warnung
- `EdgeScannerConfig`: Filterparameter und Scan-Intervalle

Hinweis: Scanner- und Trader-Client-ID müssen unterschiedlich sein.

## Starten

Arbeitsverzeichnis ist der Projektordner `V2/`.

Empfohlen (Monitor steuert alles automatisch):

```bash
python3 -m monitor.broker_monitor
```

Alternativ manuell (Entwicklung/Debugging):

```bash
python3 -m scanner.scanner_edge   # Terminal 1
python3 -m trader.trader_live     # Terminal 2
```

## Statistik-Logs (Scanner)

Der Scanner loggt eine Pipeline-Statistik in konfigurierbaren Intervallen
(`edge_scanner.stats_log_interval_blocks`) sowie einen detaillierten Snapshot alle 50 Blöcke.

Beispiel:

```text
================================================================================================
EDGE SCANNER PIPELINE STATS
================================================================================================
Total scanned: 5000
Stage                  Passed     Cum %    Step %   Filtered
------------------------------------------------------------------------------------------------
Ebene 0 Price            4500     90.0%    90.0%        500
Ebene 1 Move             1000     20.0%    22.2%       3500
Ebene 2 Volume            500     10.0%    50.0%        500
Ebene 3 Direction         250      5.0%    50.0%        250
Ebene 4 Catalyst          150      3.0%    60.0%        100
Ebene 5 Risk              120      2.4%    80.0%         30
Final Signals             120      2.4%   100.0%          0
------------------------------------------------------------------------------------------------
Funnel: 5000 -> 4500 -> 1000 -> 500 -> 250 -> 150 -> 120
================================================================================================
```

## Daten und State-Dateien

- `data/extended_symbols.csv`: Symboluniversum
- `output/signals.jsonl`: Scanner-Queue
- `output/signals_archive.jsonl`: archivierte Signalzeilen
- `state/processed_signals.json`: Trade-State
- `state/daily_losses.json`: Daily-Loss-Zähler

## Troubleshooting

Keine Signale:

- IB/TWS/Gateway läuft nicht oder API nicht aktiviert
- Filterparameter zu streng
- Keine verwertbaren historischen Daten

Keine Orders/abgebrochene Orders:

- Buying Power / Account Checks schlagen fehl
- Slippage-Limit zu eng
- Reconnect/Connection-Probleme in IB

Zu viele Queue-Warnungen:

- Trader läuft nicht oder zu langsam
- Scanner produziert mehr Signale als verarbeitet werden
- Threshold in `TradingConfig` prüfen

## Hinweise

- `main.py` ist aktuell leer und kein Einstiegspunkt.
- Projekt ist für internen/proprietären Einsatz gedacht.
