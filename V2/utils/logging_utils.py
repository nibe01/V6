"""
Multi-Logger System
Strukturierte Logs für verschiedene Event-Typen.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from enum import Enum
from typing import Optional

from utils.paths import LOGS_DIR, ensure_dirs


class LogCategory(Enum):
    """Log-Kategorien."""
    MAIN = "main"           # Hauptlog (alles)
    TRADES = "trades"       # Trade-Events (Entry/Exit)
    SIGNALS = "signals"     # Scanner-Signale
    ORDERS = "orders"       # Order-Aktivität (Send/Fill/Cancel)
    POSITIONS = "positions" # Position-Updates (Open/Close)
    PERFORMANCE = "performance"  # Performance-Metriken
    SCANNER = "scanner"    # Scanner-Laufzeit/Filter-Events
    ERRORS = "errors"       # Errors & Warnings
    DEBUG = "debug"         # Detailliertes Debug-Log


class DailyFileHandler(logging.FileHandler):
    """
    File handler that switches to a new file when the calendar date changes.

    This keeps one log file per day even for long-running 24/7 processes.
    """

    def __init__(self, log_dir: Path, file_prefix: str, encoding: str = "utf-8"):
        self.log_dir = Path(log_dir)
        self.file_prefix = file_prefix
        self.current_date = self._today()
        self.log_dir.mkdir(parents=True, exist_ok=True)

        initial_file = self.log_dir / f"{self.file_prefix}_{self.current_date}.log"
        super().__init__(initial_file, encoding=encoding)

    @staticmethod
    def _today() -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def _rotate_if_needed(self) -> None:
        today = self._today()
        if today == self.current_date:
            return

        self.acquire()
        try:
            today = self._today()
            if today == self.current_date:
                return

            if self.stream:
                self.stream.flush()
                self.stream.close()
                self.stream = None

            self.current_date = today
            new_file = self.log_dir / f"{self.file_prefix}_{self.current_date}.log"
            self.baseFilename = str(new_file.resolve())
            self.stream = self._open()
        finally:
            self.release()

    def emit(self, record: logging.LogRecord) -> None:
        self._rotate_if_needed()
        super().emit(record)


class MultiLogger:
    """
    Verwaltet mehrere Logger gleichzeitig.
    Erlaubt strukturierte Logs in verschiedene Dateien.
    """

    def __init__(self, name: str, debug_mode: bool = False):
        """
        Initialisiert Multi-Logger.

        Args:
            name: Name des Haupt-Loggers (z.B. 'trader_live')
            debug_mode: Wenn True, aktiviere DEBUG-Level Logging
        """
        self.name = name
        self.debug_mode = debug_mode
        self.loggers: dict[LogCategory, logging.Logger] = {}

        ensure_dirs()
        self._setup_loggers()

    def _setup_loggers(self):
        """Erstellt alle Logger mit eigenen Dateien."""
        # Formatter
        detailed_formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s"
        )
        simple_formatter = logging.Formatter(
            "%(asctime)s | %(message)s"
        )

        # Log-Level
        main_level = logging.DEBUG if self.debug_mode else logging.INFO

        # Erstelle Logger für jede Kategorie
        for category in LogCategory:
            logger = logging.getLogger(f"{self.name}.{category.value}")
            logger.setLevel(main_level)
            logger.handlers.clear()  # Verhindere Duplikate
            logger.propagate = False  # Verhindere Parent-Propagation

            # Datei-Handler
            if category == LogCategory.PERFORMANCE and self.name == "broker_monitor":
                # Keep monitor performance snapshots separate from trader performance logs.
                log_dir = LOGS_DIR / "monitor_performance"
            else:
                log_dir = LOGS_DIR / category.value
            log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = DailyFileHandler(
                log_dir=log_dir,
                file_prefix=category.value,
                encoding="utf-8",
            )
            file_handler.setLevel(main_level)

            # MAIN, DEBUG und ERRORS bekommen detailed formatter
            if category in [LogCategory.MAIN, LogCategory.DEBUG, LogCategory.ERRORS]:
                file_handler.setFormatter(detailed_formatter)
            else:
                file_handler.setFormatter(simple_formatter)

            logger.addHandler(file_handler)

            # Console-Handler nur für MAIN und ERRORS
            if category in [LogCategory.MAIN, LogCategory.ERRORS]:
                console_handler = logging.StreamHandler()
                console_handler.setLevel(main_level)
                console_handler.setFormatter(detailed_formatter)
                logger.addHandler(console_handler)

            self.loggers[category] = logger

        # Startup-Message
        self.info("=" * 80)
        self.info(f"Multi-Logger initialized: {self.name}")
        self.info(f"Debug Mode: {self.debug_mode}")
        self.info(f"Logs Directory: {LOGS_DIR}")
        self.info("=" * 80)

    def _is_scanner_logger(self) -> bool:
        """Detect scanner-related logger names for scanner-only file logging."""
        lowered = self.name.lower()
        return lowered == "scanner" or lowered.startswith("scanner") or ".scanner" in lowered

    # ========== MAIN LOG (Standard-Logs) ==========

    def info(self, msg: str, *args):
        """Standard Info-Log (geht in MAIN + Console)."""
        if args:
            msg = msg % args
        self.loggers[LogCategory.MAIN].info(msg)
        if self._is_scanner_logger():
            self.loggers[LogCategory.SCANNER].info(msg)
        if self.debug_mode:
            self.loggers[LogCategory.DEBUG].info(msg)

    def warning(self, msg: str, *args):
        """Warning-Log (geht in MAIN + ERRORS)."""
        if args:
            msg = msg % args
        self.loggers[LogCategory.MAIN].warning(msg)
        self.loggers[LogCategory.ERRORS].warning(msg)
        if self._is_scanner_logger():
            self.loggers[LogCategory.SCANNER].warning(msg)
        if self.debug_mode:
            self.loggers[LogCategory.DEBUG].warning(msg)

    def error(self, msg: str, *args, exc_info=False):
        """Error-Log (geht in MAIN + ERRORS + Console)."""
        if args:
            msg = msg % args
        self.loggers[LogCategory.MAIN].error(msg, exc_info=exc_info)
        self.loggers[LogCategory.ERRORS].error(msg, exc_info=exc_info)
        if self._is_scanner_logger():
            self.loggers[LogCategory.SCANNER].error(msg, exc_info=exc_info)
        if self.debug_mode:
            self.loggers[LogCategory.DEBUG].error(msg, exc_info=exc_info)

    def debug(self, msg: str, *args):
        """Debug-Log (nur wenn debug_mode=True)."""
        if self.debug_mode:
            if args:
                msg = msg % args
            self.loggers[LogCategory.DEBUG].debug(msg)
            if self._is_scanner_logger():
                self.loggers[LogCategory.SCANNER].debug(msg)

    # ========== SPECIALIZED LOGS ==========

    def trade(self, msg: str, *args):
        """
        Trade-Event: Entry/Exit eines Trades.

        Beispiele:
        - "ENTRY: AAPL | Qty: 10 | Price: $150.50 | TP: $152.61 | SL: $135.45"
        - "EXIT: AAPL | Reason: TP | P&L: $21.10 | Duration: 45min"
        """
        if args:
            msg = msg % args
        self.loggers[LogCategory.TRADES].info(msg)
        self.loggers[LogCategory.MAIN].info(f"[TRADE] {msg}")
        if self.debug_mode:
            self.loggers[LogCategory.DEBUG].info(f"[TRADE] {msg}")

    def signal(self, msg: str, *args):
        """
        Scanner-Signal.

        Beispiel:
        - "TSLA | 24h: +5.2% | 1h: +3.4% | Price: $245.30"
        """
        if args:
            msg = msg % args
        self.loggers[LogCategory.SIGNALS].info(msg)
        self.loggers[LogCategory.MAIN].info(f"[SIGNAL] {msg}")
        if self._is_scanner_logger():
            self.loggers[LogCategory.SCANNER].info(f"[SIGNAL] {msg}")
        if self.debug_mode:
            self.loggers[LogCategory.DEBUG].info(f"[SIGNAL] {msg}")

    def order(self, msg: str, *args):
        """
        Order-Event: Send/Fill/Cancel.

        Beispiele:
        - "SENT: AAPL BUY 10 @ $150.50 (Bracket 12345)"
        - "FILLED: AAPL @ $150.52 (Order 12345)"
        - "CANCELLED: AAPL (Order 12345) - Timeout"
        """
        if args:
            msg = msg % args
        self.loggers[LogCategory.ORDERS].info(msg)
        self.loggers[LogCategory.MAIN].info(f"[ORDER] {msg}")
        if self.debug_mode:
            self.loggers[LogCategory.DEBUG].info(f"[ORDER] {msg}")

    def position(self, msg: str, *args):
        """
        Position-Update: Status-Änderungen.

        Beispiele:
        - "FILLED: AAPL | Order: 12345"
        - "CLOSED: AAPL | P&L: $21.10 (1.40%)"
        """
        if args:
            msg = msg % args
        self.loggers[LogCategory.POSITIONS].info(msg)
        self.loggers[LogCategory.MAIN].info(f"[POSITION] {msg}")
        if self.debug_mode:
            self.loggers[LogCategory.DEBUG].info(f"[POSITION] {msg}")

    def performance(self, msg: str, *args):
        """
        Performance-Metrik.

        Beispiele:
        - "Win Rate: 89.5%"
        - "Daily P&L: $234.50"
        - "Total Trades: 15"
        """
        if args:
            msg = msg % args
        self.loggers[LogCategory.PERFORMANCE].info(msg)
        self.loggers[LogCategory.MAIN].info(f"[PERF] {msg}")
        if self._is_scanner_logger():
            self.loggers[LogCategory.SCANNER].info(f"[PERF] {msg}")
        if self.debug_mode:
            self.loggers[LogCategory.DEBUG].info(f"[PERF] {msg}")


# ========== GLOBAL INSTANCES ==========

_loggers: dict[str, MultiLogger] = {}


def setup_logging(name: str, debug_mode: bool = False) -> MultiLogger:
    """
    Initialisiert Multi-Logger für Modul.

    Args:
        name: Logger-Name (z.B. 'trader_live', 'scanner')
        debug_mode: Debug-Modus aktivieren

    Returns:
        MultiLogger-Instanz

    Example:
        logger = setup_logging('trader_live', debug_mode=True)
        logger.info("Starting trader...")
        logger.trade("ENTRY: AAPL @ $150.50")
        logger.error("Connection failed")
    """
    if name in _loggers:
        return _loggers[name]

    logger = MultiLogger(name, debug_mode=debug_mode)
    _loggers[name] = logger
    return logger


def get_logger(name: str) -> MultiLogger:
    """
    Gibt existierenden Logger zurück.
    Falls nicht vorhanden, erstelle mit Standard-Settings.

    Args:
        name: Logger-Name

    Returns:
        MultiLogger-Instanz
    """
    if name not in _loggers:
        return setup_logging(name, debug_mode=False)
    return _loggers[name]
