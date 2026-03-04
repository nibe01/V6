"""
Process Manager – startet und stoppt Scanner und Trader als Subprozesse.
"""
from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional


class ManagedProcess:
    """Wrapper für einen verwalteten Subprozess."""

    def __init__(self, name: str, command: list[str], cwd: Path, logger) -> None:
        self.name = name
        self.command = command
        self.cwd = cwd
        self.logger = logger
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        if self.is_running():
            self.logger.info("%s läuft bereits (pid=%s)", self.name, self.proc.pid)
            return

        self.proc = subprocess.Popen(
            self.command,
            cwd=str(self.cwd),
        )
        self.logger.info("%s gestartet (pid=%s)", self.name, self.proc.pid)

    def stop(self, timeout: float = 15.0) -> None:
        if not self.proc:
            return
        if not self.is_running():
            self.proc = None
            return

        self.logger.info("Stoppe %s (pid=%s)", self.name, self.proc.pid)
        try:
            if sys.platform.startswith("win"):
                self.proc.terminate()
            else:
                self.proc.send_signal(signal.SIGTERM)

            self.proc.wait(timeout=max(0.1, timeout))
            self.logger.info("%s sauber beendet", self.name)
        except subprocess.TimeoutExpired:
            self.logger.warning("%s reagiert nicht, erzwinge Kill", self.name)
            try:
                self.proc.kill()
                self.proc.wait(timeout=5.0)
            except Exception as kill_error:
                self.logger.error("Kill fehlgeschlagen für %s: %s", self.name, kill_error)
        except Exception as stop_error:
            self.logger.error("Fehler beim Stoppen von %s: %s", self.name, stop_error)
        finally:
            self.proc = None

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def restart(self) -> None:
        self.stop()
        time.sleep(0.2)
        self.start()


class ProcessManager:
    """
    Verwaltet Scanner- und Trader-Subprozesse.
    Startet sie zu Marktöffnung, stoppt sie bei Marktschluss.
    """

    def __init__(self, project_root: Path, logger):
        self.project_root = project_root
        self.logger = logger

        self.scanner = ManagedProcess(
            name="scanner",
            command=[sys.executable, "-m", "scanner.scanner_edge"],
            cwd=project_root,
            logger=logger,
        )
        self.trader = ManagedProcess(
            name="trader",
            command=[sys.executable, "-m", "trader.trader_live"],
            cwd=project_root,
            logger=logger,
        )

    def start_scanner(self) -> None:
        self.scanner.start()

    def start_trader(self) -> None:
        self.trader.start()

    def stop_scanner(self) -> None:
        self.scanner.stop()

    def stop_trader(self) -> None:
        self.trader.stop()

    def stop_all(self) -> None:
        self.stop_scanner()
        self.stop_trader()

    def ensure_running(self) -> None:
        """Startet Prozesse neu falls sie unerwartet gestoppt wurden."""
        if not self.scanner.is_running():
            self.logger.warning("Scanner läuft nicht mehr, starte neu")
            self.start_scanner()
        if not self.trader.is_running():
            self.logger.warning("Trader läuft nicht mehr, starte neu")
            self.start_trader()

    def get_status(self) -> dict:
        return {
            "scanner_running": self.scanner.is_running(),
            "trader_running": self.trader.is_running(),
            "scanner_pid": self.scanner.proc.pid if self.scanner.is_running() else None,
            "trader_pid": self.trader.proc.pid if self.trader.is_running() else None,
        }
