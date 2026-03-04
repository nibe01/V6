"""
Daily Loss Counter - Vereinfachte Version
Zählt Stop-Loss-Trades pro Tag und prüft Limits.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from utils.state_utils import load_state, save_state
from utils.logging_utils import get_logger

logger = get_logger(__name__)


class DailyLossCounter:
    """
    Zählt Stop-Loss Trigger pro Tag.
    Speichert Daten in state/daily_losses.json
    
    Format:
    {
        "2026-02-06": {
            "stop_losses": 3,
            "symbols": ["AAPL", "MSFT", "GOOGL"]
        }
    }
    """

    def __init__(self, state_path: Path):
        self.state_path = state_path
        self.state: Dict = load_state(state_path)
        self._ensure_today_entry()

    def _get_today_key(self) -> str:
        """Gibt heutiges Datum zurück: YYYY-MM-DD"""
        return datetime.now(timezone.utc).date().isoformat()

    def _ensure_today_entry(self):
        """Stellt sicher, dass ein Eintrag für heute existiert."""
        today = self._get_today_key()
        if today not in self.state:
            self.state[today] = {
                "stop_losses": 0,
                "symbols": [],
            }
            save_state(self.state_path, self.state)

    def get_today_stop_loss_count(self) -> int:
        """Gibt Anzahl Stop-Loss Trigger heute zurück."""
        today = self._get_today_key()
        return self.state.get(today, {}).get("stop_losses", 0)

    def add_stop_loss(self, symbol: str):
        """
        Registriert einen Stop-Loss Trigger.
        
        Args:
            symbol: Symbol des Trades
        """
        today = self._get_today_key()
        self._ensure_today_entry()

        self.state[today]["stop_losses"] += 1
        self.state[today]["symbols"].append(symbol)

        save_state(self.state_path, self.state)

        count = self.get_today_stop_loss_count()
        logger.warning(
            f"❌ STOP-LOSS triggered: {symbol} | Today total: {count}"
        )

    def is_daily_loss_limit_reached(self, max_stop_losses: int) -> bool:
        """
        Prüft, ob das tägliche Stop-Loss-Limit erreicht wurde.
        
        Args:
            max_stop_losses: Maximale Anzahl Stop-Loss Trigger pro Tag
        
        Returns:
            True wenn Limit erreicht
        """
        count = self.get_today_stop_loss_count()
        return count >= max_stop_losses

    def get_today_stats(self) -> Dict:
        """Gibt heutige Statistiken zurück."""
        today = self._get_today_key()
        return self.state.get(today, {
            "stop_losses": 0,
            "symbols": []
        })
