"""
Symbol Cooldown Manager
Prevents repeated trades in the same symbol after stop-loss.
Implements time-based cooldowns with automatic cleanup.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Set

from utils.logging_utils import get_logger
from utils.state_utils import load_state, save_state

logger = get_logger(__name__)


class SymbolCooldownManager:
    """
    Manages cooldowns for symbols after stop-loss.

    Persists to state/symbol_cooldowns.json:
    {
        "AAPL": {
            "cooldown_until": "2026-02-08T15:30:00+00:00",
            "reason": "stop_loss",
            "sl_count": 2
        }
    }
    """

    def __init__(
        self,
        state_path: Path,
        cooldown_minutes: int = 60,
    ):
        """
        Args:
            state_path: Path to symbol_cooldowns.json
            cooldown_minutes: Cooldown duration in minutes after stop-loss
        """
        self.state_path = state_path
        self.log_interval_seconds = 60
        self.cooldown_minutes = cooldown_minutes
        self.state: Dict[str, dict] = load_state(state_path)

        logger.info(
            f"Symbol Cooldown Manager initialized: {cooldown_minutes} min cooldown"
        )

    def add_cooldown(self, symbol: str, reason: str = "stop_loss") -> None:
        """
        Adds cooldown for a symbol.

        Args:
            symbol: Symbol to place on cooldown
            reason: Reason for cooldown (e.g. "stop_loss")
        """
        now = datetime.now(timezone.utc)

        old_count = 0
        if symbol in self.state:
            old_count = self.state[symbol].get("sl_count", 0)

        if reason == "open_position":
            self.state[symbol] = {
                "cooldown_until": None,
                "reason": reason,
                "sl_count": old_count,
                "last_log_at": now.isoformat(),
            }
        else:
            cooldown_until = now + timedelta(minutes=self.cooldown_minutes)
            self.state[symbol] = {
                "cooldown_until": cooldown_until.isoformat(),
                "reason": reason,
                "sl_count": old_count + 1,
                "last_log_at": now.isoformat(),
            }

        save_state(self.state_path, self.state)

        if reason == "open_position":
            logger.warning("COOLDOWN: %s blocked while position is open", symbol)
        else:
            logger.warning(
                "COOLDOWN: %s blocked until %s (%s min) | SL Count: %s",
                symbol,
                cooldown_until.strftime("%H:%M:%S"),
                self.cooldown_minutes,
                old_count + 1,
            )

    def is_on_cooldown(self, symbol: str) -> tuple[bool, Optional[str]]:
        """
        Checks whether a symbol is on cooldown.

        Args:
            symbol: Symbol to check
        Returns:
            (is_on_cooldown: bool, reason: Optional[str])
        """
        if symbol not in self.state:
            return False, None

        cooldown_data = self.state[symbol]
        cooldown_until_str = cooldown_data.get("cooldown_until")
        now = datetime.now(timezone.utc)

        if cooldown_until_str in (None, ""):
            reason = cooldown_data.get("reason", "unknown")
            if reason != "open_position":
                return False, None

            last_log_at = cooldown_data.get("last_log_at")
            if last_log_at:
                try:
                    last_log_time = datetime.fromisoformat(last_log_at)
                    if (now - last_log_time).total_seconds() < self.log_interval_seconds:
                        return True, None
                except (ValueError, TypeError):
                    pass

            cooldown_data["last_log_at"] = now.isoformat()
            save_state(self.state_path, self.state)
            return True, "open position cooldown (active while position is open)"

        try:
            cooldown_until = datetime.fromisoformat(cooldown_until_str)
        except (ValueError, TypeError):
            del self.state[symbol]
            save_state(self.state_path, self.state)
            return False, None

        if now < cooldown_until:
            last_log_at = cooldown_data.get("last_log_at")
            if last_log_at:
                try:
                    last_log_time = datetime.fromisoformat(last_log_at)
                    if (now - last_log_time).total_seconds() < self.log_interval_seconds:
                        return True, None
                except (ValueError, TypeError):
                    pass

            remaining_seconds = (cooldown_until - now).total_seconds()
            remaining_minutes = int(remaining_seconds / 60)
            reason = cooldown_data.get("reason", "unknown")
            sl_count = cooldown_data.get("sl_count", 0)

            cooldown_data["last_log_at"] = now.isoformat()
            save_state(self.state_path, self.state)

            return True, (
                f"{reason} cooldown (expires in {remaining_minutes} min, "
                f"SL count: {sl_count})"
            )

        return False, None

    def cleanup_expired(self) -> int:
        """
        Removes expired cooldowns from state.

        Returns:
            Number of cooldowns removed
        """
        now = datetime.now(timezone.utc)
        expired = []

        for symbol, data in self.state.items():
            cooldown_until_str = data.get("cooldown_until")
            if cooldown_until_str in (None, ""):
                if data.get("reason") == "open_position":
                    continue
                expired.append(symbol)
                continue

            try:
                cooldown_until = datetime.fromisoformat(cooldown_until_str)
                if now >= cooldown_until:
                    expired.append(symbol)
            except (ValueError, TypeError):
                expired.append(symbol)

        for symbol in expired:
            del self.state[symbol]

        if expired:
            save_state(self.state_path, self.state)
            logger.debug("Cleaned up %s expired cooldowns", len(expired))

        return len(expired)

    def clear_cooldown(self, symbol: str) -> bool:
        """
        Removes cooldown for a specific symbol.

        Returns:
            True if a cooldown was removed
        """
        if symbol not in self.state:
            return False

        del self.state[symbol]
        save_state(self.state_path, self.state)
        logger.info("Cooldown cleared for %s", symbol)
        return True

    def clear_cooldowns_by_reason(self, reason: str) -> int:
        """
        Removes all cooldowns for a given reason.

        Returns:
            Number of cooldowns removed
        """
        to_remove = [s for s, d in self.state.items() if d.get("reason") == reason]
        for symbol in to_remove:
            del self.state[symbol]

        if to_remove:
            save_state(self.state_path, self.state)
            logger.info(
                "Cleared %s cooldown(s) with reason '%s'", len(to_remove), reason
            )

        return len(to_remove)

    def get_cooldown_info(self, symbol: str) -> Optional[dict]:
        """
        Returns cooldown info for a symbol.

        Returns:
            Dict with cooldown_until, reason, sl_count or None
        """
        return self.state.get(symbol)

    def has_cooldowns_by_reason(self, reason: str) -> bool:
        """Check if any cooldowns exist for a given reason."""
        return any(d.get("reason") == reason for d in self.state.values())

    def clear_cooldowns_not_in_positions(
        self,
        open_symbols: Set[str],
        reason: str = "open_position",
    ) -> int:
        """Remove cooldowns for symbols not in the open positions set."""
        to_remove = [
            s for s, d in self.state.items() if d.get("reason") == reason and s not in open_symbols
        ]

        for symbol in to_remove:
            del self.state[symbol]

        if to_remove:
            save_state(self.state_path, self.state)
            logger.info(
                "Cleared %s cooldown(s) with reason '%s'", len(to_remove), reason
            )

        return len(to_remove)

    def log_active_cooldowns(self) -> None:
        """Logs all active cooldowns."""
        now = datetime.now(timezone.utc)
        active = []

        for symbol, data in self.state.items():
            cooldown_until_str = data.get("cooldown_until")
            if cooldown_until_str in (None, ""):
                if data.get("reason") == "open_position":
                    active.append(f"{symbol} (open)")
                continue

            try:
                cooldown_until = datetime.fromisoformat(cooldown_until_str)
                if now < cooldown_until:
                    remaining_min = int(
                        (cooldown_until - now).total_seconds() / 60
                    )
                    active.append(f"{symbol} ({remaining_min}m)")
            except (ValueError, TypeError):
                continue

        if active:
            logger.info("Active Cooldowns (%s): %s", len(active), ", ".join(active))
        else:
            logger.debug("No active cooldowns")
