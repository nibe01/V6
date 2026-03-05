"""
NYSE Marktzeiten-Verwaltung.
Zentrale Quelle der Wahrheit für alle Marktzeiten-Checks im System.
"""
from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

from utils.logging_utils import get_logger

try:
    import holidays  # type: ignore
except Exception:  # pragma: no cover - fallback path
    holidays = None


logger = get_logger(__name__)

ET = ZoneInfo("America/New_York")

# Fallback-Feiertage (wenn holidays-Library nicht verfügbar ist)
NYSE_HOLIDAYS_FALLBACK = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18",
    "2025-05-26", "2025-07-04", "2025-09-01", "2025-11-27",
    "2025-11-28", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-07-03", "2026-09-07", "2026-11-26",
    "2026-12-25",
}

MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)


class MarketSchedule:
    """
    Prüft NYSE-Marktzeiten unter Berücksichtigung von Wochenenden und Feiertagen.
    Alle Zeitangaben in US Eastern Time (ET).
    """

    def __init__(self) -> None:
        self._holiday_calendar = None
        if holidays is not None:
            try:
                self._holiday_calendar = holidays.NYSE()  # type: ignore[attr-defined]
            except Exception as e:
                logger.warning(
                    "Could not initialize holidays.NYSE calendar, using fallback set: %s",
                    e,
                )
                self._holiday_calendar = None
        else:
            logger.warning(
                "holidays library not available, using static NYSE fallback holidays"
            )

    @staticmethod
    def _now_et() -> datetime:
        return datetime.now(ET)

    def _open_datetime(self, d: date) -> datetime:
        return datetime.combine(d, MARKET_OPEN, tzinfo=ET)

    def _close_datetime(self, d: date) -> datetime:
        return datetime.combine(d, MARKET_CLOSE, tzinfo=ET)

    def is_market_open(self) -> bool:
        """Gibt True zurück wenn der NYSE-Markt gerade geöffnet ist."""
        now = self._now_et()
        today = now.date()
        if not self.is_trading_day(today):
            return False
        return MARKET_OPEN <= now.time() < MARKET_CLOSE

    def is_active_trading_window(
        self,
        pre_market_start_minutes: float = 0.0,
        post_market_stop_minutes: float = 0.0,
    ) -> bool:
        """
        Gibt True zurück, wenn wir im konfigurierten Aktivitätsfenster sind.

        Aktivitätsfenster = [market_open - pre_market_start_minutes,
                            market_close + post_market_stop_minutes)
        """
        now = self._now_et()
        today = now.date()
        if not self.is_trading_day(today):
            return False

        open_dt = self._open_datetime(today)
        close_dt = self._close_datetime(today)
        start_dt = open_dt - timedelta(minutes=max(0.0, pre_market_start_minutes))
        stop_dt = close_dt + timedelta(minutes=max(0.0, post_market_stop_minutes))
        return start_dt <= now < stop_dt

    def seconds_until_active_window(
        self,
        pre_market_start_minutes: float = 0.0,
        post_market_stop_minutes: float = 0.0,
    ) -> float:
        """
        Sekunden bis zum nächsten Start des konfigurierten Aktivitätsfensters.
        0 wenn wir bereits im Fenster sind.
        """
        if self.is_active_trading_window(
            pre_market_start_minutes=pre_market_start_minutes,
            post_market_stop_minutes=post_market_stop_minutes,
        ):
            return 0.0

        now = self._now_et()
        today = now.date()
        start_today = self._open_datetime(today) - timedelta(
            minutes=max(0.0, pre_market_start_minutes)
        )

        if self.is_trading_day(today) and now < start_today:
            return max(0.0, (start_today - now).total_seconds())

        next_day = self._next_trading_day(today + timedelta(days=1))
        next_start = self._open_datetime(next_day) - timedelta(
            minutes=max(0.0, pre_market_start_minutes)
        )
        return max(0.0, (next_start - now).total_seconds())

    def is_trading_day(self, d: Optional[date] = None) -> bool:
        """Gibt True zurück wenn der angegebene Tag (default: heute) ein Handelstag ist."""
        day = d or self._now_et().date()
        if day.weekday() >= 5:
            return False
        if self._holiday_calendar is not None:
            return day not in self._holiday_calendar
        return day.isoformat() not in NYSE_HOLIDAYS_FALLBACK

    def _next_trading_day(self, from_day: date) -> date:
        probe = from_day
        for _ in range(370):
            if self.is_trading_day(probe):
                return probe
            probe += timedelta(days=1)
        return from_day

    def seconds_until_open(self) -> float:
        """Sekunden bis zur nächsten Marktöffnung. 0 wenn Markt gerade offen."""
        if self.is_market_open():
            return 0.0

        now = self._now_et()
        today = now.date()

        if self.is_trading_day(today) and now.time() < MARKET_OPEN:
            target = self._open_datetime(today)
            return max(0.0, (target - now).total_seconds())

        next_day = self._next_trading_day(today + timedelta(days=1))
        target = self._open_datetime(next_day)
        return max(0.0, (target - now).total_seconds())

    def seconds_until_close(self) -> float:
        """Sekunden bis zum Marktschluss. 0 wenn Markt bereits geschlossen."""
        if not self.is_market_open():
            return 0.0
        now = self._now_et()
        close_dt = self._close_datetime(now.date())
        return max(0.0, (close_dt - now).total_seconds())

    def just_opened(self, tolerance_seconds: float = 90.0) -> bool:
        """True wenn der Markt in den letzten `tolerance_seconds` geöffnet hat."""
        now = self._now_et()
        today = now.date()
        if not self.is_trading_day(today):
            return False
        open_dt = self._open_datetime(today)
        delta = (now - open_dt).total_seconds()
        return 0.0 <= delta <= max(0.0, tolerance_seconds)

    def just_closed(self, tolerance_seconds: float = 90.0) -> bool:
        """True wenn der Markt in den letzten `tolerance_seconds` geschlossen hat."""
        now = self._now_et()
        today = now.date()
        if not self.is_trading_day(today):
            return False
        close_dt = self._close_datetime(today)
        delta = (now - close_dt).total_seconds()
        return 0.0 <= delta <= max(0.0, tolerance_seconds)

    @staticmethod
    def _format_duration(seconds: float) -> str:
        total = int(max(0.0, seconds))
        hours, remainder = divmod(total, 3600)
        minutes, _ = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"

    def get_status_string(self) -> str:
        """Lesbarer Status-String für Logging, z.B. '🟢 OPEN (closes in 4h 22m)'"""
        if self.is_market_open():
            return f"🟢 OPEN (closes in {self._format_duration(self.seconds_until_close())})"
        return f"🔴 CLOSED (opens in {self._format_duration(self.seconds_until_open())})"
