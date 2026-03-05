"""
Trading Dashboard
Shows live positions, today's performance, and trade history.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from ib_insync import IB

from utils.logging_utils import get_logger

logger = get_logger(__name__)


class TradingDashboard:
    """
    Dashboard for trading overview.

    Shows:
    - Live positions with unrealized P&L
    - Today's performance statistics
    - Closed positions history
    """

    def __init__(self) -> None:
        self.last_update = datetime.now(timezone.utc)

    def generate_dashboard(
        self,
        ib: IB,
        processed: Dict[str, dict],
        account_info: Optional[dict] = None,
    ) -> str:
        """
        Generate full dashboard string.

        Args:
            ib: IB connection
            processed: Bot state dict
            account_info: Optional account info

        Returns:
            Dashboard as formatted string
        """
        lines = []

        # Header
        now = datetime.now(timezone.utc)
        lines.append("=" * 80)
        lines.append(f"TRADING DASHBOARD - {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append("=" * 80)

        # Account status (if available)
        if account_info:
            balance = account_info.get("net_liquidation", 0)
            buying_power = account_info.get("buying_power", 0)
            total_cash = account_info.get("total_cash_value", 0)
            lines.append(
                f"Account Balance: ${balance:,.2f} | "
                f"Cash Available: ${total_cash:,.2f} | "
                f"Buying Power: ${buying_power:,.2f}"
            )
            lines.append("-" * 80)

        # Section 1: Live Positions
        live_section = self._generate_live_positions(ib, processed)
        lines.append(live_section)

        # Section 2: Today's Performance
        today_section = self._generate_today_performance(processed)
        lines.append(today_section)

        # Section 3: Closed Positions
        closed_section = self._generate_closed_positions(processed, limit=100)
        lines.append(closed_section)

        lines.append("=" * 80)

        self.last_update = now
        return "\n".join(lines)

    def _generate_live_positions(self, ib: IB, processed: Dict[str, dict]) -> str:
        """Generate live positions section."""
        lines = []
        lines.append("")
        lines.append("OPEN POSITIONS")
        lines.append("-" * 80)

        # Collect IB portfolio data for market values and P&L
        ib_positions = {}
        try:
            for item in ib.portfolio():
                symbol = item.contract.symbol
                if item.position != 0:
                    ib_positions[symbol] = {
                        "market_price": float(item.marketPrice)
                        if item.marketPrice
                        else 0.0,
                        "market_value": float(item.marketValue)
                        if item.marketValue
                        else 0.0,
                        "unrealized_pnl": float(item.unrealizedPNL)
                        if item.unrealizedPNL
                        else 0.0,
                        "quantity": int(item.position),
                    }
        except Exception as e:
            logger.warning(f"Could not fetch IB portfolio: {e}")

        # Collect open bot + manual positions
        open_positions = []
        for _, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue

            status = trade_data.get("status")
            if status in {"filled", "manual"}:
                symbol = trade_data.get("symbol")
                entry_price = trade_data.get("fill_price") or trade_data.get(
                    "entry_price", 0
                )
                quantity = trade_data.get("quantity", 0)
                opened_at = (
                    trade_data.get("opened_at")
                    or trade_data.get("filled_at")
                    or trade_data.get("signal_timestamp")
                    or trade_data.get("processed_at", "")
                )
                source = "BOT"

                if status == "manual":
                    entry_price = trade_data.get("avg_cost", entry_price)
                    opened_at = (
                        trade_data.get("opened_at")
                        or trade_data.get("first_seen_at")
                        or trade_data.get("processed_at", "")
                    )
                    source = "MANUAL"

                # Pull market price from IB
                ib_pos = ib_positions.get(symbol, {})
                current_price = ib_pos.get("market_price", entry_price)
                unrealized_pnl = ib_pos.get("unrealized_pnl", 0.0)

                # Calculate duration
                duration = self._calculate_duration(opened_at)

                # Calculate P&L if IB is unavailable
                if unrealized_pnl == 0.0 and current_price > 0 and entry_price > 0:
                    unrealized_pnl = (current_price - entry_price) * quantity

                pnl_pct = (
                    (current_price - entry_price) / entry_price * 100
                    if entry_price > 0
                    else 0.0
                )

                open_positions.append(
                    {
                        "symbol": symbol,
                        "entry_price": entry_price,
                        "current_price": current_price,
                        "quantity": quantity,
                        "unrealized_pnl": unrealized_pnl,
                        "pnl_pct": pnl_pct,
                        "duration": duration,
                        "source": source,
                    }
                )

        if not open_positions:
            lines.append("No open positions.")
        else:
            # Header
            lines.append(
                f"{'Symbol':<8} {'Entry':>10} {'Current':>10} {'Qty':>6} "
                f"{'P&L':>12} {'%':>8} {'Duration':>10} {'Src':>6}"
            )
            lines.append("-" * 80)

            # Positions
            total_pnl = 0.0
            for pos in sorted(
                open_positions, key=lambda x: x["unrealized_pnl"], reverse=True
            ):
                pnl_sign = "+" if pos["unrealized_pnl"] >= 0 else ""
                pct_sign = "+" if pos["pnl_pct"] >= 0 else ""

                lines.append(
                    f"{pos['symbol']:<8} "
                    f"${pos['entry_price']:>9.2f} "
                    f"${pos['current_price']:>9.2f} "
                    f"{pos['quantity']:>6} "
                    f"{pnl_sign}${pos['unrealized_pnl']:>10.2f} "
                    f"{pct_sign}{pos['pnl_pct']:>6.2f}% "
                    f"{pos['duration']:>10} "
                    f"{pos['source']:>6}"
                )
                total_pnl += pos["unrealized_pnl"]

            lines.append("-" * 80)
            total_pnl_sign = "+" if total_pnl >= 0 else ""
            lines.append(
                f"{'Total:':<8} {'':>10} {'':>10} "
                f"{len(open_positions):>6} "
                f"{total_pnl_sign}${total_pnl:>10.2f} {'':>8} {'':>10} {'':>6}"
            )

        return "\n".join(lines)

    def _generate_today_performance(self, processed: Dict[str, dict]) -> str:
        """Generate today's performance section."""
        lines = []
        lines.append("")
        lines.append("TODAY'S PERFORMANCE")
        lines.append("-" * 80)

        today = datetime.now(timezone.utc).date().isoformat()

        # Collect today's trades
        today_trades = []
        for _, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue

            processed_at = trade_data.get("processed_at", "")
            if processed_at.startswith(today):
                today_trades.append(trade_data)

        if not today_trades:
            lines.append("No trades today.")
            return "\n".join(lines)

        # Compute stats
        total_trades = len(today_trades)
        closed_trades = [t for t in today_trades if t.get("status") == "closed"]
        open_trades = [t for t in today_trades if t.get("status") == "filled"]

        winners = []
        losers = []
        total_pnl = 0.0

        for trade in closed_trades:
            pnl = trade.get("realized_pnl_usd", 0.0)
            total_pnl += pnl

            if pnl > 0:
                winners.append(trade)
            elif pnl < 0:
                losers.append(trade)

        num_closed = len(closed_trades)
        num_winners = len(winners)
        num_losers = len(losers)
        num_open = len(open_trades)

        win_rate = (num_winners / num_closed * 100) if num_closed > 0 else 0.0

        avg_win = (
            sum(t.get("realized_pnl_usd", 0) for t in winners) / len(winners)
            if winners
            else 0.0
        )
        avg_loss = (
            sum(t.get("realized_pnl_usd", 0) for t in losers) / len(losers)
            if losers
            else 0.0
        )

        best_trade = (
            max(winners, key=lambda x: x.get("realized_pnl_usd", 0))
            if winners
            else None
        )
        worst_trade = (
            min(losers, key=lambda x: x.get("realized_pnl_usd", 0))
            if losers
            else None
        )

        # Output
        lines.append(f"Trades Executed:        {total_trades}")
        if num_closed > 0:
            lines.append(
                f"├─ Winners (TP):        {num_winners}  "
                f"({num_winners / num_closed * 100:.1f}% of closed)"
            )
            lines.append(
                f"├─ Losers (SL):         {num_losers}  "
                f"({num_losers / num_closed * 100:.1f}% of closed)"
            )
        else:
            lines.append(f"├─ Winners (TP):        {num_winners}")
            lines.append(f"├─ Losers (SL):         {num_losers}")

        if total_trades > 0:
            lines.append(
                f"└─ Still Open:          {num_open}  "
                f"({num_open / total_trades * 100:.1f}%)"
            )
        else:
            lines.append(f"└─ Still Open:          {num_open}")
        lines.append("")

        pnl_sign = "+" if total_pnl >= 0 else ""
        lines.append(f"Realized P&L:           {pnl_sign}${total_pnl:.2f}")

        if winners:
            total_wins = sum(t.get("realized_pnl_usd", 0) for t in winners)
            lines.append(f"├─ Winners Total:       +${total_wins:.2f}")
        if losers:
            total_losses = abs(sum(t.get("realized_pnl_usd", 0) for t in losers))
            lines.append(f"└─ Losers Total:        -${total_losses:.2f}")

        lines.append("")
        if num_closed > 0:
            lines.append(
                f"Win Rate:               {win_rate:.1f}%  "
                f"({num_winners}/{num_closed} closed)"
            )
        else:
            lines.append("Win Rate:               N/A")

        if avg_win != 0 and avg_loss != 0:
            lines.append(f"Average Win:            +${avg_win:.2f}")
            lines.append(f"Average Loss:           ${avg_loss:.2f}")

        if best_trade:
            best_symbol = best_trade.get("symbol", "?")
            best_pnl = best_trade.get("realized_pnl_usd", 0)
            best_pct = best_trade.get("realized_pnl_pct", 0)
            lines.append(
                f"Best Trade:             {best_symbol}  "
                f"+${best_pnl:.2f}  (+{best_pct:.1f}%)"
            )

        if worst_trade:
            worst_symbol = worst_trade.get("symbol", "?")
            worst_pnl = worst_trade.get("realized_pnl_usd", 0)
            worst_pct = worst_trade.get("realized_pnl_pct", 0)
            lines.append(
                f"Worst Trade:            {worst_symbol}  "
                f"${worst_pnl:.2f}  ({worst_pct:.1f}%)"
            )

        return "\n".join(lines)

    def _generate_closed_positions(
        self, processed: Dict[str, dict], limit: int = 100
    ) -> str:
        """Generate closed positions section."""
        lines = []
        lines.append("")
        lines.append(f"CLOSED POSITIONS - Last {limit} Trades")
        lines.append("-" * 80)

        # Collect closed trades
        closed_trades = []
        for _, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue

            if trade_data.get("status") == "closed":
                closed_at = trade_data.get("closed_at", "")
                closed_trades.append(
                    {
                        "time": closed_at,
                        "symbol": trade_data.get("symbol", "?"),
                        "quantity": trade_data.get("quantity", 0),
                        "entry_price": trade_data.get("fill_price")
                        or trade_data.get("entry_price", 0),
                        "exit_price": trade_data.get("exit_price", 0),
                        "pnl_usd": trade_data.get("realized_pnl_usd", 0.0),
                        "pnl_pct": trade_data.get("realized_pnl_pct", 0.0),
                        "duration": self._calculate_duration(
                            trade_data.get("opened_at")
                            or trade_data.get("filled_at")
                            or trade_data.get("signal_timestamp")
                            or trade_data.get("processed_at", ""),
                            trade_data.get("closed_at", ""),
                        ),
                        "close_reason": trade_data.get("close_reason", "unknown"),
                    }
                )

        if not closed_trades:
            lines.append("No closed positions yet.")
            return "\n".join(lines)

        # Sort by time (newest first)
        closed_trades.sort(key=lambda x: x["time"], reverse=True)

        # Limit count
        closed_trades = closed_trades[:limit]

        # Header
        lines.append(
            f"{'Time':<10} {'Symbol':<8} {'Qty':>5} {'Entry':>9} {'Exit':>9} "
            f"{'P&L':>12} {'%':>8} {'Duration':>9}"
        )
        lines.append("-" * 80)

        # Trades
        for trade in closed_trades:
            time_str = (
                trade["time"][11:19]
                if len(trade["time"]) >= 19
                else trade["time"][:10]
            )

            pnl_sign = "+" if trade["pnl_usd"] >= 0 else ""
            pct_sign = "+" if trade["pnl_pct"] >= 0 else ""

            # Exit emoji
            exit_emoji = ""
            if trade["pnl_usd"] > 0:
                exit_emoji = "💚"
            elif trade["pnl_usd"] < 0:
                exit_emoji = "🔴"

            lines.append(
                f"{time_str:<10} "
                f"{trade['symbol']:<8} "
                f"{trade['quantity']:>5} "
                f"${trade['entry_price']:>8.2f} "
                f"${trade['exit_price']:>8.2f} "
                f"{pnl_sign}${trade['pnl_usd']:>10.2f} "
                f"{pct_sign}{trade['pnl_pct']:>6.2f}% "
                f"{trade['duration']:>9} {exit_emoji}"
            )

        return "\n".join(lines)

    def _calculate_duration(self, start_time: str, end_time: str = "") -> str:
        """
        Calculate duration between two timestamps.

        Args:
            start_time: ISO timestamp
            end_time: ISO timestamp (if empty, now is used)

        Returns:
            Duration string (e.g. "2h 15m")
        """
        if not start_time:
            return "?"

        try:
            start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))

            if end_time:
                end = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            else:
                end = datetime.now(timezone.utc)

            delta = end - start
            total_seconds = int(delta.total_seconds())

            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60

            if hours > 0:
                return f"{hours}h {minutes:02d}m"
            return f"{minutes}m"

        except Exception:
            return "?"

    def log_dashboard(
        self,
        ib: IB,
        processed: Dict[str, dict],
        account_info: Optional[dict] = None,
    ) -> None:
        """Log dashboard to performance log."""
        dashboard_text = self.generate_dashboard(ib, processed, account_info)

        # Split into lines and log each separately
        for line in dashboard_text.split("\n"):
            logger.performance(line)


def create_dashboard() -> TradingDashboard:
    """Factory function for dashboard."""
    return TradingDashboard()
