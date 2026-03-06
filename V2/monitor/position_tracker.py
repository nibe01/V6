from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from ib_insync import IB

from utils.daily_loss_counter import DailyLossCounter
from utils.symbol_cooldown import SymbolCooldownManager
from utils.position_reconciliation import calculate_realized_pnl, resolve_exit_price
from utils.state_retry import save_state_with_retry
from utils.trade_status import (
    BOT_STATUS_CLOSED,
    BOT_STATUS_FILLED,
    BOT_STATUS_SUBMITTED,
    is_bot_filled_status,
)


POSITION_MISSING_CONFIRMATION_CHECKS = 3


class PositionTracker:
    def __init__(
        self,
        logger,
        loss_counter: DailyLossCounter,
        cooldown_manager: SymbolCooldownManager,
        state_path: Path,
    ) -> None:
        self.logger = logger
        self.loss_counter = loss_counter
        self.cooldown_manager = cooldown_manager
        self.state_path = state_path

    def check_filled_stop_losses(
        self,
        ib: IB,
        processed: Dict[str, dict],
        already_counted: set,
    ) -> set:
        """
        Prüft alle offenen Trades und erkennt gefüllte Stop-Loss Orders.
        """
        tracked_sl_orders = {}
        for _, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue
            sl_id = trade_data.get("sl_order_id")
            symbol = trade_data.get("symbol")
            if sl_id and symbol:
                tracked_sl_orders[sl_id] = symbol

        for trade in ib.trades():
            order_id = trade.order.orderId

            if order_id not in tracked_sl_orders:
                continue
            if order_id in already_counted:
                continue
            if trade.order.orderType != "STP":
                continue

            if trade.orderStatus.status == "Filled":
                symbol = tracked_sl_orders[order_id]
                self.loss_counter.add_stop_loss(symbol)
                self.cooldown_manager.add_cooldown(symbol, reason="stop_loss")
                already_counted.add(order_id)
                self.logger.warning(
                    "🔴 AUTO-DETECTED: Stop-Loss filled for %s (Order ID: %s)",
                    symbol,
                    order_id,
                )
                self.logger.trade("EXIT: %s | Reason: SL | Order: %s", symbol, order_id)

        return already_counted

    def update_position_status(
        self,
        ib: IB,
        processed: Dict[str, dict],
    ) -> None:
        """
        Aktualisiert den Status von Bot-Positionen basierend auf IB-Daten.
        """
        changed = False

        tracked_orders = {}
        for key, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue
            order_id = trade_data.get("order_id")
            symbol = trade_data.get("symbol")
            status = trade_data.get("status")

            if order_id and symbol:
                tracked_orders[order_id] = {
                    "key": key,
                    "symbol": symbol,
                    "current_status": status,
                }

        for trade in ib.trades():
            order_id = trade.order.orderId

            if order_id not in tracked_orders:
                continue

            info = tracked_orders[order_id]
            key = info["key"]
            current_status = info["current_status"]
            ib_status = trade.orderStatus.status

            if current_status == BOT_STATUS_SUBMITTED and ib_status == "Filled":
                processed[key]["status"] = BOT_STATUS_FILLED
                processed[key]["filled_at"] = datetime.now(timezone.utc).isoformat()
                self.logger.position("FILLED: %s | Order: %s", info["symbol"], order_id)
                changed = True

        current_positions = {p.contract.symbol for p in ib.positions() if p.position != 0}
        if current_positions or not self.cooldown_manager.has_cooldowns_by_reason("open_position"):
            self.cooldown_manager.clear_cooldowns_not_in_positions(
                current_positions,
                reason="open_position",
            )
        else:
            self.logger.debug("Skipping open_position cooldown cleanup: IB positions empty")

        for key, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue

            symbol = trade_data.get("symbol")
            status = trade_data.get("status")

            if is_bot_filled_status(status) and symbol in current_positions:
                if trade_data.pop("missing_position_checks", None) is not None:
                    changed = True
                continue

            if is_bot_filled_status(status) and symbol not in current_positions:
                missing_checks = int(trade_data.get("missing_position_checks", 0)) + 1
                trade_data["missing_position_checks"] = missing_checks
                changed = True

                if missing_checks < POSITION_MISSING_CONFIRMATION_CHECKS:
                    self.logger.debug(
                        "%s: position missing check %s/%s - waiting before close confirmation",
                        symbol,
                        missing_checks,
                        POSITION_MISSING_CONFIRMATION_CHECKS,
                    )
                    continue

                entry_price = trade_data.get("fill_price") or trade_data.get("entry_price", 0)
                quantity = trade_data.get("quantity", 0)

                exit_price = resolve_exit_price(
                    ib,
                    symbol,
                    require_confirmed_exit_fill=True,
                )

                if not (exit_price and exit_price > 0):
                    # Keep trade open until we can confirm an actual exit fill from IB.
                    if (
                        missing_checks == POSITION_MISSING_CONFIRMATION_CHECKS
                        or missing_checks % 20 == 0
                    ):
                        self.logger.warning(
                            "EXIT-CHECK: %s missing from IB positions but no confirmed "
                            "exit fill yet; keeping status as FILLED",
                            symbol,
                        )
                    continue

                if entry_price > 0 and quantity > 0:
                    pnl_usd, pnl_pct = calculate_realized_pnl(entry_price, exit_price, quantity)
                    processed[key]["exit_price"] = exit_price
                    processed[key]["realized_pnl_usd"] = pnl_usd
                    processed[key]["realized_pnl_pct"] = pnl_pct
                    self.logger.trade(
                        "EXIT: %s | Entry: $%.2f -> Exit: $%.2f | P&L: $%+.2f (%+.2f%%) | Qty: %s",
                        symbol,
                        entry_price,
                        exit_price,
                        pnl_usd,
                        pnl_pct,
                        quantity,
                    )
                else:
                    self.logger.trade(
                        "EXIT: %s | Exit: $%.2f | P&L not available (missing entry/qty)",
                        symbol,
                        exit_price,
                    )

                processed[key]["status"] = BOT_STATUS_CLOSED
                processed[key]["closed_at"] = datetime.now(timezone.utc).isoformat()
                processed[key].pop("missing_position_checks", None)
                self.logger.position("CLOSED: %s", symbol)
                self.cooldown_manager.clear_cooldown(symbol)
                self.cooldown_manager.clear_cooldowns_by_reason("insufficient_cash")
                changed = True

        if changed:
            save_state_with_retry(
                self.state_path,
                processed,
                max_retries=3,
                retry_delay=0.5,
            )

    def generate_daily_report(self, processed: dict) -> str:
        today = datetime.now(timezone.utc).date()
        closed_today: list[dict] = []

        for trade_data in processed.values():
            if not isinstance(trade_data, dict):
                continue
            closed_at = trade_data.get("closed_at")
            if not closed_at:
                continue

            try:
                closed_dt = datetime.fromisoformat(str(closed_at).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            if closed_dt.astimezone(timezone.utc).date() == today:
                closed_today.append(trade_data)

        trade_count = len(closed_today)
        if trade_count == 0:
            return "EOD Report: no closed trades today."

        pnls = [float(t.get("realized_pnl_usd", 0.0) or 0.0) for t in closed_today]
        wins = [p for p in pnls if p > 0]
        total_pnl = sum(pnls)
        win_rate = (len(wins) / trade_count) * 100.0
        best_trade = max(pnls)
        worst_trade = min(pnls)

        return (
            "EOD Report | "
            f"Trades: {trade_count} | "
            f"Win Rate: {win_rate:.1f}% | "
            f"Total P&L: ${total_pnl:+.2f} | "
            f"Best: ${best_trade:+.2f} | "
            f"Worst: ${worst_trade:+.2f}"
        )
