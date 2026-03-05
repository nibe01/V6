"""
Position Reconciliation
Synchronizes bot state with IB positions and corrects drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ib_insync import IB

from utils.logging_utils import get_logger
from utils.trade_status import (
    BOT_STATUS_CLOSED,
    BOT_STATUS_MANUAL,
    BOT_STATUS_MANUAL_CLOSED,
    is_bot_active_status,
    is_bot_filled_status,
    is_manual_status,
)

logger = get_logger(__name__)


RECON_MISSING_IN_IB_CONFIRMATION_CHECKS = 3


@dataclass
class PositionDiscrepancy:
    """Describes a mismatch between bot state and IB."""

    symbol: str
    discrepancy_type: str  # missing_in_state, missing_in_ib, quantity_mismatch, orphaned_tp_sl
    bot_state: Optional[dict] = None
    ib_position: Optional[dict] = None
    severity: str = "WARNING"  # INFO, WARNING, ERROR
    suggested_action: str = ""


class PositionReconciliator:
    """
    Reconciliation manager for bot state vs. IB positions.

    Detects mismatches and applies corrections if enabled.
    """

    def __init__(self) -> None:
        self.last_reconciliation = datetime.now(timezone.utc)
        self.total_reconciliations = 0
        self.total_discrepancies_found = 0
        self.total_corrections_applied = 0

    def reconcile_positions(
        self,
        ib: IB,
        processed: Dict[str, dict],
        auto_correct: bool = True,
    ) -> List[PositionDiscrepancy]:
        """
        Run full reconciliation.

        Args:
            ib: IB connection
            processed: Bot state dict
            auto_correct: If True, apply automatic corrections

        Returns:
            List of discrepancies
        """
        self.total_reconciliations += 1
        self.last_reconciliation = datetime.now(timezone.utc)

        discrepancies: List[PositionDiscrepancy] = []

        # Step 1: IB positions
        ib_positions = self._get_ib_positions_dict(ib)

        # Step 1b: Sync manual positions with IB data
        self._sync_manual_positions(processed, ib_positions)

        # Step 2: Bot active positions
        bot_positions = self._get_bot_active_positions(processed)

        # Step 2b: Record manual positions before discrepancy checks
        self._ensure_manual_entries(ib, processed, ib_positions, bot_positions)

        # Step 3: Bot positions missing in IB
        for symbol, bot_data in bot_positions.items():
            if symbol in ib_positions:
                bot_data.pop("missing_in_ib_checks", None)
                continue

            missing_checks = int(bot_data.get("missing_in_ib_checks", 0)) + 1
            bot_data["missing_in_ib_checks"] = missing_checks

            if missing_checks < RECON_MISSING_IN_IB_CONFIRMATION_CHECKS:
                logger.debug(
                    "%s: reconciliation missing-in-IB check %s/%s - waiting before auto-close",
                    symbol,
                    missing_checks,
                    RECON_MISSING_IN_IB_CONFIRMATION_CHECKS,
                )
                continue

            if symbol not in ib_positions:
                discrepancy = PositionDiscrepancy(
                    symbol=symbol,
                    discrepancy_type="missing_in_ib",
                    bot_state=bot_data,
                    ib_position=None,
                    severity="WARNING",
                    suggested_action=(
                        "Mark position as closed (TP/SL hit or manual close)"
                    ),
                )
                discrepancies.append(discrepancy)
                self.total_discrepancies_found += 1

        # Step 4: IB positions missing in bot state
        for symbol, ib_pos in ib_positions.items():
            if symbol not in bot_positions:
                if self._has_manual_position(processed, symbol):
                    continue

                has_any_bot_history = any(
                    t.get("symbol") == symbol
                    for t in processed.values()
                    if isinstance(t, dict)
                )

                if has_any_bot_history:
                    severity = "INFO"
                    action = "Likely reopened position or manual trade"
                else:
                    severity = "WARNING"
                    action = "Manual trade detected (not tracked by bot)"

                discrepancy = PositionDiscrepancy(
                    symbol=symbol,
                    discrepancy_type="missing_in_state",
                    bot_state=None,
                    ib_position=ib_pos,
                    severity=severity,
                    suggested_action=action,
                )
                discrepancies.append(discrepancy)
                self.total_discrepancies_found += 1

        # Step 5: Quantity mismatches
        for symbol in set(bot_positions.keys()) & set(ib_positions.keys()):
            bot_qty = bot_positions[symbol].get("quantity", 0)
            ib_qty = ib_positions[symbol]["quantity"]

            if bot_qty != ib_qty:
                discrepancy = PositionDiscrepancy(
                    symbol=symbol,
                    discrepancy_type="quantity_mismatch",
                    bot_state=bot_positions[symbol],
                    ib_position=ib_positions[symbol],
                    severity="ERROR",
                    suggested_action=f"Update bot qty from {bot_qty} to {ib_qty}",
                )
                discrepancies.append(discrepancy)
                self.total_discrepancies_found += 1

        # Step 6: TP/SL orders
        orphaned_orders = self._check_orphaned_orders(ib, bot_positions)
        discrepancies.extend(orphaned_orders)

        # Step 7: Auto-correct if enabled
        if auto_correct and discrepancies:
            self._apply_corrections(discrepancies, processed, ib)

        return discrepancies

    def _has_manual_position(self, processed: Dict[str, dict], symbol: str) -> bool:
        """Check if a manual position entry exists for a symbol."""
        for trade_data in processed.values():
            if not isinstance(trade_data, dict):
                continue
            if trade_data.get("symbol") != symbol:
                continue
            if is_manual_status(trade_data.get("status")):
                return True
        return False

    def _ensure_manual_entries(
        self,
        ib: IB,
        processed: Dict[str, dict],
        ib_positions: Dict[str, dict],
        bot_positions: Dict[str, dict],
    ) -> None:
        """Ensure manual entries exist for IB positions not tracked by the bot."""
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")

        for symbol, ib_pos in ib_positions.items():
            if symbol in bot_positions:
                continue
            if self._has_manual_position(processed, symbol):
                continue

            manual_key = f"manual_{symbol}"
            opened_at = self._infer_position_opened_at_from_ib(ib, symbol) or now
            processed[manual_key] = {
                "symbol": symbol,
                "processed_at": now,
                "first_seen_at": now,
                "last_seen_at": now,
                "opened_at": opened_at,
                "status": BOT_STATUS_MANUAL,
                "note": "detected from IB positions",
                "quantity": ib_pos.get("quantity", 0),
                "avg_cost": ib_pos.get("avg_cost", 0.0),
                "market_value": ib_pos.get("market_value", 0.0),
                "unrealized_pnl": ib_pos.get("unrealized_pnl", 0.0),
            }

    def _infer_position_opened_at_from_ib(self, ib: IB, symbol: str) -> Optional[str]:
        """Best-effort timestamp for when a position was opened (latest BUY fill)."""
        latest_buy_fill_time: Optional[datetime] = None

        for trade in ib.trades():
            contract = getattr(trade, "contract", None)
            if not contract or getattr(contract, "symbol", None) != symbol:
                continue

            order = getattr(trade, "order", None)
            action = str(getattr(order, "action", "")).upper()
            if action != "BUY":
                continue

            fills = getattr(trade, "fills", None) or []
            for fill in fills:
                execution = getattr(fill, "execution", None)
                if execution is None:
                    continue

                shares = int(getattr(execution, "shares", 0) or 0)
                exec_time = getattr(execution, "time", None)
                if shares <= 0 or not isinstance(exec_time, datetime):
                    continue

                if latest_buy_fill_time is None or exec_time > latest_buy_fill_time:
                    latest_buy_fill_time = exec_time

        if latest_buy_fill_time is None:
            return None

        if latest_buy_fill_time.tzinfo is None:
            latest_buy_fill_time = latest_buy_fill_time.replace(tzinfo=timezone.utc)

        return latest_buy_fill_time.astimezone(timezone.utc).isoformat(timespec="microseconds")

    def _sync_manual_positions(
        self,
        processed: Dict[str, dict],
        ib_positions: Dict[str, dict],
    ) -> None:
        """Update manual position entries with latest IB data."""
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")

        for trade_data in processed.values():
            if not isinstance(trade_data, dict):
                continue

            if not is_manual_status(trade_data.get("status")):
                continue

            symbol = trade_data.get("symbol")
            if not symbol:
                continue

            ib_pos = ib_positions.get(symbol)
            if ib_pos:
                trade_data["quantity"] = ib_pos.get("quantity", 0)
                trade_data["avg_cost"] = ib_pos.get("avg_cost", 0.0)
                trade_data["market_value"] = ib_pos.get("market_value", 0.0)
                trade_data["unrealized_pnl"] = ib_pos.get("unrealized_pnl", 0.0)
                trade_data["last_seen_at"] = now
                if not trade_data.get("opened_at"):
                    trade_data["opened_at"] = (
                        trade_data.get("first_seen_at")
                        or trade_data.get("processed_at")
                        or now
                    )
            else:
                trade_data["status"] = BOT_STATUS_MANUAL_CLOSED
                trade_data["closed_at"] = now

    def _get_ib_positions_dict(self, ib: IB) -> Dict[str, dict]:
        """Fetch IB positions as a dict.

        Uses `ib.positions()` as primary source for existence/quantity because it is
        typically more reliable for fast state checks than portfolio snapshots.
        Portfolio values are merged in when available.
        """
        positions: Dict[str, dict] = {}

        # Primary truth for open positions
        for pos in ib.positions():
            symbol = getattr(getattr(pos, "contract", None), "symbol", None)
            qty = int(getattr(pos, "position", 0) or 0)
            if not symbol or qty == 0:
                continue

            positions[symbol] = {
                "symbol": symbol,
                "quantity": abs(qty),
                "avg_cost": 0.0,
                "market_value": 0.0,
                "unrealized_pnl": 0.0,
            }

        # Enrich with valuation data from portfolio when available
        for item in ib.portfolio():
            symbol = getattr(getattr(item, "contract", None), "symbol", None)
            qty = int(getattr(item, "position", 0) or 0)
            if not symbol or qty == 0:
                continue

            existing = positions.get(symbol)
            if existing is None:
                existing = {
                    "symbol": symbol,
                    "quantity": abs(qty),
                    "avg_cost": 0.0,
                    "market_value": 0.0,
                    "unrealized_pnl": 0.0,
                }
                positions[symbol] = existing

            existing["avg_cost"] = (
                float(item.averageCost) if getattr(item, "averageCost", None) else existing["avg_cost"]
            )
            existing["market_value"] = (
                float(item.marketValue) if getattr(item, "marketValue", None) else existing["market_value"]
            )
            existing["unrealized_pnl"] = (
                float(item.unrealizedPNL)
                if getattr(item, "unrealizedPNL", None)
                else existing["unrealized_pnl"]
            )

        return positions

    def _get_bot_active_positions(self, processed: Dict[str, dict]) -> Dict[str, dict]:
        """Fetch active bot positions."""
        active: Dict[str, dict] = {}
        for _, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue

            status = trade_data.get("status")
            symbol = trade_data.get("symbol")

            if is_bot_active_status(status) and symbol:
                active[symbol] = trade_data

        return active

    def _check_orphaned_orders(
        self, ib: IB, bot_positions: Dict[str, dict]
    ) -> List[PositionDiscrepancy]:
        """Check if TP/SL orders exist for bot positions."""
        discrepancies: List[PositionDiscrepancy] = []

        open_orders_by_symbol: Dict[str, dict] = {}
        for trade in ib.openTrades():
            symbol = trade.contract.symbol
            order_type = trade.order.orderType

            if symbol not in open_orders_by_symbol:
                open_orders_by_symbol[symbol] = {"TP": False, "SL": False}

            if (
                order_type == "LMT"
                and hasattr(trade.order, "action")
                and trade.order.action == "SELL"
            ):
                open_orders_by_symbol[symbol]["TP"] = True
            elif order_type == "STP":
                open_orders_by_symbol[symbol]["SL"] = True

        for symbol, bot_data in bot_positions.items():
            orders = open_orders_by_symbol.get(symbol, {"TP": False, "SL": False})

            if not orders["TP"] or not orders["SL"]:
                missing = []
                if not orders["TP"]:
                    missing.append("TP")
                if not orders["SL"]:
                    missing.append("SL")

                discrepancy = PositionDiscrepancy(
                    symbol=symbol,
                    discrepancy_type="orphaned_tp_sl",
                    bot_state=bot_data,
                    severity="ERROR",
                    suggested_action=(
                        f"Missing {'/'.join(missing)} order(s) - manual intervention required"
                    ),
                )
                discrepancies.append(discrepancy)
                self.total_discrepancies_found += 1

        return discrepancies

    def _apply_corrections(
        self,
        discrepancies: List[PositionDiscrepancy],
        processed: Dict[str, dict],
        ib: IB,
    ) -> int:
        """Apply automatic corrections."""
        corrections_applied = 0

        for disc in discrepancies:
            if disc.discrepancy_type == "missing_in_ib":
                success = self._mark_position_closed(
                    ib, disc.symbol, processed, reason="reconciliation"
                )
                if success:
                    logger.info(
                        f"AUTO-CORRECTED: {disc.symbol} marked as closed (missing in IB)"
                    )
                    corrections_applied += 1

            elif disc.discrepancy_type == "quantity_mismatch":
                success = self._update_position_quantity(
                    disc.symbol, processed, disc.ib_position["quantity"]
                )
                if success:
                    logger.info(
                        f"AUTO-CORRECTED: {disc.symbol} quantity updated to "
                        f"{disc.ib_position['quantity']}"
                    )
                    corrections_applied += 1

            elif disc.discrepancy_type == "orphaned_tp_sl":
                logger.error(
                    f"MANUAL ACTION REQUIRED: {disc.symbol} missing TP/SL orders"
                )

            elif disc.discrepancy_type == "missing_in_state":
                logger.info(
                    f"Manual position detected: {disc.symbol} (not bot-managed)"
                )

        self.total_corrections_applied += corrections_applied
        return corrections_applied

    def _mark_position_closed(
        self,
        ib: IB,
        symbol: str,
        processed: Dict[str, dict],
        reason: str = "reconciliation",
    ) -> bool:
        """Mark a position as closed in bot state."""
        for _, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue

            if (
                trade_data.get("symbol") == symbol
                and is_bot_filled_status(trade_data.get("status"))
            ):
                entry_price = trade_data.get("fill_price") or trade_data.get(
                    "entry_price", 0
                )
                quantity = trade_data.get("quantity", 0)

                exit_price = resolve_exit_price(ib, symbol)

                if exit_price and exit_price > 0 and entry_price > 0 and quantity > 0:
                    pnl_usd, pnl_pct = calculate_realized_pnl(
                        entry_price, exit_price, quantity
                    )
                    trade_data["exit_price"] = exit_price
                    trade_data["realized_pnl_usd"] = pnl_usd
                    trade_data["realized_pnl_pct"] = pnl_pct

                trade_data["status"] = BOT_STATUS_CLOSED
                trade_data["closed_at"] = datetime.now(timezone.utc).isoformat()
                trade_data["close_reason"] = reason
                trade_data.pop("missing_in_ib_checks", None)
                logger.position(f"CLOSED: {symbol} (Reason: {reason})")
                return True
        return False

    def _update_position_quantity(
        self,
        symbol: str,
        processed: Dict[str, dict],
        new_quantity: int,
    ) -> bool:
        """Update the quantity of a position."""
        for _, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue

            if (
                trade_data.get("symbol") == symbol
                and is_bot_filled_status(trade_data.get("status"))
            ):
                old_qty = trade_data.get("quantity", 0)
                trade_data["quantity"] = new_quantity
                trade_data["quantity_updated_at"] = datetime.now(timezone.utc).isoformat()
                logger.position(f"QTY UPDATE: {symbol} | {old_qty} -> {new_quantity}")
                return True
        return False

    def log_statistics(self) -> None:
        """Log reconciliation stats."""
        logger.info("=" * 60)
        logger.info("POSITION RECONCILIATION STATS")
        logger.info("=" * 60)
        logger.info(f"Total Reconciliations:    {self.total_reconciliations}")
        logger.info(f"Discrepancies Found:      {self.total_discrepancies_found}")
        logger.info(f"Auto-Corrections Applied: {self.total_corrections_applied}")
        logger.info(
            "Last Reconciliation:      "
            f"{self.last_reconciliation.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        logger.info("=" * 60)


def calculate_realized_pnl(
    entry_price: float,
    exit_price: float,
    quantity: int,
) -> tuple[float, float]:
    """
    Calculate realized P&L.

    Returns:
        (pnl_usd, pnl_pct)
    """
    if entry_price <= 0 or quantity <= 0:
        return 0.0, 0.0

    pnl_usd = (exit_price - entry_price) * quantity
    pnl_pct = ((exit_price - entry_price) / entry_price) * 100

    return pnl_usd, pnl_pct


def get_current_market_price(ib: IB, symbol: str) -> Optional[float]:
    """
    Fetch current market price for a symbol.

    Returns:
        Current market price or None
    """
    try:
        from ib_insync import Stock

        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)

        ticker = ib.reqMktData(contract, snapshot=True)
        ib.sleep(2)

        if ticker.last and ticker.last > 0:
            return float(ticker.last)
        if ticker.close and ticker.close > 0:
            return float(ticker.close)

        return None
    except Exception as e:
        logger.warning(f"Could not get market price for {symbol}: {e}")
        return None


def get_last_fill_price(ib: IB, symbol: str) -> Optional[float]:
    """
    Try to get the most recent fill price for a symbol from IB trades.

    Returns:
        Last fill price or None
    """
    best_price: Optional[float] = None
    best_time: Optional[datetime] = None

    for trade in ib.trades():
        contract = getattr(trade, "contract", None)
        if not contract or getattr(contract, "symbol", None) != symbol:
            continue

        fills = getattr(trade, "fills", []) or []
        for fill in fills:
            execution = getattr(fill, "execution", None)
            if not execution:
                continue
            price = getattr(execution, "price", 0) or 0
            exec_time = getattr(execution, "time", None)
            if price and price > 0:
                if best_time is None or (
                    isinstance(exec_time, datetime) and exec_time > best_time
                ):
                    best_time = exec_time if isinstance(exec_time, datetime) else best_time
                    best_price = float(price)

        if best_price:
            continue

        order_status = getattr(trade, "orderStatus", None)
        if not order_status:
            continue

        avg_fill = getattr(order_status, "avgFillPrice", 0) or 0
        last_fill = getattr(order_status, "lastFillPrice", 0) or 0
        if avg_fill > 0:
            best_price = float(avg_fill)
        elif last_fill > 0:
            best_price = float(last_fill)

    return best_price


def resolve_exit_price(ib: IB, symbol: str) -> Optional[float]:
    """
    Resolve an exit price from recent fills, with market price fallback.

    Returns:
        Exit price or None
    """
    price = get_last_fill_price(ib, symbol)
    if price and price > 0:
        return price

    return get_current_market_price(ib, symbol)
