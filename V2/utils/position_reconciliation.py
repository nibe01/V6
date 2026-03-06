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
    BOT_STATUS_FILLED,
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
    discrepancy_type: str  # missing_in_state, missing_in_ib, quantity_mismatch, partial_reduction, orphaned_tp_sl
    bot_state: Optional[dict] = None
    ib_position: Optional[dict] = None
    severity: str = "WARNING"  # INFO, WARNING, ERROR
    suggested_action: str = ""


@dataclass
class FillAggregation:
    """Aggregated fill metrics for one symbol (idempotent by execution identity)."""

    symbol: str
    buy_qty: int = 0
    sell_qty: int = 0
    buy_notional: float = 0.0
    sell_notional: float = 0.0
    commissions: float = 0.0
    matched_qty: int = 0
    remaining_qty: int = 0
    avg_buy_price: float = 0.0
    avg_sell_price: float = 0.0
    last_sell_price: float = 0.0
    gross_realized_pnl: float = 0.0
    net_realized_pnl: float = 0.0
    has_commission_data: bool = False
    missing_commission_count: int = 0
    processed_execution_count: int = 0


def _parse_fill_time_utc(raw_time) -> Optional[datetime]:
    if isinstance(raw_time, datetime):
        parsed = raw_time
    elif raw_time:
        try:
            parsed = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_execution_identity(fill, trade) -> str:
    execution = getattr(fill, "execution", None)
    commission_report = getattr(fill, "commissionReport", None)
    order = getattr(trade, "order", None)

    exec_id = str(getattr(execution, "execId", "") or "").strip()
    if exec_id:
        return f"exec:{exec_id}"

    order_id = int(getattr(order, "orderId", 0) or 0)
    perm_id = int(getattr(order, "permId", 0) or 0)
    side = str(getattr(execution, "side", "") or "").upper()
    shares = int(getattr(execution, "shares", 0) or 0)
    price = float(getattr(execution, "price", 0.0) or 0.0)
    ts = _parse_fill_time_utc(getattr(execution, "time", None))
    ts_key = ts.isoformat() if ts else ""
    comm_exec_id = str(getattr(commission_report, "execId", "") or "").strip()

    return (
        f"order:{order_id}|perm:{perm_id}|side:{side}|shares:{shares}|"
        f"price:{price:.8f}|time:{ts_key}|comm:{comm_exec_id}"
    )


def aggregate_symbol_fills(
    ib: IB,
    symbol: str,
    position_opened_at: Optional[str] = None,
) -> FillAggregation:
    """Aggregate BUY/SELL fills, commissions and net realized PnL for a symbol."""
    summary = FillAggregation(symbol=symbol)

    start_dt = _parse_fill_time_utc(position_opened_at) if position_opened_at else None

    entries: list[tuple[datetime, str, int, float, float, bool]] = []
    seen: set[str] = set()

    for trade in ib.trades():
        contract = getattr(trade, "contract", None)
        if not contract or getattr(contract, "symbol", None) != symbol:
            continue

        fills = getattr(trade, "fills", None) or []
        for fill in fills:
            execution = getattr(fill, "execution", None)
            if execution is None:
                continue

            ts = _parse_fill_time_utc(getattr(execution, "time", None))
            if ts is None:
                continue

            if start_dt is not None and ts < start_dt:
                continue

            side = str(getattr(execution, "side", "") or "").upper()
            if side not in {"BOT", "BUY", "SLD", "SELL"}:
                continue

            qty = int(getattr(execution, "shares", 0) or 0)
            price = float(getattr(execution, "price", 0.0) or 0.0)
            if qty <= 0 or price <= 0:
                continue

            identity = _build_execution_identity(fill, trade)
            if identity in seen:
                continue
            seen.add(identity)

            commission_report = getattr(fill, "commissionReport", None)
            has_commission = commission_report is not None and getattr(
                commission_report, "commission", None
            ) is not None
            commission = (
                float(getattr(commission_report, "commission", 0.0) or 0.0)
                if commission_report is not None
                else 0.0
            )

            entries.append((ts, side, qty, price, commission, has_commission))

    entries.sort(key=lambda item: item[0])

    buy_qty = 0
    sell_qty = 0
    buy_notional = 0.0
    sell_notional = 0.0
    commissions = 0.0
    missing_commission_count = 0
    commission_present = False

    for _, side, qty, price, commission, has_commission in entries:
        if side in {"BOT", "BUY"}:
            buy_qty += qty
            buy_notional += qty * price
        else:
            sell_qty += qty
            sell_notional += qty * price
            summary.last_sell_price = price

        commissions += commission
        if has_commission:
            commission_present = True
        elif side in {"SLD", "SELL"}:
            missing_commission_count += 1

    matched_qty = min(buy_qty, sell_qty)
    avg_buy = (buy_notional / buy_qty) if buy_qty > 0 else 0.0
    avg_sell = (sell_notional / sell_qty) if sell_qty > 0 else 0.0
    gross = (avg_sell - avg_buy) * matched_qty if matched_qty > 0 else 0.0
    net = gross - commissions

    summary.buy_qty = buy_qty
    summary.sell_qty = sell_qty
    summary.buy_notional = buy_notional
    summary.sell_notional = sell_notional
    summary.commissions = commissions
    summary.matched_qty = matched_qty
    summary.remaining_qty = max(0, buy_qty - sell_qty)
    summary.avg_buy_price = avg_buy
    summary.avg_sell_price = avg_sell
    summary.gross_realized_pnl = gross
    summary.net_realized_pnl = net
    summary.has_commission_data = commission_present
    summary.missing_commission_count = missing_commission_count
    summary.processed_execution_count = len(entries)

    return summary


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
        self._sync_manual_positions(ib, processed, ib_positions)

        # Step 1c: Refresh closed trade net P&L when delayed commission reports appear.
        self._refresh_closed_trade_net_pnl(ib, processed)

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
            bot_data = bot_positions[symbol]
            bot_qty = int(bot_data.get("quantity", 0) or 0)
            ib_qty = int(ib_positions[symbol]["quantity"] or 0)

            if bot_qty != ib_qty:
                fill_summary = aggregate_symbol_fills(
                    ib,
                    symbol,
                    position_opened_at=(
                        bot_data.get("opened_at")
                        or bot_data.get("filled_at")
                        or bot_data.get("signal_timestamp")
                        or bot_data.get("processed_at")
                    ),
                )

                if (
                    is_bot_filled_status(bot_data.get("status"))
                    and ib_qty < bot_qty
                    and fill_summary.sell_qty > 0
                ):
                    discrepancy = PositionDiscrepancy(
                        symbol=symbol,
                        discrepancy_type="partial_reduction",
                        bot_state=bot_data,
                        ib_position=ib_positions[symbol],
                        severity="INFO",
                        suggested_action=(
                            "Partial exit detected; keep original trade quantity and "
                            "track remaining quantity separately"
                        ),
                    )
                    discrepancies.append(discrepancy)
                    self.total_discrepancies_found += 1
                    continue

                discrepancy = PositionDiscrepancy(
                    symbol=symbol,
                    discrepancy_type="quantity_mismatch",
                    bot_state=bot_data,
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

    def _has_any_bot_history(self, processed: Dict[str, dict], symbol: str) -> bool:
        """Check whether symbol exists in non-manual bot state history."""
        for trade_data in processed.values():
            if not isinstance(trade_data, dict):
                continue
            if trade_data.get("symbol") != symbol:
                continue

            status = str(trade_data.get("status", "")).strip().lower()
            if status and status not in {"manual", "manual_closed"}:
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

            opened_at = self._infer_position_opened_at_from_ib(ib, symbol) or now

            if self._has_any_bot_history(processed, symbol):
                recovered_key = f"recovered_history_{symbol}_{now}"
                entry_price = float(ib_pos.get("avg_cost", 0.0) or 0.0)
                processed[recovered_key] = {
                    "symbol": symbol,
                    "signal_timestamp": opened_at,
                    "processed_at": now,
                    "order_id": 0,
                    "tp_order_id": None,
                    "sl_order_id": None,
                    "entry_price": entry_price,
                    "fill_price": entry_price,
                    "quantity": ib_pos.get("quantity", 0),
                    "status": BOT_STATUS_FILLED,
                    "filled_at": opened_at,
                    "opened_at": opened_at,
                    "note": "recovered_from_ib_bot_history",
                }
                logger.warning(
                    "RECOVERY: %s restored as bot trade from IB position snapshot "
                    "(history-based)",
                    symbol,
                )
                continue

            manual_key = f"manual_{symbol}"
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
        ib: IB,
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
                if (
                    trade_data.get("note") == "detected from IB positions"
                    and self._has_any_bot_history(processed, symbol)
                ):
                    opened_at = (
                        trade_data.get("opened_at")
                        or self._infer_position_opened_at_from_ib(ib, symbol)
                        or now
                    )
                    entry_price = float(ib_pos.get("avg_cost", 0.0) or 0.0)

                    trade_data["status"] = BOT_STATUS_FILLED
                    trade_data["opened_at"] = opened_at
                    trade_data["filled_at"] = trade_data.get("filled_at") or opened_at
                    trade_data["entry_price"] = entry_price
                    trade_data["fill_price"] = entry_price
                    trade_data["quantity"] = ib_pos.get("quantity", 0)
                    trade_data["note"] = "recovered_from_ib_bot_history"

                    logger.warning(
                        "RECOVERY: %s upgraded from detected manual to bot trade "
                        "(history-based)",
                        symbol,
                    )
                    continue

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
                    disc.symbol,
                    processed,
                    disc.ib_position["quantity"],
                    ib=ib,
                )
                if success:
                    logger.info(
                        f"AUTO-CORRECTED: {disc.symbol} quantity updated to "
                        f"{disc.ib_position['quantity']}"
                    )
                    corrections_applied += 1

            elif disc.discrepancy_type == "partial_reduction":
                success = self._update_position_quantity(
                    disc.symbol,
                    processed,
                    disc.ib_position["quantity"],
                    ib=ib,
                )
                if success:
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
                opened_at = (
                    trade_data.get("opened_at")
                    or trade_data.get("filled_at")
                    or trade_data.get("signal_timestamp")
                    or trade_data.get("processed_at")
                )
                fill_summary = aggregate_symbol_fills(
                    ib,
                    symbol,
                    position_opened_at=opened_at,
                )

                if fill_summary.sell_qty <= 0:
                    logger.warning(
                        "RECONCILIATION-SKIP-CLOSE: %s missing in IB snapshot but no "
                        "confirmed exit fill found; keeping status as FILLED",
                        symbol,
                    )
                    return False

                self._apply_fill_summary_to_trade(trade_data, fill_summary)

                logger.trade(
                    "EXIT: %s | sold_qty=%s | remaining_qty=%s | gross_pnl=$%+.2f | "
                    "commissions=$%.2f | net_pnl=$%+.2f",
                    symbol,
                    fill_summary.sell_qty,
                    fill_summary.remaining_qty,
                    fill_summary.gross_realized_pnl,
                    fill_summary.commissions,
                    fill_summary.net_realized_pnl,
                )

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
        ib: Optional[IB] = None,
    ) -> bool:
        """Update the quantity of a position."""
        for _, trade_data in processed.items():
            if not isinstance(trade_data, dict):
                continue

            if (
                trade_data.get("symbol") == symbol
                and is_bot_filled_status(trade_data.get("status"))
            ):
                old_qty = int(trade_data.get("quantity", 0) or 0)
                new_qty = int(new_quantity or 0)

                if new_qty < old_qty:
                    prev_remaining = int(trade_data.get("remaining_quantity", old_qty) or 0)
                    trade_data["remaining_quantity"] = new_qty
                    trade_data["quantity_updated_at"] = datetime.now(timezone.utc).isoformat()

                    if ib is not None:
                        fill_summary = aggregate_symbol_fills(
                            ib,
                            symbol,
                            position_opened_at=(
                                trade_data.get("opened_at")
                                or trade_data.get("filled_at")
                                or trade_data.get("signal_timestamp")
                                or trade_data.get("processed_at")
                            ),
                        )

                        if (
                            prev_remaining == new_qty
                            and int(trade_data.get("sold_quantity", 0) or 0)
                            == int(fill_summary.sell_qty or 0)
                            and abs(
                                float(trade_data.get("realized_pnl_usd", 0.0) or 0.0)
                                - float(fill_summary.net_realized_pnl or 0.0)
                            )
                            < 1e-9
                        ):
                            return False

                        self._apply_fill_summary_to_trade(trade_data, fill_summary)
                        logger.position(
                            "PARTIAL EXIT: %s | sold_qty=%s | remaining_qty=%s | gross_pnl=$%+.2f | "
                            "commissions=$%.2f | net_pnl=$%+.2f",
                            symbol,
                            fill_summary.sell_qty,
                            fill_summary.remaining_qty,
                            fill_summary.gross_realized_pnl,
                            fill_summary.commissions,
                            fill_summary.net_realized_pnl,
                        )
                    else:
                        logger.position(
                            "PARTIAL EXIT: %s | remaining_qty=%s (state quantity unchanged=%s)",
                            symbol,
                            new_qty,
                            old_qty,
                        )

                    return True

                trade_data["quantity"] = new_qty
                trade_data["remaining_quantity"] = new_qty
                trade_data["quantity_updated_at"] = datetime.now(timezone.utc).isoformat()
                logger.position(f"QTY UPDATE: {symbol} | {old_qty} -> {new_qty}")
                return True
        return False

    def _refresh_closed_trade_net_pnl(
        self,
        ib: IB,
        processed: Dict[str, dict],
    ) -> None:
        """Refresh net P&L for already-closed trades once commissions become available."""
        for trade_data in processed.values():
            if not isinstance(trade_data, dict):
                continue
            if trade_data.get("status") != BOT_STATUS_CLOSED:
                continue
            if not bool(trade_data.get("pnl_needs_commission_refresh")):
                continue

            symbol = trade_data.get("symbol")
            if not symbol:
                continue

            opened_at = (
                trade_data.get("opened_at")
                or trade_data.get("filled_at")
                or trade_data.get("signal_timestamp")
                or trade_data.get("processed_at")
            )
            fill_summary = aggregate_symbol_fills(
                ib,
                symbol,
                position_opened_at=opened_at,
            )

            before = float(trade_data.get("realized_pnl_usd", 0.0) or 0.0)
            self._apply_fill_summary_to_trade(trade_data, fill_summary)
            after = float(trade_data.get("realized_pnl_usd", 0.0) or 0.0)

            if abs(before - after) > 1e-9 or not bool(trade_data.get("pnl_needs_commission_refresh")):
                logger.trade(
                    "P&L REFRESH: %s | gross_pnl=$%+.2f | commissions=$%.2f | net_pnl=$%+.2f",
                    symbol,
                    fill_summary.gross_realized_pnl,
                    fill_summary.commissions,
                    fill_summary.net_realized_pnl,
                )

    def _apply_fill_summary_to_trade(
        self,
        trade_data: dict,
        fill_summary: FillAggregation,
    ) -> None:
        """Apply normalized fill summary fields to state trade payload."""
        entry_price = float(
            trade_data.get("fill_price") or trade_data.get("entry_price") or 0.0
        )

        matched_qty = int(fill_summary.matched_qty or 0)
        net_pnl = float(fill_summary.net_realized_pnl or 0.0)
        trade_data["exit_price"] = float(fill_summary.avg_sell_price or trade_data.get("exit_price") or 0.0)
        trade_data["sold_quantity"] = int(fill_summary.sell_qty or 0)
        trade_data["remaining_quantity"] = int(fill_summary.remaining_qty or 0)
        trade_data["realized_pnl_gross_usd"] = float(fill_summary.gross_realized_pnl or 0.0)
        trade_data["realized_pnl_commission_usd"] = float(fill_summary.commissions or 0.0)
        trade_data["realized_pnl_usd"] = net_pnl
        trade_data["realized_pnl_net_usd"] = net_pnl
        trade_data["realized_pnl_source"] = "ib_fills_net"
        trade_data["pnl_needs_commission_refresh"] = bool(
            fill_summary.missing_commission_count > 0
        )

        if matched_qty > 0 and entry_price > 0:
            entry_notional = entry_price * matched_qty
            trade_data["realized_pnl_pct"] = (net_pnl / entry_notional) * 100.0

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


def get_last_exit_fill_price(ib: IB, symbol: str) -> Optional[float]:
    """
    Return most recent confirmed SELL/SLD execution price for a symbol.

    Returns:
        Exit fill price or None
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

            side = str(getattr(execution, "side", "") or "").upper()
            if side not in {"SLD", "SELL"}:
                continue

            price = getattr(execution, "price", 0) or 0
            exec_time = getattr(execution, "time", None)
            if not price or price <= 0:
                continue

            if best_time is None or (
                isinstance(exec_time, datetime) and exec_time > best_time
            ):
                best_time = exec_time if isinstance(exec_time, datetime) else best_time
                best_price = float(price)

    return best_price


def resolve_exit_price(
    ib: IB,
    symbol: str,
    require_confirmed_exit_fill: bool = False,
) -> Optional[float]:
    """
    Resolve an exit price from recent fills, with market price fallback.

    Returns:
        Exit price or None
    """
    confirmed_exit_price = get_last_exit_fill_price(ib, symbol)
    if confirmed_exit_price and confirmed_exit_price > 0:
        return confirmed_exit_price

    if require_confirmed_exit_fill:
        return None

    price = get_last_fill_price(ib, symbol)
    if price and price > 0:
        return price

    return get_current_market_price(ib, symbol)
