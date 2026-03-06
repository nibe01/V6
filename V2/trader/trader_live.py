from __future__ import annotations

import json
import glob
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional

from ib_insync import IB, Stock, MarketOrder, LimitOrder, StopOrder

from config import validate_and_get_config
from utils.paths import OUTPUT_DIR, STATE_DIR
from utils.state_retry import (
    load_state_with_retry,
    save_state_with_retry,
    update_state_atomically,
)
from utils.state_utils import file_lock
from trader.order_verification import get_order_status_summary
from utils.daily_loss_counter import DailyLossCounter
from utils.ib_connection import IBConnectionManager, ConnectionConfig
from utils.account_checker import AccountChecker
from utils.order_retry import OrderRetryHandler
from utils.trading_dashboard import create_dashboard
from utils.position_reconciliation import (
    PositionReconciliator,
    calculate_realized_pnl,
    resolve_exit_price,
)
from utils.trade_status import (
    BOT_STATUS_CLOSED,
    BOT_STATUS_FILLED,
    BOT_STATUS_MANUAL,
    BOT_STATUS_SUBMITTED,
    is_bot_active_status,
    is_bot_filled_status,
    is_bot_open_order_status,
    is_ib_entry_active_status,
    is_ib_pending_status,
    is_ib_rejected_or_cancelled_status,
    is_manual_status,
)
from utils.trading_context import TradingContext
from utils.market_schedule import MarketSchedule



POSITION_MISSING_CONFIRMATION_CHECKS = 3


def _calc_qty(
    context: TradingContext,
    account_balance: float,
    available_cash: float,
    current_open_positions: int,
    price: float,
) -> tuple[int, float, str]:
    """
    Calculate quantity based on unified position sizing.

    Args:
        account_balance: Total account net liquidation
        available_cash: Available cash
        current_open_positions: Bot open positions count
        price: Entry price per share

    Returns:
        Tuple of (quantity, trade_size_usd, reason)
    """
    from utils.input_validator import validate_price

    try:
        price_valid = validate_price(
            price,
            field_name="entry_price",
            min_price=0.01,
            max_price=100000.0,
        )

        size_result = context.position_sizer.calculate_position_size(
            account_balance=account_balance,
            available_cash=available_cash,
            current_open_positions=current_open_positions,
        )

        if size_result.trade_size_usd == 0:
            return 0, 0.0, size_result.reason

        qty, qty_reason = context.position_sizer.calculate_quantity(
            position_size_usd=size_result.trade_size_usd,
            price=price_valid,
        )

        if qty <= 0:
            return 0, 0.0, qty_reason

        actual_trade_size = qty * price_valid

        return max(1, qty), actual_trade_size, f"Unified sizing: {qty_reason}"

    except Exception as e:
        context.logger.error("Error calculating quantity: %s", e)
        return 0, 0.0, f"Error: {str(e)}"


def _has_position(ib: IB, symbol: str) -> bool:
    """
    Prüft, ob bei IB eine offene Position für das Symbol existiert.

    Hinweis: Diese Prüfung ist bewusst IB-basiert und symbolweit,
    unabhängig davon, ob die Position manuell oder durch den Bot entstanden ist.
    """
    for p in ib.positions():
        if p.contract.symbol == symbol and p.position != 0:
            return True
    return False


def _parse_iso_to_utc(ts: str) -> Optional[datetime]:
    """Parse ISO timestamp into UTC datetime (best effort)."""
    if not ts:
        return None
    try:
        parsed = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _should_prune_state_entry(trade_data: dict, cutoff: datetime) -> bool:
    """Return True if state entry is old enough and safe to prune."""
    status = str(trade_data.get("status", "")).strip().lower()
    if status not in {"closed", "rejected", "manual_closed"}:
        return False

    ts_candidates = (
        trade_data.get("closed_at"),
        trade_data.get("processed_at"),
        trade_data.get("signal_timestamp"),
        trade_data.get("first_seen_at"),
    )

    for ts in ts_candidates:
        parsed = _parse_iso_to_utc(str(ts)) if ts else None
        if parsed is not None:
            return parsed < cutoff

    # If no parseable timestamp exists, keep the entry to avoid accidental data loss.
    return False


def _has_bot_position(symbol: str, processed: Dict[str, dict]) -> bool:
    """
    Prüft ob der BOT eine aktive Position in diesem Symbol hat.

    Eine Bot-Position ist definiert als:
    - Signal wurde verarbeitet (in processed)
    - Status ist ein aktiver Bot-Status (siehe utils.trade_status)

    Args:
        symbol: Symbol der Aktie
        processed: Dict mit allen verarbeiteten Signalen

    Returns:
        True wenn Bot eine aktive Position hat
    """
    for key, trade_data in processed.items():
        if not isinstance(trade_data, dict):
            continue
        trade_symbol = trade_data.get("symbol")
        status = trade_data.get("status")

        # Prüfe ob es das richtige Symbol ist
        if trade_symbol != symbol:
            continue

        # Prüfe ob Status "aktiv" ist
        if is_bot_active_status(status):
            return True

    return False


def _has_bot_open_order(symbol: str, processed: Dict[str, dict]) -> bool:
    """
    Prüft ob der BOT eine offene Order für dieses Symbol hat.

    Args:
        symbol: Symbol der Aktie
        processed: Dict mit allen verarbeiteten Signalen

    Returns:
        True wenn Bot eine offene Order hat
    """
    for key, trade_data in processed.items():
        if not isinstance(trade_data, dict):
            continue
        trade_symbol = trade_data.get("symbol")
        status = trade_data.get("status")

        if trade_symbol != symbol:
            continue

        if is_bot_open_order_status(status):
            return True

    return False


def _has_manual_position(symbol: str, processed: Dict[str, dict]) -> bool:
    """
    Checks if a manual position was recorded for this symbol.

    Args:
        symbol: Symbol of the stock
        processed: Dict of processed signals/state

    Returns:
        True if a manual position is recorded
    """
    for trade_data in processed.values():
        if not isinstance(trade_data, dict):
            continue
        if trade_data.get("symbol") != symbol:
            continue
        if is_manual_status(trade_data.get("status")):
            return True
    return False


def _has_any_bot_history(symbol: str, processed: Dict[str, dict]) -> bool:
    """
    Checks if the symbol has any non-manual history in bot state.

    This helps avoid misclassifying old bot/API positions as manual after
    restarts, day rollovers, or partial state drift.
    """
    for trade_data in processed.values():
        if not isinstance(trade_data, dict):
            continue
        if trade_data.get("symbol") != symbol:
            continue

        status = str(trade_data.get("status", "")).strip().lower()
        if status and status not in {"manual", "manual_closed"}:
            return True

    return False


def _count_bot_open_positions(processed: Dict[str, dict]) -> int:
    """
    Zaehlt die Anzahl aktiver Bot-Positionen (nicht manuelle Trades).

    Args:
        processed: Dict mit allen verarbeiteten Signalen

    Returns:
        Anzahl aktiver Bot-Positionen
    """
    active_symbols: set[str] = set()
    for _, trade_data in processed.items():
        if not isinstance(trade_data, dict):
            continue
        status = trade_data.get("status")
        symbol = trade_data.get("symbol")
        if is_bot_active_status(status) and symbol:
            active_symbols.add(symbol)
    return len(active_symbols)


def _has_open_order(ib: IB, symbol: str) -> bool:
    for o in ib.openTrades():
        if o.contract.symbol == symbol:
            return True
    return False


def _count_open_positions(ib: IB) -> int:
    """
    Zählt die Anzahl aller offenen Positionen (position != 0).
    Wichtig: Berücksichtigt nur tatsächliche Positionen, nicht pending Orders.
    """
    count = 0
    for p in ib.positions():
        if p.position != 0:
            count += 1
    return count


def _as_utc_iso(ts: datetime) -> str:
    """Normalize a datetime to UTC ISO format."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _infer_position_opened_at_from_ib(ib: IB, symbol: str) -> Optional[str]:
    """
    Best-effort inference for when a currently open position was opened.

    Uses the latest BUY fill timestamp seen in IB trades for the symbol.
    """
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

    return _as_utc_iso(latest_buy_fill_time)


def _seed_startup_positions_and_cooldowns(
    context: TradingContext,
    ib: IB,
    processed: Dict[str, dict],
    processed_path: Path,
    reconciliator: PositionReconciliator,
) -> None:
    """
    Sync manual positions from IB and seed cooldowns for open positions at startup.
    """
    try:
        discrepancies = reconciliator.reconcile_positions(
            ib=ib,
            processed=processed,
            auto_correct=True,
        )

        if discrepancies:
            context.logger.warning(
                f"RECONCILIATION (startup): {len(discrepancies)} discrepancy(ies) found"
            )
            for disc in discrepancies:
                context.logger.warning(
                    f"  - {disc.symbol}: {disc.discrepancy_type} "
                    f"({disc.severity}) | {disc.suggested_action}"
                )

        save_state_with_retry(
            processed_path,
            processed,
            max_retries=3,
            retry_delay=0.5,
        )
    except Exception as e:
        context.logger.error(f"Startup reconciliation failed: {e}")

    try:
        open_symbols = {p.contract.symbol for p in ib.positions() if p.position != 0}
    except Exception as e:
        context.logger.warning(f"Could not load IB positions for startup cooldowns: {e}")
        return

    added = 0
    for symbol in open_symbols:
        is_blocked, _ = context.cooldown_manager.is_on_cooldown(symbol)
        if is_blocked:
            continue
        context.cooldown_manager.add_cooldown(symbol, reason="open_position")
        added += 1

    context.cooldown_manager.clear_cooldowns_not_in_positions(
        open_symbols, reason="open_position"
    )

    if added:
        context.logger.info(
            f"Startup cooldowns applied for {added} open position(s)"
        )


def _check_for_filled_stop_losses(
    context: TradingContext,
    ib: IB,
    processed: Dict[str, dict],
    loss_counter: DailyLossCounter,
    already_counted: set,
) -> set:
    """
    Prüft alle offenen Trades und erkennt gefüllte Stop-Loss Orders.
    Ruft automatisch loss_counter.add_stop_loss() auf.

    Args:
        ib: IB Connection
        processed: Dict mit allen verarbeiteten Signalen (enthält sl_order_id)
        loss_counter: DailyLossCounter Instanz
        already_counted: Set von bereits gezählten SL Order-IDs

    Returns:
        Aktualisiertes Set von gezählten Order-IDs
    """
    # Sammle alle SL-Order-IDs aus processed
    tracked_sl_orders = {}
    for key, trade_data in processed.items():
        if not isinstance(trade_data, dict):
            continue
        sl_id = trade_data.get("sl_order_id")
        symbol = trade_data.get("symbol")
        if sl_id and symbol:
            tracked_sl_orders[sl_id] = symbol

    # Prüfe alle Trades in IB
    for trade in ib.trades():
        order_id = trade.order.orderId

        # Nur SL-Orders prüfen
        if order_id not in tracked_sl_orders:
            continue

        # Bereits gezählt?
        if order_id in already_counted:
            continue

        # Prüfe ob Order-Type Stop ist
        if trade.order.orderType != "STP":
            continue

        # Prüfe ob gefüllt
        if trade.orderStatus.status == "Filled":
            symbol = tracked_sl_orders[order_id]
            loss_counter.add_stop_loss(symbol)
            context.cooldown_manager.add_cooldown(symbol, reason="stop_loss")
            already_counted.add(order_id)
            context.logger.warning(
                f"🔴 AUTO-DETECTED: Stop-Loss filled for {symbol} "
                f"(Order ID: {order_id})"
            )
            context.logger.trade(
                f"EXIT: {symbol} | Reason: SL | Order: {order_id}"
            )

    return already_counted


def _update_position_status(
    context: TradingContext,
    ib: IB,
    processed: Dict[str, dict],
    state_path: Path,
) -> None:
    """
    Aktualisiert den Status von Bot-Positionen basierend auf IB-Daten.

    NEU: Erweitert mit P&L Calculation und Market Price Lookup.

    Status-Uebergaenge:
    - BOT_STATUS_SUBMITTED -> BOT_STATUS_FILLED (Entry-Order wurde gefuellt)
    - BOT_STATUS_FILLED -> BOT_STATUS_CLOSED (Position wurde geschlossen via SL/TP)

    Args:
        ib: IB Connection
        processed: Dict mit allen verarbeiteten Signalen
        state_path: Path zur processed_signals.json
    """
    changed = False

    # Sammle alle Parent-Order-IDs
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

    # Pruefe Order-Status in IB
    for trade in ib.trades():
        order_id = trade.order.orderId

        if order_id not in tracked_orders:
            continue

        info = tracked_orders[order_id]
        key = info["key"]
        current_status = info["current_status"]
        ib_status = trade.orderStatus.status

        # Uebergang: submitted -> filled
        # HINWEIS: In neuen Trades wird bereits nach wait_for_order_fill()
        # BOT_STATUS_FILLED gespeichert (hier Legacy-/Recovery-Pfad).
        # Dieser Code ist nur noch fuer Backwards-Compatibility mit alten States
        if current_status == BOT_STATUS_SUBMITTED and ib_status == "Filled":
            processed[key]["status"] = BOT_STATUS_FILLED
            now_iso = datetime.now(timezone.utc).isoformat()
            processed[key]["filled_at"] = now_iso
            if not processed[key].get("opened_at"):
                processed[key]["opened_at"] = (
                    processed[key].get("signal_timestamp")
                    or processed[key].get("processed_at")
                    or now_iso
                )
            context.logger.position(
                f"FILLED: {info['symbol']} | Order: {order_id}"
            )
            changed = True

    # Pruefe geschlossene Positionen (kein IB Position mehr)
    current_positions = {p.contract.symbol for p in ib.positions() if p.position != 0}
    if current_positions or not context.cooldown_manager.has_cooldowns_by_reason(
        "open_position"
    ):
        context.cooldown_manager.clear_cooldowns_not_in_positions(
            current_positions, reason="open_position"
        )
    else:
        context.logger.debug(
            "Skipping open_position cooldown cleanup: IB positions empty"
        )

    for key, trade_data in processed.items():
        if not isinstance(trade_data, dict):
            continue
        symbol = trade_data.get("symbol")
        status = trade_data.get("status")

        if is_bot_filled_status(status) and symbol in current_positions:
            if trade_data.pop("missing_position_checks", None) is not None:
                changed = True
            continue

        # Wenn Status filled aber keine Position mehr -> closed
        if is_bot_filled_status(status) and symbol not in current_positions:
            missing_checks = int(trade_data.get("missing_position_checks", 0)) + 1
            trade_data["missing_position_checks"] = missing_checks
            changed = True

            if missing_checks < POSITION_MISSING_CONFIRMATION_CHECKS:
                context.logger.debug(
                    "%s: position missing check %s/%s - waiting before close confirmation",
                    symbol,
                    missing_checks,
                    POSITION_MISSING_CONFIRMATION_CHECKS,
                )
                continue

            entry_price = trade_data.get("fill_price") or trade_data.get(
                "entry_price", 0
            )
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
                    context.logger.warning(
                        "EXIT-CHECK: %s missing from IB positions but no confirmed "
                        "exit fill yet; keeping status as FILLED",
                        symbol,
                    )
                continue

            if entry_price > 0 and quantity > 0:
                pnl_usd, pnl_pct = calculate_realized_pnl(
                    entry_price, exit_price, quantity
                )

                processed[key]["exit_price"] = exit_price
                processed[key]["realized_pnl_usd"] = pnl_usd
                processed[key]["realized_pnl_pct"] = pnl_pct

                context.logger.trade(
                    f"EXIT: {symbol} | Entry: ${entry_price:.2f} -> "
                    f"Exit: ${exit_price:.2f} | P&L: ${pnl_usd:+.2f} "
                    f"({pnl_pct:+.2f}%) | Qty: {quantity}"
                )
            else:
                context.logger.trade(
                    f"EXIT: {symbol} | Exit: ${exit_price:.2f} | "
                    "P&L not available (missing entry/qty)"
                )

            processed[key]["status"] = BOT_STATUS_CLOSED
            processed[key]["closed_at"] = datetime.now(timezone.utc).isoformat()
            processed[key].pop("missing_position_checks", None)
            context.logger.position(f"CLOSED: {symbol}")
            context.cooldown_manager.clear_cooldown(symbol)
            context.cooldown_manager.clear_cooldowns_by_reason("insufficient_cash")
            changed = True

    # Speichere nur wenn Aenderungen gemacht wurden
    if changed:
        save_state_with_retry(
            state_path,
            processed,
            max_retries=3,
            retry_delay=0.5,
        )


def _safe_ib_call(
    context: TradingContext,
    ib_manager: IBConnectionManager,
    operation: str,
    func,
    *args,
    **kwargs,
):
    """
    Fuehrt IB-Operation mit automatischem Reconnect bei Fehler aus.

    Args:
        ib_manager: IBConnectionManager Instanz
        operation: Name der Operation (fuer Logging)
        func: Funktion die ausgefuehrt werden soll
        *args, **kwargs: Argumente fuer func

    Returns:
        Ergebnis von func() oder None bei Fehler
    """
    max_retries = 3
    retry_delay = 2.0

    for attempt in range(1, max_retries + 1):
        try:
            # Stelle sicher dass Verbindung besteht
            ib = ib_manager.ensure_connected()

            # Fuehre Operation aus
            return func(ib, *args, **kwargs)

        except Exception as e:
            error_msg = str(e).lower()
            is_connection_error = any(
                keyword in error_msg
                for keyword in ["connection", "timeout", "disconnect", "socket", "not connected"]
            )

            if is_connection_error and attempt < max_retries:
                context.logger.warning(
                    f"Connection error in {operation} "
                    f"(attempt {attempt}/{max_retries}): {e}"
                )
                context.logger.info("Attempting reconnect...")

                try:
                    ib_manager.reconnect()
                except Exception as reconnect_error:
                    context.logger.error(f"Reconnect failed: {reconnect_error}")

                time.sleep(retry_delay)
                retry_delay *= 2
                continue

            # Kein Connection-Error oder max retries erreicht
            context.logger.error(
                f"Error in {operation}: {type(e).__name__}: {e}"
            )
            return None

    context.logger.error(f"Max retries exceeded for {operation}")
    return None


def _parse_signal_line(
    context: TradingContext,
    line: str,
) -> tuple[dict, float] | None:
    try:
        sig = json.loads(line)
    except json.JSONDecodeError:
        context.logger.warning("Bad signal JSON: %s", line.strip())
        return None

    symbol = sig.get("symbol")
    now_utc = sig.get("now_utc")
    if not symbol or not now_utc:
        context.logger.warning(
            "Bad signal line (missing symbol/now_utc): %s", sig
        )
        return None

    try:
        price = float(sig["end_price"])
    except (KeyError, TypeError, ValueError):
        context.logger.warning("Bad signal line (invalid end_price): %s", sig)
        return None

    from utils.input_validator import validate_price, ValidationError as InputValidationError

    try:
        price = validate_price(
            price,
            field_name="signal_price",
            min_price=0.01,
            max_price=100000.0,
        )
    except InputValidationError as e:
        context.logger.warning("Invalid signal price: %s", e)
        return None

    return sig, price


def send_bracket_order(
    context: TradingContext,
    ib: IB,
    symbol: str,
    entry_price: float,
    current_open_positions: int,
    cfg: dict,
) -> dict | None:
    """
    Fill-first Entry Flow:
    1) Entry-Order senden
    2) Auf Fill warten
    3) TP/SL auf Basis des tatsaechlichen Fill-Preises platzieren
    """
    from utils.input_validator import (
        validate_price,
        validate_quantity,
        validate_price_relationship,
        ValidationError as InputValidationError,
    )

    try:
        entry_price = validate_price(
            entry_price,
            field_name=f"entry_price for {symbol}",
            min_price=0.01,
            max_price=100000.0,
        )
        context.logger.debug("%s: entry_price validated: $%.2f", symbol, entry_price)
    except InputValidationError as e:
        context.logger.error("Invalid entry_price for %s: %s", symbol, e)
        return None

    tr_cfg = cfg["trading"]

    try:
        account_info = context.account_checker.get_account_info(
            ib, force_refresh=True
        )
    except Exception as e:
        context.logger.error(
            "Could not get account info for position sizing: %s", e
        )
        return None

    if account_info is None:
        context.logger.error("Cannot calculate position size without account info")
        return None

    qty, trade_size_usd, sizing_reason = _calc_qty(
        context=context,
        account_balance=account_info.net_liquidation,
        available_cash=account_info.total_cash_value,
        current_open_positions=current_open_positions,
        price=entry_price,
    )
    if qty <= 0:
        context.logger.info("Skip %s: %s", symbol, sizing_reason)
        return None

    context.logger.info(
        "%s: Unified Position Size $%.2f (%s shares @ $%.2f) - %s",
        symbol,
        trade_size_usd,
        qty,
        entry_price,
        sizing_reason,
    )

    try:
        can_afford, reason = context.account_checker.can_afford_trade(
            ib=ib,
            trade_amount=trade_size_usd,
            symbol=symbol,
            safety_margin=0.1,
            force_refresh=True,
        )

        if not can_afford:
            context.logger.warning("Cannot afford %s trade: %s", symbol, reason)
            return None

        context.logger.debug("Account check passed for %s", symbol)
    except Exception as e:
        context.logger.error("Error during account check for %s: %s", symbol, e)
        context.logger.error(
            "\ud83d\uded1 ABORTING %s trade due to account check failure", symbol
        )
        return None

    try:
        qty = validate_quantity(
            qty,
            field_name=f"quantity for {symbol}",
            min_qty=1,
            max_qty=1000000,
        )
    except InputValidationError as e:
        context.logger.error("Invalid quantity for %s: %s", symbol, e)
        return None

    context.logger.order(f"SENDING ENTRY: {symbol} BUY {qty} @ ${entry_price:.2f}")

    contract = Stock(symbol, "SMART", "USD")
    context.rate_limiter.wait_if_needed(
        request_type="qualify_contract",
        is_historical=False,
    )
    ib.qualifyContracts(contract)

    # Explizite Order-ID für Entry
    parent_id = ib.client.getReqId()

    if tr_cfg.use_limit_entry:
        # Limit Order mit Slippage-Toleranz als Entry
        # Limit-Preis = Signal-Preis + erlaubte Slippage
        limit_price = round(entry_price * (1 + tr_cfg.max_entry_slippage_pct / 100), 2)

        try:
            limit_price = validate_price(
                limit_price,
                field_name=f"entry_limit_price for {symbol}",
                min_price=0.01,
                max_price=100000.0,
            )
        except InputValidationError as e:
            context.logger.error("Invalid entry limit price for %s: %s", symbol, e)
            return None

        parent = LimitOrder("BUY", qty, limit_price)
        context.logger.info(
            "%s: Using Limit Entry @ $%.2f (signal: $%.2f, max slippage: %.1f%%)",
            symbol, limit_price, entry_price, tr_cfg.max_entry_slippage_pct,
        )
    else:
        # Fallback: Market Order (nicht empfohlen)
        parent = MarketOrder("BUY", qty)
        context.logger.warning(
            "%s: Using Market Entry (no slippage protection!)", symbol,
        )

    parent.orderId = parent_id
    parent.transmit = True
    parent.tif = "DAY"

    retry_handler = OrderRetryHandler(max_retries=3, retry_delay=2.0)

    context.rate_limiter.wait_if_needed(
        request_type="place_order", is_historical=False
    )

    parent_success, parent_error = retry_handler.place_order_with_retry(
        ib=ib,
        contract=contract,
        order=parent,
        symbol=symbol,
    )

    if not parent_success:
        context.logger.error(
            f"PARENT ORDER REJECTED: {symbol} | Error: {parent_error}"
        )
        return None

    context.logger.order(
        f"SENT ENTRY: {symbol} BUY {qty} @ ${entry_price:.2f} (Order {parent_id})"
    )

    # ✅ STEP 2: Wait for Entry Order Fill (NEU!)
    fill_success, fill_status, fill_price, filled_qty = wait_for_order_fill(
        context=context,
        ib=ib,
        order_id=parent_id,
        symbol=symbol,
        timeout_seconds=30.0,
    )

    if not fill_success:
        context.logger.error(
            f"❌ Entry order NOT filled for {symbol}: {fill_status}"
        )

        # Final safety check before cancellation:
        # avoid canceling an order that was actually filled shortly after timeout.
        recovered_fill_price = fill_price
        recovered_filled_qty = filled_qty

        for trade in ib.trades():
            if trade.order.orderId != parent_id:
                continue

            late_status = str(getattr(trade.orderStatus, "status", "") or "")
            late_filled_qty = int(getattr(trade.orderStatus, "filled", 0) or 0)
            late_fill_price = float(
                getattr(trade.orderStatus, "avgFillPrice", 0.0) or 0.0
            )

            if late_status == "Filled" or late_filled_qty > 0:
                fill_success = True
                fill_status = f"Recovered before cancel ({late_status or 'Unknown'})"
                recovered_filled_qty = late_filled_qty
                recovered_fill_price = late_fill_price
                break

        if not fill_success:
            for position in ib.positions():
                if position.contract.symbol != symbol:
                    continue
                pos_qty = int(abs(getattr(position, "position", 0) or 0))
                if pos_qty <= 0:
                    continue

                fill_success = True
                fill_status = "PositionRecoveredBeforeCancel"
                recovered_filled_qty = pos_qty
                recovered_fill_price = float(
                    getattr(position, "avgCost", 0.0) or 0.0
                )
                break

        if fill_success and recovered_filled_qty > 0:
            fill_price = (
                recovered_fill_price
                if recovered_fill_price > 0
                else entry_price
            )
            filled_qty = recovered_filled_qty
            context.logger.warning(
                "✅ Late fill recovered before cancel: %s | Qty: %s @ $%.2f | %s",
                symbol,
                filled_qty,
                fill_price,
                fill_status,
            )
        else:
            try:
                for trade in ib.trades():
                    if trade.order.orderId == parent_id:
                        ib.cancelOrder(trade.order)
                        break
            except Exception as e:
                context.logger.warning(
                    "Could not cancel unfilled entry order %s for %s: %s",
                    parent_id,
                    symbol,
                    e,
                )

            return None

    if filled_qty <= 0:
        context.logger.error("❌ Filled quantity is zero for %s", symbol)
        return None

    if filled_qty < qty:
        context.logger.warning(
            f"⚠️ PARTIAL FILL DETECTED: {symbol} | "
            f"Requested: {qty}, Filled: {filled_qty} "
            f"({filled_qty / qty * 100:.1f}%)"
        )

    tp_price = round(fill_price * (1 + tr_cfg.take_profit_pct / 100), 2)
    sl_price = round(fill_price * (1 - tr_cfg.stop_loss_pct / 100), 2)

    try:
        tp_price = validate_price(
            tp_price,
            field_name=f"tp_price for {symbol}",
            min_price=0.01,
            max_price=100000.0,
        )
        sl_price = validate_price(
            sl_price,
            field_name=f"sl_price for {symbol}",
            min_price=0.01,
            max_price=100000.0,
        )
        validate_price_relationship(
            fill_price,
            tp_price,
            sl_price,
            is_long=True,
        )
    except InputValidationError as e:
        context.logger.error(
            "Invalid TP/SL based on fill price for %s: %s",
            symbol,
            e,
        )
        return None

    tp_id = ib.client.getReqId()
    sl_id = ib.client.getReqId()
    oca_group = f"exit_{symbol}_{parent_id}"

    tp = LimitOrder("SELL", filled_qty, tp_price)
    tp.orderId = tp_id
    tp.tif = "GTC"
    tp.transmit = True
    tp.ocaGroup = oca_group
    tp.ocaType = 1

    sl = StopOrder("SELL", filled_qty, sl_price)
    sl.orderId = sl_id
    sl.tif = "GTC"
    sl.transmit = True
    sl.ocaGroup = oca_group
    sl.ocaType = 1

    exit_retry_handler = OrderRetryHandler(max_retries=2, retry_delay=1.0)

    context.rate_limiter.wait_if_needed(
        request_type="place_order", is_historical=False
    )
    tp_success, tp_error = exit_retry_handler.place_order_with_retry(
        ib=ib,
        contract=contract,
        order=tp,
        symbol=symbol,
    )

    context.rate_limiter.wait_if_needed(
        request_type="place_order", is_historical=False
    )
    sl_success, sl_error = exit_retry_handler.place_order_with_retry(
        ib=ib,
        contract=contract,
        order=sl,
        symbol=symbol,
    )

    if not tp_success:
        context.logger.error(
            "TP ORDER REJECTED after fill: %s | Error: %s",
            symbol,
            tp_error,
        )
    if not sl_success:
        context.logger.error(
            "SL ORDER REJECTED after fill: %s | Error: %s",
            symbol,
            sl_error,
        )

    if not tp_success or not sl_success:
        tp_status = get_order_status_summary(ib, tp_id) or "NOT_FOUND"
        sl_status = get_order_status_summary(ib, sl_id) or "NOT_FOUND"
        context.logger.warning(
            "%s: Position open but exit protection incomplete. TP=%s, SL=%s. Manual check required.",
            symbol,
            tp_status,
            sl_status,
        )

    strict_emergency_exit = bool(
        getattr(tr_cfg, "force_emergency_exit_if_any_protection_missing", False)
    )
    should_emergency_exit = (
        (not tp_success and not sl_success)
        or (strict_emergency_exit and (not tp_success or not sl_success))
    )

    # Emergency Exit: Wenn Schutzorders fehlschlagen, Position sofort schließen
    if should_emergency_exit:
        mode = "ANY_MISSING" if strict_emergency_exit else "BOTH_MISSING"
        context.logger.error(
            "EMERGENCY EXIT TRIGGERED (%s): %s | TP success=%s, SL success=%s",
            mode,
            symbol,
            tp_success,
            sl_success,
        )

        emergency_order_id = ib.client.getReqId()
        emergency_exit = MarketOrder("SELL", filled_qty)
        emergency_exit.orderId = emergency_order_id
        emergency_exit.tif = "DAY"
        emergency_exit.transmit = True

        emergency_retry_handler = OrderRetryHandler(max_retries=3, retry_delay=1.0)

        context.rate_limiter.wait_if_needed(
            request_type="place_order", is_historical=False
        )
        emergency_success, emergency_error = emergency_retry_handler.place_order_with_retry(
            ib=ib,
            contract=contract,
            order=emergency_exit,
            symbol=symbol,
        )

        if not emergency_success:
            context.logger.error(
                "CRITICAL: Emergency exit placement failed for %s | Error: %s",
                symbol,
                emergency_error,
            )
            return None

        emergency_filled, emergency_status, emergency_fill_price, emergency_filled_qty = (
            wait_for_order_fill(
                context=context,
                ib=ib,
                order_id=emergency_order_id,
                symbol=symbol,
                timeout_seconds=30.0,
            )
        )

        if emergency_filled:
            context.logger.trade(
                "EMERGENCY EXIT FILLED: %s | Qty: %s @ $%.2f (Order %s)",
                symbol,
                emergency_filled_qty,
                emergency_fill_price,
                emergency_order_id,
            )
        else:
            context.logger.error(
                "CRITICAL: Emergency exit not confirmed for %s | Status: %s | Filled: %s",
                symbol,
                emergency_status,
                emergency_filled_qty,
            )

        return None

    context.logger.order(
        "EXIT ORDERS SET: %s | Qty: %s | TP: $%.2f (%s) | SL: $%.2f (%s)",
        symbol,
        filled_qty,
        tp_price,
        tp_id,
        sl_price,
        sl_id,
    )

    # ✅ STEP 2b: Slippage Validation (NEU!)
    if fill_price > 0 and entry_price > 0:
        actual_slippage_pct = ((fill_price - entry_price) / entry_price) * 100

        if actual_slippage_pct > 0:
            context.logger.info(
                "%s: Slippage: +%.2f%% (signal: $%.2f → fill: $%.2f)",
                symbol, actual_slippage_pct, entry_price, fill_price,
            )
        elif actual_slippage_pct < 0:
            context.logger.info(
                "%s: Price improvement: %.2f%% (signal: $%.2f → fill: $%.2f)",
                symbol, actual_slippage_pct, entry_price, fill_price,
            )

        # Warnung bei hoher Slippage (auch wenn innerhalb Limit)
        if actual_slippage_pct > tr_cfg.max_entry_slippage_pct:
            context.logger.warning(
                "⚠️ %s: EXCESSIVE SLIPPAGE: %.2f%% > max %.1f%% "
                "(signal: $%.2f, fill: $%.2f) - Trade wird behalten, aber logged",
                symbol, actual_slippage_pct, tr_cfg.max_entry_slippage_pct,
                entry_price, fill_price,
            )

        context.logger.info(
            "%s: Exit levels set from fill price -> TP $%.2f | SL $%.2f",
            symbol,
            tp_price,
            sl_price,
        )

    # ✅ STEP 3: Verify Position exists (NEU!)
    position_verified = verify_position_opened(
        context,
        ib,
        symbol,
        filled_qty,
    )

    if not position_verified:
        context.logger.error(
            f"❌ Position verification failed for {symbol} "
            f"despite order being filled!"
        )

    # ✅ SUCCESS: Order gefuellt und Position verifiziert
    context.logger.trade(
        f"ENTRY: {symbol} | Qty: {qty} | Entry: ${fill_price:.2f} | "
        f"TP: ${tp_price:.2f} | SL: ${sl_price:.2f}"
    )

    return {
        "parent_id": parent_id,
        "tp_id": tp_id,
        "sl_id": sl_id,
        "fill_price": fill_price,
        "quantity": filled_qty,
        "requested_quantity": qty,
        "is_partial_fill": filled_qty < qty,
    }


def wait_for_order_fill(
    context: TradingContext,
    ib: IB,
    order_id: int,
    symbol: str,
    timeout_seconds: float = 30.0,
    check_interval: float = 0.5,
) -> tuple[bool, str, float, int]:
    """
    Wartet darauf dass eine Order gefuellt wird.

    Args:
        ib: IB Connection
        order_id: Order ID der zu pruefenden Order
        symbol: Symbol der Order (fuer Logging)
        timeout_seconds: Max. Wartezeit in Sekunden
        check_interval: Pruef-Intervall in Sekunden

    Returns:
        tuple: (success: bool, status: str, fill_price: float, filled_qty: int)
            - success: True wenn Order vollstaendig gefuellt wurde
            - status: Order-Status (Filled, Cancelled, etc.)
            - fill_price: Durchschnittlicher Fill-Preis (0.0 wenn nicht gefuellt)
            - filled_qty: Tatsaechlich gefuellte Quantity (0 wenn nicht gefuellt)
    """
    start_time = time.time()
    last_status = "Unknown"

    context.logger.info(f"⏳ Waiting for order fill: {symbol} (Order {order_id})")

    while time.time() - start_time < timeout_seconds:
        for trade in ib.trades():
            if trade.order.orderId == order_id:
                status = trade.orderStatus.status
                last_status = status

                if status == "Filled":
                    fill_price = trade.orderStatus.avgFillPrice
                    filled_qty = int(trade.orderStatus.filled)
                    remaining_qty = int(trade.orderStatus.remaining)
                    total_qty = int(trade.order.totalQuantity)

                    if filled_qty < total_qty:
                        context.logger.warning(
                            f"⚠️ PARTIAL FILL: {symbol} | "
                            f"Filled: {filled_qty}/{total_qty} shares @ ${fill_price:.2f} | "
                            f"Remaining: {remaining_qty}"
                        )
                        if remaining_qty > 0:
                            context.logger.info(
                                f"⏳ Waiting for remaining {remaining_qty} shares..."
                            )
                            break

                    context.logger.order(
                        f"FILLED: {symbol} | Qty: {filled_qty}/{total_qty} "
                        f"@ ${fill_price:.2f} (Order {order_id})"
                    )
                    return True, status, fill_price, filled_qty

                if is_ib_rejected_or_cancelled_status(status):
                    filled_qty = int(trade.orderStatus.filled)
                    context.logger.warning(
                        f"❌ Order CANCELLED/REJECTED: {symbol} (Order {order_id}) "
                        f"Status: {status} | Filled: {filled_qty}"
                    )
                    return False, status, 0.0, filled_qty

                if is_ib_pending_status(status):
                    context.logger.debug(
                        f"⏳ Order pending: {symbol} (Order {order_id}) "
                        f"Status: {status}"
                    )
                    break

        ib.sleep(check_interval)

    def _extract_fill_from_trade() -> tuple[float, int]:
        for trade in ib.trades():
            if trade.order.orderId != order_id:
                continue

            order_status = trade.orderStatus
            fill_price = float(getattr(order_status, "avgFillPrice", 0.0) or 0.0)
            filled_qty = int(getattr(order_status, "filled", 0) or 0)

            if (fill_price <= 0 or filled_qty <= 0) and getattr(trade, "fills", None):
                weighted_sum = 0.0
                qty_sum = 0
                for fill in trade.fills:
                    execution = getattr(fill, "execution", None)
                    if execution is None:
                        continue
                    shares = int(getattr(execution, "shares", 0) or 0)
                    price = float(getattr(execution, "price", 0.0) or 0.0)
                    if shares <= 0 or price <= 0:
                        continue
                    weighted_sum += shares * price
                    qty_sum += shares

                if qty_sum > 0:
                    filled_qty = max(filled_qty, qty_sum)
                    fill_price = weighted_sum / qty_sum

            return fill_price, filled_qty

        return 0.0, 0

    def _extract_fill_from_position() -> tuple[float, int]:
        for position in ib.positions():
            if position.contract.symbol != symbol:
                continue
            qty = int(abs(getattr(position, "position", 0) or 0))
            if qty <= 0:
                continue
            avg_cost = float(getattr(position, "avgCost", 0.0) or 0.0)
            return avg_cost, qty
        return 0.0, 0

    ib.sleep(0.5)
    fill_price, filled_qty = _extract_fill_from_trade()
    if filled_qty > 0:
        if fill_price <= 0:
            position_price, _ = _extract_fill_from_position()
            fill_price = position_price

        if fill_price > 0:
            context.logger.warning(
                "✅ Late fill recovered after timeout: %s (Order %s) | Qty: %s @ $%.2f",
                symbol,
                order_id,
                filled_qty,
                fill_price,
            )
            return True, "Filled (recovered after timeout)", fill_price, filled_qty

    position_price, position_qty = _extract_fill_from_position()
    if position_qty > 0:
        recovered_price = fill_price if fill_price > 0 else position_price
        context.logger.warning(
            "✅ Position-based fill recovery: %s (Order %s) | Qty: %s @ $%.2f",
            symbol,
            order_id,
            position_qty,
            recovered_price,
        )
        return True, "PositionRecoveredAfterTimeout", recovered_price, position_qty

    elapsed = time.time() - start_time
    context.logger.error(
        f"⏱️ Order fill TIMEOUT: {symbol} (Order {order_id}) "
        f"Last status: {last_status} after {elapsed:.1f}s | "
        f"Filled: {filled_qty}"
    )
    return False, f"Timeout ({last_status})", 0.0, filled_qty
    return False, f"Timeout ({last_status})", 0.0, filled_qty

def wait_for_exit_orders_live(
    context: TradingContext,
    ib: IB,
    symbol: str,
    tp_order_id: int,
    sl_order_id: int,
    timeout_seconds: float = 8.0,
    check_interval: float = 0.5,
) -> tuple[bool, bool, bool, str, str]:
    """
    Verify TP and SL orders are visible/active at IB within a short retry window.

    Returns:
        tuple:
            overall_success,
            tp_live,
            sl_live,
            tp_status,
            sl_status
    """
    start_time = time.time()
    last_tp_status = "NOT_FOUND"
    last_sl_status = "NOT_FOUND"

    def _status_for(order_id: int) -> str:
        for trade in ib.trades():
            if trade.order.orderId == order_id:
                return str(getattr(trade.orderStatus, "status", "UNKNOWN") or "UNKNOWN")
        return "NOT_FOUND"

    def _is_live(status: str) -> bool:
        return is_ib_pending_status(status) or status == "Filled"

    context.logger.info(
        "⏳ Verifying exit protection at broker: %s | TP %s | SL %s",
        symbol,
        tp_order_id,
        sl_order_id,
    )

    while time.time() - start_time < timeout_seconds:
        tp_status = _status_for(tp_order_id)
        sl_status = _status_for(sl_order_id)
        last_tp_status = tp_status
        last_sl_status = sl_status

        tp_live = _is_live(tp_status)
        sl_live = _is_live(sl_status)

        if tp_live and sl_live:
            context.logger.order(
                "EXIT PROTECTION VERIFIED: %s | TP=%s (%s) | SL=%s (%s)",
                symbol,
                tp_order_id,
                tp_status,
                sl_order_id,
                sl_status,
            )
            return True, True, True, tp_status, sl_status

        if is_ib_rejected_or_cancelled_status(tp_status) or is_ib_rejected_or_cancelled_status(sl_status):
            break

        ib.sleep(check_interval)

    tp_live = _is_live(last_tp_status)
    sl_live = _is_live(last_sl_status)
    overall_success = tp_live and sl_live

    if not overall_success:
        context.logger.error(
            "❌ Exit protection verification failed: %s | TP=%s (%s) | SL=%s (%s)",
            symbol,
            tp_order_id,
            last_tp_status,
            sl_order_id,
            last_sl_status,
        )

    return overall_success, tp_live, sl_live, last_tp_status, last_sl_status
    verify_exit_timeout = float(
        getattr(tr_cfg, "exit_protection_verify_timeout_seconds", 8.0)
    )
    verify_exit_interval = float(
        getattr(tr_cfg, "exit_protection_verify_check_interval_seconds", 0.5)
    )

    if tp_success and sl_success:
        (
            protection_ok,
            tp_live,
            sl_live,
            tp_live_status,
            sl_live_status,
        ) = wait_for_exit_orders_live(
            context=context,
            ib=ib,
            symbol=symbol,
            tp_order_id=tp_id,
            sl_order_id=sl_id,
            timeout_seconds=verify_exit_timeout,
            check_interval=verify_exit_interval,
        )

        if not protection_ok:
            if not tp_live:
                tp_success = False
            if not sl_live:
                sl_success = False

            context.logger.error(
                "%s: Exit protection not confirmed after retry window. TP=%s, SL=%s",
                symbol,
                tp_live_status,
                sl_live_status,
            )


def verify_position_opened(
    context: TradingContext,
    ib: IB,
    symbol: str,
    expected_quantity: int,
) -> bool:
    """
    Verifiziert dass eine Position tatsaechlich geoeffnet wurde.

    Args:
        ib: IB Connection
        symbol: Symbol der Position
        expected_quantity: Erwartete Positionsgroesse

    Returns:
        bool: True wenn Position existiert
    """
    ib.sleep(1)

    for position in ib.positions():
        if position.contract.symbol == symbol:
            actual_qty = abs(position.position)
            if actual_qty == expected_quantity:
                context.logger.position(f"VERIFIED: {symbol} | Qty: {actual_qty}")
                return True
            context.logger.warning(
                f"⚠️ Position size mismatch: {symbol} "
                f"expected={expected_quantity}, actual={actual_qty}"
            )
            return True

    context.logger.error(f"❌ Position NOT found: {symbol}")
    return False


def _build_recovered_bot_trade_from_ib(
    context: TradingContext,
    ib: IB,
    symbol: str,
) -> Optional[dict]:
    """
    Build a best-effort bot trade state from IB for an untracked open position.

    Returns:
        dict with keys `state_key` and `state_data`, or None if recovery is not possible.
    """
    current_client_id = getattr(getattr(ib, "client", None), "clientId", None)

    ib_qty = 0
    ib_avg_cost = 0.0
    for position in ib.positions():
        if position.contract.symbol != symbol:
            continue
        qty = int(abs(position.position))
        if qty <= 0:
            continue
        ib_qty = qty
        ib_avg_cost = float(getattr(position, "avgCost", 0.0) or 0.0)
        break

    if ib_qty <= 0:
        return None

    candidate_trade = None
    candidate_order_id = -1
    for trade in ib.trades():
        if trade.contract.symbol != symbol:
            continue

        order = trade.order
        action = str(getattr(order, "action", "")).upper()
        if action != "BUY":
            continue

        order_client_id = getattr(order, "clientId", None)
        if (
            current_client_id is not None
            and order_client_id is not None
            and order_client_id != current_client_id
        ):
            continue

        status = str(getattr(trade.orderStatus, "status", ""))
        if not is_ib_entry_active_status(status):
            continue

        order_id = int(getattr(order, "orderId", -1) or -1)
        if order_id > candidate_order_id:
            candidate_order_id = order_id
            candidate_trade = trade

    if candidate_trade is None:
        return None

    tp_order_id = None
    sl_order_id = None
    for open_trade in ib.openTrades():
        if open_trade.contract.symbol != symbol:
            continue

        open_order = open_trade.order
        action = str(getattr(open_order, "action", "")).upper()
        if action != "SELL":
            continue

        open_order_client_id = getattr(open_order, "clientId", None)
        if (
            current_client_id is not None
            and open_order_client_id is not None
            and open_order_client_id != current_client_id
        ):
            continue

        order_type = str(getattr(open_order, "orderType", "")).upper()
        if order_type == "LMT" and tp_order_id is None:
            tp_order_id = int(open_order.orderId)
        elif order_type == "STP" and sl_order_id is None:
            sl_order_id = int(open_order.orderId)

    order_status = str(getattr(candidate_trade.orderStatus, "status", ""))
    filled_qty = int(getattr(candidate_trade.orderStatus, "filled", 0) or 0)

    fill_price = float(getattr(candidate_trade.orderStatus, "avgFillPrice", 0.0) or 0.0)
    if fill_price <= 0:
        fill_price = ib_avg_cost

    entry_price = float(getattr(candidate_trade.order, "lmtPrice", 0.0) or 0.0)
    if entry_price <= 0:
        entry_price = fill_price

    if entry_price <= 0:
        return None

    now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    opened_at = _infer_position_opened_at_from_ib(ib, symbol) or now
    state_key = f"recovered_{symbol}_{now}"
    is_filled = order_status == "Filled" and (filled_qty > 0 or ib_qty > 0)

    state_data = {
        "symbol": symbol,
        "signal_timestamp": now,
        "processed_at": now,
        "order_id": int(getattr(candidate_trade.order, "orderId", 0) or 0),
        "tp_order_id": tp_order_id,
        "sl_order_id": sl_order_id,
        "entry_price": entry_price,
        "fill_price": fill_price if fill_price > 0 else entry_price,
        "quantity": filled_qty if filled_qty > 0 else ib_qty,
        "status": BOT_STATUS_FILLED if is_filled else BOT_STATUS_SUBMITTED,
        "filled_at": now if is_filled else None,
        "opened_at": opened_at,
        "note": "recovered_from_ib_untracked_position",
    }

    context.logger.warning(
        "RECOVERY: %s detected as untracked IB position, restoring as bot trade (order=%s, status=%s)",
        symbol,
        state_data["order_id"],
        state_data["status"],
    )

    return {"state_key": state_key, "state_data": state_data}


def _build_history_based_bot_trade_from_ib(
    context: TradingContext,
    ib: IB,
    symbol: str,
) -> Optional[dict]:
    """
    Fallback recovery for symbols with bot history but no recoverable active BUY order.

    Uses current IB position snapshot and keeps source as bot-managed.
    """
    qty = 0
    avg_cost = 0.0

    for position in ib.positions():
        if position.contract.symbol != symbol:
            continue
        qty = int(abs(position.position))
        avg_cost = float(getattr(position, "avgCost", 0.0) or 0.0)
        break

    if qty <= 0:
        return None

    now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    opened_at = _infer_position_opened_at_from_ib(ib, symbol) or now
    entry_price = avg_cost if avg_cost > 0 else 0.0

    state_key = f"recovered_history_{symbol}_{now}"
    state_data = {
        "symbol": symbol,
        "signal_timestamp": opened_at,
        "processed_at": now,
        "order_id": 0,
        "tp_order_id": None,
        "sl_order_id": None,
        "entry_price": entry_price,
        "fill_price": entry_price,
        "quantity": qty,
        "status": BOT_STATUS_FILLED,
        "filled_at": opened_at,
        "opened_at": opened_at,
        "note": "recovered_from_ib_bot_history",
    }

    context.logger.warning(
        "RECOVERY: %s restored as bot trade from IB position snapshot (history-based)",
        symbol,
    )

    return {"state_key": state_key, "state_data": state_data}


def _ensure_exit_protection_for_filled_positions(
    context: TradingContext,
    ib: IB,
    processed: Dict[str, dict],
    cfg: dict,
    state_path: Path,
) -> None:
    """
    Ensure filled bot positions always have both TP and SL orders.
    Attempts to place missing protection orders and persists IDs into state.
    """
    from utils.input_validator import (
        ValidationError as InputValidationError,
        validate_price,
        validate_price_relationship,
    )

    tr_cfg = cfg["trading"]
    strict_emergency_exit = bool(
        getattr(tr_cfg, "force_emergency_exit_if_any_protection_missing", False)
    )

    ib_positions: Dict[str, dict] = {}
    for p in ib.positions():
        symbol = p.contract.symbol
        qty = int(abs(p.position))
        if qty <= 0:
            continue
        ib_positions[symbol] = {
            "qty": qty,
            "avg_cost": float(getattr(p, "avgCost", 0.0) or 0.0),
        }

    open_exits: Dict[str, dict] = {}
    for trade in ib.openTrades():
        symbol = trade.contract.symbol
        order = trade.order
        action = str(getattr(order, "action", "")).upper()
        if action != "SELL":
            continue

        order_type = str(getattr(order, "orderType", "")).upper()
        if symbol not in open_exits:
            open_exits[symbol] = {"tp_id": None, "sl_id": None}

        if order_type == "LMT" and open_exits[symbol]["tp_id"] is None:
            open_exits[symbol]["tp_id"] = int(order.orderId)
        elif order_type == "STP" and open_exits[symbol]["sl_id"] is None:
            open_exits[symbol]["sl_id"] = int(order.orderId)

    changed = False

    for _, trade_data in processed.items():
        if not isinstance(trade_data, dict):
            continue

        if not is_bot_filled_status(trade_data.get("status")):
            continue

        symbol = trade_data.get("symbol")
        if not symbol or symbol not in ib_positions:
            continue

        exits = open_exits.get(symbol, {"tp_id": None, "sl_id": None})
        has_tp = bool(exits.get("tp_id"))
        has_sl = bool(exits.get("sl_id"))

        if has_tp and has_sl:
            if not trade_data.get("tp_order_id"):
                trade_data["tp_order_id"] = exits["tp_id"]
                changed = True
            if not trade_data.get("sl_order_id"):
                trade_data["sl_order_id"] = exits["sl_id"]
                changed = True
            continue

        fill_price = float(trade_data.get("fill_price", 0.0) or 0.0)
        entry_price = float(trade_data.get("entry_price", 0.0) or 0.0)
        ref_price = fill_price if fill_price > 0 else entry_price
        if ref_price <= 0:
            ref_price = float(ib_positions[symbol].get("avg_cost", 0.0) or 0.0)

        if ref_price <= 0:
            context.logger.error(
                "%s: Cannot re-create TP/SL (missing reference price)",
                symbol,
            )
            continue

        tp_price = round(ref_price * (1 + tr_cfg.take_profit_pct / 100), 2)
        sl_price = round(ref_price * (1 - tr_cfg.stop_loss_pct / 100), 2)

        try:
            tp_price = validate_price(
                tp_price,
                field_name=f"recovery_tp_price for {symbol}",
                min_price=0.01,
                max_price=100000.0,
            )
            sl_price = validate_price(
                sl_price,
                field_name=f"recovery_sl_price for {symbol}",
                min_price=0.01,
                max_price=100000.0,
            )
            validate_price_relationship(ref_price, tp_price, sl_price, is_long=True)
        except InputValidationError as e:
            context.logger.error("%s: Recovery TP/SL validation failed: %s", symbol, e)
            continue

        qty = int(
            trade_data.get("quantity", 0)
            or ib_positions[symbol].get("qty", 0)
            or 0
        )
        if qty <= 0:
            context.logger.error("%s: Cannot re-create TP/SL (qty=0)", symbol)
            continue

        contract = Stock(symbol, "SMART", "USD")
        context.rate_limiter.wait_if_needed(
            request_type="qualify_contract",
            is_historical=False,
        )
        ib.qualifyContracts(contract)

        retry_handler = OrderRetryHandler(max_retries=2, retry_delay=1.0)
        oca_group = f"exit_recover_{symbol}_{int(time.time())}"

        tp_success = has_tp
        sl_success = has_sl

        if not has_tp:
            tp_id = ib.client.getReqId()
            tp_order = LimitOrder("SELL", qty, tp_price)
            tp_order.orderId = tp_id
            tp_order.tif = "GTC"
            tp_order.transmit = True
            tp_order.ocaGroup = oca_group
            tp_order.ocaType = 1

            context.rate_limiter.wait_if_needed(
                request_type="place_order", is_historical=False
            )
            tp_success, tp_error = retry_handler.place_order_with_retry(
                ib=ib,
                contract=contract,
                order=tp_order,
                symbol=symbol,
            )

            if tp_success:
                trade_data["tp_order_id"] = tp_id
                open_exits.setdefault(symbol, {})["tp_id"] = tp_id
                changed = True
                context.logger.order(
                    "TP RECOVERY SET: %s | Qty: %s | Price: $%.2f | Order %s",
                    symbol,
                    qty,
                    tp_price,
                    tp_id,
                )
            else:
                context.logger.error(
                    "%s: TP recovery order rejected: %s",
                    symbol,
                    tp_error,
                )

        if not has_sl:
            sl_id = ib.client.getReqId()
            sl_order = StopOrder("SELL", qty, sl_price)
            sl_order.orderId = sl_id
            sl_order.tif = "GTC"
            sl_order.transmit = True
            sl_order.ocaGroup = oca_group
            sl_order.ocaType = 1

            context.rate_limiter.wait_if_needed(
                request_type="place_order", is_historical=False
            )
            sl_success, sl_error = retry_handler.place_order_with_retry(
                ib=ib,
                contract=contract,
                order=sl_order,
                symbol=symbol,
            )

            if sl_success:
                trade_data["sl_order_id"] = sl_id
                open_exits.setdefault(symbol, {})["sl_id"] = sl_id
                changed = True
                context.logger.order(
                    "SL RECOVERY SET: %s | Qty: %s | Price: $%.2f | Order %s",
                    symbol,
                    qty,
                    sl_price,
                    sl_id,
                )
            else:
                context.logger.error(
                    "%s: SL recovery order rejected: %s",
                    symbol,
                    sl_error,
                )

        still_missing_both = not tp_success and not sl_success
        still_missing_any = not tp_success or not sl_success
        should_emergency_exit = (
            still_missing_both
            or (strict_emergency_exit and still_missing_any)
        )

        if should_emergency_exit:
            mode = "ANY_MISSING" if strict_emergency_exit else "BOTH_MISSING"
            context.logger.error(
                "EMERGENCY EXIT (recovery %s): %s | TP success=%s, SL success=%s",
                mode,
                symbol,
                tp_success,
                sl_success,
            )

            emergency_order_id = ib.client.getReqId()
            emergency_exit = MarketOrder("SELL", qty)
            emergency_exit.orderId = emergency_order_id
            emergency_exit.tif = "DAY"
            emergency_exit.transmit = True

            context.rate_limiter.wait_if_needed(
                request_type="place_order", is_historical=False
            )
            emergency_retry_handler = OrderRetryHandler(max_retries=3, retry_delay=1.0)
            emergency_success, emergency_error = emergency_retry_handler.place_order_with_retry(
                ib=ib,
                contract=contract,
                order=emergency_exit,
                symbol=symbol,
            )

            if emergency_success:
                context.logger.trade(
                    "EMERGENCY EXIT SENT (recovery): %s | Qty: %s | Order %s",
                    symbol,
                    qty,
                    emergency_order_id,
                )
            else:
                context.logger.error(
                    "CRITICAL: Emergency exit failed during TP/SL recovery for %s: %s",
                    symbol,
                    emergency_error,
                )

    if changed:
        save_state_with_retry(
            state_path,
            processed,
            max_retries=3,
            retry_delay=0.5,
        )


def log_daily_summary(
    context: TradingContext,
    ib: IB,
    processed: Dict[str, dict],
    loss_counter: DailyLossCounter,
) -> None:
    """Loggt Tages-Zusammenfassung."""
    from datetime import datetime

    today_str = datetime.now().strftime('%Y-%m-%d')

    trades_today = [
        t for t in processed.values()
        if isinstance(t, dict) and
        t.get('processed_at', '').startswith(today_str)
    ]

    if not trades_today:
        return

    closed_trades = [t for t in trades_today if t.get('status') == 'closed']

    if not closed_trades:
        return

    account_info = context.account_checker.get_account_info(
        ib, force_refresh=False
    )

    context.logger.performance("=" * 60)
    context.logger.performance("DAILY SUMMARY")
    context.logger.performance("=" * 60)
    if account_info:
        context.logger.performance(
            f"Cash Available: ${account_info.total_cash_value:,.2f}"
        )
    context.logger.performance(f"Total Trades: {len(closed_trades)}")
    context.logger.performance(
        f"Open Positions: {len([t for t in trades_today if is_bot_active_status(t.get('status'))])}"
    )
    context.logger.performance(
        f"Stop Losses Hit: {loss_counter.get_today_stop_loss_count()}"
    )
    context.logger.performance("=" * 60)


def main():
    from utils.trading_context import create_trading_context

    cfg = validate_and_get_config()
    ib_cfg = cfg["ib"]
    monitor_cfg = cfg["monitor"]
    tr_cfg = cfg["trading"]

    signals_path = Path(OUTPUT_DIR) / "signals.jsonl"
    archive_path = Path(OUTPUT_DIR) / "signals_archive.jsonl"
    processed_path = Path(STATE_DIR) / "processed_signals.json"
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    context = create_trading_context(
        cfg=cfg,
        state_dir=STATE_DIR,
        debug_mode=False,
    )

    archive_rotate_bytes = 10 * 1024 * 1024
    signal_queue_warning_bytes = max(
        1 * 1024 * 1024,
        int(getattr(tr_cfg, "signal_queue_warning_bytes", 50 * 1024 * 1024)),
    )
    signal_queue_rotate_bytes = max(
        5 * 1024 * 1024,
        int(getattr(tr_cfg, "signal_queue_rotate_bytes", 250 * 1024 * 1024)),
    )
    signal_queue_retention_files = max(
        1,
        int(getattr(tr_cfg, "signal_queue_retention_files", 30)),
    )
    signal_queue_warning_interval_seconds = max(
        60,
        int(
            getattr(
                tr_cfg,
                "signal_queue_warning_interval_seconds",
                900,
            )
        ),
    )
    processed_state_retention_days = max(
        1,
        int(getattr(tr_cfg, "processed_state_retention_days", 45)),
    )
    processed_state_cleanup_interval_seconds = max(
        60,
        int(getattr(tr_cfg, "processed_state_cleanup_interval_seconds", 3600)),
    )

    def rotate_archive_if_needed() -> None:
        if not archive_path.exists():
            return
        try:
            if archive_path.stat().st_size < archive_rotate_bytes:
                return
        except Exception as e:
            context.logger.warning("Could not stat archive file: %s", e)
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        rotated_path = archive_path.with_name(
            f"signals_archive_{timestamp}.jsonl"
        )
        try:
            archive_path.rename(rotated_path)
            context.logger.info("Rotated archive to %s", rotated_path)
        except Exception as e:
            context.logger.warning("Could not rotate archive file: %s", e)

    signal_offset = 0
    signal_partial_line = ""
    last_signal_queue_warning_at = 0.0
    last_state_cleanup_at = 0.0

    def monitor_signal_queue_size() -> None:
        """
        Monitor signals queue file growth and warn when it becomes too large.

        Warning-only by design: the trader does not truncate the queue file to avoid
        race conditions with the scanner process.
        """
        nonlocal last_signal_queue_warning_at

        if not signals_path.exists():
            return

        try:
            current_size = signals_path.stat().st_size
        except Exception as e:
            context.logger.warning("Could not stat signals queue file: %s", e)
            return

        if current_size < signal_queue_warning_bytes:
            return

        now_ts = time.time()
        if now_ts - last_signal_queue_warning_at < signal_queue_warning_interval_seconds:
            return

        mb = current_size / (1024 * 1024)
        warn_mb = signal_queue_warning_bytes / (1024 * 1024)
        context.logger.warning(
            "Signals queue file is large: %.1f MB (threshold %.1f MB). "
            "Reader uses incremental offsets, but file is not auto-truncated for safety.",
            mb,
            warn_mb,
        )
        last_signal_queue_warning_at = now_ts

    def read_new_signal_lines() -> list[str]:
        """
        Read only newly appended complete lines from signals file.

        Uses file offset + partial line buffering to avoid read/clear races.
        """
        nonlocal signal_offset, signal_partial_line

        if not signals_path.exists():
            return []

        try:
            file_size = signals_path.stat().st_size
        except Exception as e:
            context.logger.warning("Could not stat signals file: %s", e)
            return []

        if file_size < signal_offset:
            context.logger.warning(
                "Signals file was truncated/rotated. Resetting read offset."
            )
            signal_offset = 0
            signal_partial_line = ""

        try:
            with open(signals_path, "r", encoding="utf-8") as f:
                f.seek(signal_offset)
                chunk = f.read()
                signal_offset = f.tell()
        except Exception as e:
            context.logger.warning("Could not read signals incrementally: %s", e)
            return []

        if not chunk and not signal_partial_line:
            return []

        data = f"{signal_partial_line}{chunk}"
        complete_lines = data.splitlines()

        if data and not data.endswith("\n"):
            signal_partial_line = complete_lines.pop() if complete_lines else data
        else:
            signal_partial_line = ""

        return [line for line in complete_lines if line.strip()]

    def compact_signal_queue_if_safe() -> None:
        """
        Compact consumed queue file when trader is fully caught up.

        Safety rule: only rotate if no unread bytes remain (offset == file size)
        and no partial line is buffered.
        """
        nonlocal signal_offset, signal_partial_line

        if not signals_path.exists() or signal_partial_line:
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        rotated_path = signals_path.with_name(f"signals_queue_{timestamp}.jsonl")

        try:
            with file_lock(signals_path, timeout=10.0, mode="exclusive"):
                if not signals_path.exists():
                    return

                file_size = signals_path.stat().st_size
                if file_size < signal_queue_rotate_bytes:
                    return

                # Re-check under lock to avoid TOCTOU races with scanner writes.
                if signal_offset < file_size:
                    return

                signals_path.rename(rotated_path)
                context.logger.info(
                    "Compacted consumed signal queue: %s -> %s",
                    signals_path,
                    rotated_path,
                )
                signal_offset = 0
                signal_partial_line = ""
        except Exception as e:
            context.logger.warning("Could not compact signals queue: %s", e)
            return

        try:
            rotated_files = sorted(
                glob.glob(str(signals_path.with_name("signals_queue_*.jsonl"))),
                key=lambda p: Path(p).stat().st_mtime,
                reverse=True,
            )
        except Exception as e:
            context.logger.warning("Could not list rotated signal queues: %s", e)
            return

        for old_path in rotated_files[signal_queue_retention_files:]:
            try:
                Path(old_path).unlink(missing_ok=True)
                context.logger.info("Removed old rotated signal queue: %s", old_path)
            except Exception as e:
                context.logger.warning("Could not remove old rotated queue %s: %s", old_path, e)

    def prune_processed_state_if_due(force: bool = False) -> None:
        """Prune old closed/rejected/manual_closed entries from processed state."""
        nonlocal processed, last_state_cleanup_at

        now_ts = time.time()
        if not force and (now_ts - last_state_cleanup_at) < processed_state_cleanup_interval_seconds:
            return

        cutoff = datetime.now(timezone.utc) - timedelta(days=processed_state_retention_days)

        def _prune(state: Dict[str, dict]) -> Dict[str, dict]:
            pruned = {}
            removed_count = 0
            for k, v in state.items():
                if isinstance(v, dict) and _should_prune_state_entry(v, cutoff):
                    removed_count += 1
                    continue
                pruned[k] = v

            if removed_count:
                context.logger.info(
                    "State retention cleanup: removed %s old entries (retention=%s days)",
                    removed_count,
                    processed_state_retention_days,
                )
            return pruned

        updated = update_state_atomically(
            processed_path,
            _prune,
            max_retries=5,
        )

        if updated:
            processed = load_state_with_retry(processed_path, max_retries=2)
            last_state_cleanup_at = now_ts

    processed: Dict[str, dict] = load_state_with_retry(
        processed_path,
        max_retries=5,
        retry_delay=0.5,
    )

    # Daily Loss Protection initialisieren
    loss_counter_path = Path(STATE_DIR) / "daily_losses.json"
    loss_counter = DailyLossCounter(loss_counter_path)

    reconciliator = PositionReconciliator()
    context.logger.info("Position Reconciliator initialized")

    # Set fuer bereits gezaehlte SL-Orders
    counted_sl_orders: set = set()

    # NEU: Connection Manager erstellen
    conn_config = ConnectionConfig(
        host=ib_cfg.host,
        port=ib_cfg.port,
        client_id=ib_cfg.trader_client_id,
        max_retries=10,
        initial_retry_delay=1.0,
        max_retry_delay=60.0,
    )
    ib_manager = IBConnectionManager(conn_config)

    # Initial connect mit Retry-Logik
    try:
        ib = ib_manager.connect()
    except ConnectionError as e:
        context.logger.error(f"Could not establish initial connection: {e}")
        context.logger.error(
            "Please check TWS/Gateway is running and settings are correct"
        )
        return

    context.logger.info("✅ Connected to IB")

    context.account_checker.log_account_status(ib)

    try:
        account_info = context.account_checker.get_account_info(
            ib, force_refresh=True
        )
        if account_info:
            bot_positions = _count_bot_open_positions(processed)
            context.position_sizer.log_position_info(
                account_balance=account_info.net_liquidation,
                available_cash=account_info.total_cash_value,
                current_open_positions=bot_positions,
            )
    except Exception as e:
        context.logger.warning(f"Could not log position sizing info: {e}")

    # Log Daily Loss Protection Status
    sl_count = loss_counter.get_today_stop_loss_count()
    context.logger.info(
        f"Daily Loss Protection: {sl_count}/{tr_cfg.max_daily_stop_losses} "
        f"stop-losses today"
    )

    _seed_startup_positions_and_cooldowns(
        context=context,
        ib=ib,
        processed=processed,
        processed_path=processed_path,
        reconciliator=reconciliator,
    )
    prune_processed_state_if_due(force=True)

    # Log Bot-Positionen
    bot_positions = _count_bot_open_positions(processed)

    # Verwende safe call fuer IB-Operationen
    def get_ib_positions(ib):
        return _count_open_positions(ib)

    ib_positions = (
        _safe_ib_call(context, ib_manager, "get_positions", get_ib_positions) or 0
    )

    context.logger.info(
        f"Position Status: {bot_positions} bot positions, "
        f"{ib_positions} total IB positions"
    )
    if ib_positions > bot_positions:
        context.logger.warning(
            f"⚠️ {ib_positions - bot_positions} manual/external positions detected!"
        )

    # Letzte Connection-Check Zeit
    last_connection_check = time.time()
    connection_check_interval = ib_cfg.connection_check_interval_seconds
    market_schedule = MarketSchedule()
    pre_market_start_minutes = max(0.0, float(monitor_cfg.pre_market_start_minutes))
    post_market_stop_minutes = max(0.0, float(monitor_cfg.post_market_stop_minutes))

    try:
        loop_counter = 0
        manual_symbols_logged: set[str] = set()
        failed_entry_block_until: Dict[str, float] = {}

        while True:
            if not market_schedule.is_active_trading_window(
                pre_market_start_minutes=pre_market_start_minutes,
                post_market_stop_minutes=post_market_stop_minutes,
            ):
                wait_seconds = min(
                    market_schedule.seconds_until_active_window(
                        pre_market_start_minutes=pre_market_start_minutes,
                        post_market_stop_minutes=post_market_stop_minutes,
                    ),
                    60.0,
                )
                context.logger.info(
                    "Außerhalb Aktivitätsfenster (%s) – Trader pausiert für %.0fs",
                    market_schedule.get_status_string(),
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            loop_counter += 1

            # NEU: Periodischer Connection-Check (alle 30 Sekunden)
            current_time = time.time()
            if current_time - last_connection_check > connection_check_interval:
                if not ib_manager.check_connection():
                    context.logger.warning("Connection check failed, reconnecting...")
                    try:
                        ib = ib_manager.reconnect()
                        context.logger.info("✅ Reconnected successfully")
                    except Exception as e:
                        context.logger.error(f"Reconnect failed: {e}")
                        context.logger.warning("Will retry on next check...")
                last_connection_check = current_time

            # Periodische Summary (alle 100 Loops = ca. alle 100 Sekunden)
            if loop_counter % 100 == 0:
                log_daily_summary(context, ib, processed, loss_counter)

            # Dashboard Logging (alle 100 Loops = ~100 Sekunden)
            if loop_counter % 100 == 0:
                try:
                    account_info_dict = None
                    acc_info = context.account_checker.get_account_info(
                        ib, force_refresh=False
                    )
                    if acc_info:
                        account_info_dict = {
                            "net_liquidation": acc_info.net_liquidation,
                            "buying_power": acc_info.buying_power,
                            "total_cash_value": acc_info.total_cash_value,
                        }

                    context.dashboard.log_dashboard(ib, processed, account_info_dict)
                except Exception as e:
                    context.logger.error(f"Error logging dashboard: {e}")

            if loop_counter % 200 == 0:
                try:
                    account_info = context.account_checker.get_account_info(
                        ib, force_refresh=True
                    )
                    if account_info:
                        context.logger.info(
                            "Account Status: Balance=$%.2f, Buying Power=$%.2f",
                            account_info.net_liquidation,
                            account_info.buying_power,
                        )
                except Exception as e:
                    context.logger.warning("Could not log account status: %s", e)

            if loop_counter % 500 == 0:
                try:
                    context.rate_limiter.log_statistics()
                except Exception as e:
                    context.logger.warning("Could not log rate limiter stats: %s", e)

            if loop_counter % 1000 == 0:
                try:
                    context.cooldown_manager.cleanup_expired()
                    context.cooldown_manager.log_active_cooldowns()
                except Exception as e:
                    context.logger.warning("Could not cleanup cooldowns: %s", e)

            if loop_counter % 100 == 0:
                monitor_signal_queue_size()
                compact_signal_queue_if_safe()

            if loop_counter % 30 == 0:
                prune_processed_state_if_due(force=False)

            # Position Reconciliation (alle 100 Loops = ~100 Sekunden)
            if loop_counter % 100 == 0:
                try:
                    discrepancies = reconciliator.reconcile_positions(
                        ib=ib,
                        processed=processed,
                        auto_correct=True,
                    )

                    if discrepancies:
                        context.logger.warning(
                            f"RECONCILIATION: {len(discrepancies)} discrepancy(ies) found"
                        )
                        for disc in discrepancies:
                            context.logger.warning(
                                f"  - {disc.symbol}: {disc.discrepancy_type} "
                                f"({disc.severity}) | {disc.suggested_action}"
                            )

                        if any(d.severity in ["ERROR", "WARNING"] for d in discrepancies):
                            save_state_with_retry(
                                processed_path,
                                processed,
                                max_retries=3,
                                retry_delay=0.5,
                            )
                except Exception as e:
                    context.logger.error(
                        f"Error during position reconciliation: {e}"
                    )

            # Reconciliation Stats (alle 1000 Loops)
            if loop_counter % 1000 == 0:
                try:
                    reconciliator.log_statistics()
                except Exception as e:
                    context.logger.warning(
                        f"Could not log reconciliation stats: {e}"
                    )

            # Stop-Loss und Position-Status Monitoring (alle 5 Loops)
            if loop_counter % 5 == 0:
                # Wrapped in safe call
                def check_sl(ib):
                    return _check_for_filled_stop_losses(
                        context, ib, processed, loss_counter, counted_sl_orders
                    )

                result = _safe_ib_call(
                    context, ib_manager, "check_stop_losses", check_sl
                )
                if result is not None:
                    counted_sl_orders = result

                # Position-Status Update
                def update_status(ib):
                    _update_position_status(context, ib, processed, processed_path)
                    return True

                _safe_ib_call(context, ib_manager, "update_position_status", update_status)

                def ensure_exit_protection(ib):
                    _ensure_exit_protection_for_filled_positions(
                        context=context,
                        ib=ib,
                        processed=processed,
                        cfg=cfg,
                        state_path=processed_path,
                    )
                    return True

                _safe_ib_call(
                    context,
                    ib_manager,
                    "ensure_exit_protection",
                    ensure_exit_protection,
                )

            if not signals_path.exists():
                time.sleep(1)
                continue

            lines = read_new_signal_lines()

            for line in lines:
                parsed = _parse_signal_line(context, line)
                if parsed is None:
                    continue
                sig, price = parsed
                key = f"{sig['symbol']}_{sig['now_utc']}"
                if key in processed:
                    continue

                symbol = sig["symbol"]

                block_until = failed_entry_block_until.get(symbol, 0.0)
                now_ts = time.time()
                if block_until > now_ts:
                    remaining = int(max(1, block_until - now_ts))
                    context.logger.info(
                        f"Skip {symbol}: retry block active ({remaining}s remaining)"
                    )
                    continue

                if block_until > 0 and block_until <= now_ts:
                    failed_entry_block_until.pop(symbol, None)

                # Bot-Position Checks (keine IB-Calls noetig)
                if _has_bot_position(symbol, processed):
                    context.logger.info(f"Skip {symbol}: bot position exists")
                    continue

                if _has_bot_open_order(symbol, processed):
                    context.logger.info(f"Skip {symbol}: bot open order exists")
                    continue

                if _has_manual_position(symbol, processed):
                    context.logger.info(
                        f"Skip {symbol}: manual/external position recorded"
                    )
                    continue

                # Harte Sperre: Keine neue Order wenn bei IB bereits offene Order existiert
                def check_open_order(ib):
                    return _has_open_order(ib, symbol)

                has_ib_open_order = _safe_ib_call(
                    context, ib_manager, "check_open_order", check_open_order
                )

                # Fail-safe: Bei unklarem IB-Status niemals Entry versuchen
                if has_ib_open_order is None:
                    context.logger.warning(
                        f"Skip {symbol}: IB open-order state unknown (connection/error)"
                    )
                    continue

                if has_ib_open_order:
                    context.logger.info(f"Skip {symbol}: IB open order exists")
                    continue

                # Double-Check mit IB (mit safe call)
                def check_position(ib):
                    return _has_position(ib, symbol)

                has_ib_position = _safe_ib_call(
                    context, ib_manager, "check_position", check_position
                )

                # Fail-safe: Bei unklarem IB-Status niemals Entry versuchen
                if has_ib_position is None:
                    context.logger.warning(
                        f"Skip {symbol}: IB position state unknown (connection/error)"
                    )
                    continue

                if has_ib_position:
                    if not _has_manual_position(symbol, processed):
                        recovered = _build_recovered_bot_trade_from_ib(
                            context=context,
                            ib=ib,
                            symbol=symbol,
                        )

                        if not recovered and _has_any_bot_history(symbol, processed):
                            recovered = _build_history_based_bot_trade_from_ib(
                                context=context,
                                ib=ib,
                                symbol=symbol,
                            )

                        if recovered:
                            def add_recovered_trade(state):
                                state[recovered["state_key"]] = recovered["state_data"]
                                return state

                            update_state_atomically(
                                processed_path,
                                add_recovered_trade,
                                max_retries=5,
                            )
                            processed = load_state_with_retry(
                                processed_path,
                                max_retries=2,
                            )
                            continue

                        if symbol not in manual_symbols_logged:
                            context.logger.warning(
                                f"⚠️ {symbol}: IB shows position but not in processed! "
                                f"(Manual trade or sync issue) - Skipping"
                            )
                            manual_symbols_logged.add(symbol)

                        def add_manual(state):
                            manual_key = f"manual_{symbol}"
                            now = datetime.now(timezone.utc).isoformat(
                                timespec="microseconds"
                            )
                            opened_at = _infer_position_opened_at_from_ib(ib, symbol) or now
                            entry = state.get(manual_key)
                            if isinstance(entry, dict):
                                entry["last_seen_at"] = now
                                entry.setdefault("opened_at", opened_at)
                                state[manual_key] = entry
                                return state

                            state[manual_key] = {
                                "symbol": symbol,
                                "processed_at": now,
                                "first_seen_at": now,
                                "last_seen_at": now,
                                "opened_at": opened_at,
                                "status": BOT_STATUS_MANUAL,
                                "note": "detected from IB positions",
                            }
                            return state

                        update_state_atomically(
                            processed_path,
                            add_manual,
                            max_retries=5,
                        )
                        processed = load_state_with_retry(
                            processed_path, max_retries=2
                        )
                    continue

                # Daily Loss Check
                if loss_counter.is_daily_loss_limit_reached(
                    tr_cfg.max_daily_stop_losses
                ):
                    sl_count = loss_counter.get_today_stop_loss_count()
                    context.logger.warning(
                        f"🛑 DAILY LOSS LIMIT REACHED! "
                        f"Stop-Losses today: {sl_count}/{tr_cfg.max_daily_stop_losses} | "
                        f"Skipping {symbol}"
                    )
                    continue

                is_blocked, block_reason = context.cooldown_manager.is_on_cooldown(
                    symbol
                )
                if is_blocked:
                    if block_reason:
                        context.logger.info(f"Skip {symbol}: {block_reason}")
                    continue

                current_open_positions = _count_bot_open_positions(processed)

                # Order senden (mit safe call)
                def send_order(ib):
                    try:
                        return send_bracket_order(
                            context,
                            ib,
                            symbol,
                            price,
                            current_open_positions,
                            cfg,
                        )
                    except Exception as e:
                        context.logger.error(
                            f"Exception in send_bracket_order for {symbol}: "
                            f"{type(e).__name__}: {e}"
                        )
                        return None

                order_ids = _safe_ib_call(
                    context, ib_manager, "send_bracket_order", send_order
                )

                if order_ids is None:
                    context.logger.warning(
                        f"Failed to open trade for {symbol} "
                        f"(order submission or fill failed)"
                    )

                    retry_block_seconds = max(
                        0, int(getattr(tr_cfg, "entry_retry_block_seconds", 60))
                    )
                    if retry_block_seconds > 0:
                        failed_entry_block_until[symbol] = (
                            time.time() + retry_block_seconds
                        )
                        context.logger.warning(
                            f"{symbol}: retry blocked for {retry_block_seconds}s "
                            f"after failed entry"
                        )

                    def add_rejection(state):
                        state[key] = {
                            "symbol": symbol,
                            "signal_timestamp": sig["now_utc"],
                            "processed_at": datetime.now(timezone.utc).isoformat(
                                timespec="microseconds"
                            ),
                            "entry_price": price,
                            "status": "rejected",
                            "reject_reason": "order_failed_or_insufficient_cash",
                        }
                        return state

                    update_state_atomically(
                        processed_path,
                        add_rejection,
                        max_retries=5,
                    )
                    continue

                # Atomic update to avoid race conditions with scanner
                def add_trade(state):
                    opened_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
                    state[key] = {
                        "symbol": symbol,
                        "signal_timestamp": sig["now_utc"],
                        "processed_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
                        "order_id": order_ids["parent_id"],
                        "tp_order_id": order_ids["tp_id"],
                        "sl_order_id": order_ids["sl_id"],
                        "entry_price": price,
                        "fill_price": order_ids.get("fill_price", price),
                        "quantity": order_ids.get("quantity", 0),
                        "status": BOT_STATUS_FILLED,
                        "filled_at": opened_at,
                        "opened_at": opened_at,
                    }
                    return state

                if not update_state_atomically(processed_path, add_trade, max_retries=5):
                    context.logger.error(f"Failed to save trade state for {symbol}")
                    # Trade opened, but state could not be saved
                else:
                    # Refresh local copy
                    processed = load_state_with_retry(processed_path, max_retries=2)
                    failed_entry_block_until.pop(symbol, None)

            if lines:
                try:
                    rotate_archive_if_needed()
                    with open(archive_path, "a", encoding="utf-8") as archive:
                        archive.writelines(f"{line}\n" for line in lines)
                    context.logger.debug(
                        "Archived %s new signals from %s",
                        len(lines),
                        signals_path,
                    )
                except Exception as e:
                    context.logger.warning(
                        "Could not archive signals file lines: %s",
                        e,
                    )

            compact_signal_queue_if_safe()

            time.sleep(1)

    except KeyboardInterrupt:
        context.logger.info("Trader stopped by user")
    except Exception as e:
        context.logger.error(
            f"Unexpected error in main loop: {type(e).__name__}: {e}",
            exc_info=True,
        )
    finally:
        context.logger.info("Shutting down...")
        ib_manager.disconnect()
        context.logger.info("Disconnected from IB")


def print_dashboard_snapshot():
    """
    Utility function to fetch dashboard manually.
    Can be called from another script or REPL.
    """
    from utils.state_retry import load_state_with_retry
    from utils.ib_connection import IBConnectionManager, ConnectionConfig
    from utils.account_checker import AccountChecker

    cfg = validate_and_get_config()
    ib_cfg = cfg["ib"]

    processed_path = Path(STATE_DIR) / "processed_signals.json"
    processed = load_state_with_retry(processed_path, max_retries=2)

    conn_config = ConnectionConfig(
        host=ib_cfg.host,
        port=ib_cfg.port,
        client_id=ib_cfg.trader_client_id + 100,
        max_retries=3,
    )
    ib_manager = IBConnectionManager(conn_config)

    try:
        ib = ib_manager.connect()

        acc_checker = AccountChecker()
        acc_info = acc_checker.get_account_info(ib, force_refresh=True)

        account_info_dict = None
        if acc_info:
            account_info_dict = {
                "net_liquidation": acc_info.net_liquidation,
                "buying_power": acc_info.buying_power,
            }

        dashboard_instance = create_dashboard()
        dashboard_text = dashboard_instance.generate_dashboard(
            ib, processed, account_info_dict
        )

        print(dashboard_text)

    finally:
        ib_manager.disconnect()


if __name__ == "__main__":
    main()
