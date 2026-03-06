"""
Broker Monitor – läuft 24/7.
Verantwortlichkeiten:
  1. IB-Verbindung (client_id=10) dauerhaft aufrecht erhalten
  2. Marktzeiten überwachen und Scanner+Trader starten/stoppen
  3. Positionen und Trade-Status auch außerhalb Marktzeiten überwachen
  4. End-of-Day Report nach Marktschluss erstellen
  5. Heartbeat-Check und automatischer Reconnect
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import validate_and_get_config
from monitor.position_tracker import PositionTracker
from monitor.process_manager import ProcessManager
from utils.account_checker import AccountChecker
from utils.daily_loss_counter import DailyLossCounter
from utils.ib_connection import ConnectionConfig, IBConnectionManager
from utils.logging_utils import setup_logging
from utils.market_schedule import ET, MarketSchedule
from utils.paths import PROJECT_ROOT, STATE_DIR
from utils.state_retry import load_state_with_retry
from utils.state_utils import save_state
from utils.symbol_cooldown import SymbolCooldownManager


PERFORMANCE_LOG_INTERVAL_SECONDS = 15 * 60


def _build_monitor_state(
    *,
    ib_connected: bool,
    market_open: bool,
    process_status: dict,
    processed: dict,
    ib,
) -> dict:
    open_positions = 0
    if ib is not None and ib_connected:
        try:
            open_positions = sum(1 for p in ib.positions() if p.position != 0)
        except Exception:
            open_positions = 0

    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.date()
    today_trades = 0
    today_pnl = 0.0

    for trade_data in processed.values():
        if not isinstance(trade_data, dict):
            continue

        processed_at = trade_data.get("processed_at")
        if not processed_at:
            continue

        try:
            ts = datetime.fromisoformat(str(processed_at).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        if ts.astimezone(timezone.utc).date() != today_utc:
            continue

        today_trades += 1
        today_pnl += float(trade_data.get("realized_pnl_usd", 0.0) or 0.0)

    return {
        "last_heartbeat": now_utc.isoformat(),
        "market_open": market_open,
        "scanner_running": bool(process_status.get("scanner_running", False)),
        "trader_running": bool(process_status.get("trader_running", False)),
        "ib_connected": ib_connected,
        "open_positions": open_positions,
        "today_trades": today_trades,
        "today_pnl_usd": round(today_pnl, 2),
    }


def _log_monitor_performance(logger, monitor_state: dict, market_status: str) -> None:
    """Backward compatible compact snapshot helper."""
    logger.performance(
        "Monitor Snapshot | Market: %s | IB: %s | Scanner: %s | Trader: %s | "
        "Open Positions: %s | Today Trades: %s | Today P&L: $%+.2f",
        market_status,
        "connected" if monitor_state.get("ib_connected") else "disconnected",
        "running" if monitor_state.get("scanner_running") else "stopped",
        "running" if monitor_state.get("trader_running") else "stopped",
        int(monitor_state.get("open_positions", 0) or 0),
        int(monitor_state.get("today_trades", 0) or 0),
        float(monitor_state.get("today_pnl_usd", 0.0) or 0.0),
    )


def _parse_iso_utc(ts: object) -> Optional[datetime]:
    if not ts:
        return None
    try:
        parsed = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


_PERF_WIDTH = 102


def _pf_divider() -> str:
    """Return a full-width heavy divider line."""
    return "═" * _PERF_WIDTH


def _pf_header(title: str) -> str:
    """Return a boxed section header line with fixed width."""
    inner_width = _PERF_WIDTH - 2
    prefix = f"─ {title} "
    tail_len = max(0, inner_width - len(prefix))
    return f"┌{prefix}{'─' * tail_len}┐"


def _pf_footer() -> str:
    """Return a boxed section footer line with fixed width."""
    return f"└{'─' * (_PERF_WIDTH - 2)}┘"


def _pf_line(content: str = "") -> str:
    """Return a boxed content line."""
    content_width = _PERF_WIDTH - 5
    text = str(content)
    if len(text) > content_width:
        text = text[:content_width]
    return f"│  {text.ljust(content_width)}│"


def _pf_duration(start_iso: str) -> str:
    """Calculate duration from ISO timestamp to now (UTC)."""
    if not start_iso:
        return "?"

    try:
        start = datetime.fromisoformat(str(start_iso).replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - start.astimezone(timezone.utc)
        total_seconds = max(0, int(delta.total_seconds()))
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        return f"{hours}h {minutes:02d}m"
    except Exception:
        return "?"


def _log_monitor_performance_verbose(
    *,
    logger,
    monitor_state: dict,
    market_status: str,
    processed: dict,
    ib,
    account_info,
) -> None:
    try:
        now_utc = datetime.now(timezone.utc)
        today_utc = now_utc.date()

        open_statuses = {
            "filled",
            "submitted",
            "manual",
            "presubmitted",
            "pendingsubmit",
            "pending_submit",
            "pending",
        }
        closed_statuses = {"closed", "manual_closed"}

        status_counts: dict[str, int] = {}
        todays_trades: list[dict] = []
        closed_today_rows: list[dict] = []
        open_state_rows: list[dict] = []

        for trade_data in processed.values():
            if not isinstance(trade_data, dict):
                continue

            status = str(trade_data.get("status", "")).strip().lower() or "unknown"
            status_counts[status] = status_counts.get(status, 0) + 1

            processed_dt = _parse_iso_utc(trade_data.get("processed_at"))
            if processed_dt is not None and processed_dt.date() == today_utc:
                todays_trades.append(trade_data)

            symbol = str(trade_data.get("symbol") or "?")
            quantity = int(trade_data.get("quantity") or 0)
            entry_price = float(
                trade_data.get("fill_price")
                or trade_data.get("entry_price")
                or trade_data.get("avg_cost")
                or 0.0
            )

            if status in open_statuses:
                opened_at = (
                    trade_data.get("filled_at")
                    or trade_data.get("first_seen_at")
                    or trade_data.get("signal_timestamp")
                    or trade_data.get("processed_at")
                    or ""
                )
                source = "MANUAL" if status == "manual" else "BOT"
                open_state_rows.append(
                    {
                        "symbol": symbol,
                        "status": status,
                        "quantity": quantity,
                        "entry_price": entry_price,
                        "source": source,
                        "opened_at": str(opened_at),
                    }
                )

            closed_at = _parse_iso_utc(trade_data.get("closed_at"))
            if status in closed_statuses and closed_at is not None and closed_at.date() == today_utc:
                closed_today_rows.append(
                    {
                        "closed_at": closed_at,
                        "symbol": symbol,
                        "quantity": quantity,
                        "entry_price": entry_price,
                        "exit_price": float(trade_data.get("exit_price") or 0.0),
                        "pnl": float(trade_data.get("realized_pnl_usd") or 0.0),
                    }
                )

        # Today's performance aggregates (based on processed_at == today UTC)
        todays_open = []
        todays_closed = []
        for t in todays_trades:
            st = str(t.get("status", "")).strip().lower()
            if st in open_statuses:
                todays_open.append(t)
            if st in closed_statuses:
                todays_closed.append(t)

        winners = [t for t in todays_closed if float(t.get("realized_pnl_usd") or 0.0) > 0]
        losers = [t for t in todays_closed if float(t.get("realized_pnl_usd") or 0.0) < 0]
        sl_hits = [
            t for t in todays_closed if str(t.get("close_reason", "")).strip().lower() == "stop_loss"
        ]
        realized_today = sum(float(t.get("realized_pnl_usd") or 0.0) for t in todays_closed)
        closed_count = len(todays_closed)
        win_rate = (len(winners) / closed_count * 100.0) if closed_count > 0 else 0.0

        # Live market data via ib.portfolio() (reference: trading_dashboard._generate_live_positions)
        ib_portfolio: dict[str, dict] = {}
        ib_connected = bool(monitor_state.get("ib_connected")) and ib is not None
        if ib_connected:
            try:
                for item in ib.portfolio():
                    symbol = item.contract.symbol
                    if item.position != 0:
                        ib_portfolio[symbol] = {
                            "market_price": float(item.marketPrice or 0.0),
                            "unrealized_pnl": float(item.unrealizedPNL or 0.0),
                            "quantity": int(item.position),
                        }
            except Exception as e:
                logger.performance(_pf_line(f"[IB Portfolio Fehler: {e}]"))

        open_rows: list[dict] = []
        total_entry_cost = 0.0
        total_qty = 0
        total_pnl = 0.0
        total_pnl_available = True

        for row in open_state_rows:
            symbol = row["symbol"]
            entry_price = float(row["entry_price"] or 0.0)
            quantity = int(row["quantity"] or 0)
            source = row["source"]
            opened_at = row["opened_at"]

            ib_pos = ib_portfolio.get(symbol, {})
            has_live = ib_connected

            current_price = ib_pos.get("market_price", entry_price) if has_live else None
            unrealized_pnl = ib_pos.get("unrealized_pnl", 0.0) if has_live else None

            if has_live and unrealized_pnl == 0.0 and current_price and entry_price > 0:
                unrealized_pnl = (current_price - entry_price) * quantity

            pnl_pct = (
                ((current_price - entry_price) / entry_price * 100.0)
                if has_live and current_price is not None and entry_price > 0
                else None
            )

            entry_cost = entry_price * quantity
            duration = _pf_duration(opened_at)

            total_entry_cost += entry_cost
            total_qty += quantity
            if has_live and unrealized_pnl is not None:
                total_pnl += float(unrealized_pnl)
            else:
                total_pnl_available = False

            open_rows.append(
                {
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "entry_cost": entry_cost,
                    "quantity": quantity,
                    "unrealized_pnl": unrealized_pnl,
                    "pnl_pct": pnl_pct,
                    "duration": duration,
                    "source": source,
                }
            )

        open_rows.sort(
            key=lambda r: float(r["unrealized_pnl"]) if r["unrealized_pnl"] is not None else float("-inf"),
            reverse=True,
        )

        ib_label = "🟢 Connected" if bool(monitor_state.get("ib_connected")) else "🔴 Disconnected"
        market_label = "🟢 Open" if bool(monitor_state.get("market_open")) else "🔴 Closed"
        scanner_label = "🟢 Running" if bool(monitor_state.get("scanner_running")) else "⏸ Stopped"
        trader_label = "🟢 Running" if bool(monitor_state.get("trader_running")) else "⏸ Stopped"

        logger.performance(_pf_divider())
        logger.performance(
            _pf_line(
                "BROKER MONITOR - PERFORMANCE SNAPSHOT  |  "
                f"{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}  |  {market_status}"
            )
        )
        logger.performance(_pf_divider())
        logger.performance("")

        logger.performance(_pf_header("SYSTEM STATUS"))
        logger.performance(_pf_line(f"IB Connection:   {ib_label:<16} Scanner:   {scanner_label}"))
        logger.performance(_pf_line(f"Market:          {market_label:<16} Trader:    {trader_label}"))
        logger.performance(
            _pf_line(
                f"Open Positions:  {int(monitor_state.get('open_positions', 0) or 0)}"
                f"{' ' * 21}Today Trades:  {int(monitor_state.get('today_trades', 0) or 0)}"
            )
        )
        logger.performance(_pf_footer())
        logger.performance("")

        logger.performance(_pf_header("ACCOUNT"))
        if account_info:
            logger.performance(
                _pf_line(
                    "Net Liquidation:   "
                    f"${float(account_info.get('net_liquidation', 0.0) or 0.0):,.2f}"
                )
            )
            logger.performance(
                _pf_line(
                    "Total Cash (USD):  "
                    f"${float(account_info.get('total_cash_value', 0.0) or 0.0):,.2f}"
                )
            )
            logger.performance(
                _pf_line(
                    "Buying Power:      "
                    f"${float(account_info.get('buying_power', 0.0) or 0.0):,.2f}"
                )
            )
        else:
            logger.performance(_pf_line("Account data not available."))
        logger.performance(_pf_footer())
        logger.performance("")

        logger.performance(_pf_header("TODAY'S PERFORMANCE"))
        logger.performance(
            _pf_line(
                f"Trades:            {len(todays_trades)}        "
                f"(Closed: {closed_count} | Open: {len(todays_open)})"
            )
        )
        logger.performance(
            _pf_line(
                f"Winners (TP):      {len(winners)}        "
                f"({win_rate:.1f}% of closed)"
            )
        )
        loss_rate = (len(losers) / closed_count * 100.0) if closed_count > 0 else 0.0
        logger.performance(
            _pf_line(
                f"Losers  (SL):      {len(losers)}        "
                f"({loss_rate:.1f}% of closed)"
            )
        )
        logger.performance(_pf_line(f"Realized P&L:     ${realized_today:+,.2f}"))
        # Keep $ prefix format exactly.
        logger.performance(_pf_line(f"Stop-Losses hit:   {len(sl_hits)}"))
        logger.performance(_pf_footer())
        logger.performance("")

        logger.performance(_pf_header("OPEN POSITIONS"))
        if not ib_connected:
            logger.performance(_pf_line("IB not connected - market prices unavailable. Showing state data only."))

        if not open_rows:
            logger.performance(_pf_line("No open positions."))
        else:
            header = (
                f"{'Symbol':<8}  {'Entry':>10}  {'Current':>10}  {'Entry Cost':>13}  "
                f"{'Qty':>5}  {'P&L':>12}  {'%':>8}  {'Duration':>10}  {'Src':>6}"
            )
            sep = (
                f"{'─' * 8}  {'─' * 9}  {'─' * 9}  {'─' * 12}  "
                f"{'─' * 3}  {'─' * 10}  {'─' * 6}  {'─' * 10}  {'─' * 3}"
            )
            logger.performance(_pf_line(header))
            logger.performance(_pf_line(sep))

            for row in open_rows:
                entry_str = f"${row['entry_price']:,.2f}"
                current_str = (
                    f"${float(row['current_price']):,.2f}"
                    if row["current_price"] is not None
                    else "--"
                )
                entry_cost_str = f"${row['entry_cost']:,.2f}"
                pnl_str = (
                    f"${float(row['unrealized_pnl']):+,.2f}"
                    if row["unrealized_pnl"] is not None
                    else "--"
                )
                pct_str = (
                    f"{float(row['pnl_pct']):+,.2f}%"
                    if row["pnl_pct"] is not None
                    else "--"
                )

                line = (
                    f"{row['symbol']:<8}  {entry_str:>10}  {current_str:>10}  {entry_cost_str:>13}  "
                    f"{int(row['quantity']):>5}  {pnl_str:>12}  {pct_str:>8}  "
                    f"{row['duration']:>10}  {row['source']:>6}"
                )
                logger.performance(_pf_line(line))

            logger.performance(_pf_line("─" * 92))
            total_pnl_str = f"${total_pnl:+,.2f}" if total_pnl_available else "--"
            total_line = (
                f"{'Total':<8}  {'':>10}  {'':>10}  {f'${total_entry_cost:,.2f}':>13}  "
                f"{total_qty:>5}  {total_pnl_str:>12}"
            )
            logger.performance(_pf_line(total_line))
        logger.performance(_pf_footer())
        logger.performance("")

        logger.performance(_pf_header("CLOSED TODAY"))
        if not closed_today_rows:
            logger.performance(_pf_line("No trades closed today."))
        else:
            closed_today_sorted = sorted(
                closed_today_rows,
                key=lambda r: r["closed_at"],
                reverse=True,
            )
            logger.performance(_pf_line(f"{'Time':<8}  {'Symbol':<8}  {'Qty':>4}  {'Entry':>10}  {'Exit':>10}  {'P&L':>10}"))
            logger.performance(_pf_line(f"{'─' * 8}  {'─' * 8}  {'─' * 3}  {'─' * 9}  {'─' * 9}  {'─' * 10}"))
            for row in closed_today_sorted:
                pnl_val = float(row["pnl"])
                emoji = "💚" if pnl_val > 0 else "🔴"
                line = (
                    f"{row['closed_at'].strftime('%H:%M:%S'):<8}  {row['symbol']:<8}  {int(row['quantity']):>4}  "
                    f"${float(row['entry_price']):>9.2f}  ${float(row['exit_price']):>9.2f}  "
                    f"${pnl_val:+9.2f} {emoji}"
                )
                logger.performance(_pf_line(line))
        logger.performance(_pf_footer())
        logger.performance("")

        logger.performance(_pf_header("TRADE STATUS SUMMARY"))
        logger.performance(
            _pf_line(
                "filled: {filled}    submitted: {submitted}    closed: {closed}    "
                "rejected: {rejected}    manual: {manual}    manual_closed: {manual_closed}".format(
                    filled=status_counts.get("filled", 0),
                    submitted=status_counts.get("submitted", 0),
                    closed=status_counts.get("closed", 0),
                    rejected=status_counts.get("rejected", 0),
                    manual=status_counts.get("manual", 0),
                    manual_closed=status_counts.get("manual_closed", 0),
                )
            )
        )
        logger.performance(_pf_footer())
        logger.performance(_pf_divider())
    except Exception as e:
        logger.error(
            "_log_monitor_performance_verbose fehlgeschlagen: %s", e, exc_info=True
        )


def main() -> None:
    logger = setup_logging("broker_monitor", debug_mode=False)
    cfg = validate_and_get_config()

    ib_cfg = cfg["ib"]
    monitor_cfg = cfg["monitor"]

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    processed_path = Path(STATE_DIR) / "processed_signals.json"
    monitor_state_path = Path(STATE_DIR) / "monitor_state.json"
    loss_counter_path = Path(STATE_DIR) / "daily_losses.json"
    cooldown_path = Path(STATE_DIR) / "symbol_cooldowns.json"

    market_schedule = MarketSchedule()
    process_manager = ProcessManager(project_root=PROJECT_ROOT, logger=logger)

    loss_counter = DailyLossCounter(loss_counter_path)
    cooldown_manager = SymbolCooldownManager(
        state_path=cooldown_path,
        cooldown_minutes=int(cfg["trading"].symbol_cooldown_minutes),
    )
    position_tracker = PositionTracker(
        logger=logger,
        loss_counter=loss_counter,
        cooldown_manager=cooldown_manager,
        state_path=processed_path,
    )
    account_checker = AccountChecker(cache_seconds=30.0)

    conn_config = ConnectionConfig(
        host=ib_cfg.host,
        port=ib_cfg.port,
        client_id=ib_cfg.monitor_client_id,
        max_retries=10,
        initial_retry_delay=1.0,
        max_retry_delay=60.0,
    )
    ib_manager = IBConnectionManager(conn_config)

    try:
        ib = ib_manager.connect()
        logger.info("✅ Broker monitor mit IB verbunden")
    except Exception as e:
        logger.error("Initiale IB-Verbindung fehlgeschlagen: %s", e)
        ib = None

    heartbeat_interval = max(5.0, float(monitor_cfg.heartbeat_interval_seconds))
    position_interval = max(5.0, float(monitor_cfg.position_update_interval_seconds))
    account_interval = max(10.0, float(monitor_cfg.account_update_interval_seconds))
    pre_market_start_minutes = max(0.0, float(monitor_cfg.pre_market_start_minutes))
    post_market_stop_minutes = max(0.0, float(monitor_cfg.post_market_stop_minutes))

    counted_sl_orders: set = set()
    last_position_update = 0.0
    last_account_update = 0.0
    last_performance_log = 0.0
    scanner_started_for_session = False
    eod_done_for_day: str | None = None

    logger.info("Broker Monitor gestartet (24/7) | %s", market_schedule.get_status_string())

    try:
        while True:
            loop_start = time.time()
            try:
                if not ib_manager.check_connection():
                    logger.warning("IB-Verbindung getrennt, reconnect...")
                    try:
                        ib = ib_manager.reconnect()
                        logger.info("✅ Reconnect erfolgreich")
                    except Exception as reconnect_error:
                        logger.error("Reconnect fehlgeschlagen: %s", reconnect_error)
                        ib = None
                else:
                    ib = ib_manager.ensure_connected()

                market_open = market_schedule.is_market_open()
                process_window_active = market_schedule.is_active_trading_window(
                    pre_market_start_minutes=pre_market_start_minutes,
                    post_market_stop_minutes=post_market_stop_minutes,
                )
                today_et = datetime.now(ET).date().isoformat()
                process_status = process_manager.get_status()

                if market_schedule.just_opened(tolerance_seconds=max(90.0, heartbeat_interval + 5.0)):
                    logger.info("Marktöffnung erkannt (%s), starte Scanner+Trader", market_schedule.get_status_string())
                    process_manager.start_scanner()
                    process_manager.start_trader()
                    scanner_started_for_session = True

                if process_window_active and not scanner_started_for_session:
                    process_manager.start_scanner()
                    process_manager.start_trader()
                    scanner_started_for_session = True

                if market_schedule.just_closed(tolerance_seconds=max(90.0, heartbeat_interval + 5.0)):
                    if bool(monitor_cfg.end_of_day_report) and eod_done_for_day != today_et:
                        processed_for_report = load_state_with_retry(processed_path, max_retries=3, retry_delay=0.3)
                        report = position_tracker.generate_daily_report(processed_for_report)
                        logger.performance(report)
                        eod_done_for_day = today_et

                if not process_window_active:
                    if process_manager.get_status().get("scanner_running") or process_manager.get_status().get("trader_running"):
                        logger.info(
                            "Außerhalb des Aktivitätsfensters (pre=%s min, post=%s min), stoppe Scanner+Trader",
                            pre_market_start_minutes,
                            post_market_stop_minutes,
                        )
                        process_manager.stop_all()
                        scanner_started_for_session = False

                if process_window_active:
                    process_manager.ensure_running()

                now_ts = time.time()
                processed = load_state_with_retry(processed_path, max_retries=3, retry_delay=0.3)

                if ib is not None and ib_manager.check_connection() and (now_ts - last_position_update >= position_interval):
                    try:
                        counted_sl_orders = position_tracker.check_filled_stop_losses(
                            ib=ib,
                            processed=processed,
                            already_counted=counted_sl_orders,
                        )
                        position_tracker.update_position_status(ib=ib, processed=processed)
                    except Exception as position_error:
                        logger.error("Fehler bei Positionstracking: %s", position_error)
                    last_position_update = now_ts

                if ib is not None and ib_manager.check_connection() and (now_ts - last_account_update >= account_interval):
                    try:
                        info = account_checker.get_account_info(ib, force_refresh=True)
                        if info:
                            logger.info(
                                "Account: NetLiq=$%.2f | Cash=$%.2f | BuyingPower=$%.2f",
                                info.net_liquidation,
                                info.total_cash_value,
                                info.buying_power,
                            )
                    except Exception as account_error:
                        logger.warning("Account-Update fehlgeschlagen: %s", account_error)
                    last_account_update = now_ts

                process_status = process_manager.get_status()
                monitor_state = _build_monitor_state(
                    ib_connected=ib_manager.check_connection(),
                    market_open=market_open,
                    process_status=process_status,
                    processed=processed,
                    ib=ib,
                )
                if not save_state(monitor_state_path, monitor_state):
                    logger.warning("Konnte monitor_state nicht speichern")

                if now_ts - last_performance_log >= PERFORMANCE_LOG_INTERVAL_SECONDS:
                    account_info_dict = None
                    if ib is not None and ib_manager.check_connection():
                        try:
                            acc_info = account_checker.get_account_info(ib, force_refresh=False)
                            if acc_info:
                                account_info_dict = {
                                    "net_liquidation": acc_info.net_liquidation,
                                    "buying_power": acc_info.buying_power,
                                    "total_cash_value": acc_info.total_cash_value,
                                }
                        except Exception as account_snapshot_error:
                            logger.warning(
                                "Account-Snapshot für Performance-Log fehlgeschlagen: %s",
                                account_snapshot_error,
                            )

                    _log_monitor_performance_verbose(
                        logger=logger,
                        monitor_state=monitor_state,
                        market_status=market_schedule.get_status_string(),
                        processed=processed,
                        ib=ib if ib_manager.check_connection() else None,
                        account_info=account_info_dict,
                    )
                    last_performance_log = now_ts

                logger.debug("Heartbeat: %s", market_schedule.get_status_string())

            except Exception as loop_error:
                logger.error("Fehler im Monitor-Loop: %s", loop_error, exc_info=True)

            elapsed = time.time() - loop_start
            time.sleep(max(1.0, heartbeat_interval - elapsed))

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt empfangen, fahre Monitor herunter...")
    finally:
        try:
            process_manager.stop_all()
        except Exception as process_stop_error:
            logger.error("Fehler beim Stoppen der Subprozesse: %s", process_stop_error)
        ib_manager.disconnect()
        logger.info("Broker Monitor beendet")


if __name__ == "__main__":
    main()
