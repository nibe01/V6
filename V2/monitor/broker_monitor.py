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


PERFORMANCE_LOG_INTERVAL_SECONDS = 30 * 60


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


def _log_monitor_performance_verbose(
    *,
    logger,
    monitor_state: dict,
    market_status: str,
    processed: dict,
    ib,
    account_info,
) -> None:
    """Write a detailed multi-section monitor snapshot to performance logs."""
    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.date()

    status_counts: dict[str, int] = {}
    open_state_rows: list[dict] = []
    closed_today_rows: list[dict] = []
    closed_recent_rows: list[dict] = []

    closed_statuses = {"closed", "manual_closed", "rejected"}
    open_statuses = {"filled", "submitted", "manual", "presubmitted", "pendingsubmit", "pending_submit", "pending"}

    for trade_data in processed.values():
        if not isinstance(trade_data, dict):
            continue

        status = str(trade_data.get("status", "")).strip().lower() or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1

        symbol = str(trade_data.get("symbol") or "?")
        quantity = int(trade_data.get("quantity", 0) or 0)
        entry_price = float(
            trade_data.get("fill_price")
            or trade_data.get("entry_price")
            or trade_data.get("avg_cost")
            or 0.0
        )

        opened_at = (
            trade_data.get("opened_at")
            or trade_data.get("filled_at")
            or trade_data.get("first_seen_at")
            or trade_data.get("signal_timestamp")
            or trade_data.get("processed_at")
        )

        if status in open_statuses:
            open_state_rows.append(
                {
                    "symbol": symbol,
                    "status": status,
                    "quantity": quantity,
                    "entry_price": entry_price,
                    "opened_at": str(opened_at or ""),
                }
            )

        if status in closed_statuses:
            closed_at = _parse_iso_utc(
                trade_data.get("closed_at")
                or trade_data.get("processed_at")
            )
            pnl = float(trade_data.get("realized_pnl_usd", 0.0) or 0.0)
            row = {
                "closed_at": closed_at,
                "symbol": symbol,
                "status": status,
                "quantity": quantity,
                "entry_price": entry_price,
                "exit_price": float(trade_data.get("exit_price", 0.0) or 0.0),
                "pnl": pnl,
            }
            closed_recent_rows.append(row)
            if closed_at is not None and closed_at.date() == today_utc:
                closed_today_rows.append(row)

    ib_open_rows: list[dict] = []
    if ib is not None and bool(monitor_state.get("ib_connected")):
        try:
            for pos in ib.positions():
                qty = int(getattr(pos, "position", 0) or 0)
                if qty == 0:
                    continue
                contract = getattr(pos, "contract", None)
                symbol = getattr(contract, "symbol", "?") if contract else "?"
                avg_cost = float(getattr(pos, "avgCost", 0.0) or 0.0)
                ib_open_rows.append(
                    {
                        "symbol": symbol,
                        "quantity": qty,
                        "avg_cost": avg_cost,
                    }
                )
        except Exception as ib_pos_error:
            logger.performance("IB Open Positions konnten nicht gelesen werden: %s", ib_pos_error)

    closed_today_pnl = sum(float(r["pnl"]) for r in closed_today_rows)
    total_realized = sum(float(r["pnl"]) for r in closed_recent_rows)

    logger.performance("=" * 100)
    logger.performance("MONITOR PERFORMANCE SNAPSHOT | %s", now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"))
    logger.performance("=" * 100)

    logger.performance(
        "System | Market: %s | IB: %s | Scanner: %s | Trader: %s",
        market_status,
        "connected" if monitor_state.get("ib_connected") else "disconnected",
        "running" if monitor_state.get("scanner_running") else "stopped",
        "running" if monitor_state.get("trader_running") else "stopped",
    )
    logger.performance(
        "Monitor State | Open IB Positions: %s | Today Trades: %s | Today P&L: $%+.2f",
        int(monitor_state.get("open_positions", 0) or 0),
        int(monitor_state.get("today_trades", 0) or 0),
        float(monitor_state.get("today_pnl_usd", 0.0) or 0.0),
    )

    if account_info:
        logger.performance(
            "Account | NetLiq: $%.2f | Cash: $%.2f | BuyingPower: $%.2f",
            float(account_info.get("net_liquidation", 0.0) or 0.0),
            float(account_info.get("total_cash_value", 0.0) or 0.0),
            float(account_info.get("buying_power", 0.0) or 0.0),
        )

    logger.performance("State Entries | Total: %s | Realized P&L Total: $%+.2f", sum(status_counts.values()), total_realized)
    logger.performance(
        "State Status Count | submitted=%s filled=%s closed=%s manual=%s manual_closed=%s rejected=%s unknown=%s",
        status_counts.get("submitted", 0),
        status_counts.get("filled", 0),
        status_counts.get("closed", 0),
        status_counts.get("manual", 0),
        status_counts.get("manual_closed", 0),
        status_counts.get("rejected", 0),
        status_counts.get("unknown", 0),
    )

    logger.performance("-" * 100)
    logger.performance("OPEN POSITIONS (IB) | Count: %s", len(ib_open_rows))
    if ib_open_rows:
        for row in sorted(ib_open_rows, key=lambda r: r["symbol"]):
            logger.performance(
                "IB Open | %s | Qty: %s | AvgCost: $%.2f",
                row["symbol"],
                row["quantity"],
                row["avg_cost"],
            )
    else:
        logger.performance("IB Open | none")

    logger.performance("-" * 100)
    logger.performance("OPEN POSITIONS (STATE) | Count: %s", len(open_state_rows))
    if open_state_rows:
        for row in sorted(open_state_rows, key=lambda r: (r["symbol"], r["status"])):
            logger.performance(
                "State Open | %s | Status: %s | Qty: %s | Entry: $%.2f | Opened: %s",
                row["symbol"],
                row["status"],
                row["quantity"],
                row["entry_price"],
                row["opened_at"] or "n/a",
            )
    else:
        logger.performance("State Open | none")

    logger.performance("-" * 100)
    logger.performance(
        "CLOSED POSITIONS TODAY (STATE) | Count: %s | Realized P&L: $%+.2f",
        len(closed_today_rows),
        closed_today_pnl,
    )
    if closed_today_rows:
        closed_today_sorted = sorted(
            closed_today_rows,
            key=lambda r: r["closed_at"] or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        for row in closed_today_sorted:
            closed_at_str = row["closed_at"].strftime("%H:%M:%S") if row["closed_at"] else "n/a"
            logger.performance(
                "Closed Today | %s | %s | Qty: %s | Entry: $%.2f | Exit: $%.2f | P&L: $%+.2f",
                closed_at_str,
                row["symbol"],
                row["quantity"],
                row["entry_price"],
                row["exit_price"],
                row["pnl"],
            )
    else:
        logger.performance("Closed Today | none")

    logger.performance("-" * 100)
    logger.performance("CLOSED POSITIONS RECENT (STATE) | Last 20")
    if closed_recent_rows:
        closed_recent_sorted = sorted(
            closed_recent_rows,
            key=lambda r: r["closed_at"] or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )[:20]
        for row in closed_recent_sorted:
            closed_at_str = (
                row["closed_at"].strftime("%Y-%m-%d %H:%M:%S")
                if row["closed_at"]
                else "n/a"
            )
            logger.performance(
                "Closed Recent | %s | %s | Status: %s | Qty: %s | Entry: $%.2f | Exit: $%.2f | P&L: $%+.2f",
                closed_at_str,
                row["symbol"],
                row["status"],
                row["quantity"],
                row["entry_price"],
                row["exit_price"],
                row["pnl"],
            )
    else:
        logger.performance("Closed Recent | none")

    logger.performance("=" * 100)


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
