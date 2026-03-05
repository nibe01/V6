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
