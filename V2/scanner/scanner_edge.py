# scanner/scanner_edge.py
"""
Edge Scanner - 6-Level Filter Logic for Statistical Edge Detection

Usage:
    cd "/Users/nielsbergmann/Programmieren/V4/V2"
    python3 -m scanner.scanner_edge
"""
from __future__ import annotations

import sys
import json
import time
import logging
from threading import Lock
from pathlib import Path
from datetime import datetime, timezone

from ib_insync import IB, Stock

# Allow running this file directly (without -m) by adding the package root to sys.path.
if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.logging_utils import setup_logging
from utils.market_schedule import MarketSchedule
from utils.data_utils import load_extended_symbols
from utils.paths import OUTPUT_DIR
from config import validate_and_get_config
from utils.ib_connection import IBConnectionManager, ConnectionConfig
from utils.rate_limiter import get_rate_limiter, RateLimitConfig
from scanner.historical_signals import fetch_bars_df
from scanner.edge_filters import (
    filter_price_range,
    filter_movement_capability,
    filter_volume_activity,
    filter_directional_edge,
    filter_catalyst,
    filter_risk_control,
)
from scanner.edge_signals import EdgeSignal, create_edge_signal


STAGE_KEYS = [
    "total_scanned",
    "passed_ebene_1",
    "passed_ebene_2",
    "passed_ebene_3",
    "passed_ebene_4",
    "passed_ebene_5",
    "final_signals",
]


def _new_stats_counter() -> dict:
    return {key: 0 for key in STAGE_KEYS}


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def _safe_inc(counter: dict, key: str, lock: Lock, amount: int = 1) -> None:
    with lock:
        counter[key] = counter.get(key, 0) + amount


def log_block_summary(block_no: int, block_stats: dict, logger) -> None:
    logger.debug(
        "Block %s Summary | total=%s, e1=%s, e2=%s, e3=%s, e4=%s, e5=%s, final=%s",
        block_no,
        block_stats["total_scanned"],
        block_stats["passed_ebene_1"],
        block_stats["passed_ebene_2"],
        block_stats["passed_ebene_3"],
        block_stats["passed_ebene_4"],
        block_stats["passed_ebene_5"],
        block_stats["final_signals"],
    )


def log_periodic_summary(cumulative: dict, logger, blocks_processed: int) -> None:
    total = cumulative["total_scanned"]
    if total <= 0:
        return

    logger.info(
        "Summary @ block %s | symbols=%s | e1=%s (%.1f%%) | e2=%s (%.1f%%) | "
        "e3=%s (%.1f%%) | e4=%s (%.1f%%) | e5=%s (%.1f%%) | final=%s (%.1f%%)",
        blocks_processed,
        total,
        cumulative["passed_ebene_1"],
        _pct(cumulative["passed_ebene_1"], total),
        cumulative["passed_ebene_2"],
        _pct(cumulative["passed_ebene_2"], max(1, cumulative["passed_ebene_1"])),
        cumulative["passed_ebene_3"],
        _pct(cumulative["passed_ebene_3"], max(1, cumulative["passed_ebene_2"])),
        cumulative["passed_ebene_4"],
        _pct(cumulative["passed_ebene_4"], max(1, cumulative["passed_ebene_3"])),
        cumulative["passed_ebene_5"],
        _pct(cumulative["passed_ebene_5"], max(1, cumulative["passed_ebene_4"])),
        cumulative["final_signals"],
        _pct(cumulative["final_signals"], total),
    )


def log_detailed_performance(
    logger,
    cumulative: dict,
    window_stats: dict,
    blocks_processed: int,
    window_start_block: int,
    rate_limiter,
    is_connected: bool,
) -> None:
    total = window_stats["total_scanned"]
    if total <= 0:
        return

    e1 = window_stats["passed_ebene_1"]
    e2 = window_stats["passed_ebene_2"]
    e3 = window_stats["passed_ebene_3"]
    e4 = window_stats["passed_ebene_4"]
    e5 = window_stats["passed_ebene_5"]
    final_signals = window_stats["final_signals"]

    rate_stats = rate_limiter.get_statistics()

    logger.info("=" * 80)
    logger.info(
        "Performance Stats (Block %s-%s, %s symbols)",
        window_start_block,
        blocks_processed,
        total,
    )
    logger.info("=" * 80)
    logger.info("Ebene 1 (Movement):   %5d/%-5d (%5.1f%%) ✅", e1, total, _pct(e1, total))
    logger.info("Ebene 2 (Volume):     %5d/%-5d (%5.1f%%) ✅", e2, e1, _pct(e2, max(1, e1)))
    logger.info("Ebene 3 (Direction):  %5d/%-5d (%5.1f%%) ✅", e3, e2, _pct(e3, max(1, e2)))
    logger.info("Ebene 4 (Catalyst):   %5d/%-5d (%5.1f%%) ✅", e4, e3, _pct(e4, max(1, e3)))
    logger.info("Ebene 5 (Risk):       %5d/%-5d (%5.1f%%) ✅", e5, e4, _pct(e5, max(1, e4)))
    logger.info(
        "Final Signals:       %5d/%-5d (%5.1f%%) 🎯",
        final_signals,
        total,
        _pct(final_signals, total),
    )
    logger.info("")
    logger.info(
        "Conversion Funnel: %s ▶ %s ▶ %s ▶ %s ▶ %s ▶ %s 🎯",
        total,
        e1,
        e2,
        e3,
        e4,
        final_signals,
    )
    logger.info(
        "Rate Limiter: %s/%s historical requests used",
        rate_stats.get("historical_rate", 0),
        rate_limiter.config.historical_data_per_10min,
    )
    logger.info("Connection: %s", "🟢 Connected" if is_connected else "🔴 Disconnected")
    logger.info("=" * 80)

    cumulative_total = cumulative["total_scanned"]
    if cumulative_total > 0:
        logger.signal(
            "Cumulative since start | symbols=%s | final=%s (%.2f%%)",
            cumulative_total,
            cumulative["final_signals"],
            _pct(cumulative["final_signals"], cumulative_total),
        )


def log_shutdown_stats(
    logger,
    cumulative: dict,
    start_time: float,
    blocks_processed: int,
    rate_limiter,
) -> None:
    total = cumulative["total_scanned"]
    runtime_sec = max(0.0, time.time() - start_time)
    runtime_min = runtime_sec / 60.0

    logger.info("=" * 80)
    logger.info("Edge Scanner Final Performance")
    logger.info("=" * 80)
    logger.info("Runtime:            %.1fs (%.2f min)", runtime_sec, runtime_min)
    logger.info("Blocks Processed:   %s", blocks_processed)
    logger.info("Total Symbols:      %s", total)
    logger.info("Final Signals:      %s", cumulative["final_signals"])

    if total > 0:
        logger.info("Avg pass E1:        %.2f%%", _pct(cumulative["passed_ebene_1"], total))
        logger.info("Avg pass E2|E1:     %.2f%%", _pct(cumulative["passed_ebene_2"], max(1, cumulative["passed_ebene_1"])))
        logger.info("Avg pass E3|E2:     %.2f%%", _pct(cumulative["passed_ebene_3"], max(1, cumulative["passed_ebene_2"])))
        logger.info("Avg pass E4|E3:     %.2f%%", _pct(cumulative["passed_ebene_4"], max(1, cumulative["passed_ebene_3"])))
        logger.info("Avg pass E5|E4:     %.2f%%", _pct(cumulative["passed_ebene_5"], max(1, cumulative["passed_ebene_4"])))
        logger.info("Avg final|total:    %.2f%%", _pct(cumulative["final_signals"], total))

    rate_stats = rate_limiter.get_statistics()
    logger.info(
        "Rate limiter totals: req=%s, throttled=%s, wait=%.2fs",
        rate_stats.get("total_requests", 0),
        rate_stats.get("throttled_count", 0),
        rate_stats.get("total_wait_time", 0.0),
    )
    logger.info("=" * 80)


def log_filter_statistics(stats: dict, logger):
    """
    Log detailed filter statistics.
    
    Args:
        stats: Dictionary with filter statistics
        logger: Logger instance
    """
    total = stats['total_scanned']
    if total == 0:
        return
    
    logger.info("=" * 96)
    logger.info("EDGE SCANNER PIPELINE STATS")
    logger.info("=" * 96)
    logger.info("Total scanned: %d", total)
    logger.info(
        "%-18s %10s %9s %9s %10s",
        "Stage",
        "Passed",
        "Cum %",
        "Step %",
        "Filtered",
    )
    logger.info("-" * 96)

    stages = [
        ("Ebene 0 Price", stats["passed_ebene_0"]),
        ("Ebene 1 Move", stats["passed_ebene_1"]),
        ("Ebene 2 Volume", stats["passed_ebene_2"]),
        ("Ebene 3 Direction", stats["passed_ebene_3"]),
        ("Ebene 4 Catalyst", stats["passed_ebene_4"]),
        ("Ebene 5 Risk", stats["passed_ebene_5"]),
        ("Final Signals", stats["final_signals"]),
    ]

    prev_passed = total
    for stage_name, passed in stages:
        cumulative_pct = _pct(passed, total)
        step_pct = _pct(passed, prev_passed)
        filtered_count = max(0, prev_passed - passed)
        logger.info(
            "%-18s %10d %8.1f%% %8.1f%% %10d",
            stage_name,
            passed,
            cumulative_pct,
            step_pct,
            filtered_count,
        )
        prev_passed = passed

    logger.info("-" * 96)
    logger.info(
        "Funnel: %d -> %d -> %d -> %d -> %d -> %d -> %d",
        total,
        stats["passed_ebene_0"],
        stats["passed_ebene_1"],
        stats["passed_ebene_2"],
        stats["passed_ebene_3"],
        stats["passed_ebene_4"],
        stats["final_signals"],
    )
    logger.info("=" * 96)


def save_signal_for_trader(signal: EdgeSignal, output_path: Path):
    """
    Save an Edge signal in trader-compatible format.

    The trader expects:
    - symbol, now_utc, end_price
    - passed, reason
    - pct_changes, start_prices
    - edge_metrics
    """
    trader_signal = {
        "symbol": signal.symbol,
        "now_utc": signal.timestamp,
        "end_price": float(signal.price),
        "passed": True,
        "reason": "edge_scanner_6_level_filter_passed",
        "pct_changes": {},
        "start_prices": {},
        "edge_metrics": {
            "range_1h_pct": float(signal.range_1h_pct),
            "atr_1h_pct": float(signal.atr_1h_pct),
            "recent_range_pct": float(signal.recent_range_pct),
            "is_frozen": bool(signal.is_frozen),
            "rvol": float(signal.rvol),
            "vol_accelerating": bool(signal.vol_accelerating),
            "above_vwap": bool(signal.above_vwap),
            "vwap_distance_pct": float(signal.vwap_distance_pct),
            "relative_strength_pct": float(signal.relative_strength_pct),
            "higher_lows": bool(signal.higher_lows),
            "broke_pm_high": bool(signal.broke_pm_high),
            "flag_breakout": bool(signal.flag_breakout),
            "vwap_reclaim": bool(signal.vwap_reclaim),
            "spread_pct": float(signal.spread_pct),
            "bid": float(signal.bid),
            "ask": float(signal.ask),
            "spread_method": str(signal.spread_method),
            "confidence": float(signal.spread_confidence),
        },
    }

    with open(output_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(trader_signal) + "\n")


def main():
    logger = setup_logging("scanner_edge", debug_mode=False)
    
    # Quiet ib_insync logging
    logging.getLogger("ib_insync").setLevel(logging.WARNING)
    
    cfg = validate_and_get_config()
    edge_cfg = cfg["edge_scanner"]
    ib_cfg = cfg["ib"]
    strat_cfg = cfg["strategy"]
    stats_log_interval_blocks = max(
        1, int(getattr(edge_cfg, "stats_log_interval_blocks", 10))
    )
    detailed_log_interval_blocks = 50
    
    symbols = load_extended_symbols()
    logger.info(f"Loaded universe: {len(symbols)} symbols")
    logger.info(f"BLOCK_SIZE={edge_cfg.block_size}, SLEEP={edge_cfg.block_sleep_seconds}")
    logger.info(f"Stats log interval: every {stats_log_interval_blocks} block(s)")
    logger.info("Edge Scanner (Primary Scanner)")
    logger.info("Edge Scanner with 6-Level Filter Logic")
    
    # Output setup
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "signals.jsonl"
    logger.info(f"Signals output: {out_path} (Primary Scanner)")
    
    logger.info("=" * 80)
    logger.info("Setting up IB Connection Manager")
    logger.info("=" * 80)
    
    conn_config = ConnectionConfig(
        host=ib_cfg.host,
        port=ib_cfg.port,
        client_id=ib_cfg.client_id,
        max_retries=10,
        initial_retry_delay=1.0,
        max_retry_delay=60.0,
        backoff_multiplier=2.0,
        connection_timeout=10.0,
    )
    
    ib_manager = IBConnectionManager(conn_config)
    
    logger.info("=" * 80)
    logger.info("Setting up Rate Limiter")
    logger.info("=" * 80)
    
    rate_limit_config = RateLimitConfig(
        max_requests_per_second=40,
        burst_limit=80,
        historical_data_per_10min=5000,
    )
    
    rate_limiter = get_rate_limiter(rate_limit_config)
    
    logger.info("Rate Limiter Config:")
    logger.info(
        f"  Historical Data Limit: {rate_limit_config.historical_data_per_10min}/10min"
    )
    logger.info(f"  Aggregate Rate Limit: {rate_limit_config.max_requests_per_second}/s")
    
    try:
        ib = ib_manager.connect()
    except ConnectionError as e:
        logger.error("Could not establish initial connection: %s", e)
        logger.error("Please check TWS/Gateway is running and settings are correct")
        return
    
    logger.info("✅ Connected to IB with auto-reconnect support")
    
    # SPY contract for relative strength calculation
    spy_contract = Stock("SPY", "SMART", "USD")
    ib.qualifyContracts(spy_contract)
    logger.info("✅ SPY contract qualified for relative strength calculations")
    
    # Contract cache
    contract_cache: dict[str, Stock] = {}
    
    def get_contract(sym: str) -> Stock:
        c = contract_cache.get(sym)
        if c is None:
            c = Stock(sym, "SMART", "USD")
            ib_local = ib_manager.ensure_connected()
            rate_limiter.wait_if_needed(
                request_type="qualify_contract",
                is_historical=False,
            )
            ib_local.qualifyContracts(c)
            contract_cache[sym] = c
        return c
    
    stats = {
        'total_scanned': 0,
        'passed_ebene_0': 0,
        'passed_ebene_1': 0,
        'passed_ebene_2': 0,
        'passed_ebene_3': 0,
        'passed_ebene_4': 0,
        'passed_ebene_5': 0,
        'final_signals': 0,
    }
    cumulative_stats = _new_stats_counter()
    detailed_window_stats = _new_stats_counter()
    stats_lock = Lock()
    scanner_start_time = time.time()
    detailed_window_start_block = 1
    
    i = 0
    blocks_processed = 0
    market_schedule = MarketSchedule()
    try:
        while True:
            if not market_schedule.is_market_open():
                wait_seconds = min(market_schedule.seconds_until_open(), 60.0)
                logger.info(
                    "Markt geschlossen (%s) – Scanner pausiert für %.0fs",
                    market_schedule.get_status_string(),
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            if not ib_manager.check_connection():
                logger.warning("Connection lost, attempting reconnect...")
                try:
                    ib = ib_manager.reconnect()
                    logger.info("✅ Reconnected successfully")
                except ConnectionError as e:
                    logger.error("Reconnect failed: %s", e)
                    logger.error("Will retry on next iteration...")
                    time.sleep(5)
                    continue
            
            block = symbols[i:i + edge_cfg.block_size]
            if not block:
                i = 0
                continue
            
            block_no = (i // edge_cfg.block_size) + 1
            blocks_processed += 1
            block_stats = _new_stats_counter()
            logger.info(
                f"Scanning block {block_no}: {block[0]} → {block[-1]} (n={len(block)})"
            )
            
            # Fetch SPY bars once per block
            try:
                ib = ib_manager.ensure_connected()
                spy_bars_df = fetch_bars_df(
                    ib=ib,
                    contract=spy_contract,
                    duration="1 D",
                    bar_size="5 mins",
                    what_to_show="TRADES",
                    use_rth=ib_cfg.use_rth,
                    rate_limiter=rate_limiter,
                )
            except Exception as e:
                logger.warning("Could not fetch SPY bars: %s. Using empty DataFrame.", e)
                import pandas as pd
                spy_bars_df = pd.DataFrame()
            
            try:
                for sym in block:
                    stats['total_scanned'] += 1
                    _safe_inc(block_stats, 'total_scanned', stats_lock)
                    _safe_inc(cumulative_stats, 'total_scanned', stats_lock)
                    _safe_inc(detailed_window_stats, 'total_scanned', stats_lock)
                    
                    try:
                        contract = get_contract(sym)
                        
                        # Fetch bars
                        try:
                            ib = ib_manager.ensure_connected()
                            bars_df = fetch_bars_df(
                                ib=ib,
                                contract=contract,
                                duration=strat_cfg.duration,
                                bar_size=strat_cfg.bar_size,
                                what_to_show=strat_cfg.what_to_show,
                                use_rth=ib_cfg.use_rth,
                                rate_limiter=rate_limiter,
                            )
                        except ConnectionError as e:
                            logger.debug("%s: Connection error during fetch_bars: %s", sym, e)
                            continue
                        except Exception as e:
                            logger.debug("%s: Error fetching bars: %s", sym, e)
                            continue
                        
                        if bars_df.empty:
                            logger.debug("%s: No bar data available", sym)
                            continue

                        # === Ebene 0: Price Range (Pre-Filter) ===
                        e0 = filter_price_range(bars_df, edge_cfg.price_range)
                        if not e0['passed']:
                            logger.debug(
                                f"{sym}: FILTERED by Ebene 0 (Price Range) - "
                                f"Price: ${e0['metrics']['current_price']:.2f} "
                                f"(Range: ${e0['metrics']['min_price']:.2f} - "
                                f"${e0['metrics']['max_price']:.2f})"
                            )
                            continue
                        stats['passed_ebene_0'] += 1
                        
                        # === Ebene 1: Movement Capability ===
                        e1 = filter_movement_capability(bars_df, edge_cfg.movement)
                        if not e1['passed']:
                            logger.debug(
                                f"{sym}: FILTERED by Ebene 1 (Movement) - "
                                f"Range: {e1['metrics']['1h_range_pct']:.2f}%, "
                                f"ATR: {e1['metrics']['atr_1h_pct']:.2f}%, "
                                f"Recent: {e1['metrics'].get('recent_range_pct', 0.0):.2f}%, "
                                f"Frozen: {e1['metrics'].get('is_frozen', True)}"
                            )
                            continue
                        stats['passed_ebene_1'] += 1
                        _safe_inc(block_stats, 'passed_ebene_1', stats_lock)
                        _safe_inc(cumulative_stats, 'passed_ebene_1', stats_lock)
                        _safe_inc(detailed_window_stats, 'passed_ebene_1', stats_lock)
                        
                        # === Ebene 2: Volume Activity ===
                        e2 = filter_volume_activity(bars_df, edge_cfg.volume)
                        if not e2['passed']:
                            logger.debug(
                                f"{sym}: FILTERED by Ebene 2 (Volume) - "
                                f"RVOL: {e2['metrics']['rvol']:.2f}, "
                                f"MedianVol5m: {e2['metrics'].get('median_5m_volume', 0.0):.0f}, "
                                f"Avg$5m: {e2['metrics'].get('avg_5m_dollar_volume', 0.0):.0f}, "
                                f"Last$5m: {e2['metrics'].get('last_5m_dollar_volume', 0.0):.0f}, "
                                f"Reason: {e2['metrics'].get('reason', 'unknown')}"
                            )
                            continue
                        stats['passed_ebene_2'] += 1
                        _safe_inc(block_stats, 'passed_ebene_2', stats_lock)
                        _safe_inc(cumulative_stats, 'passed_ebene_2', stats_lock)
                        _safe_inc(detailed_window_stats, 'passed_ebene_2', stats_lock)
                        
                        # === Ebene 3: Directional Edge ===
                        e3 = filter_directional_edge(bars_df, spy_bars_df, edge_cfg.direction)
                        if not e3['passed']:
                            logger.debug(
                                f"{sym}: FILTERED by Ebene 3 (Direction) - "
                                f"VWAP: {e3['metrics']['above_vwap']}, "
                                f"RS: {e3['metrics']['relative_strength']:.2f}%"
                            )
                            continue
                        stats['passed_ebene_3'] += 1
                        _safe_inc(block_stats, 'passed_ebene_3', stats_lock)
                        _safe_inc(cumulative_stats, 'passed_ebene_3', stats_lock)
                        _safe_inc(detailed_window_stats, 'passed_ebene_3', stats_lock)
                        
                        # === Ebene 4: Catalyst (OPTIONAL) ===
                        if edge_cfg.catalyst.enabled:
                            e4 = filter_catalyst(bars_df, edge_cfg.catalyst)
                            if not e4['passed']:
                                logger.debug(
                                    f"{sym}: FILTERED by Ebene 4 (Catalyst) - "
                                    f"No trigger detected"
                                )
                                continue
                        else:
                            e4 = {'passed': True, 'metrics': {
                                'broke_pm_high': False,
                                'flag_breakout': False,
                                'vwap_reclaim': False,
                            }}
                        stats['passed_ebene_4'] += 1
                        _safe_inc(block_stats, 'passed_ebene_4', stats_lock)
                        _safe_inc(cumulative_stats, 'passed_ebene_4', stats_lock)
                        _safe_inc(detailed_window_stats, 'passed_ebene_4', stats_lock)
                        
                        # === Ebene 5: Risk Control (OPTIONAL) ===
                        if edge_cfg.risk.enabled:
                            e5 = filter_risk_control(ib, contract, edge_cfg.risk)
                            if e5['metrics'].get('fallback_used'):
                                logger.debug(
                                    f"{sym}: Using fallback price for risk control"
                                )
                            if not e5['passed']:
                                logger.debug(
                                    f"{sym}: FILTERED by Ebene 5 (Risk) - "
                                    f"Spread: {e5['metrics']['spread_pct']:.3f}%"
                                )
                                continue
                        else:
                            # Risk filter disabled - create dummy metrics
                            e5 = {'passed': True, 'metrics': {
                                'spread_pct': 0.0,
                                'bid': 0.0,
                                'ask': 0.0,
                            }}
                        stats['passed_ebene_5'] += 1
                        _safe_inc(block_stats, 'passed_ebene_5', stats_lock)
                        _safe_inc(cumulative_stats, 'passed_ebene_5', stats_lock)
                        _safe_inc(detailed_window_stats, 'passed_ebene_5', stats_lock)
                        
                        # ✅ SIGNAL PASSED ALL FILTERS!
                        current_price = bars_df['close'].iloc[-1]
                        timestamp = datetime.now(timezone.utc).isoformat()
                        
                        signal = create_edge_signal(
                            symbol=sym,
                            timestamp=timestamp,
                            price=current_price,
                            ebene_1=e1['metrics'],
                            ebene_2=e2['metrics'],
                            ebene_3=e3['metrics'],
                            ebene_4=e4['metrics'],
                            ebene_5=e5['metrics'],
                        )
                        
                        save_signal_for_trader(signal, out_path)
                        stats['final_signals'] += 1
                        _safe_inc(block_stats, 'final_signals', stats_lock)
                        _safe_inc(cumulative_stats, 'final_signals', stats_lock)
                        _safe_inc(detailed_window_stats, 'final_signals', stats_lock)
                        
                        logger.signal(
                            f"{sym} | "
                            f"Price: ${signal.price:.2f} | "
                            f"Range: {e1['metrics']['1h_range_pct']:.2f}% | "
                            f"RVOL: {e2['metrics']['rvol']:.2f} | "
                            f"RS: {e3['metrics']['relative_strength']:.2f}% | "
                            f"Spread: {e5['metrics']['spread_pct']:.3f}%"
                        )
                        
                    except Exception as e:
                        logger.warning(f"{sym}: error during evaluation: {e}")
            
            except KeyboardInterrupt:
                raise
            
            i += edge_cfg.block_size
            if i >= len(symbols):
                i = 0

            log_block_summary(block_no, block_stats, logger)
            
            if blocks_processed % stats_log_interval_blocks == 0:
                connection_status = (
                    "🟢 Connected" if ib_manager.check_connection() else "🔴 Disconnected"
                )
                logger.debug(
                    f"Connection Status: {connection_status} | Blocks processed: {blocks_processed}"
                )
                log_periodic_summary(cumulative_stats, logger, blocks_processed)
            
            if blocks_processed % stats_log_interval_blocks == 0:
                log_filter_statistics(stats, logger)
                rate_limiter.log_statistics()

            if blocks_processed % detailed_log_interval_blocks == 0:
                is_connected = ib_manager.check_connection()
                log_detailed_performance(
                    logger=logger,
                    cumulative=cumulative_stats,
                    window_stats=detailed_window_stats,
                    blocks_processed=blocks_processed,
                    window_start_block=detailed_window_start_block,
                    rate_limiter=rate_limiter,
                    is_connected=is_connected,
                )
                detailed_window_stats = _new_stats_counter()
                detailed_window_start_block = blocks_processed + 1
            
            time.sleep(edge_cfg.block_sleep_seconds)
    
    except KeyboardInterrupt:
        logger.info("Edge Scanner stopped by user (Ctrl+C). Clean exit.")
    finally:
        logger.info("=" * 80)
        logger.info("Edge Scanner shutting down...")
        logger.info("=" * 80)
        
        # Log final statistics
        log_filter_statistics(stats, logger)
        log_shutdown_stats(
            logger=logger,
            cumulative=cumulative_stats,
            start_time=scanner_start_time,
            blocks_processed=blocks_processed,
            rate_limiter=rate_limiter,
        )
        
        try:
            if ib_manager and ib_manager.ib:
                try:
                    ib_manager.ib.sleep(0.2)
                except Exception:
                    pass
            
            if ib_manager:
                ib_manager.disconnect()
            
            logger.info("✅ Edge Scanner disconnected cleanly")
        except Exception as e:
            logger.error("Error during shutdown: %s", e)


if __name__ == "__main__":
    main()
