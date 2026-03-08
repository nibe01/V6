# scanner/edge_filters.py
"""
6-level filter logic for Edge Scanner.
Each filter returns a dict with 'passed' (bool) and 'metrics' (dict).
"""
from __future__ import annotations

import math
import pandas as pd
from typing import Dict, Any
from datetime import time as dt_time

from utils.technical_indicators import (
    calculate_atr_1h_scaled,
    calculate_1h_range,
    calculate_recent_range,
    check_stock_frozen,
    calculate_rvol,
    calculate_vwap,
)
from utils.logging_utils import get_logger


logger = get_logger(__name__)


def filter_price_range(bars_df: pd.DataFrame, config) -> Dict[str, Any]:
    """
    Ebene 0: Price Range (Pre-Filter)
    Checks if stock price is within tradeable range.

    Args:
        bars_df: DataFrame with price data
        config: PriceRangeConfig with min/max thresholds

    Returns:
        Dict with 'passed' (bool) and 'metrics' (dict)
    """
    if bars_df.empty:
        return {
            'passed': False,
            'metrics': {
                'current_price': 0.0,
                'min_price': config.min_price,
                'max_price': config.max_price,
            }
        }

    current_price = float(bars_df['close'].iloc[-1])
    passed = config.min_price <= current_price <= config.max_price

    return {
        'passed': passed,
        'metrics': {
            'current_price': current_price,
            'min_price': config.min_price,
            'max_price': config.max_price,
        }
    }


def filter_movement_capability(bars_df: pd.DataFrame, config) -> Dict[str, Any]:
    """
    Ebene 1: Movement Capability
    Checks if the stock had sufficient historical movement and
    is not currently frozen.
    
    Args:
        bars_df: DataFrame with 5-min bars
        config: MovementConfig with thresholds
    
    Returns:
        Dict with 'passed' (bool) and 'metrics' (dict)
    """
    if bars_df.empty or len(bars_df) < 14:
        return {
            'passed': False,
            'metrics': {
                '1h_range_pct': 0.0,
                'atr_1h_pct': 0.0,
                'recent_range_pct': 0.0,
                'is_frozen': True,
            }
        }

    # Historical movement (1h)
    range_1h_pct = calculate_1h_range(bars_df, bars_per_hour=12)
    atr_1h_pct = calculate_atr_1h_scaled(bars_df, period=14, bar_size_minutes=5)

    historical_passed = (
        range_1h_pct >= config.min_1h_range_pct and
        atr_1h_pct >= config.min_atr_pct
    )

    # Recent movement (configurable window)
    recent_window_minutes = getattr(config, 'recent_window_minutes', 15)
    frozen_lookback_bars = getattr(config, 'frozen_lookback_bars', 3)
    frozen_min_range_pct = getattr(config, 'frozen_min_range_pct', 0.1)
    frozen_min_volume = getattr(config, 'frozen_min_volume', 100.0)
    min_recent_range_pct = getattr(config, 'min_recent_range_pct', 0.2)

    recent_range_pct = calculate_recent_range(
        bars_df,
        recent_minutes=recent_window_minutes,
    )
    is_frozen = check_stock_frozen(
        bars_df,
        lookback_bars=frozen_lookback_bars,
        min_range_pct=frozen_min_range_pct,
        min_volume=frozen_min_volume,
    )

    recent_passed = recent_range_pct >= min_recent_range_pct and not is_frozen

    passed = historical_passed and recent_passed

    if not passed:
        if not historical_passed:
            logger.debug(
                "Movement: FILTERED - Low historical movement "
                "(1h: %.2f%%, ATR: %.2f%%)",
                range_1h_pct,
                atr_1h_pct,
            )
        elif not recent_passed:
            logger.debug(
                "Movement: FILTERED - Stock frozen "
                "(recent 15min: %.2f%%, frozen: %s)",
                recent_range_pct,
                is_frozen,
            )

    return {
        'passed': passed,
        'metrics': {
            '1h_range_pct': range_1h_pct,
            'atr_1h_pct': atr_1h_pct,
            'recent_range_pct': recent_range_pct,
            'is_frozen': is_frozen,
        }
    }


def filter_volume_activity(bars_df: pd.DataFrame, config) -> Dict[str, Any]:
    """
    Ebene 2: Volume Activity
    Checks if price movement is backed by real capital.
    
    Args:
        bars_df: DataFrame with volume data
        config: VolumeConfig with thresholds
    
    Returns:
        Dict with 'passed' (bool) and 'metrics' (dict)
    """
    if bars_df.empty or len(bars_df) < 3:
        return {
            'passed': False,
            'metrics': {
                'rvol': 0.0,
                'vol_accelerating': False,
                'median_5m_volume': 0.0,
                'avg_5m_dollar_volume': 0.0,
                'last_5m_dollar_volume': 0.0,
                'reason': 'insufficient_bars',
            }
        }

    required_cols = {'close', 'volume'}
    if not required_cols.issubset(bars_df.columns):
        return {
            'passed': False,
            'metrics': {
                'rvol': 0.0,
                'vol_accelerating': False,
                'median_5m_volume': 0.0,
                'avg_5m_dollar_volume': 0.0,
                'last_5m_dollar_volume': 0.0,
                'reason': 'missing_columns',
            }
        }

    volume_series = pd.to_numeric(bars_df['volume'], errors='coerce').fillna(0.0)
    close_series = pd.to_numeric(bars_df['close'], errors='coerce').fillna(0.0)
    dollar_volume_series = volume_series * close_series

    # Calculate RVOL using robust hybrid method
    rvol_raw = calculate_rvol(bars_df, lookback_days=10)
    rvol_available = rvol_raw is not None
    rvol = float(rvol_raw) if rvol_available else 0.0

    # Check volume acceleration (last 3 bars increasing)
    last_3_volumes = volume_series.tail(3).values
    vol_accelerating = False
    if len(last_3_volumes) >= 3:
        vol_accelerating = all(
            last_3_volumes[i] < last_3_volumes[i + 1]
            for i in range(len(last_3_volumes) - 1)
        )

    # Liquidity quality metrics (last ~1h on 5m bars)
    recent_window = min(12, len(volume_series))
    recent_volumes = volume_series.tail(recent_window)
    recent_dollar_volumes = dollar_volume_series.tail(recent_window)

    median_5m_volume = float(recent_volumes.median()) if not recent_volumes.empty else 0.0
    avg_5m_dollar_volume = (
        float(recent_dollar_volumes.mean()) if not recent_dollar_volumes.empty else 0.0
    )
    last_5m_dollar_volume = float(dollar_volume_series.iloc[-1]) if len(dollar_volume_series) else 0.0

    min_rvol = float(getattr(config, 'min_rvol', 1.0))
    min_median_5m_volume = float(getattr(config, 'min_median_5m_volume', 1500.0))
    min_avg_5m_dollar_volume = float(getattr(config, 'min_avg_5m_dollar_volume', 50000.0))
    min_last_5m_dollar_volume = float(getattr(config, 'min_last_5m_dollar_volume', 25000.0))
    require_volume_acceleration = bool(getattr(config, 'require_volume_acceleration', False))

    # If RVOL cannot be computed, skip RVOL check instead of hard-failing.
    rvol_pass = (not rvol_available) or (rvol >= min_rvol)
    median_vol_pass = median_5m_volume >= min_median_5m_volume
    avg_dollar_pass = avg_5m_dollar_volume >= min_avg_5m_dollar_volume
    last_dollar_pass = last_5m_dollar_volume >= min_last_5m_dollar_volume
    acceleration_pass = (not require_volume_acceleration) or vol_accelerating

    passed = (
        rvol_pass
        and median_vol_pass
        and avg_dollar_pass
        and last_dollar_pass
        and acceleration_pass
    )

    failure_reasons = []
    if not rvol_pass:
        failure_reasons.append(f'low_rvol({rvol:.2f}<{min_rvol})')
    if not median_vol_pass:
        failure_reasons.append('low_median_5m_volume')
    if not avg_dollar_pass:
        failure_reasons.append('low_avg_5m_dollar_volume')
    if not last_dollar_pass:
        failure_reasons.append('low_last_5m_dollar_volume')
    if not acceleration_pass:
        failure_reasons.append('volume_not_accelerating')

    reason = 'passed' if passed else '|'.join(failure_reasons)
    if not rvol_available:
        reason = f"{reason}|rvol_unavailable_skipped"
    
    return {
        'passed': passed,
        'metrics': {
            'rvol': rvol,
            'rvol_available': rvol_available,
            'vol_accelerating': vol_accelerating,
            'median_5m_volume': median_5m_volume,
            'avg_5m_dollar_volume': avg_5m_dollar_volume,
            'last_5m_dollar_volume': last_5m_dollar_volume,
            'reason': reason,
        }
    }


def filter_directional_edge(
    bars_df: pd.DataFrame,
    spy_bars_df: pd.DataFrame,
    config
) -> Dict[str, Any]:
    """
    Ebene 3: Directional Edge
    Checks if movement has directional bias (upward).
    
    Args:
        bars_df: DataFrame for the stock
        spy_bars_df: DataFrame for SPY (market benchmark)
        config: DirectionConfig with thresholds
    
    Returns:
        Dict with 'passed' (bool) and 'metrics' (dict)
    """
    if bars_df.empty or len(bars_df) < 13:
        return {
            'passed': False,
            'metrics': {
                'above_vwap': False,
                'vwap_distance_pct': 0.0,
                'relative_strength': 0.0,
                'higher_lows': False,
            }
        }
    
    # Calculate VWAP
    vwap = calculate_vwap(bars_df)
    current_price = bars_df['close'].iloc[-1]
    
    above_vwap = current_price > vwap
    vwap_distance_pct = 0.0
    if vwap > 0:
        vwap_distance_pct = ((current_price / vwap) - 1) * 100
    
    # Calculate Relative Strength vs SPY
    relative_strength = 0.0
    if not spy_bars_df.empty and len(spy_bars_df) >= 13 and len(bars_df) >= 13:
        stock_change = (bars_df['close'].iloc[-1] / bars_df['close'].iloc[-13]) - 1
        spy_change = (spy_bars_df['close'].iloc[-1] / spy_bars_df['close'].iloc[-13]) - 1
        relative_strength = stock_change - spy_change
    
    # Market Structure: higher lows?
    higher_lows = False
    if len(bars_df) >= 4:
        last_4_lows = bars_df['low'].tail(4).values
        higher_lows = all(
            last_4_lows[i] <= last_4_lows[i + 1]
            for i in range(len(last_4_lows) - 1)
        )
    
    # Check thresholds
    passed = above_vwap and relative_strength > config.min_relative_strength
    
    return {
        'passed': passed,
        'metrics': {
            'above_vwap': above_vwap,
            'vwap_distance_pct': vwap_distance_pct,
            'relative_strength': relative_strength * 100,  # Convert to percentage
            'higher_lows': higher_lows,
        }
    }


def filter_catalyst(bars_df: pd.DataFrame, config) -> Dict[str, Any]:
    """
    Ebene 4: Catalyst/Trigger (OPTIONAL)
    Checks for specific trigger events (breakouts, flags, etc).
    
    Args:
        bars_df: DataFrame with price data
        config: CatalystConfig
    
    Returns:
        Dict with 'passed' (bool) and 'metrics' (dict)
    """
    if bars_df.empty or len(bars_df) < 10:
        return {
            'passed': False,
            'metrics': {
                'broke_pm_high': False,
                'flag_breakout': False,
                'vwap_reclaim': False,
            }
        }
    
    current_price = bars_df['close'].iloc[-1]
    
    # Pre-Market High Breakout (4:00 - 9:30)
    broke_pm_high = False
    try:
        if hasattr(bars_df.index, 'time'):
            premarket = bars_df.between_time('04:00', '09:29', inclusive='left')
            if not premarket.empty:
                pm_high = premarket['high'].max()
                broke_pm_high = current_price > pm_high * 1.001  # 0.1% buffer
    except Exception:
        pass
    
    # Flag Breakout: Consolidation → Expansion
    flag_breakout = False
    if len(bars_df) >= 10:
        try:
            # Calculate ranges for last 10 bars
            ranges = []
            for i in range(-10, 0):
                bar_range = (bars_df['high'].iloc[i] - bars_df['low'].iloc[i])
                bar_close = bars_df['close'].iloc[i]
                if bar_close > 0:
                    ranges.append(bar_range / bar_close)
            
            if len(ranges) >= 10:
                avg_range_before = sum(ranges[:-3]) / 7  # First 7 bars
                current_range = ranges[-1]  # Last bar
                if avg_range_before > 0:
                    flag_breakout = current_range > avg_range_before * 1.5
        except Exception:
            pass
    
    # VWAP Reclaim: Was below, now above
    vwap_reclaim = False
    if len(bars_df) >= 2:
        try:
            vwap = calculate_vwap(bars_df)
            prev_close = bars_df['close'].iloc[-2]
            prev_below_vwap = prev_close < vwap
            now_above_vwap = current_price > vwap
            vwap_reclaim = prev_below_vwap and now_above_vwap
        except Exception:
            pass
    
    # At least one trigger must be active
    passed = broke_pm_high or flag_breakout or vwap_reclaim
    
    return {
        'passed': passed,
        'metrics': {
            'broke_pm_high': broke_pm_high,
            'flag_breakout': flag_breakout,
            'vwap_reclaim': vwap_reclaim,
        }
    }


def filter_risk_control(ib, contract, config) -> Dict[str, Any]:
    """
    Ebene 5: Risk Control with HYBRID approach.

    Works with Historical Data Only:
    1) Try live bid/ask (best)
    2) Fallback to historical spread estimation
    3) Confidence-aware thresholding

    Args:
        ib: IB connection
        contract: Stock contract
        config: RiskConfig with max_spread_pct threshold

    Returns:
        Dict with 'passed' (bool) and 'metrics' (dict)
    """
    try:
        from scanner.liquidity_estimators import estimate_spread_hybrid
    except ImportError:
        logger.error("liquidity_estimators.py not found - cannot estimate spreads")
        return {
            'passed': False,
            'metrics': {
                'spread_pct': 999.0,
                'method': 'import_error',
                'confidence': 0.0,
                'bid': 0.0,
                'ask': 0.0,
                'fallback_used': True,
            }
        }

    try:
        symbol = getattr(contract, 'symbol', 'UNKNOWN')

        # ========== ATTEMPT 1: LIVE BID/ASK ==========
        try:
            ticker = ib.reqMktData(contract, snapshot=True)
            ib.sleep(1.0)

            bid = ticker.bid if ticker and ticker.bid and ticker.bid > 0 else None
            ask = ticker.ask if ticker and ticker.ask and ticker.ask > 0 else None

            if bid and ask and ask > bid:
                bid = float(bid)
                ask = float(ask)
                mid_price = (bid + ask) / 2
                spread_pct = ((ask - bid) / mid_price) * 100 if mid_price > 0 else 999.0
                passed = spread_pct <= config.max_spread_pct

                logger.debug(
                    "%s: Risk Control LIVE - spread=%.3f%% passed=%s",
                    symbol,
                    spread_pct,
                    passed,
                )

                return {
                    'passed': passed,
                    'metrics': {
                        'spread_pct': float(spread_pct),
                        'method': 'live_bid_ask',
                        'confidence': 1.0,
                        'bid': bid,
                        'ask': ask,
                        'effective_threshold': float(config.max_spread_pct),
                        'threshold_adjustment': 'strict',
                        'fallback_used': False,
                    }
                }

        except Exception as exc:
            logger.debug("%s: Live data unavailable: %s", symbol, exc)

        # ========== ATTEMPT 2: HISTORICAL ESTIMATION ==========
        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr='5 D',
                barSizeSetting='5 mins',
                whatToShow='TRADES',
                useRTH=True,
            )

            if not bars:
                logger.debug("%s: Risk Control no historical bars", symbol)
                return {
                    'passed': False,
                    'metrics': {
                        'spread_pct': 999.0,
                        'method': 'no_bars',
                        'confidence': 0.0,
                        'bid': 0.0,
                        'ask': 0.0,
                        'fallback_used': True,
                    }
                }

            from ib_insync import util
            bars_df = util.df(bars)

            if bars_df.empty or len(bars_df) < 50:
                logger.debug("%s: Risk Control insufficient bars (%d < 50)", symbol, len(bars_df))
                return {
                    'passed': False,
                    'metrics': {
                        'spread_pct': 999.0,
                        'method': 'insufficient_bars',
                        'confidence': 0.0,
                        'bid': 0.0,
                        'ask': 0.0,
                        'fallback_used': True,
                    }
                }

            estimation = estimate_spread_hybrid(bars_df)
            spread_pct = float(estimation.get('spread_pct', 999.0))
            method = estimation.get('method', 'unknown')
            confidence = float(estimation.get('confidence', 0.0))
            all_estimates = estimation.get('all_estimates', {})
            liquidity_metrics = estimation.get('liquidity_metrics', {})

            if confidence >= 0.7:
                effective_threshold = float(config.max_spread_pct)
                threshold_note = 'strict'
            elif confidence >= 0.4:
                effective_threshold = float(config.max_spread_pct) * 1.15
                threshold_note = 'moderate'
            else:
                effective_threshold = float(config.max_spread_pct) * 1.30
                threshold_note = 'lenient'

            passed = spread_pct <= effective_threshold

            current_price = float(bars_df['close'].iloc[-1]) if 'close' in bars_df.columns else 0.0
            half_spread_dollars = (spread_pct / 100) * current_price / 2 if current_price > 0 else 0.0
            estimated_bid = current_price - half_spread_dollars
            estimated_ask = current_price + half_spread_dollars

            logger.debug(
                "%s: Risk Control ESTIMATED - spread=%.3f%% method=%s confidence=%.1f%% threshold=%.2f%% (%s) passed=%s",
                symbol,
                spread_pct,
                method,
                confidence * 100,
                effective_threshold,
                threshold_note,
                passed,
            )

            if len(all_estimates) > 1:
                estimates_str = ", ".join([f"{k}={v:.3f}%" for k, v in all_estimates.items()])
                logger.debug("%s: Risk Control estimates: %s", symbol, estimates_str)

            if liquidity_metrics:
                logger.debug(
                    "%s: Liquidity score=%s dollar_vol=$%.1fM adv=%.0fk",
                    symbol,
                    liquidity_metrics.get('score', 0),
                    liquidity_metrics.get('dollar_volume', 0.0) / 1e6,
                    liquidity_metrics.get('avg_daily_volume', 0.0) / 1e3,
                )

            return {
                'passed': passed,
                'metrics': {
                    'spread_pct': spread_pct,
                    'method': f'estimated_{method}',
                    'confidence': confidence,
                    'bid': float(estimated_bid),
                    'ask': float(estimated_ask),
                    'effective_threshold': float(effective_threshold),
                    'threshold_adjustment': threshold_note,
                    'all_estimates': {k: float(v) for k, v in all_estimates.items()},
                    'liquidity_score': liquidity_metrics.get('score', 0),
                    'dollar_volume': liquidity_metrics.get('dollar_volume', 0.0),
                    'fallback_used': True,
                }
            }

        except Exception as exc:
            logger.warning("%s: Historical spread estimation failed: %s", symbol, exc)
            return {
                'passed': False,
                'metrics': {
                    'spread_pct': 999.0,
                    'method': 'estimation_error',
                    'confidence': 0.0,
                    'bid': 0.0,
                    'ask': 0.0,
                    'fallback_used': True,
                }
            }

    except Exception as exc:
        logger.error("Risk Control filter critical error: %s", exc)
        return {
            'passed': False,
            'metrics': {
                'spread_pct': 999.0,
                'method': 'critical_error',
                'confidence': 0.0,
                'bid': 0.0,
                'ask': 0.0,
                'fallback_used': True,
            }
        }
