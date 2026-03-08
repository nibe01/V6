# utils/technical_indicators.py
"""
Technical indicators for Edge Scanner.
Provides ATR, VWAP, and RVOL calculations.
"""
from __future__ import annotations

import math
import logging
import pandas as pd
import numpy as np
from typing import Optional


logger = logging.getLogger(__name__)


def calculate_atr(bars_df: pd.DataFrame, period: int = 14) -> float:
    """
    Calculate Average True Range (ATR) as percentage of price.
    
    Args:
        bars_df: DataFrame with 'high', 'low', 'close' columns
        period: Number of periods for ATR calculation
    
    Returns:
        ATR as percentage of current price
    """
    if bars_df.empty or len(bars_df) < period:
        return 0.0
    
    high = bars_df['high']
    low = bars_df['low']
    close = bars_df['close']
    
    # True Range calculation
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    
    # Convert to percentage
    current_price = close.iloc[-1]
    if current_price <= 0:
        return 0.0
    
    atr_pct = (atr / current_price) * 100
    return atr_pct


def calculate_vwap(bars_df: pd.DataFrame) -> float:
    """
    Calculate Volume-Weighted Average Price for the current day.
    
    Args:
        bars_df: DataFrame with 'high', 'low', 'close', 'volume' columns
    
    Returns:
        VWAP value
    """
    if bars_df.empty:
        return 0.0
    
    # Typical price
    typical_price = (bars_df['high'] + bars_df['low'] + bars_df['close']) / 3
    
    # VWAP = sum(typical_price * volume) / sum(volume)
    total_volume = bars_df['volume'].sum()
    if total_volume <= 0:
        return 0.0
    
    vwap = (typical_price * bars_df['volume']).sum() / total_volume
    return vwap


def calculate_rvol(bars_df: pd.DataFrame, lookback_days: int = 10) -> Optional[float]:
    """
    Calculate Relative Volume using HYBRID approach with multiple fallback methods.

    This robust implementation ensures RVOL works reliably with Historical Data.

    Methods (in order of preference):
    1. Exact time matching (best, most precise)
    2. Time window matching ±15min (good, more flexible)
    3. Hourly average comparison (acceptable)
    4. Daily RVOL (robust fallback)
    5. Volume percentile (last resort)

    Args:
        bars_df: DataFrame with 'volume' column and datetime index
        lookback_days: Number of days to look back (default: 10)

    Returns:
        RVOL (relative volume ratio)
        - 1.0 = average volume
        - >1.0 = above average (more activity)
        - <1.0 = below average (less activity)
    """
    if bars_df.empty or not hasattr(bars_df.index, 'time'):
        return 1.0

    current_volume = bars_df['volume'].iloc[-1]
    if current_volume <= 0:
        return 1.0

    # ========== METHOD 1: Exact Time Matching (Best) ==========
    try:
        current_time = bars_df.index[-1].time()
        hist_volumes = []

        for day_offset in range(1, lookback_days + 1):
            target_time = bars_df.index[-1] - pd.Timedelta(days=day_offset)
            target_date = target_time.date()

            matching = bars_df[
                (bars_df.index.normalize().date == target_date) &
                (bars_df.index.time == current_time)
            ]

            if not matching.empty:
                vol = matching['volume'].iloc[0]
                if vol > 0:
                    hist_volumes.append(vol)

        if len(hist_volumes) >= 5:
            avg_volume = np.mean(hist_volumes)
            if avg_volume > 0:
                rvol = current_volume / avg_volume
                logger.debug(f"RVOL Method 1 (Exact): {rvol:.2f} ({len(hist_volumes)} samples)")
                return float(rvol)

    except Exception as e:
        logger.debug(f"RVOL Method 1 failed: {e}")

    # ========== METHOD 2: Time Window Matching ±15min (Good) ==========
    try:
        current_hour = bars_df.index[-1].hour
        current_minute = bars_df.index[-1].minute

        hist_volumes_window = []

        for day_offset in range(1, lookback_days + 1):
            target_date = (bars_df.index[-1] - pd.Timedelta(days=day_offset)).date()

            daily_bars = bars_df[bars_df.index.normalize().date == target_date]

            if not daily_bars.empty:
                for idx, row in daily_bars.iterrows():
                    bar_hour = idx.hour
                    bar_minute = idx.minute

                    time_diff = abs((bar_hour * 60 + bar_minute) - (current_hour * 60 + current_minute))

                    if time_diff <= 15:
                        vol = row['volume']
                        if vol > 0:
                            hist_volumes_window.append(vol)
                            break

        if len(hist_volumes_window) >= 4:
            avg_volume = np.mean(hist_volumes_window)
            if avg_volume > 0:
                rvol = current_volume / avg_volume
                logger.debug(f"RVOL Method 2 (Window): {rvol:.2f} ({len(hist_volumes_window)} samples)")
                return float(rvol)

    except Exception as e:
        logger.debug(f"RVOL Method 2 failed: {e}")

    # ========== METHOD 3: Hourly Average (Acceptable) ==========
    try:
        current_hour = bars_df.index[-1].hour
        same_hour_bars = bars_df[bars_df.index.hour == current_hour]

        if len(same_hour_bars) >= 10:
            avg_hour_volume = same_hour_bars['volume'].mean()

            if avg_hour_volume > 0:
                rvol = current_volume / avg_hour_volume
                logger.debug(f"RVOL Method 3 (Hourly): {rvol:.2f} ({len(same_hour_bars)} samples)")
                return float(rvol)

    except Exception as e:
        logger.debug(f"RVOL Method 3 failed: {e}")

    # ========== METHOD 4: Daily RVOL (Robust Fallback) ==========
    try:
        daily_rvol = calculate_daily_rvol(bars_df, lookback_days)

        if daily_rvol > 0:
            logger.debug(f"RVOL Method 4 (Daily): {daily_rvol:.2f}")
            return float(daily_rvol)

    except Exception as e:
        logger.debug(f"RVOL Method 4 failed: {e}")

    # ========== METHOD 5: Volume Percentile (Last Resort) ==========
    try:
        current_hour = bars_df.index[-1].hour
        same_hour = bars_df[bars_df.index.hour == current_hour]

        if len(same_hour) >= 20:
            percentile = (same_hour['volume'] < current_volume).sum() / len(same_hour)

            if percentile >= 0.5:
                rvol = 1.0 + (percentile - 0.5) * 2
            else:
                rvol = 0.5 + percentile

            rvol = max(0.3, min(rvol, 3.0))

            logger.debug(f"RVOL Method 5 (Percentile): {rvol:.2f} (p{percentile * 100:.0f})")
            return float(rvol)

    except Exception as e:
        logger.debug(f"RVOL Method 5 failed: {e}")

    logger.warning("RVOL: All methods failed, no historical data available - returning None")
    return None


def calculate_daily_rvol(bars_df: pd.DataFrame, lookback_days: int = 10) -> float:
    """
    Calculate Daily Relative Volume (more robust than intraday RVOL).

    Compares TODAY'S volume so far vs. historical average volume at same time.

    This works better with Historical Data because:
    - Less sensitive to exact timestamps
    - Uses cumulative daily volume
    - More stable calculation

    Args:
        bars_df: DataFrame with volume and datetime index
        lookback_days: Number of days for average (default: 10)

    Returns:
        Daily RVOL ratio (1.0 = average, >1.0 = above average)
    """
    if bars_df.empty:
        return 1.0

    try:
        now = bars_df.index[-1]
        current_date = now.date()
        current_time = now.time()

        today_bars = bars_df[bars_df.index.date == current_date]
        volume_today = today_bars['volume'].sum()

        if volume_today <= 0:
            return 1.0

        historical_volumes = []

        for day_offset in range(1, lookback_days + 1):
            past_date = (now - pd.Timedelta(days=day_offset)).date()

            past_day_bars = bars_df[
                (bars_df.index.date == past_date) &
                (bars_df.index.time <= current_time)
            ]

            if not past_day_bars.empty:
                vol = past_day_bars['volume'].sum()
                if vol > 0:
                    historical_volumes.append(vol)

        if len(historical_volumes) < 3:
            return 1.0

        avg_volume = np.mean(historical_volumes)

        if avg_volume <= 0:
            return 1.0

        daily_rvol = volume_today / avg_volume

        return float(daily_rvol)

    except Exception as e:
        logger.warning(f"Daily RVOL calculation error: {e}")
        return 1.0


def calculate_atr_1h_scaled(bars_df: pd.DataFrame, period: int = 14, bar_size_minutes: int = 5) -> float:
    """
    Calculate ATR and scale it to 1-hour timeframe.
    
    Args:
        bars_df: DataFrame with 'high', 'low', 'close' columns
        period: Number of periods for ATR calculation
        bar_size_minutes: Size of each bar in minutes (default: 5)
    
    Returns:
        ATR scaled to 1-hour as percentage of current price
    """
    atr_5m = calculate_atr(bars_df, period)
    
    # Scale from bar_size_minutes to 60 minutes using square root of time
    bars_per_hour = 60 / bar_size_minutes
    atr_1h = atr_5m * math.sqrt(bars_per_hour)
    
    return atr_1h


def calculate_1h_range(bars_df: pd.DataFrame, bars_per_hour: int = 12) -> float:
    """
    Calculate the range (high - low) over the last hour as percentage.
    
    Args:
        bars_df: DataFrame with 'high', 'low', 'close' columns
        bars_per_hour: Number of bars in 1 hour (12 for 5-min bars)
    
    Returns:
        Range as percentage of current price
    """
    if bars_df.empty or len(bars_df) < bars_per_hour:
        return 0.0
    
    recent_bars = bars_df.tail(bars_per_hour)
    range_high = recent_bars['high'].max()
    range_low = recent_bars['low'].min()
    range_abs = range_high - range_low
    
    current_price = bars_df['close'].iloc[-1]
    if current_price <= 0:
        return 0.0
    
    range_pct = (range_abs / current_price) * 100
    return range_pct


def calculate_recent_range(bars_df: pd.DataFrame, recent_minutes: int = 15) -> float:
    """
    Calculate price range over a recent period as percentage of current price.

    Args:
        bars_df: DataFrame with 'high', 'low', 'close' columns
        recent_minutes: Recent period to check in minutes

    Returns:
        Recent range as percentage
    """
    if bars_df.empty:
        return 0.0

    bar_size_minutes = 5
    recent_bars_count = max(recent_minutes // bar_size_minutes, 2)

    if len(bars_df) < recent_bars_count:
        return 0.0

    recent_bars = bars_df.tail(recent_bars_count)
    range_high = recent_bars['high'].max()
    range_low = recent_bars['low'].min()
    range_abs = range_high - range_low

    current_price = bars_df['close'].iloc[-1]
    if current_price <= 0:
        return 0.0

    range_pct = (range_abs / current_price) * 100
    return float(range_pct)


def check_stock_frozen(
    bars_df: pd.DataFrame,
    lookback_bars: int = 3,
    min_range_pct: float = 0.1,
    min_volume: float = 100.0,
) -> bool:
    """
    Check if stock appears frozen based on very low recent movement/volume.

    Args:
        bars_df: DataFrame with OHLCV columns
        lookback_bars: Number of recent bars to inspect

    Returns:
        True if stock appears frozen, otherwise False
    """
    if bars_df.empty or len(bars_df) < lookback_bars:
        return True

    recent_bars = bars_df.tail(lookback_bars)

    price_range = recent_bars['high'].max() - recent_bars['low'].min()
    avg_price = recent_bars['close'].mean()

    if avg_price > 0:
        range_pct = (price_range / avg_price) * 100
        if range_pct < min_range_pct:
            return True

    recent_volume = recent_bars['volume'].sum()
    if recent_volume < min_volume:
        return True

    unique_closes = recent_bars['close'].nunique()
    if unique_closes == 1:
        return True

    return False
