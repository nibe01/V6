"""
Liquidity and spread estimation from Historical Data.
No live market data required.

Methods:
1. Roll Model (1984) - Primary
2. High-Low estimator - Fallback
3. Effective spread proxy - Validation/Fallback
4. Liquidity score mapping - Robust fallback
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

from utils.logging_utils import get_logger


logger = get_logger(__name__)


def _invalid_estimate() -> Tuple[float, float]:
    return 999.0, 0.0


def estimate_spread_roll(bars_df: pd.DataFrame, min_observations: int = 50) -> Tuple[float, float]:
    """
    Estimate bid-ask spread using Roll (1984).

    Spread = 2 * sqrt(-Cov(ΔP_t, ΔP_t-1)) when serial covariance is negative.
    """
    if bars_df.empty or len(bars_df) < min_observations:
        logger.debug("Roll: Insufficient data (%d < %d)", len(bars_df), min_observations)
        return _invalid_estimate()

    try:
        if 'close' not in bars_df.columns:
            return _invalid_estimate()

        price_changes = bars_df['close'].diff().dropna()
        if len(price_changes) < 30:
            return _invalid_estimate()

        lagged_changes = price_changes.shift(1)
        serial_cov = price_changes.cov(lagged_changes)

        if serial_cov is None or np.isnan(serial_cov) or serial_cov >= 0:
            logger.debug("Roll: Positive/invalid covariance (%s), fallback", serial_cov)
            return _invalid_estimate()

        spread_dollars = 2 * np.sqrt(-serial_cov)
        avg_price = bars_df['close'].mean()
        if avg_price <= 0:
            return _invalid_estimate()

        spread_pct = (spread_dollars / avg_price) * 100
        spread_pct = max(0.01, min(float(spread_pct), 10.0))

        confidence = min(len(price_changes) / 100, 1.0)
        logger.debug("Roll: spread=%.3f%% confidence=%.1f%%", spread_pct, confidence * 100)
        return spread_pct, float(confidence)

    except Exception as exc:
        logger.warning("Roll estimation error: %s", exc)
        return _invalid_estimate()


def estimate_spread_high_low(bars_df: pd.DataFrame) -> Tuple[float, float]:
    """
    Estimate spread from High-Low ranges (Parkinson-volatility-based proxy).
    """
    if bars_df.empty or len(bars_df) < 20:
        logger.debug("HighLow: Insufficient data (%d < 20)", len(bars_df))
        return _invalid_estimate()

    try:
        if not {'high', 'low'}.issubset(bars_df.columns):
            return _invalid_estimate()

        hl = bars_df[['high', 'low']].copy()
        hl = hl[(hl['high'] > 0) & (hl['low'] > 0) & (hl['high'] >= hl['low'])]
        if hl.empty:
            return _invalid_estimate()

        hl_ratios = np.log(hl['high'] / hl['low'])
        parkinson_vol = np.sqrt((hl_ratios.pow(2).mean()) / (4 * np.log(2)))

        spread_pct = max(0.01, min(float(parkinson_vol * 0.25 * 100), 10.0))
        confidence = min(len(hl) / 50, 0.8)

        logger.debug("HighLow: spread=%.3f%% confidence=%.1f%%", spread_pct, confidence * 100)
        return spread_pct, float(confidence)

    except Exception as exc:
        logger.warning("HighLow estimation error: %s", exc)
        return _invalid_estimate()


def estimate_spread_effective(bars_df: pd.DataFrame) -> Tuple[float, float]:
    """
    Estimate effective spread from volume-weighted absolute mid-price changes.
    """
    if bars_df.empty or len(bars_df) < 30:
        logger.debug("Effective: Insufficient data (%d < 30)", len(bars_df))
        return _invalid_estimate()

    try:
        required = {'close', 'high', 'low', 'volume'}
        if not required.issubset(bars_df.columns):
            return _invalid_estimate()

        df = bars_df[['close', 'high', 'low', 'volume']].copy()
        df = df[(df['high'] > 0) & (df['low'] > 0) & (df['volume'] >= 0)]
        if df.empty:
            return _invalid_estimate()

        df['mid_price'] = (df['high'] + df['low']) / 2
        df['price_change'] = df['mid_price'].diff().abs().fillna(0)

        total_volume = df['volume'].sum()
        if total_volume <= 0:
            return _invalid_estimate()

        df['volume_weight'] = df['volume'] / total_volume
        weighted_change = (df['price_change'] * df['volume_weight']).sum()

        avg_price = df['mid_price'].mean()
        if avg_price <= 0:
            return _invalid_estimate()

        spread_pct = (2 * weighted_change / avg_price) * 100
        spread_pct = max(0.01, min(float(spread_pct), 10.0))
        confidence = min(len(df) / 60, 0.85)

        logger.debug("Effective: spread=%.3f%% confidence=%.1f%%", spread_pct, confidence * 100)
        return spread_pct, float(confidence)

    except Exception as exc:
        logger.warning("Effective spread estimation error: %s", exc)
        return _invalid_estimate()


def calculate_liquidity_score(bars_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate liquidity score 0-100 and derive spread estimate from score.
    """
    if bars_df.empty or len(bars_df) < 20:
        return {
            'score': 0,
            'dollar_volume': 0.0,
            'avg_daily_volume': 0.0,
            'price': 0.0,
            'estimated_spread_from_score': 999.0,
        }

    try:
        required = {'close', 'volume'}
        if not required.issubset(bars_df.columns):
            return {
                'score': 0,
                'dollar_volume': 0.0,
                'avg_daily_volume': 0.0,
                'price': 0.0,
                'estimated_spread_from_score': 999.0,
            }

        current_price = float(bars_df['close'].iloc[-1])
        if current_price <= 0:
            return {
                'score': 0,
                'dollar_volume': 0.0,
                'avg_daily_volume': 0.0,
                'price': 0.0,
                'estimated_spread_from_score': 999.0,
            }

        df = bars_df[['close', 'volume']].copy()
        if isinstance(df.index, pd.DatetimeIndex):
            day_key = df.index.date
        else:
            day_key = pd.Series(np.arange(len(df)))

        df['dollar_vol'] = df['volume'] * df['close']
        daily_dollar_vol = df.groupby(day_key)['dollar_vol'].sum()
        daily_volume = df.groupby(day_key)['volume'].sum()

        avg_dollar_vol = float(daily_dollar_vol.mean()) if not daily_dollar_vol.empty else 0.0
        avg_daily_volume = float(daily_volume.mean()) if not daily_volume.empty else 0.0

        score = 0

        if avg_dollar_vol >= 100_000_000:
            score += 40
        elif avg_dollar_vol >= 50_000_000:
            score += 35
        elif avg_dollar_vol >= 20_000_000:
            score += 30
        elif avg_dollar_vol >= 10_000_000:
            score += 25
        elif avg_dollar_vol >= 5_000_000:
            score += 20
        elif avg_dollar_vol >= 2_000_000:
            score += 15
        elif avg_dollar_vol >= 1_000_000:
            score += 10
        elif avg_dollar_vol >= 500_000:
            score += 5

        if avg_daily_volume >= 10_000_000:
            score += 30
        elif avg_daily_volume >= 5_000_000:
            score += 25
        elif avg_daily_volume >= 2_000_000:
            score += 20
        elif avg_daily_volume >= 1_000_000:
            score += 15
        elif avg_daily_volume >= 500_000:
            score += 10
        elif avg_daily_volume >= 200_000:
            score += 5

        if current_price >= 200:
            score += 30
        elif current_price >= 100:
            score += 28
        elif current_price >= 50:
            score += 25
        elif current_price >= 30:
            score += 22
        elif current_price >= 20:
            score += 18
        elif current_price >= 15:
            score += 15
        elif current_price >= 10:
            score += 12
        elif current_price >= 8:
            score += 8
        elif current_price >= 5:
            score += 5

        if score >= 90:
            estimated_spread = 0.10
        elif score >= 80:
            estimated_spread = 0.15
        elif score >= 70:
            estimated_spread = 0.25
        elif score >= 60:
            estimated_spread = 0.35
        elif score >= 50:
            estimated_spread = 0.55
        elif score >= 40:
            estimated_spread = 0.80
        elif score >= 30:
            estimated_spread = 1.20
        elif score >= 20:
            estimated_spread = 1.80
        elif score >= 10:
            estimated_spread = 2.50
        else:
            estimated_spread = 4.00

        return {
            'score': int(score),
            'dollar_volume': avg_dollar_vol,
            'avg_daily_volume': avg_daily_volume,
            'price': current_price,
            'estimated_spread_from_score': float(estimated_spread),
        }

    except Exception as exc:
        logger.warning("Liquidity score calculation error: %s", exc)
        return {
            'score': 0,
            'dollar_volume': 0.0,
            'avg_daily_volume': 0.0,
            'price': 0.0,
            'estimated_spread_from_score': 999.0,
        }


def estimate_spread_hybrid(bars_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Hybrid spread estimation combining all methods with confidence-aware selection.
    """
    if bars_df.empty or len(bars_df) < 20:
        return {
            'spread_pct': 999.0,
            'method': 'insufficient_data',
            'confidence': 0.0,
            'all_estimates': {},
            'liquidity_metrics': {},
        }

    estimates: Dict[str, float] = {}
    confidences: Dict[str, float] = {}

    roll_spread, roll_conf = estimate_spread_roll(bars_df, min_observations=50)
    if roll_spread < 900:
        estimates['roll'] = roll_spread
        confidences['roll'] = roll_conf

    hl_spread, hl_conf = estimate_spread_high_low(bars_df)
    if hl_spread < 900:
        estimates['high_low'] = hl_spread
        confidences['high_low'] = hl_conf

    eff_spread, eff_conf = estimate_spread_effective(bars_df)
    if eff_spread < 900:
        estimates['effective'] = eff_spread
        confidences['effective'] = eff_conf

    liquidity_metrics = calculate_liquidity_score(bars_df)
    liquidity_spread = liquidity_metrics.get('estimated_spread_from_score', 999.0)
    if liquidity_spread < 900:
        estimates['liquidity_score'] = float(liquidity_spread)
        confidences['liquidity_score'] = min(len(bars_df) / 100, 0.75)

    if not estimates:
        logger.warning("Hybrid: All estimation methods failed")
        return {
            'spread_pct': 999.0,
            'method': 'all_failed',
            'confidence': 0.0,
            'all_estimates': {},
            'liquidity_metrics': liquidity_metrics,
        }

    if 'roll' in estimates and confidences['roll'] >= 0.7:
        spread = estimates['roll']
        method = 'roll_primary'
        confidence = confidences['roll']

        if 'liquidity_score' in estimates and estimates['liquidity_score'] > 0:
            diff_pct = abs(spread - estimates['liquidity_score']) / estimates['liquidity_score']
            if diff_pct > 0.8:
                confidence *= 0.8
                logger.debug("Hybrid: Roll vs Liquidity disagreement: %.0f%%", diff_pct * 100)

        logger.debug("Hybrid Strategy 1: High confidence Roll (%.1f%%)", confidence * 100)

    elif 'roll' in estimates and confidences['roll'] >= 0.4:
        available_keys = list(estimates.keys())
        available_estimates = [estimates[k] for k in available_keys]
        available_confidences = [confidences[k] for k in available_keys]

        total_conf = sum(available_confidences)
        if total_conf > 0:
            spread = sum(est * conf for est, conf in zip(available_estimates, available_confidences)) / total_conf
        else:
            spread = float(np.mean(available_estimates))

        method = f"weighted_average_{len(available_estimates)}_methods"
        confidence = float(np.mean(available_confidences))
        logger.debug("Hybrid Strategy 2: Weighted average (%.1f%%)", confidence * 100)

    else:
        if 'liquidity_score' in estimates:
            spread = estimates['liquidity_score']
            method = 'liquidity_score'
            confidence = confidences['liquidity_score']
        elif 'high_low' in estimates:
            spread = estimates['high_low']
            method = 'high_low'
            confidence = confidences['high_low']
        elif 'effective' in estimates:
            spread = estimates['effective']
            method = 'effective'
            confidence = confidences['effective']
        else:
            spread = 999.0
            method = 'fallback'
            confidence = 0.0

        logger.debug("Hybrid Strategy 3: Alternative (%s, %.1f%%)", method, confidence * 100)

    spread = max(0.01, min(float(spread), 10.0))
    return {
        'spread_pct': spread,
        'method': method,
        'confidence': float(confidence),
        'all_estimates': {k: float(v) for k, v in estimates.items()},
        'liquidity_metrics': liquidity_metrics,
    }
