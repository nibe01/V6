from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from ib_insync import IB, Stock, util

from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class SignalResult:
    symbol: str
    passed: bool
    reason: str
    now_utc: str
    pct_changes: Dict[str, float]
    start_prices: Dict[str, float]
    end_price: float


def _pct_change(start: float, end: float) -> float:
    """
    Calculates percentage change from start to end with input validation.
    """
    from utils.input_validator import is_valid_number, safe_division

    if not is_valid_number(start) or not is_valid_number(end):
        return 0.0

    if start <= 0:
        return 0.0

    change = safe_division(
        end - start,
        start,
        default=0.0,
        field_name="pct_change",
    )

    return change * 100.0


def _find_price_at_or_before(bars_df, target_ts_utc: datetime) -> Optional[float]:
    if bars_df.empty:
        return None

    dates = bars_df["date"]

    def to_utc(dt):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    best_price = None
    best_dt = None
    for dt, close in zip(dates, bars_df["close"]):
        dtu = to_utc(dt)
        if dtu <= target_ts_utc and (best_dt is None or dtu > best_dt):
            try:
                from utils.input_validator import is_valid_number

                close_float = float(close)
                if is_valid_number(close_float) and close_float > 0:
                    best_dt = dtu
                    best_price = close_float
                else:
                    continue
            except (TypeError, ValueError):
                continue
    return best_price


def fetch_bars_df(
    ib: IB,
    contract: Stock,          # <- NEU
    duration: str,
    bar_size: str,
    what_to_show: str,
    use_rth: bool,
    rate_limiter=None,
):
    if rate_limiter:
        rate_limiter.wait_if_needed(
            request_type="historical_data",
            is_historical=True,
        )

    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr=duration,
        barSizeSetting=bar_size,
        whatToShow=what_to_show,
        useRTH=use_rth,
        formatDate=1,
        keepUpToDate=False,
    )
    bars_df = util.df(bars)
    if bars_df is None:
        # Keep scanner call-sites safe: they expect a DataFrame with `.empty`.
        import pandas as pd

        return pd.DataFrame()

    return bars_df


def evaluate_rules_from_bars(
    symbol: str,
    bars_df,
    rules: List,
    rule_operator: str,
) -> SignalResult:
    """
    Evaluate all rules against the bars_df and determine if signal passed.
    
    Args:
        symbol: Stock symbol
        bars_df: DataFrame with historical bars (must have 'date' and 'close' columns)
        rules: List of RuleConfig objects with enabled, name, threshold_pct, lookback_seconds
        rule_operator: "ANY" (at least one rule passes) or "ALL" (all enabled rules pass)
    
    Returns:
        SignalResult with passed status and details
    """
    from utils.input_validator import sanitize_bar_data, validate_price

    now_utc = datetime.now(timezone.utc).isoformat()

    if not sanitize_bar_data(bars_df):
        logger.warning("%s: Bar data validation failed", symbol)
        return SignalResult(
            symbol=symbol,
            passed=False,
            reason="Invalid bar data (NaN, Infinity, or negative values)",
            now_utc=now_utc,
            pct_changes={},
            start_prices={},
            end_price=0.0,
        )
    
    if bars_df.empty:
        return SignalResult(
            symbol=symbol,
            passed=False,
            reason="No bars data available",
            now_utc=now_utc,
            pct_changes={},
            start_prices={},
            end_price=0.0,
        )
    
    # Current price is the last close
    try:
        end_price_raw = bars_df["close"].iloc[-1]
        end_price = validate_price(
            end_price_raw,
            field_name=f"end_price for {symbol}",
            min_price=0.01,
            max_price=1000000.0,
        )
    except Exception as e:
        logger.error("%s: Invalid end_price: %s", symbol, e)
        return SignalResult(
            symbol=symbol,
            passed=False,
            reason=f"Invalid end_price: {e}",
            now_utc=now_utc,
            pct_changes={},
            start_prices={},
            end_price=0.0,
        )
    
    pct_changes = {}
    start_prices = {}
    rule_results = []
    
    # Evaluate each enabled rule
    for rule in rules:
        if not rule.enabled:
            continue
        
        # Calculate lookback time
        target_ts_utc = datetime.now(timezone.utc) - timedelta(seconds=rule.lookback_seconds)
        
        # Find price at or before the lookback time
        start_price = _find_price_at_or_before(bars_df, target_ts_utc)
        
        if start_price is None:
            # No data for this lookback period
            rule_passed = False
            pct_change = 0.0
        else:
            pct_change = _pct_change(start_price, end_price)
            rule_passed = pct_change >= rule.threshold_pct
        
        pct_changes[rule.name] = pct_change
        start_prices[rule.name] = start_price if start_price is not None else 0.0
        rule_results.append(rule_passed)
    
    # Determine overall result based on operator
    if len(rule_results) == 0:
        # No enabled rules
        passed = False
        reason = "No enabled rules"
    elif rule_operator == "ANY":
        # At least one rule must pass
        passed = any(rule_results)
        reason = f"ANY operator: {sum(rule_results)}/{len(rule_results)} rules passed"
    elif rule_operator == "ALL":
        # All rules must pass
        passed = all(rule_results)
        reason = f"ALL operator: {sum(rule_results)}/{len(rule_results)} rules passed"
    else:
        passed = False
        reason = f"Unknown rule_operator: {rule_operator}"
    
    return SignalResult(
        symbol=symbol,
        passed=passed,
        reason=reason,
        now_utc=now_utc,
        pct_changes=pct_changes,
        start_prices=start_prices,
        end_price=end_price,
    )


def to_json_dict(signal_result: SignalResult) -> dict:
    """Convert SignalResult dataclass to JSON-serializable dictionary."""
    return asdict(signal_result)
