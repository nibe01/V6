# scanner/edge_signals.py
"""
Signal structure for Edge Scanner.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class EdgeSignal:
    """
    Edge Scanner signal with metrics from all 5 filter levels.
    """
    symbol: str
    timestamp: str
    price: float
    
    # Ebene 1: Movement Capability
    range_1h_pct: float
    atr_1h_pct: float
    recent_range_pct: float
    is_frozen: bool
    
    # Ebene 2: Volume Activity
    rvol: float
    vol_accelerating: bool
    
    # Ebene 3: Directional Edge
    above_vwap: bool
    vwap_distance_pct: float
    relative_strength_pct: float
    higher_lows: bool
    
    # Ebene 4: Catalyst (optional)
    broke_pm_high: bool
    flag_breakout: bool
    vwap_reclaim: bool
    
    # Ebene 5: Risk Control
    spread_pct: float
    bid: float
    ask: float
    spread_method: str = "unknown"
    spread_confidence: float = 0.0


def to_json_dict(signal: EdgeSignal) -> dict:
    """
    Convert EdgeSignal to JSON-serializable dictionary.
    
    Args:
        signal: EdgeSignal instance
    
    Returns:
        Dictionary representation
    """
    return asdict(signal)


def create_edge_signal(
    symbol: str,
    timestamp: str,
    price: float,
    ebene_1: dict,
    ebene_2: dict,
    ebene_3: dict,
    ebene_4: dict,
    ebene_5: dict,
) -> EdgeSignal:
    """
    Factory function to create EdgeSignal from filter results.
    
    Args:
        symbol: Stock symbol
        timestamp: ISO timestamp
        price: Current price
        ebene_1: Metrics from movement filter
        ebene_2: Metrics from volume filter
        ebene_3: Metrics from directional filter
        ebene_4: Metrics from catalyst filter
        ebene_5: Metrics from risk filter
    
    Returns:
        EdgeSignal instance
    """
    return EdgeSignal(
        symbol=symbol,
        timestamp=timestamp,
        price=price,
        # Ebene 1
        range_1h_pct=ebene_1.get('1h_range_pct', 0.0),
        atr_1h_pct=ebene_1.get('atr_1h_pct', 0.0),
        recent_range_pct=ebene_1.get('recent_range_pct', 0.0),
        is_frozen=ebene_1.get('is_frozen', True),
        # Ebene 2
        rvol=ebene_2.get('rvol', 0.0),
        vol_accelerating=ebene_2.get('vol_accelerating', False),
        # Ebene 3
        above_vwap=ebene_3.get('above_vwap', False),
        vwap_distance_pct=ebene_3.get('vwap_distance_pct', 0.0),
        relative_strength_pct=ebene_3.get('relative_strength', 0.0),
        higher_lows=ebene_3.get('higher_lows', False),
        # Ebene 4
        broke_pm_high=ebene_4.get('broke_pm_high', False),
        flag_breakout=ebene_4.get('flag_breakout', False),
        vwap_reclaim=ebene_4.get('vwap_reclaim', False),
        # Ebene 5
        spread_pct=ebene_5.get('spread_pct', 0.0),
        bid=ebene_5.get('bid', 0.0),
        ask=ebene_5.get('ask', 0.0),
        spread_method=ebene_5.get('method', 'unknown'),
        spread_confidence=ebene_5.get('confidence', 0.0),
    )
