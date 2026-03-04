from dataclasses import dataclass
from typing import List, Literal, Optional


RuleOp = Literal["ANY", "ALL"]  # ANY = mindestens eine Regel, ALL = alle aktiven Regeln


@dataclass(frozen=True)
class RuleConfig:
    enabled: bool
    name: str
    # percent_change >= threshold_pct
    threshold_pct: float
    # lookback in seconds (e.g. 3600 = 1h, 86400 = 24h)
    lookback_seconds: int


@dataclass(frozen=True)
class ScannerConfig:
    block_size: int = 50
    block_sleep_seconds: int = 1
    # max symbols to process per block (optional limit; None = all)
    max_symbols_per_block: Optional[int] = None


@dataclass(frozen=True)
class IBConfig:
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 11  # Scanner uses this
    monitor_client_id: int = 10  # Broker Monitor uses this
    trader_client_id: int = 12  # Trader uses this
    # Historical only: we will use delayed / historical bars; no streaming market data required.
    use_rth: bool = True  # Regular Trading Hours only (True recommended)
    connection_check_interval_seconds: float = 30.0  # Health-check interval


@dataclass(frozen=True)
class MonitorConfig:
    heartbeat_interval_seconds: float = 30.0
    position_update_interval_seconds: float = 60.0
    account_update_interval_seconds: float = 300.0
    end_of_day_report: bool = True
    # Minuten vor Marktöffnung, ab wann der Monitor Prozesse starten darf
    pre_market_start_minutes: float = 2.0
    # Minuten nach Marktschluss, nach denen Prozesse gestoppt werden
    post_market_stop_minutes: float = 5.0


@dataclass(frozen=True)
class StrategyConfig:
    # Wie Regeln kombiniert werden: ANY oder ALL
    rule_operator: RuleOp = "ALL"

    # Bis zu 5 Regeln (du kannst später thresholds/lookbacks anpassen oder enabled togglen)
    rules: List[RuleConfig] = None

    # Bar Settings
    # Für 24h und 1h ist 5 mins meistens ausreichend und schnell
    bar_size: str = "1 min"     # '1 min' ist genauer aber langsamer
    duration: str = "5 D"       # Performance-Kompromiss: schnell genug und RVOL-kompatibel
    what_to_show: str = "TRADES" # TRADES / MIDPOINT etc.


@dataclass(frozen=True)
class TradingConfig:
    # Position Sizing (einheitlich fuer alle Trades)
    position_size_pct: float = 10.0  # Prozent des Kontowerts pro Trade (z.B. 10% = 0.10)

    # Take Profit / Stop Loss
    take_profit_pct: float = 1.4
    stop_loss_pct: float = 10.0

    # Max Open Trades (automatisch oder manuell)
    auto_calculate_max_trades: bool = True
    manual_max_open_trades: int = 10
    safety_reserve_pct: float = 10.0

    # Risk Management
    max_daily_stop_losses: int = 30
    symbol_cooldown_minutes: int = 60
    entry_retry_block_seconds: int = 60
    signal_queue_warning_bytes: int = 50 * 1024 * 1024
    signal_queue_warning_interval_seconds: int = 900

    # Order Execution
    max_entry_slippage_pct: float = 1.0
    use_limit_entry: bool = True
    exit_protection_verify_timeout_seconds: float = 8.0
    exit_protection_verify_check_interval_seconds: float = 0.5
    force_emergency_exit_if_any_protection_missing: bool = True  # Notaus: Kein Trade, wenn wichtige Schutzmechanismen fehlen



# ========== Edge Scanner Configurations ==========

@dataclass(frozen=True)
class MovementConfig:
    """Configuration for Ebene 1: Movement Capability"""
    min_1h_range_pct: float = 1.2  # Minimum 1-hour range as percentage
    min_atr_pct: float = 0.2     # Minimum ATR (1h scaled) as percentage
    min_recent_range_pct: float = 0.2      # Minimum range over recent window
    recent_window_minutes: int = 15        # Recent movement window in minutes
    frozen_lookback_bars: int = 3          # Bars inspected for frozen detection
    frozen_min_range_pct: float = 0.1      # Below this range stock is considered frozen
    frozen_min_volume: float = 100.0       # Minimum summed volume in frozen window


@dataclass(frozen=True)
class VolumeConfig:
    """Configuration for Ebene 2: Volume Activity"""
    min_rvol: float = 1.0  # Relative activity vs historical baseline
    min_median_5m_volume: float = 1500.0  # Minimum median shares per 5m bar
    min_avg_5m_dollar_volume: float = 50000.0  # Minimum avg $ volume per 5m bar
    min_last_5m_dollar_volume: float = 25000.0  # Minimum $ volume in latest 5m bar
    require_volume_acceleration: bool = False  # Optional strict momentum in volume


@dataclass(frozen=True)
class DirectionConfig:
    """Configuration for Ebene 3: Directional Edge"""
    min_relative_strength: float = 0.0  # Minimum RS vs SPY (0.0 = outperformance only)


@dataclass(frozen=True)
class CatalystConfig:
    """Configuration for Ebene 4: Catalyst/Trigger"""
    enabled: bool = True  # Optional filter - can be disabled


@dataclass(frozen=True)
class RiskConfig:
    """Configuration for Ebene 5: Risk Control"""
    enabled: bool = True          # Enable hybrid risk control
    max_spread_pct: float = 1.0   # Toleranter für geschätzte Spreads


@dataclass(frozen=True)
class PriceRangeConfig:
    """Configuration for Ebene 0: Price Range Filter (Pre-Filter)"""
    min_price: float = 8.0
    max_price: float = 1000.0


@dataclass(frozen=True)
class EdgeScannerConfig:
    """Configuration for Edge Scanner with 5-level filter logic"""
    block_size: int = 50
    block_sleep_seconds: float = 0.0
    stats_log_interval_blocks: int = 5
    
    # Sub-configurations for each filter level
    price_range: PriceRangeConfig = None
    movement: MovementConfig = None
    volume: VolumeConfig = None
    direction: DirectionConfig = None
    catalyst: CatalystConfig = None
    risk: RiskConfig = None
    
    def __post_init__(self):
        # Initialize sub-configs if not provided
        if self.price_range is None:
            object.__setattr__(self, 'price_range', PriceRangeConfig())
        if self.movement is None:
            object.__setattr__(self, 'movement', MovementConfig())
        if self.volume is None:
            object.__setattr__(self, 'volume', VolumeConfig())
        if self.direction is None:
            object.__setattr__(self, 'direction', DirectionConfig())
        if self.catalyst is None:
            object.__setattr__(self, 'catalyst', CatalystConfig())
        if self.risk is None:
            object.__setattr__(self, 'risk', RiskConfig())



def get_config():
    rules = [
        RuleConfig(enabled=True,  name="up_24h_5pct", threshold_pct=5.0, lookback_seconds=24 * 3600),
        RuleConfig(enabled=True,  name="up_1h_3pct",  threshold_pct=1.0, lookback_seconds=1 * 3600),

        # Drei weitere Regeln: standardmäßig aus (enabled=False) -> du kannst sie bei Bedarf anschalten
        RuleConfig(enabled=False, name="rule3_placeholder", threshold_pct=0.0, lookback_seconds=0),
        RuleConfig(enabled=False, name="rule4_placeholder", threshold_pct=0.0, lookback_seconds=0),
        RuleConfig(enabled=False, name="rule5_placeholder", threshold_pct=0.0, lookback_seconds=0),
    ]

    return {
        "scanner": ScannerConfig(),
        "ib": IBConfig(),
        "monitor": MonitorConfig(),
        "strategy": StrategyConfig(rules=rules),
        "trading": TradingConfig(),
        "edge_scanner": EdgeScannerConfig(
            price_range=PriceRangeConfig(),
            movement=MovementConfig(),
            volume=VolumeConfig(),
            direction=DirectionConfig(),
            catalyst=CatalystConfig(),
            risk=RiskConfig(),
        ),
    }


def validate_and_get_config():
    """
    Loads config and validates it.
    Exits with a clear error message on validation errors.

    Returns:
        dict: Validated config.
    """
    from utils.config_validator import validate_config

    cfg = get_config()

    validate_config(cfg, exit_on_error=True)

    return cfg
