"""Central configuration for the V2 trading system.

This file is the single source of truth for runtime settings.
All values are validated at startup by `utils/config_validator.py`.
"""

from dataclasses import dataclass
import importlib
from typing import List, Literal, Optional


# Rule combination mode for strategy rules:
# - ANY: at least one enabled rule must pass
# - ALL: all enabled rules must pass
RuleOp = Literal["ANY", "ALL"]


@dataclass(frozen=True)
class RuleConfig:
    """Single momentum rule used by the strategy block."""

    enabled: bool
    name: str
    threshold_pct: float  # Required percent move over lookback window.
    lookback_seconds: int  # Lookback window in seconds.


@dataclass(frozen=True)
class ScannerConfig:
    """Generic scanner pacing settings."""

    block_size: int = 50  # Number of symbols processed per block.
    block_sleep_seconds: int = 1  # Pause between blocks (seconds).
    max_symbols_per_block: Optional[int] = None  # Optional hard cap for debugging.


@dataclass(frozen=True)
class IBConfig:
    """Interactive Brokers connection settings."""

    host: str = "127.0.0.1"  # TWS/Gateway host.
    port: int = 7497  # 7497 paper TWS, 7496 live TWS, 4001/4002 gateway.
    client_id: int = 11  # Scanner client ID.
    monitor_client_id: int = 10  # Broker monitor client ID.
    trader_client_id: int = 12  # Trader client ID.
    use_rth: bool = False  # False includes pre-/post-market bars.
    connection_check_interval_seconds: float = 30.0  # Reconnect health-check interval.


@dataclass(frozen=True)
class MonitorConfig:
    """24/7 monitor loop and process control window."""

    heartbeat_interval_seconds: float = 30.0  # Main monitor loop cadence.
    position_update_interval_seconds: float = 60.0  # Position/state sync cadence.
    account_update_interval_seconds: float = 300.0  # Account metrics refresh cadence.
    end_of_day_report: bool = True  # Emit EOD summary after market close.
    pre_market_start_minutes: float = 150  # Start scanner/trader X minutes before 09:30 ET.
    post_market_stop_minutes: float = 5.0  # Keep scanner/trader X minutes after 16:00 ET.


@dataclass(frozen=True)
class StrategyConfig:
    """Signal strategy settings shared by scanner components."""

    rule_operator: RuleOp = "ALL"
    rules: List[RuleConfig] = None
    bar_size: str = "1 min"  # Historical bar granularity.
    duration: str = "10 D"  # Erhöht für zuverlässige RVOL-Berechnung (mind. 5 Handelstage nötig)
    what_to_show: str = "TRADES"  # IB data source: TRADES/MIDPOINT/BID/ASK/...


@dataclass(frozen=True)
class TradingConfig:
    """Execution, risk and long-running retention behavior."""

    # Position sizing
    position_size_pct: float = 10.0  # Percent of account net liquidation per trade.
    auto_calculate_max_trades: bool = True  # Derive max open trades from size and account.
    manual_max_open_trades: int = 10  # Used only when auto_calculate_max_trades=False.
    safety_reserve_pct: float = 10.0  # Cash reserve not used for new entries.

    # Profit/loss exits
    take_profit_pct: float = 1.4  # TP distance from entry/fill in percent.
    stop_loss_pct: float = 10.0  # SL distance from entry/fill in percent.

    # Risk controls
    max_daily_stop_losses: int = 3  # Trading halt after this many SL events in one day.
    symbol_cooldown_minutes: int = 120  # Block re-entry per symbol after specific events.
    entry_retry_block_seconds: int = 60  # Temporary block after failed entry attempt.

    # Signal queue monitoring/retention
    signal_queue_warning_bytes: int = 50 * 1024 * 1024  # Warn when queue exceeds this size.
    signal_queue_warning_interval_seconds: int = 900  # Minimum seconds between queue warnings.
    signal_queue_rotate_bytes: int = 250 * 1024 * 1024  # Compact consumed queue beyond this size.
    signal_queue_retention_files: int = 30  # Keep this many rotated queue files.

    # Processed state retention
    processed_state_retention_days: int = 45  # Keep closed/rejected/manual_closed entries.
    processed_state_cleanup_interval_seconds: int = 3600  # How often cleanup runs.

    # Entry/exit execution safeguards
    max_entry_slippage_pct: float = 1.0  # Allowed entry slippage vs signal price.
    use_limit_entry: bool = True  # True=limit entry with slippage cap, False=market entry.
    exit_protection_verify_timeout_seconds: float = 8.0  # Wait window for TP/SL visibility.
    exit_protection_verify_check_interval_seconds: float = 0.5  # Poll interval for TP/SL verification.
    force_emergency_exit_if_any_protection_missing: bool = True  # Flat position if TP/SL protection is incomplete.


# ============================
# Edge scanner filter settings
# ============================

@dataclass(frozen=True)
class MovementConfig:
    """Ebene 1: movement capability filters."""

    min_1h_range_pct: float = 1.2  # Minimum high-low range over 1h.
    min_atr_pct: float = 0.2  # Minimum ATR proxy in percent.
    min_recent_range_pct: float = 0.2  # Minimum short-term range.
    recent_window_minutes: int = 15  # Window for recent range check.
    frozen_lookback_bars: int = 3  # Bars used for "frozen stock" detection.
    frozen_min_range_pct: float = 0.1  # Below this range is considered frozen.
    frozen_min_volume: float = 100.0  # Minimum summed volume in frozen check window.


@dataclass(frozen=True)
class VolumeConfig:
    """Ebene 2: volume and liquidity filters."""

    min_rvol: float = 1.0  # Relative volume floor.
    min_median_5m_volume: float = 1500.0  # Median 5m volume floor.
    min_avg_5m_dollar_volume: float = 50000.0  # Average 5m dollar volume floor.
    min_last_5m_dollar_volume: float = 25000.0  # Last 5m dollar volume floor.
    require_volume_acceleration: bool = False  # Require rising volume profile.


@dataclass(frozen=True)
class DirectionConfig:
    """Ebene 3: directional edge filters."""

    min_relative_strength: float = 0.0  # Outperformance threshold vs SPY.


@dataclass(frozen=True)
class CatalystConfig:
    """Ebene 4: catalyst/trigger filters."""

    enabled: bool = True  # Disable to skip catalyst checks.


@dataclass(frozen=True)
class RiskConfig:
    """Ebene 5: spread/risk sanity filters."""

    enabled: bool = True
    max_spread_pct: float = 1.0  # Maximum tolerated spread estimate.


@dataclass(frozen=True)
class PriceRangeConfig:
    """Ebene 0: pre-filter by absolute price range."""

    min_price: float = 8.0
    max_price: float = 1000.0


@dataclass(frozen=True)
class EdgeScannerConfig:
    """Top-level edge scanner settings and nested filter configs."""

    block_size: int = 50  # Symbols per scanner block.
    block_sleep_seconds: float = 0.0  # Pause between blocks.
    stats_log_interval_blocks: int = 5  # Summary log interval.

    price_range: PriceRangeConfig = None
    movement: MovementConfig = None
    volume: VolumeConfig = None
    direction: DirectionConfig = None
    catalyst: CatalystConfig = None
    risk: RiskConfig = None

    def __post_init__(self) -> None:
        if self.price_range is None:
            object.__setattr__(self, "price_range", PriceRangeConfig())
        if self.movement is None:
            object.__setattr__(self, "movement", MovementConfig())
        if self.volume is None:
            object.__setattr__(self, "volume", VolumeConfig())
        if self.direction is None:
            object.__setattr__(self, "direction", DirectionConfig())
        if self.catalyst is None:
            object.__setattr__(self, "catalyst", CatalystConfig())
        if self.risk is None:
            object.__setattr__(self, "risk", RiskConfig())


def get_config() -> dict:
    """Build the full runtime config object graph."""
    rules = [
        RuleConfig(
            enabled=True,
            name="up_24h_5pct",
            threshold_pct=5.0,
            lookback_seconds=24 * 3600,
        ),
        RuleConfig(
            enabled=True,
            name="up_1h_3pct",
            threshold_pct=1.0,
            lookback_seconds=1 * 3600,
        ),
        RuleConfig(
            enabled=False,
            name="rule3_placeholder",
            threshold_pct=0.0,
            lookback_seconds=0,
        ),
        RuleConfig(
            enabled=False,
            name="rule4_placeholder",
            threshold_pct=0.0,
            lookback_seconds=0,
        ),
        RuleConfig(
            enabled=False,
            name="rule5_placeholder",
            threshold_pct=0.0,
            lookback_seconds=0,
        ),
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


def _check_runtime_dependencies() -> None:
    """Fail fast with a clear message when required runtime packages are missing."""
    required_modules = {
        "ib_insync": "ib-insync",
        "pandas": "pandas",
        "numpy": "numpy",
    }
    missing = []

    for module_name, package_name in required_modules.items():
        try:
            importlib.import_module(module_name)
        except Exception:
            missing.append(package_name)

    if missing:
        missing_str = ", ".join(sorted(missing))
        raise RuntimeError(
            "Missing required Python packages: "
            f"{missing_str}. "
            "Install dependencies with: pip install -r requirements.txt"
        )


def validate_and_get_config() -> dict:
    """Build config, validate it and return the validated config."""
    from utils.config_validator import validate_config

    _check_runtime_dependencies()

    cfg = get_config()
    validate_config(cfg, exit_on_error=True)
    return cfg
