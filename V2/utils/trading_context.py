"""
Trading Context
Encapsulates trading dependencies to avoid globals.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from utils.logging_utils import MultiLogger
from utils.account_checker import AccountChecker
from utils.rate_limiter import RateLimiter
from utils.unified_position_sizer import UnifiedPositionSizer
from utils.symbol_cooldown import SymbolCooldownManager
from utils.trading_dashboard import TradingDashboard


@dataclass
class TradingContext:
    """
    Container for trading dependencies.

    Benefits:
    - No global state
    - Type-safe dependencies
    - Easier testing
    """

    logger: MultiLogger
    account_checker: AccountChecker
    rate_limiter: RateLimiter
    position_sizer: UnifiedPositionSizer
    cooldown_manager: SymbolCooldownManager
    dashboard: TradingDashboard

    @classmethod
    def create(
        cls,
        logger: MultiLogger,
        account_checker: AccountChecker,
        rate_limiter: RateLimiter,
        position_sizer: UnifiedPositionSizer,
        cooldown_manager: SymbolCooldownManager,
        dashboard: TradingDashboard,
    ) -> "TradingContext":
        """Factory method for TradingContext."""
        return cls(
            logger=logger,
            account_checker=account_checker,
            rate_limiter=rate_limiter,
            position_sizer=position_sizer,
            cooldown_manager=cooldown_manager,
            dashboard=dashboard,
        )

    def log_initialization(self) -> None:
        """Log successful initialization of all components."""
        self.logger.info("=" * 80)
        self.logger.info("TRADING CONTEXT INITIALIZED")
        self.logger.info("=" * 80)
        self.logger.info("Logger:            MultiLogger")
        self.logger.info("Account Checker:   AccountChecker")
        self.logger.info("Rate Limiter:      RateLimiter")
        self.logger.info("Position Sizer:    UnifiedPositionSizer")
        self.logger.info("Cooldown Manager:  SymbolCooldownManager")
        self.logger.info("Dashboard:         TradingDashboard")
        self.logger.info("=" * 80)


def create_trading_context(
    cfg: dict,
    state_dir: Path,
    debug_mode: bool = False,
) -> TradingContext:
    """
    Create a fully initialized TradingContext from config.

    Args:
        cfg: Config dict
        state_dir: State directory path
        debug_mode: Debug mode for logging

    Returns:
        Initialized TradingContext
    """
    from utils.logging_utils import setup_logging
    from utils.rate_limiter import get_rate_limiter, RateLimitConfig
    from utils.unified_position_sizer import create_unified_position_sizer

    tr_cfg = cfg["trading"]

    logger = setup_logging("trader_live", debug_mode=debug_mode)

    account_checker = AccountChecker(
        account_id=None,
        cache_seconds=30.0,
    )

    # Rate Limiter with increased historical data limit
    # IB API limit: ~6000/10min (measured)
    # We use: 5000/10min (safety buffer: 1000)
    # Exceeding this may trigger "HMDS query returned no data"
    rate_limit_config = RateLimitConfig(
        max_requests_per_second=40,
        burst_limit=80,
        historical_data_per_10min=5000,
    )
    rate_limiter = get_rate_limiter(rate_limit_config)

    logger.info("Rate Limiter initialized with increased limits:")
    logger.info(
        f"  Historical Data: {rate_limit_config.historical_data_per_10min}/10min"
    )

    position_sizer = create_unified_position_sizer(cfg)

    cooldown_path = state_dir / "symbol_cooldowns.json"
    cooldown_manager = SymbolCooldownManager(
        state_path=cooldown_path,
        cooldown_minutes=tr_cfg.symbol_cooldown_minutes,
    )

    dashboard = TradingDashboard()

    context = TradingContext.create(
        logger=logger,
        account_checker=account_checker,
        rate_limiter=rate_limiter,
        position_sizer=position_sizer,
        cooldown_manager=cooldown_manager,
        dashboard=dashboard,
    )

    context.log_initialization()
    return context
