"""
Dynamic position sizing based on account size.
Implements tiered maximum limits for risk control.
"""

from dataclasses import dataclass
from typing import Optional

from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class PositionSizingConfig:
    """
    Configuration for dynamic position sizing.

    base_pct: Base percentage of account to use per trade (default 20%).
    This allows for minimum 5 simultaneous positions (100% / 20% = 5).

    Tiered limits: maximum trade sizes based on account tiers.
    Prevents single trades from becoming too large as account grows.
    """

    base_pct: float = 0.20
    tier_limits: dict = None

    def __post_init__(self) -> None:
        if self.tier_limits is None:
            self.tier_limits = {
                20_000: 2_000,
                50_000: 5_000,
                100_000: 10_000,
                250_000: 20_000,
                500_000: 25_000,
                1_000_000: 50_000,
                float("inf"): 50_000,
            }


class DynamicPositionSizer:
    """
    Calculates optimal position size based on account size and tiered limits.
    """

    def __init__(self, config: Optional[PositionSizingConfig] = None) -> None:
        self.config = config or PositionSizingConfig()

        logger.info("Dynamic Position Sizer initialized:")
        logger.info(f"  Base percentage: {self.config.base_pct * 100:.1f}%")
        logger.info("  Tiered Limits:")

        sorted_tiers = sorted(
            [(k, v) for k, v in self.config.tier_limits.items() if k != float("inf")],
            key=lambda x: x[0],
        )

        for tier_max, trade_max in sorted_tiers:
            logger.info(
                f"    Up to ${tier_max:,} account -> Max ${trade_max:,} per trade"
            )

        logger.info(
            f"    Above ${sorted_tiers[-1][0]:,} -> Max ${self.config.tier_limits[float('inf')]:,} per trade (hard cap)"
        )

    def get_tier_limit(self, account_size: float) -> float:
        """
        Get the maximum trade size for given account size based on tiers.
        """
        sorted_tiers = sorted(self.config.tier_limits.items(), key=lambda x: x[0])

        for tier_threshold, max_trade_size in sorted_tiers:
            if account_size <= tier_threshold:
                return max_trade_size

        return self.config.tier_limits[float("inf")]

    def calculate_position_size(
        self,
        account_size: float,
        buying_power: Optional[float] = None,
        safety_margin: float = 0.10,
        buying_power_label: str = "buying power",
    ) -> tuple[float, str]:
        """
        Calculate optimal position size with all constraints applied.

        Returns:
            Tuple of (position_size, reason)
        """
        base_size = account_size * self.config.base_pct
        tier_limit = self.get_tier_limit(account_size)

        if base_size <= tier_limit:
            position_size = base_size
            reason = f"Base size ({self.config.base_pct * 100:.0f}% of account)"
        else:
            position_size = tier_limit
            reason = (
                f"Tier limit (account ${account_size:,.0f} -> max ${tier_limit:,.0f})"
            )

        position_size_with_margin = position_size * (1 - safety_margin)

        if buying_power is not None:
            max_from_buying_power = buying_power * 0.95

            if position_size_with_margin > max_from_buying_power:
                position_size_with_margin = max_from_buying_power
                reason = f"Limited by {buying_power_label} (${buying_power:,.0f})"

        return position_size_with_margin, reason

    def get_position_info(self, account_size: float) -> dict:
        """
        Get detailed information about position sizing for given account.
        """
        base_size = account_size * self.config.base_pct
        tier_limit = self.get_tier_limit(account_size)
        actual_size = min(base_size, tier_limit)

        return {
            "account_size": account_size,
            "base_pct": self.config.base_pct * 100,
            "base_size": base_size,
            "tier_limit": tier_limit,
            "actual_size": actual_size,
            "max_simultaneous_trades": int(account_size / actual_size),
            "position_as_pct_of_account": (actual_size / account_size) * 100,
            "is_tier_limited": base_size > tier_limit,
        }

    def log_position_info(self, account_size: float) -> None:
        """Log detailed position sizing information."""
        info = self.get_position_info(account_size)

        logger.info("=" * 60)
        logger.info("POSITION SIZING INFO")
        logger.info("=" * 60)
        logger.info(f"Account Size:              ${info['account_size']:,.2f}")
        logger.info(f"Base Percentage:           {info['base_pct']:.1f}%")
        logger.info(f"Base Trade Size:           ${info['base_size']:,.2f}")
        logger.info(f"Tier Limit:                ${info['tier_limit']:,.2f}")
        logger.info(f"Actual Trade Size:         ${info['actual_size']:,.2f}")
        logger.info(f"Max Simultaneous Trades:   {info['max_simultaneous_trades']}")
        logger.info(f"Position as % of Account:  {info['position_as_pct_of_account']:.2f}%")

        if info["is_tier_limited"]:
            logger.info("Status:                    TIER LIMITED (using tier cap, not base %)")
        else:
            logger.info("Status:                    USING BASE % (within tier limit)")

        logger.info("=" * 60)
