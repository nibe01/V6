"""
Unified position sizing for consistent trade sizes.
"""

from __future__ import annotations

from dataclasses import dataclass

from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class UnifiedPositionSizerConfig:
    """Config for unified position sizing."""

    position_size_pct: float
    auto_calculate_max_trades: bool
    manual_max_open_trades: int
    safety_reserve_pct: float


@dataclass
class PositionSizeResult:
    """Result of position sizing calculation."""

    trade_size_usd: float
    max_open_trades: int
    reason: str


class UnifiedPositionSizer:
    """Calculates uniform position sizes and dynamic max open trades."""

    def __init__(self, config: UnifiedPositionSizerConfig) -> None:
        self.config = config
        logger.info("=" * 80)
        logger.info("UNIFIED POSITION SIZER INITIALIZED")
        logger.info("=" * 80)
        logger.info(
            "Position Size:              %s%% of account",
            config.position_size_pct,
        )
        logger.info(
            "Auto Calculate Max Trades:  %s",
            config.auto_calculate_max_trades,
        )
        if not config.auto_calculate_max_trades:
            logger.info(
                "Manual Max Open Trades:     %s",
                config.manual_max_open_trades,
            )
        logger.info("Safety Reserve:             %s%%", config.safety_reserve_pct)
        logger.info("=" * 80)

    def calculate_position_size(
        self,
        account_balance: float,
        available_cash: float,
        current_open_positions: int,
    ) -> PositionSizeResult:
        """Calculate uniform position size and max open trades."""
        safety_reserve = available_cash * (self.config.safety_reserve_pct / 100)
        usable_cash = available_cash - safety_reserve

        position_size_usd = account_balance * (self.config.position_size_pct / 100)

        if self.config.auto_calculate_max_trades:
            max_trades = (
                int(100 / self.config.position_size_pct)
                if self.config.position_size_pct
                else 0
            )
            max_trades = max(1, max_trades)
            reason = (
                "Auto-calculated from position_size_pct "
                f"({self.config.position_size_pct:.2f}%)"
            )
        else:
            max_trades = self.config.manual_max_open_trades
            reason = f"Manual override (config: {max_trades})"

        if current_open_positions >= max_trades:
            return PositionSizeResult(
                trade_size_usd=0.0,
                max_open_trades=max_trades,
                reason=f"Max trades reached ({current_open_positions}/{max_trades})",
            )

        if usable_cash < position_size_usd:
            return PositionSizeResult(
                trade_size_usd=0.0,
                max_open_trades=max_trades,
                reason=(
                    f"Insufficient cash (${usable_cash:,.0f} < "
                    f"${position_size_usd:,.0f})"
                ),
            )

        return PositionSizeResult(
            trade_size_usd=position_size_usd,
            max_open_trades=max_trades,
            reason=reason,
        )

    def calculate_quantity(
        self,
        position_size_usd: float,
        price: float,
    ) -> tuple[int, str]:
        """Calculate order quantity based on position size and price."""
        if position_size_usd <= 0:
            return 0, "Position size is zero"

        if price <= 0:
            return 0, "Invalid price"

        qty_float = position_size_usd / price
        qty = int(qty_float)

        if qty == 0:
            return 0, (
                f"Price too high (${price:.2f}) for position size "
                f"${position_size_usd:,.0f}"
            )

        actual_size = qty * price
        return qty, f"${actual_size:,.0f} ({qty} shares @ ${price:.2f})"

    def log_position_info(
        self,
        account_balance: float,
        available_cash: float,
        current_open_positions: int,
    ) -> None:
        """Log current position sizing info."""
        result = self.calculate_position_size(
            account_balance=account_balance,
            available_cash=available_cash,
            current_open_positions=current_open_positions,
        )

        logger.info("=" * 80)
        logger.info("POSITION SIZING INFO")
        logger.info("=" * 80)
        logger.info("Account Balance:            $%s", f"{account_balance:,.2f}")
        logger.info("Available Cash:             $%s", f"{available_cash:,.2f}")
        logger.info(
            "Position Size per Trade:    $%s (%s%%)",
            f"{result.trade_size_usd:,.2f}",
            self.config.position_size_pct,
        )
        logger.info("Max Open Trades:            %s", result.max_open_trades)
        logger.info(
            "Current Open Positions:     %s/%s",
            current_open_positions,
            result.max_open_trades,
        )
        logger.info("Calculation Method:         %s", result.reason)
        logger.info("=" * 80)


def create_unified_position_sizer(cfg: dict) -> UnifiedPositionSizer:
    """Factory for UnifiedPositionSizer."""
    tr_cfg = cfg["trading"]

    config = UnifiedPositionSizerConfig(
        position_size_pct=tr_cfg.position_size_pct,
        auto_calculate_max_trades=tr_cfg.auto_calculate_max_trades,
        manual_max_open_trades=tr_cfg.manual_max_open_trades,
        safety_reserve_pct=tr_cfg.safety_reserve_pct,
    )

    return UnifiedPositionSizer(config)
