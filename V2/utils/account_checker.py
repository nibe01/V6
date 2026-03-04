"""
Account Balance Checker
Checks account balance and buying power before trade execution.
Prevents rejected orders from insufficient funds.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ib_insync import IB

from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class AccountInfo:
    """Account information from IB."""
    net_liquidation: float
    total_cash_value: float
    usd_cash_value: float
    total_cash_all_currencies: float
    buying_power: float
    excess_liquidity: float
    maintenance_margin: float
    currency: str = "USD"


class AccountChecker:
    """
    Checks account balance and buying power.

    Features:
    - Fetches account data from IB
    - Checks if there is enough capital for a trade
    - Respects buying power limits
    - Warns on low balances
    - Caches account data for performance
    """

    def __init__(self, account_id: Optional[str] = None, cache_seconds: float = 30.0):
        self.account_id = account_id
        self.cache_seconds = cache_seconds

        self._cached_info: Optional[AccountInfo] = None
        self._cache_timestamp: float = 0.0
        self._last_warning_balance: float = 0.0

    def get_account_info(
        self, ib: IB, force_refresh: bool = False
    ) -> Optional[AccountInfo]:
        import time

        def _to_float(raw) -> float:
            try:
                return float(raw)
            except (TypeError, ValueError):
                try:
                    return float(str(raw).replace(",", ""))
                except Exception:
                    return 0.0

        if not force_refresh and self._cached_info:
            age = time.time() - self._cache_timestamp
            if age < self.cache_seconds:
                logger.debug(f"Using cached account info (age: {age:.1f}s)")
                return self._cached_info

        try:
            from utils.rate_limiter import get_rate_limiter

            rate_limiter = get_rate_limiter()
            rate_limiter.wait_if_needed(
                request_type="account_summary",
                is_historical=False,
            )

            account_values = ib.accountSummary(self.account_id)

            if not account_values:
                logger.warning("No account data received from IB")
                return None

            by_tag_currency = {
                (item.tag, getattr(item, "currency", "")): item.value
                for item in account_values
            }

            def value_for(tag: str, currency: str) -> float:
                return _to_float(by_tag_currency.get((tag, currency), 0.0))

            net_liquidation = value_for("NetLiquidation", "USD")
            if net_liquidation <= 0:
                net_liquidation = value_for("NetLiquidation", "BASE")

            total_cash_all = value_for("TotalCashValue", "BASE")
            if total_cash_all <= 0:
                total_cash_all = value_for("TotalCashValue", "USD")

            usd_cash = value_for("CashBalance", "USD")
            if usd_cash == 0:
                usd_cash = value_for("TotalCashValue", "USD")
            if usd_cash == 0 and total_cash_all > 0:
                usd_cash = total_cash_all

            buying_power = value_for("BuyingPower", "USD")
            if buying_power <= 0:
                buying_power = value_for("BuyingPower", "BASE")

            excess_liquidity = value_for("ExcessLiquidity", "USD")
            if excess_liquidity <= 0:
                excess_liquidity = value_for("ExcessLiquidity", "BASE")

            maintenance_margin = value_for("MaintMarginReq", "USD")
            if maintenance_margin <= 0:
                maintenance_margin = value_for("MaintMarginReq", "BASE")

            currency = "USD"

            account_info = AccountInfo(
                net_liquidation=net_liquidation,
                total_cash_value=usd_cash,
                usd_cash_value=usd_cash,
                total_cash_all_currencies=total_cash_all,
                buying_power=buying_power,
                excess_liquidity=excess_liquidity,
                maintenance_margin=maintenance_margin,
                currency=currency,
            )

            self._cached_info = account_info
            self._cache_timestamp = time.time()

            logger.debug(
                f"Account Info: Balance=${net_liquidation:.2f}, "
                f"USD Cash=${usd_cash:.2f}, "
                f"Total Cash (all currencies)=${total_cash_all:.2f}, "
                f"Buying Power=${buying_power:.2f}"
            )

            if usd_cash < 0:
                logger.warning(
                    "USD cash is negative: $%.2f (total cash all currencies: $%.2f)",
                    usd_cash,
                    total_cash_all,
                )

            return account_info

        except Exception as e:
            logger.error(f"Error getting account info: {e}")
            return None

    def can_afford_trade(
        self,
        ib: IB,
        trade_amount: float,
        symbol: str = "",
        safety_margin: float = 0.1,
        force_refresh: bool = False,
    ) -> tuple[bool, str]:
        account_info = self.get_account_info(ib, force_refresh=force_refresh)

        if not account_info:
            return False, "Could not retrieve account information"

        required_capital = trade_amount * (1 + safety_margin)

        if account_info.usd_cash_value < required_capital:
            return (
                False,
                "Insufficient USD cash: "
                f"${account_info.usd_cash_value:.2f} available, "
                f"${required_capital:.2f} required "
                f"(including {safety_margin * 100:.0f}% margin)",
            )

        if account_info.excess_liquidity < trade_amount:
            return (
                False,
                "Insufficient excess liquidity: "
                f"${account_info.excess_liquidity:.2f} available, "
                f"${trade_amount:.2f} required",
            )

        if account_info.net_liquidation < trade_amount * 3:
            if abs(account_info.net_liquidation - self._last_warning_balance) > trade_amount:
                logger.warning(
                    "Low account balance: "
                    f"${account_info.net_liquidation:.2f} "
                    f"(only {account_info.net_liquidation / trade_amount:.1f}x trade size)"
                )
                self._last_warning_balance = account_info.net_liquidation

        logger.debug(
            f"Can afford {symbol} trade: "
            f"${trade_amount:.2f} required, "
            f"${account_info.usd_cash_value:.2f} USD available"
        )

        return True, "OK"

    def get_max_position_size(
        self,
        ib: IB,
        price_per_share: float,
        max_percentage: float = 0.2,
    ) -> int:
        account_info = self.get_account_info(ib)

        if not account_info or price_per_share <= 0:
            return 0

        max_usd = account_info.net_liquidation * max_percentage
        max_usd = min(max_usd, account_info.usd_cash_value * 0.9)

        max_qty = int(max_usd / price_per_share)

        logger.debug(
            f"Max position size: {max_qty} shares "
            f"(${max_usd:.2f} / ${price_per_share:.2f})"
        )

        return max(0, max_qty)

    def log_account_status(self, ib: IB) -> None:
        account_info = self.get_account_info(ib, force_refresh=True)

        if not account_info:
            logger.warning("Could not retrieve account status")
            return

        logger.info("=" * 60)
        logger.info("ACCOUNT STATUS")
        logger.info("=" * 60)
        logger.info(f"Net Liquidation:    ${account_info.net_liquidation:12,.2f}")
        logger.info(f"USD Cash Available: ${account_info.usd_cash_value:12,.2f}")
        logger.info(f"Total Cash (all):   ${account_info.total_cash_all_currencies:12,.2f}")
        logger.info(f"Buying Power:       ${account_info.buying_power:12,.2f}")
        logger.info(f"Excess Liquidity:   ${account_info.excess_liquidity:12,.2f}")
        logger.info(f"Maintenance Margin: ${account_info.maintenance_margin:12,.2f}")
        logger.info(f"Currency:           {account_info.currency:>14}")
        logger.info("=" * 60)
