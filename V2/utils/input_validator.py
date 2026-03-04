"""
Input Validator
Validates numeric inputs (prices, quantities) before use.
Prevents crashes from NaN, Infinity, negative values, etc.
"""

from __future__ import annotations

import math
from typing import Any

from utils.logging_utils import get_logger

logger = get_logger(__name__)


class ValidationError(Exception):
    """Raised when input validation fails."""


def is_valid_number(value: Any) -> bool:
    """
    Checks if value is a valid, finite number.

    Args:
        value: Value to check

    Returns:
        True if valid number (not NaN, not Infinity)
    """
    if value is None:
        return False

    try:
        num = float(value)
    except (TypeError, ValueError):
        return False

    if math.isnan(num) or math.isinf(num):
        return False

    return True


def validate_price(
    price: Any,
    field_name: str,
    min_price: float = 0.01,
    max_price: float = 1000000.0,
    allow_zero: bool = False,
) -> float:
    """
    Validates a price value.

    Args:
        price: Price to validate
        field_name: Name of field (for error messages)
        min_price: Minimum allowed price
        max_price: Maximum allowed price
        allow_zero: Whether to allow price = 0

    Returns:
        Validated price

    Raises:
        ValidationError: If price is invalid
    """
    if not is_valid_number(price):
        raise ValidationError(
            f"{field_name} is not a valid number: {price} "
            f"(type: {type(price).__name__})"
        )

    price_float = float(price)

    if price_float < 0:
        raise ValidationError(f"{field_name} cannot be negative: {price_float}")

    if not allow_zero and price_float == 0:
        raise ValidationError(f"{field_name} cannot be zero")

    if price_float < min_price:
        raise ValidationError(
            f"{field_name} is below minimum: {price_float} < {min_price}"
        )

    if price_float > max_price:
        raise ValidationError(
            f"{field_name} exceeds maximum: {price_float} > {max_price}"
        )

    return price_float


def validate_quantity(
    quantity: Any,
    field_name: str = "quantity",
    min_qty: int = 1,
    max_qty: int = 1000000,
) -> int:
    """
    Validates a quantity (share count).

    Args:
        quantity: Quantity to validate
        field_name: Name of field (for error messages)
        min_qty: Minimum allowed quantity
        max_qty: Maximum allowed quantity

    Returns:
        Validated quantity as int

    Raises:
        ValidationError: If quantity is invalid
    """
    if not is_valid_number(quantity):
        raise ValidationError(
            f"{field_name} is not a valid number: {quantity} "
            f"(type: {type(quantity).__name__})"
        )

    qty_float = float(quantity)

    if qty_float < 0:
        raise ValidationError(f"{field_name} cannot be negative: {qty_float}")

    if qty_float == 0:
        raise ValidationError(f"{field_name} cannot be zero")

    qty_int = int(qty_float)

    if qty_int < min_qty:
        raise ValidationError(
            f"{field_name} is below minimum: {qty_int} < {min_qty}"
        )

    if qty_int > max_qty:
        raise ValidationError(
            f"{field_name} exceeds maximum: {qty_int} > {max_qty}"
        )

    return qty_int


def validate_percentage(
    percentage: Any,
    field_name: str,
    min_pct: float = 0.0,
    max_pct: float = 100.0,
) -> float:
    """
    Validates a percentage value.

    Args:
        percentage: Percentage to validate
        field_name: Name of field (for error messages)
        min_pct: Minimum allowed percentage
        max_pct: Maximum allowed percentage

    Returns:
        Validated percentage

    Raises:
        ValidationError: If percentage is invalid
    """
    if not is_valid_number(percentage):
        raise ValidationError(
            f"{field_name} is not a valid number: {percentage} "
            f"(type: {type(percentage).__name__})"
        )

    pct_float = float(percentage)

    if pct_float < min_pct:
        raise ValidationError(
            f"{field_name} is below minimum: {pct_float} < {min_pct}"
        )

    if pct_float > max_pct:
        raise ValidationError(
            f"{field_name} exceeds maximum: {pct_float} > {max_pct}"
        )

    return pct_float


def validate_price_relationship(
    entry_price: float,
    tp_price: float,
    sl_price: float,
    is_long: bool = True,
) -> None:
    """
    Validates relationship between entry, TP, and SL prices.

    For LONG positions:
    - TP must be > entry
    - SL must be < entry

    For SHORT positions:
    - TP must be < entry
    - SL must be > entry
    """
    if is_long:
        if tp_price <= entry_price:
            raise ValidationError(
                "Invalid TP for LONG position: "
                f"TP ({tp_price}) must be > entry ({entry_price})"
            )

        if sl_price >= entry_price:
            raise ValidationError(
                "Invalid SL for LONG position: "
                f"SL ({sl_price}) must be < entry ({entry_price})"
            )
    else:
        if tp_price >= entry_price:
            raise ValidationError(
                "Invalid TP for SHORT position: "
                f"TP ({tp_price}) must be < entry ({entry_price})"
            )

        if sl_price <= entry_price:
            raise ValidationError(
                "Invalid SL for SHORT position: "
                f"SL ({sl_price}) must be > entry ({entry_price})"
            )


def safe_division(
    numerator: float,
    denominator: float,
    default: float = 0.0,
    field_name: str = "value",
) -> float:
    """
    Safe division that handles zero, NaN, and Infinity.
    """
    if not is_valid_number(numerator) or not is_valid_number(denominator):
        logger.warning(
            "Invalid division inputs for %s: %s / %s, returning %s",
            field_name,
            numerator,
            denominator,
            default,
        )
        return default

    if denominator == 0:
        logger.warning(
            "Division by zero for %s: %s / 0, returning %s",
            field_name,
            numerator,
            default,
        )
        return default

    result = numerator / denominator

    if not is_valid_number(result):
        logger.warning(
            "Invalid division result for %s: %s / %s = %s, returning %s",
            field_name,
            numerator,
            denominator,
            result,
            default,
        )
        return default

    return result


def sanitize_bar_data(bars_df) -> bool:
    """
    Validates bar data from IB API.

    Checks for:
    - NaN values
    - Infinity values
    - Negative prices
    - Missing data

    Returns:
        True if data is valid
    """
    if bars_df is None or bars_df.empty:
        logger.warning("Bar data is empty or None")
        return False

    required_cols = ["open", "high", "low", "close", "volume"]
    for col in required_cols:
        if col not in bars_df.columns:
            logger.error("Missing required column: %s", col)
            return False

    if bars_df[required_cols].isnull().any().any():
        logger.warning("Bar data contains NaN values")
        for col in required_cols:
            if bars_df[col].isnull().any():
                count = bars_df[col].isnull().sum()
                logger.warning("  %s: %s NaN values", col, count)
        return False

    for col in ["open", "high", "low", "close"]:
        if (bars_df[col] == float("inf")).any() or (bars_df[col] == float("-inf")).any():
            logger.warning("Bar data contains Infinity in %s", col)
            return False

    for col in ["open", "high", "low", "close"]:
        if (bars_df[col] < 0).any():
            logger.warning("Bar data contains negative prices in %s", col)
            return False

    for col in ["open", "high", "low", "close"]:
        if (bars_df[col] == 0).any():
            logger.warning("Bar data contains zero prices in %s", col)

    return True
