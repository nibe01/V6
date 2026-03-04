"""
Order Retry Handler
Retry system for rejected orders with basic error classification.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional, Callable

from ib_insync import IB, Order, Contract

from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class OrderRejection:
    """Details about a rejected order."""
    order_id: int
    symbol: str
    reason: str
    is_retriable: bool
    suggested_action: Optional[str] = None


class OrderRetryHandler:
    """Handles order rejections with simple retry logic."""

    RETRIABLE_PATTERNS = [
        "price too far",
        "price is too far",
        "outside the current",
        "system is busy",
        "temporarily unavailable",
        "timeout",
        "connection",
        "order size exceeds",
        "size violation",
    ]

    PERMANENT_PATTERNS = [
        "insufficient",
        "margin",
        "buying power",
        "duplicate",
        "invalid contract",
        "not tradable",
        "market closed",
        "halted",
        "suspended",
    ]

    def __init__(self, max_retries: int = 3, retry_delay: float = 2.0):
        """
        Args:
            max_retries: Maximum retry attempts
            retry_delay: Base delay between retries (with backoff)
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def classify_rejection(self, reason: str, symbol: str, order_id: int) -> OrderRejection:
        """
        Classifies an order rejection.

        Args:
            reason: Rejection reason from IB
            symbol: Order symbol
            order_id: Order ID

        Returns:
            OrderRejection classification
        """
        reason_lower = reason.lower()

        is_retriable = any(pattern in reason_lower for pattern in self.RETRIABLE_PATTERNS)
        is_permanent = any(pattern in reason_lower for pattern in self.PERMANENT_PATTERNS)

        if is_permanent:
            is_retriable = False
            suggested_action = "Skip trade - permanent error"
        elif is_retriable:
            if "price" in reason_lower:
                suggested_action = "Retry with adjusted limit price"
            elif "size" in reason_lower or "exceeds" in reason_lower:
                suggested_action = "Retry with reduced quantity"
            else:
                suggested_action = "Retry after delay"
        else:
            is_retriable = False
            suggested_action = "Skip trade - unknown error"

        return OrderRejection(
            order_id=order_id,
            symbol=symbol,
            reason=reason,
            is_retriable=is_retriable,
            suggested_action=suggested_action,
        )

    def place_order_with_retry(
        self,
        ib: IB,
        contract: Contract,
        order: Order,
        symbol: str,
        on_rejection_callback: Optional[Callable[[OrderRejection], None]] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Places an order with retry on rejection.

        Args:
            ib: IB connection
            contract: Contract
            order: Order
            symbol: Symbol (for logging)
            on_rejection_callback: Optional callback on rejection

        Returns:
            (success: bool, error_message: Optional[str])
        """
        order_id = order.orderId

        for attempt in range(1, self.max_retries + 1):
            try:
                trade = ib.placeOrder(contract, order)
                ib.sleep(1.0)

                status = trade.orderStatus.status

                if status in ["Submitted", "PreSubmitted", "Filled"]:
                    if attempt > 1:
                        logger.info(
                            f"Order accepted after {attempt} attempts: "
                            f"{symbol} (Order {order_id})"
                        )
                    return True, None

                if status in ["Cancelled", "ApiCancelled", "Inactive"]:
                    rejection_reason = "Unknown"
                    if hasattr(trade, "log") and trade.log:
                        for log_entry in trade.log:
                            if hasattr(log_entry, "message"):
                                rejection_reason = log_entry.message
                                break

                    rejection = self.classify_rejection(
                        rejection_reason, symbol, order_id
                    )

                    logger.warning(
                        f"Order rejected (Attempt {attempt}/{self.max_retries}): "
                        f"{symbol} | Reason: {rejection_reason} | "
                        f"Retriable: {rejection.is_retriable}"
                    )

                    if on_rejection_callback:
                        on_rejection_callback(rejection)

                    if not rejection.is_retriable:
                        logger.error(
                            f"Permanent rejection: {symbol} | {rejection.suggested_action}"
                        )
                        return False, rejection.reason

                    if attempt >= self.max_retries:
                        logger.error(
                            f"Max retries exceeded: {symbol} | "
                            f"Giving up after {self.max_retries} attempts"
                        )
                        return False, f"Max retries: {rejection.reason}"

                    delay = self.retry_delay * (2 ** (attempt - 1))
                    logger.info(
                        f"Retrying {symbol} | "
                        f"Waiting {delay:.1f}s before attempt {attempt + 1}..."
                    )
                    time.sleep(delay)
                    continue

                logger.debug(
                    f"Order pending: {symbol} (Order {order_id}) | Status: {status}"
                )
                return True, None

            except Exception as e:
                logger.error(
                    f"Exception placing order {symbol} (Attempt {attempt}): "
                    f"{type(e).__name__}: {e}"
                )

                if attempt >= self.max_retries:
                    return False, str(e)

                delay = self.retry_delay * (2 ** (attempt - 1))
                time.sleep(delay)

        return False, "Max retries exceeded"


def get_order_rejection_reason(ib: IB, order_id: int) -> Optional[str]:
    """
    Gets rejection reason for an order ID.

    Args:
        ib: IB connection
        order_id: Order ID

    Returns:
        Rejection reason or None
    """
    for trade in ib.trades():
        if trade.order.orderId == order_id:
            if hasattr(trade, "log") and trade.log:
                for log_entry in trade.log:
                    if hasattr(log_entry, "message"):
                        msg = log_entry.message
                        if any(
                            keyword in msg.lower()
                            for keyword in ["reject", "cancel", "error", "invalid"]
                        ):
                            return msg
    return None
