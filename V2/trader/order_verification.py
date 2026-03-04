"""
Order verification for IB bracket orders.
Ensures parent + TP + SL are submitted to IB.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from ib_insync import IB, Trade

from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class BracketOrderResult:
    """Result of bracket order verification."""

    success: bool
    parent_order_id: int
    parent_status: str
    tp_order_id: Optional[int] = None
    tp_status: Optional[str] = None
    sl_order_id: Optional[int] = None
    sl_status: Optional[str] = None
    error_message: Optional[str] = None
    verification_time: float = 0.0


def verify_bracket_order(
    ib: IB,
    parent_order_id: int,
    expected_tp_id: int,
    expected_sl_id: int,
    timeout: float = 10.0,
    check_interval: float = 0.5,
) -> BracketOrderResult:
    """
    Verify that a bracket order (parent + TP + SL) arrived at IB.

    Args:
        ib: IB connection instance.
        parent_order_id: Parent order ID.
        expected_tp_id: Expected TP order ID.
        expected_sl_id: Expected SL order ID.
        timeout: Max wait time in seconds.
        check_interval: Interval between checks.

    Returns:
        BracketOrderResult with status of all orders.
    """
    start_time = time.time()

    logger.info(
        "Verifying bracket order: Parent=%s, TP=%s, SL=%s",
        parent_order_id,
        expected_tp_id,
        expected_sl_id,
    )

    parent_trade: Optional[Trade] = None
    tp_trade: Optional[Trade] = None
    sl_trade: Optional[Trade] = None

    while (time.time() - start_time) < timeout:
        ib.sleep(check_interval)

        all_trades = ib.trades()

        parent_trade = next(
            (t for t in all_trades if t.order.orderId == parent_order_id),
            None,
        )
        tp_trade = next(
            (t for t in all_trades if t.order.orderId == expected_tp_id),
            None,
        )
        sl_trade = next(
            (t for t in all_trades if t.order.orderId == expected_sl_id),
            None,
        )

        if parent_trade and tp_trade and sl_trade:
            verification_time = time.time() - start_time
            result = BracketOrderResult(
                success=True,
                parent_order_id=parent_order_id,
                parent_status=parent_trade.orderStatus.status,
                tp_order_id=expected_tp_id,
                tp_status=tp_trade.orderStatus.status,
                sl_order_id=expected_sl_id,
                sl_status=sl_trade.orderStatus.status,
                verification_time=verification_time,
            )

            logger.info(
                "Bracket order verified in %.2fs: Parent=%s, TP=%s, SL=%s",
                verification_time,
                result.parent_status,
                result.tp_status,
                result.sl_status,
            )
            return result

    verification_time = time.time() - start_time

    error_parts = []
    if not parent_trade:
        error_parts.append(f"Parent {parent_order_id} missing")
    if not tp_trade:
        error_parts.append(f"TP {expected_tp_id} missing")
    if not sl_trade:
        error_parts.append(f"SL {expected_sl_id} missing")

    error_message = ", ".join(error_parts)

    result = BracketOrderResult(
        success=False,
        parent_order_id=parent_order_id,
        parent_status=parent_trade.orderStatus.status if parent_trade else "NOT_FOUND",
        tp_order_id=expected_tp_id if tp_trade else None,
        tp_status=tp_trade.orderStatus.status if tp_trade else "NOT_FOUND",
        sl_order_id=expected_sl_id if sl_trade else None,
        sl_status=sl_trade.orderStatus.status if sl_trade else "NOT_FOUND",
        error_message=error_message,
        verification_time=verification_time,
    )

    logger.error(
        "Bracket order verification failed after %.2fs: %s",
        verification_time,
        error_message,
    )
    return result


def cancel_bracket_order(
    ib: IB,
    parent_order_id: int,
    tp_order_id: Optional[int] = None,
    sl_order_id: Optional[int] = None,
) -> bool:
    """Cancel a bracket order (all components)."""
    logger.warning("Canceling bracket order: Parent=%s", parent_order_id)

    all_trades = ib.trades()
    cancelled_count = 0

    parent_trade = next(
        (t for t in all_trades if t.order.orderId == parent_order_id),
        None,
    )
    if parent_trade:
        ib.cancelOrder(parent_trade.order)
        logger.info("Cancelled Parent %s", parent_order_id)
        cancelled_count += 1

    if tp_order_id:
        tp_trade = next(
            (t for t in all_trades if t.order.orderId == tp_order_id),
            None,
        )
        if tp_trade:
            ib.cancelOrder(tp_trade.order)
            logger.info("Cancelled TP %s", tp_order_id)
            cancelled_count += 1

    if sl_order_id:
        sl_trade = next(
            (t for t in all_trades if t.order.orderId == sl_order_id),
            None,
        )
        if sl_trade:
            ib.cancelOrder(sl_trade.order)
            logger.info("Cancelled SL %s", sl_order_id)
            cancelled_count += 1

    ib.sleep(1)

    logger.warning("Bracket order cancel attempted: %s orders", cancelled_count)
    return cancelled_count > 0


def get_order_status_summary(ib: IB, order_id: int) -> Optional[str]:
    """Return current order status or None if not found."""
    all_trades = ib.trades()
    trade = next((t for t in all_trades if t.order.orderId == order_id), None)
    if trade:
        return trade.orderStatus.status
    return None
