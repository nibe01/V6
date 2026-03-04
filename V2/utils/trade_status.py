from __future__ import annotations

from typing import Any


BOT_STATUS_SUBMITTED = "submitted"
BOT_STATUS_FILLED = "filled"
BOT_STATUS_CLOSED = "closed"
BOT_STATUS_MANUAL = "manual"
BOT_STATUS_MANUAL_CLOSED = "manual_closed"

BOT_ACTIVE_STATUSES = frozenset(
    {
        BOT_STATUS_SUBMITTED,
        BOT_STATUS_FILLED,
    }
)

BOT_OPEN_ORDER_STATUSES = frozenset(
    {
        BOT_STATUS_SUBMITTED,
        "presubmitted",
        "pendingsubmit",
        "pending_submit",
        "pending",
    }
)

IB_STATUS_FILLED = "Filled"
IB_PENDING_STATUSES = frozenset({"Submitted", "PreSubmitted", "PendingSubmit"})
IB_ACTIVE_ENTRY_STATUSES = frozenset({IB_STATUS_FILLED, *IB_PENDING_STATUSES})
IB_REJECTED_OR_CANCELLED_STATUSES = frozenset(
    {"Cancelled", "ApiCancelled", "Inactive"}
)


def normalize_status(status: Any) -> str:
    """Normalize unknown status value to a lowercase string."""
    return str(status).strip().lower()


def is_bot_active_status(status: Any) -> bool:
    return normalize_status(status) in BOT_ACTIVE_STATUSES


def is_bot_open_order_status(status: Any) -> bool:
    return normalize_status(status) in BOT_OPEN_ORDER_STATUSES


def is_bot_filled_status(status: Any) -> bool:
    return normalize_status(status) == BOT_STATUS_FILLED


def is_manual_status(status: Any) -> bool:
    return normalize_status(status) == BOT_STATUS_MANUAL


def is_ib_pending_status(status: Any) -> bool:
    return str(status).strip() in IB_PENDING_STATUSES


def is_ib_entry_active_status(status: Any) -> bool:
    return str(status).strip() in IB_ACTIVE_ENTRY_STATUSES


def is_ib_rejected_or_cancelled_status(status: Any) -> bool:
    return str(status).strip() in IB_REJECTED_OR_CANCELLED_STATUSES
