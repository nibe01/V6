"""
Retry wrappers for state operations.
Automatic retry on lock failures and corruption.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, Any, Callable

from utils.logging_utils import get_logger
from utils.state_utils import (
    load_state,
    save_state,
    recover_from_backup,
    FileLockException,
    CorruptedStateException,
)

logger = get_logger(__name__)


def load_state_with_retry(
    state_path: Path,
    max_retries: int = 3,
    retry_delay: float = 0.5,
) -> Dict[str, Any]:
    """
    Load state with automatic retry on errors.

    Args:
        state_path: Path to state file
        max_retries: Maximum retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        Dict with state data

    Raises:
        Exception: If all retries fail
    """
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            return load_state(state_path)

        except FileLockException as e:
            last_exception = e
            logger.warning(
                f"Lock acquisition failed (attempt {attempt}/{max_retries}): {e}"
            )
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)
                continue

        except CorruptedStateException as e:
            last_exception = e
            logger.error(f"State file corrupted: {e}")

            # Try recovery from backup
            if attempt == 1:
                logger.info("Attempting recovery from backup...")
                if recover_from_backup(state_path):
                    continue

            # Recovery failed
            logger.error("Recovery failed, returning empty state")
            return {}

        except Exception as e:
            last_exception = e
            logger.error(
                f"Unexpected error loading state "
                f"(attempt {attempt}/{max_retries}): "
                f"{type(e).__name__}: {e}"
            )
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)
                continue

    logger.error(f"Failed to load state after {max_retries} attempts")
    if last_exception:
        raise last_exception
    return {}


def save_state_with_retry(
    state_path: Path,
    data: Dict[str, Any],
    max_retries: int = 3,
    retry_delay: float = 0.5,
) -> bool:
    """
    Save state with automatic retry on errors.

    Args:
        state_path: Path to state file
        data: Data to save
        max_retries: Maximum retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        bool: True if saved successfully
    """
    for attempt in range(1, max_retries + 1):
        try:
            success = save_state(state_path, data)
            if success:
                return True

            logger.warning(
                f"Save state returned False "
                f"(attempt {attempt}/{max_retries})"
            )

            if attempt < max_retries:
                time.sleep(retry_delay * attempt)
                continue

        except FileLockException as e:
            logger.warning(
                f"Lock acquisition failed during save "
                f"(attempt {attempt}/{max_retries}): {e}"
            )
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)
                continue

        except Exception as e:
            logger.error(
                f"Unexpected error saving state "
                f"(attempt {attempt}/{max_retries}): "
                f"{type(e).__name__}: {e}"
            )
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)
                continue

    logger.error(f"Failed to save state after {max_retries} attempts")
    return False


def update_state_atomically(
    state_path: Path,
    update_func: Callable[[Dict[str, Any]], Dict[str, Any]],
    max_retries: int = 5,
) -> bool:
    """
    Atomic state update: Load -> Modify -> Save with retry.

    Args:
        state_path: Path to state file
        update_func: Function that modifies state
        max_retries: Maximum retry attempts

    Returns:
        bool: True if successful

    Example:
        def add_entry(state):
            state["new_key"] = "value"
            return state

        update_state_atomically(path, add_entry)
    """
    for attempt in range(1, max_retries + 1):
        try:
            state = load_state_with_retry(state_path, max_retries=2)

            updated_state = update_func(state)

            if save_state_with_retry(state_path, updated_state, max_retries=2):
                return True

            logger.warning(
                f"Atomic update save failed "
                f"(attempt {attempt}/{max_retries})"
            )
            time.sleep(0.5 * attempt)

        except Exception as e:
            logger.error(
                f"Error in atomic update "
                f"(attempt {attempt}/{max_retries}): "
                f"{type(e).__name__}: {e}"
            )
            if attempt < max_retries:
                time.sleep(0.5 * attempt)
                continue

    logger.error(f"Atomic update failed after {max_retries} attempts")
    return False
