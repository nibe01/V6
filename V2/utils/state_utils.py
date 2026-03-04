"""
State management with process-safe file locking.
Prevents race conditions between scanner and trader.
"""

from __future__ import annotations

import json
import os
import time
import tempfile
import shutil
from pathlib import Path
from typing import Any, Dict
from contextlib import contextmanager

from utils.logging_utils import get_logger

logger = get_logger(__name__)

# Platform-specific locking
try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False
    try:
        import msvcrt
        HAS_MSVCRT = True
    except ImportError:
        HAS_MSVCRT = False
        logger.warning("No file locking available (fcntl/msvcrt missing)")


class FileLockException(Exception):
    """Raised when file lock cannot be acquired."""


class CorruptedStateException(Exception):
    """Raised when state file is corrupted."""


@contextmanager
def file_lock(file_path: Path, timeout: float = 10.0, mode: str = "exclusive"):
    """
    Context manager for process-safe file locking.

    Args:
        file_path: Path to the file to lock
        timeout: Max wait time for lock acquisition in seconds
        mode: "exclusive" (write) or "shared" (read)

    Yields:
        File handle for the locked file

    Raises:
        FileLockException: If lock cannot be acquired

    Example:
        with file_lock(path, timeout=5.0) as f:
            data = json.load(f)
    """
    lock_path = file_path.parent / f".{file_path.name}.lock"
    lock_file = None
    lock_acquired = False

    try:
        # Create lock file
        lock_file = open(lock_path, "w", encoding="utf-8")

        start_time = time.time()
        lock_mode = (
            fcntl.LOCK_EX if mode == "exclusive" else fcntl.LOCK_SH
        ) if HAS_FCNTL else None

        # Try to acquire lock (with timeout)
        while time.time() - start_time < timeout:
            try:
                if HAS_FCNTL:
                    # Unix: fcntl
                    fcntl.flock(lock_file.fileno(), lock_mode | fcntl.LOCK_NB)
                    lock_acquired = True
                    break
                if HAS_MSVCRT:
                    # Windows: msvcrt (no shared mode)
                    msvcrt.locking(
                        lock_file.fileno(),
                        msvcrt.LK_NBLCK,
                        1,
                    )
                    lock_acquired = True
                    break
                # Fallback: no real locking available
                logger.warning("File locking not available, proceeding without lock")
                lock_acquired = True
                break
            except (IOError, OSError):
                # Lock not available yet, wait briefly
                time.sleep(0.1)

        if not lock_acquired:
            elapsed = time.time() - start_time
            raise FileLockException(
                f"Could not acquire lock for {file_path} after {elapsed:.1f}s"
            )

        logger.debug(f"Lock acquired for {file_path}")

        # Open the target file
        if not file_path.exists():
            file_path.touch()

        with open(file_path, "r+", encoding="utf-8") as f:
            yield f

    finally:
        # Unlock
        if lock_acquired and lock_file:
            try:
                if HAS_FCNTL:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                elif HAS_MSVCRT:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                logger.debug(f"Lock released for {file_path}")
            except Exception as e:
                logger.warning(f"Error releasing lock: {e}")

        # Close and delete lock file
        if lock_file:
            try:
                lock_file.close()
                if lock_path.exists():
                    lock_path.unlink()
            except Exception as e:
                logger.warning(f"Error cleaning up lock file: {e}")


def load_state(state_path: Path) -> Dict[str, Any]:
    """
    Load state file with process-safe locking and error handling.

    Args:
        state_path: Path to the state file

    Returns:
        Dict with state data (empty dict if file does not exist)

    Raises:
        CorruptedStateException: If file is corrupted and cannot be recovered
    """
    if not state_path.exists():
        logger.info(f"State file does not exist, creating new: {state_path}")
        return {}

    # Try main file, then backup if main is corrupt
    for attempt, path in enumerate([state_path, _get_backup_path(state_path)], 1):
        if not path.exists():
            continue

        try:
            with file_lock(path, timeout=10.0, mode="shared") as f:
                content = f.read()

                if not content or content.strip() == "":
                    logger.warning(f"State file is empty: {path}")
                    return {}

                data = json.loads(content)

                if not isinstance(data, dict):
                    raise ValueError(f"State is not a dict: {type(data)}")

                logger.debug(f"Loaded state from {path}: {len(data)} entries")
                return data

        except json.JSONDecodeError as e:
            logger.error(
                f"JSON decode error in {path}: {e} "
                f"(attempt {attempt}/2)"
            )
            if attempt == 2:
                # Both files are corrupt
                raise CorruptedStateException(
                    f"Both state file and backup are corrupted: {state_path}"
                ) from e
            # Try backup
            continue

        except FileLockException as e:
            logger.error(f"Could not acquire lock: {e}")
            raise

        except Exception as e:
            logger.error(
                f"Unexpected error loading state from {path}: "
                f"{type(e).__name__}: {e}"
            )
            if attempt == 2:
                raise
            continue

    logger.warning("No valid state file found, returning empty dict")
    return {}


def save_state(state_path: Path, data: Dict[str, Any]) -> bool:
    """
    Save state file with process-safe locking and atomic write.

    Uses "write to temp + atomic rename" to prevent corruption on crashes.

    Args:
        state_path: Path to the state file
        data: Data to save

    Returns:
        bool: True if saved successfully
    """
    if not isinstance(data, dict):
        logger.error(f"Cannot save non-dict state: {type(data)}")
        return False

    try:
        with file_lock(state_path, timeout=10.0, mode="exclusive"):
            # Create backup of current file (if exists)
            if state_path.exists():
                _create_backup(state_path)

            # Serialize to JSON
            json_data = json.dumps(data, indent=2, sort_keys=True)

            # Write to temp file (atomic write pattern)
            temp_fd, temp_path = tempfile.mkstemp(
                dir=state_path.parent,
                prefix=f".{state_path.name}.tmp",
                text=True,
            )

            try:
                with os.fdopen(temp_fd, "w", encoding="utf-8") as temp_file:
                    temp_file.write(json_data)
                    temp_file.flush()
                    os.fsync(temp_file.fileno())

                # Atomic rename (overwrites old file)
                temp_path_obj = Path(temp_path)
                temp_path_obj.replace(state_path)

                logger.debug(f"Saved state to {state_path}: {len(data)} entries")
                return True

            except Exception:
                # Cleanup temp file on error
                try:
                    Path(temp_path).unlink()
                except Exception:
                    pass
                raise

    except Exception as e:
        logger.error(
            f"Error saving state to {state_path}: "
            f"{type(e).__name__}: {e}"
        )
        return False


def _get_backup_path(state_path: Path) -> Path:
    """Return path to backup file."""
    return state_path.parent / f"{state_path.name}.backup"


def _create_backup(state_path: Path) -> None:
    """Create backup of the state file."""
    if not state_path.exists():
        return

    backup_path = _get_backup_path(state_path)

    try:
        shutil.copy2(state_path, backup_path)
        logger.debug(f"Created backup: {backup_path}")
    except Exception as e:
        logger.warning(f"Could not create backup: {e}")


def recover_from_backup(state_path: Path) -> bool:
    """
    Recover state file from backup.

    Args:
        state_path: Path to corrupted state file

    Returns:
        bool: True if recovery succeeded
    """
    backup_path = _get_backup_path(state_path)

    if not backup_path.exists():
        logger.error(f"No backup found for {state_path}")
        return False

    try:
        # Validate backup
        with open(backup_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("Backup is not a dict")

        # Restore
        shutil.copy2(backup_path, state_path)
        logger.info(f"Recovered state from backup: {state_path}")
        return True

    except Exception as e:
        logger.error(f"Backup recovery failed: {e}")
        return False


def validate_state_file(state_path: Path) -> tuple[bool, str]:
    """
    Validate state file without locking.

    Args:
        state_path: Path to the state file

    Returns:
        tuple: (is_valid: bool, error_message: str)
    """
    if not state_path.exists():
        return False, "File does not exist"

    try:
        with open(state_path, "r", encoding="utf-8") as f:
            content = f.read()

            if not content or content.strip() == "":
                return False, "File is empty"

            data = json.loads(content)

            if not isinstance(data, dict):
                return False, f"Content is not a dict: {type(data)}"

            return True, "Valid"

    except json.JSONDecodeError as e:
        return False, f"JSON decode error: {e}"

    except Exception as e:
        return False, f"Error: {type(e).__name__}: {e}"
