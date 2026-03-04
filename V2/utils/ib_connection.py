"""
IB Connection Manager mit automatischer Reconnect-Logik.
Robuste Verbindungsverwaltung fuer Interactive Brokers TWS/Gateway.
"""

from __future__ import annotations

import time
from typing import Optional, Callable
from dataclasses import dataclass

from ib_insync import IB

from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class ConnectionConfig:
    """Konfiguration fuer IB-Verbindung."""
    host: str
    port: int
    client_id: int
    max_retries: int = 10
    initial_retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    backoff_multiplier: float = 2.0
    connection_timeout: float = 10.0


class IBConnectionManager:
    """
    Verwaltet IB-Verbindung mit automatischer Wiederverbindung.

    Features:
    - Exponential Backoff bei Fehlern
    - Connection Health Checks
    - Automatische Reconnects
    - Fehlertoleranz
    """

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.ib: Optional[IB] = None
        self.is_connected = False
        self.connection_attempts = 0
        self.last_connection_time = 0.0
        self.current_retry_delay = self.config.initial_retry_delay

    def connect(self) -> IB:
        """
        Stellt Verbindung zu TWS/Gateway her mit Retry-Logik.

        Returns:
            IB: Verbundenes IB-Objekt

        Raises:
            ConnectionError: Wenn alle Versuche fehlschlagen
        """
        retry_delay = self.current_retry_delay

        for attempt in range(1, self.config.max_retries + 1):
            try:
                logger.debug(
                    f"Connecting to IB (Attempt {attempt}/{self.config.max_retries}): "
                    f"{self.config.host}:{self.config.port} "
                    f"clientId={self.config.client_id}"
                )

                # Erstelle neue IB-Instanz
                self.ib = IB()

                # Verbinde mit Timeout
                self.ib.connect(
                    host=self.config.host,
                    port=self.config.port,
                    clientId=self.config.client_id,
                    timeout=self.config.connection_timeout,
                )

                # Pruefe ob Verbindung wirklich steht
                if not self.ib.isConnected():
                    raise ConnectionError("Connection established but not active")

                self.is_connected = True
                self.connection_attempts = attempt
                self.last_connection_time = time.time()
                self.current_retry_delay = self.config.initial_retry_delay

                logger.info(
                    f"✅ Successfully connected to IB "
                    f"(took {attempt} attempt(s))"
                )

                return self.ib

            except Exception as e:
                logger.warning(
                    f"Connection attempt {attempt}/{self.config.max_retries} failed: "
                    f"{type(e).__name__}: {e}"
                )

                # Cleanup
                if self.ib:
                    try:
                        self.ib.disconnect()
                    except Exception:
                        pass
                    self.ib = None

                self.is_connected = False

                # Letzter Versuch?
                if attempt >= self.config.max_retries:
                    logger.error(
                        f"❌ Failed to connect to IB after {self.config.max_retries} attempts"
                    )
                    raise ConnectionError(
                        f"Could not connect to IB after {self.config.max_retries} attempts"
                    ) from e

                # Warte mit Exponential Backoff
                logger.info(f"Retrying in {retry_delay:.1f} seconds...")
                time.sleep(retry_delay)

                # Erhoehe Delay fuer naechsten Versuch
                retry_delay = min(
                    retry_delay * self.config.backoff_multiplier,
                    self.config.max_retry_delay,
                )
                self.current_retry_delay = retry_delay

        raise ConnectionError("Max retries exceeded")

    def check_connection(self) -> bool:
        """
        Prueft ob Verbindung noch aktiv ist.

        Returns:
            bool: True wenn verbunden
        """
        if not self.ib:
            return False

        try:
            return self.ib.isConnected()
        except Exception:
            return False

    def reconnect(self) -> IB:
        """
        Trennt und verbindet neu.

        Returns:
            IB: Neu verbundenes IB-Objekt
        """
        logger.warning("Reconnecting to IB...")

        # Alte Verbindung trennen
        self.disconnect()

        # Neu verbinden
        return self.connect()

    def disconnect(self) -> None:
        """Trennt Verbindung sauber."""
        if self.ib:
            try:
                if self.ib.isConnected():
                    self.ib.disconnect()
                    logger.info("Disconnected from IB")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self.ib = None
                self.is_connected = False

    def ensure_connected(self) -> IB:
        """
        Stellt sicher dass Verbindung besteht, reconnected falls noetig.

        Returns:
            IB: Verbundenes IB-Objekt
        """
        if not self.check_connection():
            logger.warning("Connection lost, attempting reconnect...")
            return self.reconnect()
        return self.ib


def with_connection_retry(func: Callable) -> Callable:
    """
    Decorator fuer automatische Reconnects bei Connection-Errors.

    Usage:
        @with_connection_retry
        def my_function(ib, ...):
            # Code der IB-Verbindung benoetigt
    """
    def wrapper(*args, **kwargs):
        max_retries = 3
        retry_delay = 2.0

        for attempt in range(1, max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Pruefe ob es ein Connection-Error ist
                error_msg = str(e).lower()
                is_connection_error = any(
                    keyword in error_msg
                    for keyword in ["connection", "timeout", "disconnect", "socket"]
                )

                if not is_connection_error or attempt >= max_retries:
                    raise

                logger.warning(
                    f"Connection error in {func.__name__}, "
                    f"retry {attempt}/{max_retries}: {e}"
                )
                time.sleep(retry_delay)
                retry_delay *= 2

        raise ConnectionError(f"Max retries exceeded in {func.__name__}")

    return wrapper
