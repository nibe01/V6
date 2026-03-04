"""
IB API Rate Limiter
Prevents API bans by throttling requests to stay within IB's limits.

IB API Limits:
- 50 messages per second (aggregate)
- 100 messages per second burst (short duration)
- Historical Data: ~1680 requests in 10 minutes (measured for this account)

This implementation uses conservative limits (89-80% of measured maximums)
to provide a safety margin and prevent pacing violations.

Configuration:
- max_requests_per_second: 40 (80% of IB's 50/s limit)
- burst_limit: 80 (80% of IB's 100/s burst)
- historical_data_per_10min: 1500 (89% of measured 1680/10min)

Measured limits (from test_ib_limits.py):
- Account Type: Live Professional
- Sequential Test: 1723/10min
- Batch Parallel Test: 1663/10min
- Average: ~1680/10min
- Using: 1500/10min (89% safety margin)
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Optional, Callable, Any

from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    max_requests_per_second: int = 40
    burst_limit: int = 80
    historical_data_per_10min: int = 1500
    window_seconds: float = 1.0
    historical_window_seconds: float = 600.0


class RateLimiter:
    """
    Rate limiter for IB API calls.

    Features:
    - Tracks API calls per second
    - Enforces rate limits with automatic throttling
    - Separate tracking for historical data requests
    - Thread-safe for concurrent access
    """

    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()

        self._requests: deque[float] = deque()
        self._historical_requests: deque[float] = deque()

        self._lock = Lock()

        self.total_requests = 0
        self.throttled_count = 0
        self.total_wait_time = 0.0

        logger.info("Rate Limiter initialized:")
        logger.info(
            f"  Max requests/sec: {self.config.max_requests_per_second} (IB limit: 50/s)"
        )
        logger.info(f"  Burst limit: {self.config.burst_limit} (IB limit: 100/s)")
        logger.info(
            "  Historical data limit: "
            f"{self.config.historical_data_per_10min}/10min "
            "(Measured: ~1680/10min, Safety: 89%)"
        )
        logger.info("  Account Type: Live Professional (measured)")

    def _clean_old_requests(self, current_time: float) -> None:
        cutoff = current_time - self.config.window_seconds
        while self._requests and self._requests[0] < cutoff:
            self._requests.popleft()

        hist_cutoff = current_time - self.config.historical_window_seconds
        while self._historical_requests and self._historical_requests[0] < hist_cutoff:
            self._historical_requests.popleft()

    def _get_current_rate(self) -> int:
        current_time = time.time()
        self._clean_old_requests(current_time)
        return len(self._requests)

    def _get_historical_rate(self) -> int:
        current_time = time.time()
        self._clean_old_requests(current_time)
        return len(self._historical_requests)

    def _calculate_wait_time(self, is_historical: bool = False) -> float:
        current_rate = self._get_current_rate()
        historical_rate = self._get_historical_rate()

        if current_rate >= self.config.max_requests_per_second:
            if self._requests:
                oldest = self._requests[0]
                wait = (oldest + self.config.window_seconds) - time.time()
                return max(0.0, wait)

        if is_historical and historical_rate >= self.config.historical_data_per_10min:
            if self._historical_requests:
                oldest = self._historical_requests[0]
                wait = (oldest + self.config.historical_window_seconds) - time.time()
                return max(0.0, wait)

        return 0.0

    def wait_if_needed(
        self, request_type: str = "general", is_historical: bool = False
    ) -> float:
        with self._lock:
            wait_time = self._calculate_wait_time(is_historical)

            if wait_time > 0:
                self.throttled_count += 1
                self.total_wait_time += wait_time

                logger.warning(
                    "Rate limit approaching: "
                    f"waiting {wait_time:.2f}s before {request_type} request "
                    f"(current rate: {self._get_current_rate()}/s, "
                    f"historical: {self._get_historical_rate()}/10min)"
                )

                time.sleep(wait_time)

            current_time = time.time()
            self._requests.append(current_time)
            if is_historical:
                self._historical_requests.append(current_time)

            self.total_requests += 1

            return wait_time

    def execute_with_limit(
        self,
        func: Callable,
        request_type: str = "api_call",
        is_historical: bool = False,
        *args,
        **kwargs,
    ) -> Any:
        self.wait_if_needed(request_type, is_historical)
        return func(*args, **kwargs)

    def get_statistics(self) -> dict:
        with self._lock:
            current_rate = self._get_current_rate()
            historical_rate = self._get_historical_rate()

            return {
                "total_requests": self.total_requests,
                "throttled_count": self.throttled_count,
                "total_wait_time": self.total_wait_time,
                "current_rate": current_rate,
                "historical_rate": historical_rate,
                "avg_wait_time": (
                    self.total_wait_time / self.throttled_count
                    if self.throttled_count > 0
                    else 0.0
                ),
                "throttle_percentage": (
                    (self.throttled_count / self.total_requests * 100)
                    if self.total_requests > 0
                    else 0.0
                ),
            }

    def log_statistics(self) -> None:
        stats = self.get_statistics()

        logger.info("=" * 60)
        logger.info("RATE LIMITER STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total Requests:     {stats['total_requests']}")
        logger.info(
            "Throttled:          "
            f"{stats['throttled_count']} ({stats['throttle_percentage']:.1f}%)"
        )
        logger.info(f"Total Wait Time:    {stats['total_wait_time']:.2f}s")
        logger.info(f"Avg Wait Time:      {stats['avg_wait_time']:.3f}s")
        logger.info(f"Current Rate:       {stats['current_rate']} req/s")
        logger.info(f"Historical Rate:    {stats['historical_rate']} req/10min")
        logger.info("=" * 60)

    def reset_statistics(self) -> None:
        with self._lock:
            self.total_requests = 0
            self.throttled_count = 0
            self.total_wait_time = 0.0
            logger.info("Rate limiter statistics reset")


_global_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter(config: Optional[RateLimitConfig] = None) -> RateLimiter:
    global _global_rate_limiter

    if _global_rate_limiter is None:
        _global_rate_limiter = RateLimiter(config)

    return _global_rate_limiter


def reset_rate_limiter() -> None:
    global _global_rate_limiter
    _global_rate_limiter = None
