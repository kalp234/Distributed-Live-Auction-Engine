"""[T2] Fault-tolerance primitives: circuit breaker + retry with exponential backoff.

CircuitBreaker
    CLOSED     normal; consecutive failures are counted
    OPEN       after `threshold` consecutive failures: calls are refused immediately (fail
               fast / route elsewhere) instead of each one waiting for a timeout
    HALF_OPEN  after `reset_timeout` one trial call is let through; success closes the
               breaker, failure re-opens it (with a doubled timeout, capped)
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Awaitable, Callable, TypeVar

log = logging.getLogger("resilience")
T = TypeVar("T")


class CircuitBreaker:
    def __init__(self, name: str, threshold: int = 2, reset_timeout: float = 2.0, max_timeout: float = 10.0) -> None:
        self.name, self.threshold = name, threshold
        self.base_timeout, self.max_timeout = reset_timeout, max_timeout
        self.timeout = reset_timeout
        self.failures = 0
        self.state = "closed"
        self.opened_at = 0.0
        self.trips = 0

    def allow(self) -> bool:
        if self.state == "open" and time.monotonic() - self.opened_at >= self.timeout:
            self.state = "half_open"          # let one trial request through
            return True
        return self.state != "open"

    def success(self) -> None:
        if self.state != "closed":
            log.warning("circuit %s CLOSED (recovered)", self.name)
        self.state, self.failures, self.timeout = "closed", 0, self.base_timeout

    def failure(self) -> None:
        self.failures += 1
        if self.state == "half_open":
            self.timeout = min(self.timeout * 2, self.max_timeout)
            self._open()
        elif self.state == "closed" and self.failures >= self.threshold:
            self._open()

    def _open(self) -> None:
        if self.state != "open":
            self.trips += 1
            log.warning("circuit %s OPEN for %.1fs after %d failure(s)", self.name, self.timeout, self.failures)
        self.state, self.opened_at = "open", time.monotonic()

    def snapshot(self) -> dict[str, Any]:
        return {"state": self.state, "failures": self.failures, "trips": self.trips}


async def retry(fn: Callable[[], Awaitable[T]], *, attempts: int, base_delay: float,
                retry_if: Callable[[BaseException], bool], what: str = "operation") -> T:
    """Run `fn`; on a retryable error wait base_delay·2^i (+ jitter) and try again."""
    for i in range(attempts):
        try:
            return await fn()
        except BaseException as e:
            if i == attempts - 1 or not retry_if(e):
                raise
            delay = base_delay * (2 ** i) * (0.8 + 0.4 * random.random())
            log.info("%s failed (%s: %s); retry %d/%d in %.2fs", what, type(e).__name__, e, i + 1, attempts - 1, delay)
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")
