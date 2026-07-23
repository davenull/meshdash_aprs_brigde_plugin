from __future__ import annotations

import time
from typing import Callable, Dict, Optional


class TokenBucket:
    """Standard token bucket: capacity tokens, refilled at rate
    tokens/second. available() and spend() are split so a caller can
    check multiple buckets before committing to spend from any of them
    (see RateLimiter.allow) -- avoids draining a shared bucket on an
    attempt that a *different* bucket was always going to reject. Not
    thread-safe by itself; each direction's bridge processes packets on
    a single thread, so no additional locking is needed here."""

    def __init__(
        self,
        rate_per_sec: float,
        capacity: float,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be > 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._rate = rate_per_sec
        self._capacity = capacity
        self._clock = clock or time.monotonic
        self._tokens = capacity
        self._last_refill = self._clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._last_refill)
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def available(self) -> bool:
        self._refill()
        return self._tokens >= 1.0

    def spend(self) -> bool:
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def allow(self) -> bool:
        """Convenience for standalone use: check-and-spend in one call."""
        return self.spend()


class RateLimiter:
    """Per-direction and per-callsign token buckets, per CLAUDE.md.
    A transmission must pass both its direction's shared bucket and its
    callsign's own bucket -- the shared bucket protects RF airtime /
    mesh channel load in aggregate, the per-callsign bucket stops one
    registered user from starving everyone else's share of it."""

    def __init__(
        self,
        direction_rate_per_sec: float,
        direction_capacity: float,
        per_callsign_rate_per_sec: float,
        per_callsign_capacity: float,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._per_callsign_rate = per_callsign_rate_per_sec
        self._per_callsign_capacity = per_callsign_capacity
        self._direction_bucket = TokenBucket(direction_rate_per_sec, direction_capacity, self._clock)
        self._callsign_buckets: Dict[str, TokenBucket] = {}

    def _bucket_for(self, callsign: str) -> TokenBucket:
        bucket = self._callsign_buckets.get(callsign)
        if bucket is None:
            bucket = TokenBucket(self._per_callsign_rate, self._per_callsign_capacity, self._clock)
            self._callsign_buckets[callsign] = bucket
        return bucket

    def allow(self, callsign: str) -> bool:
        callsign_bucket = self._bucket_for(callsign)
        # Check both before spending from either -- a callsign that's
        # over its own limit shouldn't also drain the shared bucket on
        # every rejected attempt.
        if not self._direction_bucket.available() or not callsign_bucket.available():
            return False
        self._direction_bucket.spend()
        callsign_bucket.spend()
        return True
