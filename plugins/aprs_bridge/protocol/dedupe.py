from __future__ import annotations

import time
from typing import Callable, Hashable, Optional


class DedupeCache:
    """TTL-based signature cache for suppressing duplicate traffic.

    Two things this exists for, per CLAUDE.md:
    - Loop/dupe suppression: a TNC with multiple demodulators (or a
      digipeated retransmission) commonly delivers the same over-the-air
      packet more than once. Confirmed live: Direwolf's diversity
      reception logged the identical inbound APRS message twice with the
      same timestamp.
    - Self-origination guard: mark() our own outbound frames' signatures
      so that if we hear them again (TNC TX echo, digipeat loopback),
      seen() reports them as already-seen instead of re-gating our own
      traffic back out the other direction.

    Not thread-safe by itself; callers sharing one instance across
    threads (the RF RX thread and the mesh pubsub callback both touch the
    same cache) must serialize access -- see the lock in the bridge
    classes that own a DedupeCache.
    """

    def __init__(self, ttl_seconds: float = 30.0, clock: Optional[Callable[[], float]] = None) -> None:
        self._ttl = ttl_seconds
        self._clock = clock or time.monotonic
        self._seen: dict = {}

    def _purge_expired(self, now: float) -> None:
        expired = [sig for sig, expires_at in self._seen.items() if expires_at <= now]
        for sig in expired:
            del self._seen[sig]

    def seen(self, signature: Hashable) -> bool:
        """Returns True if signature was already marked and hasn't
        expired. Does not itself mark the signature -- callers that want
        check-and-mark-atomically should use seen_or_mark()."""
        now = self._clock()
        self._purge_expired(now)
        expires_at = self._seen.get(signature)
        return expires_at is not None and expires_at > now

    def mark(self, signature: Hashable) -> None:
        self._seen[signature] = self._clock() + self._ttl

    def seen_or_mark(self, signature: Hashable) -> bool:
        """Atomically check-and-mark: returns True if signature was
        already seen (caller should treat this as a duplicate and skip
        it), otherwise marks it as seen now and returns False."""
        if self.seen(signature):
            return True
        self.mark(signature)
        return False
