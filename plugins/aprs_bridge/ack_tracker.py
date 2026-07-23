from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple


@dataclass
class _PendingAck:
    addressee: str
    kiss_frame: bytes
    node_id: str
    attempts: int
    next_retry_at: float


class MsgnoGenerator:
    """Generates 3-digit, zero-padded, wrapping message numbers
    ("001".."999"), fitting the APRS message-number field's 1-5 char
    limit with room to spare."""

    def __init__(self, start: int = 1) -> None:
        self._next = start

    def next(self) -> str:
        msgno = "%03d" % self._next
        self._next += 1
        if self._next > 999:
            self._next = 1
        return msgno


class AckTracker:
    """Tracks outbound APRS messages awaiting an ackNNN reply and
    retransmits them on a decaying schedule until acked or exhausted, per
    CLAUDE.md: "Retransmit un-ACKed outbound messages on a decaying
    schedule; stop on ACK." Not thread-safe by itself -- main.py only
    touches this from asyncio tasks on the single MeshDash event loop.

    on_acked and on_exhausted, if given, are called with (msgno,
    addressee, node_id) when the real APRS recipient acks a message, or
    when a message is given up on after max_attempts -- the mesh sender
    otherwise has no way to learn whether their message ever actually
    reached anyone. Kept as plain callbacks rather than importing
    connection_manager/event_loop here so this class stays a pure,
    MeshDash-independent scheduler; main.py wires the callbacks to an
    actual mesh DM."""

    def __init__(
        self,
        transport_send: Callable[[bytes], bool],
        logger: logging.Logger,
        retry_intervals: Tuple[float, ...] = (30.0, 60.0, 120.0),
        max_attempts: int = 4,
        clock: Optional[Callable[[], float]] = None,
        on_acked: Optional[Callable[[str, str, str], None]] = None,
        on_exhausted: Optional[Callable[[str, str, str], None]] = None,
    ) -> None:
        self._transport_send = transport_send
        self._logger = logger
        self._retry_intervals = retry_intervals
        self._max_attempts = max_attempts
        self._clock = clock or time.monotonic
        self._on_acked = on_acked
        self._on_exhausted = on_exhausted
        self._pending: Dict[str, _PendingAck] = {}

    def _interval_after(self, attempts: int) -> float:
        if not self._retry_intervals:
            return 60.0
        idx = min(attempts - 1, len(self._retry_intervals) - 1)
        return self._retry_intervals[idx]

    def track(self, msgno: str, addressee: str, kiss_frame: bytes, node_id: str) -> None:
        now = self._clock()
        self._pending[msgno] = _PendingAck(
            addressee=addressee,
            kiss_frame=kiss_frame,
            node_id=node_id,
            attempts=1,
            next_retry_at=now + self._interval_after(1),
        )

    def ack(self, msgno: str) -> bool:
        """Clears a pending message on receipt of its ack. Returns True
        if msgno was actually pending (False for a stray/duplicate/
        foreign ack we weren't waiting on)."""
        pending = self._pending.pop(msgno, None)
        if pending is None:
            return False
        if self._on_acked is not None:
            try:
                self._on_acked(msgno, pending.addressee, pending.node_id)
            except Exception:
                self._logger.exception("aprs_bridge: on_acked callback raised")
        return True

    def poll(self) -> None:
        """Resends due retries; drops entries that have exhausted
        max_attempts. Call periodically (e.g. every 10s)."""
        now = self._clock()
        exhausted = []
        for msgno, pending in self._pending.items():
            if pending.next_retry_at > now:
                continue
            if pending.attempts >= self._max_attempts:
                exhausted.append(msgno)
                continue
            if self._transport_send(pending.kiss_frame):
                self._logger.info(
                    "aprs_bridge: retransmitting unacked message %s to %s (attempt %d/%d)",
                    msgno, pending.addressee, pending.attempts + 1, self._max_attempts,
                )
            else:
                self._logger.warning(
                    "aprs_bridge: retransmit of message %s to %s failed to send",
                    msgno, pending.addressee,
                )
            pending.attempts += 1
            pending.next_retry_at = now + self._interval_after(pending.attempts)
        for msgno in exhausted:
            pending = self._pending.pop(msgno)
            self._logger.warning(
                "aprs_bridge: giving up on message %s to %s after %d attempts, never acked",
                msgno, pending.addressee, pending.attempts,
            )
            if self._on_exhausted is not None:
                try:
                    self._on_exhausted(msgno, pending.addressee, pending.node_id)
                except Exception:
                    self._logger.exception("aprs_bridge: on_exhausted callback raised")

    def pending_count(self) -> int:
        return len(self._pending)
