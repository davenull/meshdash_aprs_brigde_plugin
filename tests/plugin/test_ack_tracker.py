import logging

from aprs_bridge.ack_tracker import AckTracker, MsgnoGenerator


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestMsgnoGenerator:
    def test_generates_zero_padded_three_digits(self):
        gen = MsgnoGenerator(start=1)
        assert gen.next() == "001"
        assert gen.next() == "002"

    def test_wraps_after_999(self):
        gen = MsgnoGenerator(start=999)
        assert gen.next() == "999"
        assert gen.next() == "001"


def _tracker(clock, sent_frames=None, retry_intervals=(30.0, 60.0, 120.0), max_attempts=4):
    sent_frames = sent_frames if sent_frames is not None else []

    def transport_send(data: bytes) -> bool:
        sent_frames.append(data)
        return True

    return AckTracker(
        transport_send=transport_send,
        logger=logging.getLogger("test.ack_tracker"),
        retry_intervals=retry_intervals,
        max_attempts=max_attempts,
        clock=clock,
    ), sent_frames


def test_ack_clears_pending_and_returns_true():
    clock = FakeClock()
    tracker, _sent = _tracker(clock)
    tracker.track("001", "WU2Z", b"frame-bytes")
    assert tracker.pending_count() == 1
    assert tracker.ack("001") is True
    assert tracker.pending_count() == 0


def test_ack_for_unknown_msgno_returns_false():
    clock = FakeClock()
    tracker, _sent = _tracker(clock)
    assert tracker.ack("999") is False


def test_poll_before_first_interval_does_not_retransmit():
    clock = FakeClock()
    tracker, sent = _tracker(clock)
    tracker.track("001", "WU2Z", b"frame")
    clock.advance(29)
    tracker.poll()
    assert sent == []


def test_poll_retransmits_after_first_interval():
    clock = FakeClock()
    tracker, sent = _tracker(clock)
    tracker.track("001", "WU2Z", b"frame")
    clock.advance(30)
    tracker.poll()
    assert sent == [b"frame"]


def test_retransmit_schedule_is_decaying():
    clock = FakeClock()
    tracker, sent = _tracker(clock, retry_intervals=(30.0, 60.0, 120.0), max_attempts=10)
    tracker.track("001", "WU2Z", b"frame")

    clock.advance(30)
    tracker.poll()
    assert len(sent) == 1  # 1st retry at t=30

    clock.advance(30)  # t=60, only 30s since last retry -- next interval is 60s
    tracker.poll()
    assert len(sent) == 1  # not due yet

    clock.advance(30)  # t=90, 60s since last retry -- due
    tracker.poll()
    assert len(sent) == 2  # 2nd retry at t=90

    clock.advance(120)  # t=210, 120s since last retry -- due (3rd interval)
    tracker.poll()
    assert len(sent) == 3


def test_gives_up_after_max_attempts():
    clock = FakeClock()
    tracker, sent = _tracker(clock, retry_intervals=(10.0,), max_attempts=3)
    tracker.track("001", "WU2Z", b"frame")  # attempt 1 (the initial send via track, not poll)

    clock.advance(10)
    tracker.poll()  # attempt 2
    assert len(sent) == 1
    assert tracker.pending_count() == 1

    clock.advance(10)
    tracker.poll()  # attempt 3
    assert len(sent) == 2
    assert tracker.pending_count() == 1

    clock.advance(10)
    tracker.poll()  # attempts exhausted (3 >= max_attempts=3) -- dropped, no further send
    assert len(sent) == 2
    assert tracker.pending_count() == 0


def test_ack_stops_further_retransmission():
    clock = FakeClock()
    tracker, sent = _tracker(clock, retry_intervals=(10.0,), max_attempts=5)
    tracker.track("001", "WU2Z", b"frame")

    clock.advance(10)
    tracker.poll()
    assert len(sent) == 1

    tracker.ack("001")

    clock.advance(100)
    tracker.poll()
    assert len(sent) == 1  # no more retries after ack


def test_multiple_pending_messages_tracked_independently():
    clock = FakeClock()
    tracker, sent = _tracker(clock, retry_intervals=(10.0,), max_attempts=5)
    tracker.track("001", "WU2Z", b"frame-1")
    tracker.track("002", "N0CALL", b"frame-2")

    tracker.ack("001")

    clock.advance(10)
    tracker.poll()
    assert sent == [b"frame-2"]
