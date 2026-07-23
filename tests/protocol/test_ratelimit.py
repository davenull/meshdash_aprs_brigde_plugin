import pytest

from aprs_bridge.protocol.ratelimit import RateLimiter, TokenBucket


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestTokenBucket:
    def test_starts_full(self):
        bucket = TokenBucket(rate_per_sec=1, capacity=3, clock=FakeClock())
        assert bucket.allow() is True
        assert bucket.allow() is True
        assert bucket.allow() is True
        assert bucket.allow() is False

    def test_refills_over_time(self):
        clock = FakeClock()
        bucket = TokenBucket(rate_per_sec=1, capacity=1, clock=clock)
        assert bucket.allow() is True
        assert bucket.allow() is False
        clock.advance(1.0)
        assert bucket.allow() is True

    def test_does_not_exceed_capacity(self):
        clock = FakeClock()
        bucket = TokenBucket(rate_per_sec=1, capacity=2, clock=clock)
        clock.advance(100)  # would overfill without the capacity cap
        assert bucket.allow() is True
        assert bucket.allow() is True
        assert bucket.allow() is False

    def test_available_does_not_spend(self):
        clock = FakeClock()
        bucket = TokenBucket(rate_per_sec=1, capacity=1, clock=clock)
        assert bucket.available() is True
        assert bucket.available() is True  # still available, not spent
        assert bucket.spend() is True
        assert bucket.available() is False

    def test_rejects_non_positive_rate_or_capacity(self):
        with pytest.raises(ValueError):
            TokenBucket(rate_per_sec=0, capacity=1)
        with pytest.raises(ValueError):
            TokenBucket(rate_per_sec=1, capacity=0)


class TestRateLimiter:
    def test_allows_within_both_limits(self):
        limiter = RateLimiter(
            direction_rate_per_sec=10, direction_capacity=10,
            per_callsign_rate_per_sec=10, per_callsign_capacity=10,
            clock=FakeClock(),
        )
        assert limiter.allow("W4BRD-13") is True

    def test_per_callsign_limit_blocks_that_callsign_only(self):
        clock = FakeClock()
        limiter = RateLimiter(
            direction_rate_per_sec=100, direction_capacity=100,
            per_callsign_rate_per_sec=1, per_callsign_capacity=1,
            clock=clock,
        )
        assert limiter.allow("W4BRD-13") is True
        assert limiter.allow("W4BRD-13") is False  # this callsign exhausted
        assert limiter.allow("WU2Z") is True  # different callsign, own bucket

    def test_direction_limit_blocks_everyone(self):
        clock = FakeClock()
        limiter = RateLimiter(
            direction_rate_per_sec=1, direction_capacity=1,
            per_callsign_rate_per_sec=100, per_callsign_capacity=100,
            clock=clock,
        )
        assert limiter.allow("W4BRD-13") is True
        assert limiter.allow("WU2Z") is False  # shared bucket exhausted

    def test_rejected_callsign_attempt_does_not_drain_shared_bucket(self):
        # Regression guard: a callsign hammering past its own limit must
        # not also burn down the shared direction bucket on every
        # rejected attempt, or it could starve every other registered
        # user even though none of its own messages are getting through.
        clock = FakeClock()
        limiter = RateLimiter(
            direction_rate_per_sec=100, direction_capacity=2,
            per_callsign_rate_per_sec=1, per_callsign_capacity=1,
            clock=clock,
        )
        assert limiter.allow("W4BRD-13") is True  # spends 1 from each bucket
        for _ in range(10):
            assert limiter.allow("W4BRD-13") is False  # own bucket empty each time
        # Direction bucket should still have its second token untouched.
        assert limiter.allow("WU2Z") is True

    def test_refills_independently_per_callsign(self):
        clock = FakeClock()
        limiter = RateLimiter(
            direction_rate_per_sec=100, direction_capacity=100,
            per_callsign_rate_per_sec=1, per_callsign_capacity=1,
            clock=clock,
        )
        assert limiter.allow("W4BRD-13") is True
        assert limiter.allow("W4BRD-13") is False
        clock.advance(1.0)
        assert limiter.allow("W4BRD-13") is True
