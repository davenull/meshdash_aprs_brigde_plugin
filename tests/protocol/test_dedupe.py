from aprs_bridge.protocol.dedupe import DedupeCache


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_seen_false_for_unmarked_signature():
    cache = DedupeCache(ttl_seconds=30, clock=FakeClock())
    assert cache.seen(("a", "b", "c")) is False


def test_mark_then_seen_true():
    clock = FakeClock()
    cache = DedupeCache(ttl_seconds=30, clock=clock)
    cache.mark(("a", "b", "c"))
    assert cache.seen(("a", "b", "c")) is True


def test_signature_expires_after_ttl():
    clock = FakeClock()
    cache = DedupeCache(ttl_seconds=30, clock=clock)
    cache.mark(("a", "b", "c"))
    clock.advance(29.9)
    assert cache.seen(("a", "b", "c")) is True
    clock.advance(0.2)
    assert cache.seen(("a", "b", "c")) is False


def test_seen_or_mark_first_call_false_second_true():
    cache = DedupeCache(ttl_seconds=30, clock=FakeClock())
    assert cache.seen_or_mark(("x",)) is False
    assert cache.seen_or_mark(("x",)) is True


def test_seen_or_mark_different_signatures_independent():
    cache = DedupeCache(ttl_seconds=30, clock=FakeClock())
    assert cache.seen_or_mark(("x",)) is False
    assert cache.seen_or_mark(("y",)) is False
    assert cache.seen_or_mark(("x",)) is True
    assert cache.seen_or_mark(("y",)) is True


def test_expired_entries_are_purged_not_just_ignored():
    clock = FakeClock()
    cache = DedupeCache(ttl_seconds=10, clock=clock)
    cache.mark(("a",))
    clock.advance(11)
    cache.seen(("a",))  # triggers purge
    assert ("a",) not in cache._seen


def test_remarking_resets_ttl():
    clock = FakeClock()
    cache = DedupeCache(ttl_seconds=10, clock=clock)
    cache.mark(("a",))
    clock.advance(9)
    cache.mark(("a",))  # re-mark before expiry resets the clock
    clock.advance(9)
    assert cache.seen(("a",)) is True  # would have expired at t=10 without the re-mark
