import asyncio
import logging
import time
from types import SimpleNamespace

from aprs_bridge import registry
from aprs_bridge.ack_tracker import AckTracker
from aprs_bridge.bridge import RfToMeshBridge
from aprs_bridge.config import BridgeConfig
from aprs_bridge.protocol import ax25, aprs_message, kiss
from aprs_bridge.protocol.dedupe import DedupeCache
from aprs_bridge.protocol.ratelimit import RateLimiter


def _wait_until(predicate, timeout=5, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _make_config(**overrides):
    defaults = dict(
        tnc_mode="kiss_tcp",
        tnc_host="127.0.0.1",
        tnc_port=8001,
        kiss_port=0,
        gateway_callsign="W4BRD-13",
        aprs_tocall="APZBRD",
        digi_path=("WIDE1-1", "WIDE2-1"),
        mesh_channel_index=0,
        registry_db_path=":memory:",
        dedupe_ttl_sec=30.0,
        rate_limit_per_min=6000.0,  # effectively unlimited by default
        rate_limit_burst=1000.0,
        per_callsign_rate_limit_per_min=6000.0,
        per_callsign_rate_limit_burst=1000.0,
        ack_retry_intervals_sec=(30.0, 60.0, 120.0),
        ack_max_attempts=4,
    )
    defaults.update(overrides)
    return BridgeConfig(**defaults)


def _make_bridge(
    tmp_path,
    fake_connection_manager,
    running_event_loop,
    sent_rf_frames=None,
    mesh_nodes=None,
    **cfg_overrides,
):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    cfg = _make_config(registry_db_path=str(tmp_path / "reg.db"), **cfg_overrides)
    sent_rf_frames = sent_rf_frames if sent_rf_frames is not None else []
    meshtastic_data = SimpleNamespace(nodes=mesh_nodes or {}, local_node_id=None)

    def transport_send(data: bytes) -> bool:
        sent_rf_frames.append(data)
        return True

    dedupe = DedupeCache(ttl_seconds=cfg.dedupe_ttl_sec)
    ack_tracker = AckTracker(
        transport_send=transport_send,
        logger=logging.getLogger("test.bridge.ack"),
        retry_intervals=cfg.ack_retry_intervals_sec,
        max_attempts=cfg.ack_max_attempts,
    )
    rate_limiter = RateLimiter(
        direction_rate_per_sec=cfg.rate_limit_per_min / 60.0,
        direction_capacity=cfg.rate_limit_burst,
        per_callsign_rate_per_sec=cfg.per_callsign_rate_limit_per_min / 60.0,
        per_callsign_capacity=cfg.per_callsign_rate_limit_burst,
    )

    bridge = RfToMeshBridge(
        cfg=cfg,
        registry_conn=conn,
        connection_manager=fake_connection_manager,
        meshtastic_data=meshtastic_data,
        event_loop=running_event_loop,
        logger=logging.getLogger("test.bridge"),
        transport_send=transport_send,
        dedupe=dedupe,
        ack_tracker=ack_tracker,
        rate_limiter=rate_limiter,
    )
    return bridge, conn, sent_rf_frames, ack_tracker


def _build_rf_frame(addressee: str, text: str, msgno=None, source="N0CALL-10") -> bytes:
    info = aprs_message.encode_message(addressee, text, msgno)
    ax25_frame = ax25.build_ui_frame("APZ019", source, ["WIDE1-1", "WIDE2-1"], info)
    return ax25_frame


def test_message_delivered_to_every_device_registered_under_the_callsign(
    tmp_path, fake_connection_manager, running_event_loop
):
    # An operator running more than one mesh node can register all of
    # them under the same callsign. With no last_active_node record yet
    # (neither device has ever sent mesh->RF under this callsign), an
    # incoming RF message for that callsign falls back to reaching every
    # device.
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!11111111")
    registry.add_registration(conn, "W4BRD-13", "!22222222")

    frame = _build_rf_frame("W4BRD-13", "Testing", msgno="003")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 2)
    destinations = {s["destinationId"] for s in fake_connection_manager.sent}
    assert destinations == {"!11111111", "!22222222"}
    assert all(s["text"] == "N0CALL-10: Testing" for s in fake_connection_manager.sent)
    # Still exactly one ack, not one per device -- it's one RF event.
    assert _wait_until(lambda: len(sent_rf_frames) == 1)


def test_message_routed_only_to_last_active_device_when_multiple_registered(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!11111111")
    registry.add_registration(conn, "W4BRD-13", "!22222222")
    registry.set_last_active_node(conn, "W4BRD-13", "!22222222")

    frame = _build_rf_frame("W4BRD-13", "Testing", msgno="003")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert fake_connection_manager.sent[0]["destinationId"] == "!22222222"


def test_message_falls_back_to_fan_out_when_last_active_device_unregistered(
    tmp_path, fake_connection_manager, running_event_loop
):
    # last_active_node pointed at a device that has since been removed
    # from the registry -- don't drop the message, fall back to every
    # remaining registered device instead.
    bridge, conn, _sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!11111111")
    registry.add_registration(conn, "W4BRD-13", "!22222222")
    registry.set_last_active_node(conn, "W4BRD-13", "!33333333")  # no longer registered

    frame = _build_rf_frame("W4BRD-13", "Testing", msgno="003")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 2)
    destinations = {s["destinationId"] for s in fake_connection_manager.sent}
    assert destinations == {"!11111111", "!22222222"}


def test_message_to_registered_callsign_is_delivered_to_mesh(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "Testing", msgno="003")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    sent = fake_connection_manager.sent[0]
    assert sent["destinationId"] == "!aabbccdd"
    assert sent["text"] == "N0CALL-10: Testing"
    assert sent["channelIndex"] == 0


def test_message_to_unregistered_callsign_is_dropped(
    tmp_path, fake_connection_manager, running_event_loop
):
    # Addressee matches neither a registered callsign nor (with no mesh
    # nodes configured here) any live node's short name -- dropped either way.
    bridge, _conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)

    frame = _build_rf_frame("NOBODY", "hi there")
    bridge.on_ax25_frame(frame)

    time.sleep(0.2)  # give the (nonexistent) delivery a moment to have fired if it were going to
    assert fake_connection_manager.sent == []
    assert sent_rf_frames == []


def test_message_with_msgno_triggers_rf_ack(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "Testing", msgno="003", source="N0CALL-10")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    ack_kiss_frame = sent_rf_frames[0]

    port, command, ax25_bytes = kiss.decode_frame(ack_kiss_frame)
    assert (port, command) == (0, 0)
    parsed = ax25.parse_ui_frame(ax25_bytes)
    assert parsed.source == "W4BRD-13"
    assert parsed.destination == "APZBRD"
    ack_message = aprs_message.decode_message(parsed.info)
    assert ack_message.addressee == "N0CALL-10"
    assert ack_message.text == "ack003"


def test_message_without_msgno_does_not_trigger_ack(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "no ack here")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    time.sleep(0.1)
    assert sent_rf_frames == []


def test_delivery_skipped_when_connection_manager_not_ready(
    tmp_path, running_event_loop
):
    from tests.conftest import FakeConnectionManager

    not_ready_cm = FakeConnectionManager(ready=False)
    bridge, conn, _, _ack_tracker = _make_bridge(tmp_path, not_ready_cm, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "hello")
    bridge.on_ax25_frame(frame)

    time.sleep(0.2)
    assert not_ready_cm.sent == []


def test_non_message_ax25_frame_is_ignored(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    # A position report, not a message -- info field doesn't start with ':'.
    ax25_frame = ax25.build_ui_frame(
        "APZ019", "N0CALL-10", ["WIDE1-1", "WIDE2-1"], b"!4903.50N/07201.75W-Test"
    )
    bridge.on_ax25_frame(ax25_frame)

    time.sleep(0.2)
    assert fake_connection_manager.sent == []
    assert sent_rf_frames == []


def test_malformed_ax25_bytes_do_not_raise(tmp_path, fake_connection_manager, running_event_loop):
    bridge, _conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_ax25_frame(b"\x00\x01\x02not a real ax25 frame")  # must not raise


def test_duplicate_rf_frame_delivered_only_once(tmp_path, fake_connection_manager, running_event_loop):
    # Two byte-identical KISS/AX.25 frames, as if the same over-the-air
    # transmission were decoded twice (e.g. a multi-demodulator TNC).
    bridge, conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "Testing", msgno="003")
    bridge.on_ax25_frame(frame)
    bridge.on_ax25_frame(frame)  # identical frame, as if heard twice

    time.sleep(0.2)
    assert len(fake_connection_manager.sent) == 1


def test_digipeated_repeat_with_different_path_delivered_only_once(
    tmp_path, fake_connection_manager, running_event_loop
):
    # Regression guard for the actual behavior observed live: the same
    # message arrived twice via the user's own W4BRD-1 digipeater -- a
    # direct copy and a digipeated repeat, which are two genuinely
    # different AX.25 frames (different path) carrying identical APRS
    # message content. Dedup is content-based (source, addressee, text,
    # msgno), not path-based, precisely so this case is still caught.
    bridge, conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    info = aprs_message.encode_message("WU2Z", "Testing", "003")
    direct_frame = ax25.build_ui_frame("APZ019", "N0CALL-10", ["WIDE1-1", "WIDE2-1"], info)
    digipeated_frame = ax25.build_ui_frame(
        "APZ019", "N0CALL-10", ["W4BRD-1*", "WIDE2-1"], info
    )
    assert direct_frame != digipeated_frame  # genuinely different frames

    bridge.on_ax25_frame(direct_frame)
    bridge.on_ax25_frame(digipeated_frame)

    time.sleep(0.2)
    assert len(fake_connection_manager.sent) == 1


def test_duplicate_message_is_reacked_not_redelivered(
    tmp_path, fake_connection_manager, running_event_loop
):
    # If our first ack transmission is lost (RF collision, etc.), the
    # sender's APRS client retries the message. Without re-acking that
    # retry, the sender would retry forever -- this is the exact
    # question this test answers "yes" to: does duplicate/retried mail
    # still get acked so the sender's retry loop terminates.
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "Testing", msgno="003", source="N0CALL-10")
    bridge.on_ax25_frame(frame)
    assert _wait_until(lambda: len(sent_rf_frames) == 1)  # first ack

    bridge.on_ax25_frame(frame)  # sender retries, identical frame
    assert _wait_until(lambda: len(sent_rf_frames) == 2)  # re-acked

    # Still only delivered to mesh once.
    time.sleep(0.1)
    assert len(fake_connection_manager.sent) == 1

    for ack_kiss_frame in sent_rf_frames:
        _port, _cmd, ax25_bytes = kiss.decode_frame(ack_kiss_frame)
        parsed = ax25.parse_ui_frame(ax25_bytes)
        ack_message = aprs_message.decode_message(parsed.info)
        assert ack_message.addressee == "N0CALL-10"
        assert ack_message.text == "ack003"


def test_message_without_msgno_is_not_reacked_on_retry(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "no msgno here")  # msgno=None
    bridge.on_ax25_frame(frame)
    bridge.on_ax25_frame(frame)

    time.sleep(0.2)
    assert len(fake_connection_manager.sent) == 1
    assert sent_rf_frames == []  # nothing to ack -- no msgno was ever present


def test_rate_limited_message_is_not_marked_seen_and_can_succeed_on_retry(
    tmp_path, fake_connection_manager, running_event_loop
):
    # A rate-limited (undelivered, unacked) attempt must not be mistaken
    # for an already-delivered duplicate -- otherwise a legitimate retry
    # would get silently "re-acked" for a message that was never
    # actually delivered, or never get another chance to go through.
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path,
        fake_connection_manager,
        running_event_loop,
        per_callsign_rate_limit_per_min=60.0,
        per_callsign_rate_limit_burst=1.0,
    )
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    bridge.on_ax25_frame(_build_rf_frame("WU2Z", "first", msgno="001", source="N0CALL-1"))
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert _wait_until(lambda: len(sent_rf_frames) == 1)  # ack for the first, successful message

    # Second, different message from a different sender: rate-limited
    # (bucket capacity is 1), so must not be delivered or acked.
    bridge.on_ax25_frame(_build_rf_frame("WU2Z", "second", msgno="002", source="N0CALL-2"))
    time.sleep(0.2)
    assert len(fake_connection_manager.sent) == 1
    assert len(sent_rf_frames) == 1  # no ack added for the rate-limited attempt


def test_ack_addressed_to_gateway_clears_ack_tracker(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _sent, ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    ack_tracker.track("005", "WU2Z", b"some-kiss-frame-bytes", "!node0001")
    assert ack_tracker.pending_count() == 1

    ack_info = aprs_message.build_ack("W4BRD-13", "005")
    ack_frame = ax25.build_ui_frame("APZ019", "WU2Z", ["WIDE1-1", "WIDE2-1"], ack_info)
    bridge.on_ax25_frame(ack_frame)

    assert ack_tracker.pending_count() == 0
    # An ack addressed to us is never mesh-delivered or treated as a
    # registration lookup -- it's purely ack-tracker bookkeeping.
    time.sleep(0.2)
    assert fake_connection_manager.sent == []


def test_message_addressed_to_gateway_callsign_still_delivers_if_registered(
    tmp_path, fake_connection_manager, running_event_loop
):
    # Regression guard: nothing stops an operator from registering their
    # own mesh node under the same callsign as the gateway itself (real
    # setup we hit live). A genuine, non-ack message addressed to that
    # callsign must still be delivered to mesh -- it must not be
    # silently swallowed just because message.addressee happens to equal
    # cfg.gateway_callsign.
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")  # same callsign as gateway_callsign

    frame = _build_rf_frame("W4BRD-13", "Testing", msgno="003", source="N0CALL-10")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    sent = fake_connection_manager.sent[0]
    assert sent["destinationId"] == "!aabbccdd"
    assert sent["text"] == "N0CALL-10: Testing"
    assert _wait_until(lambda: len(sent_rf_frames) == 1)  # still gets a normal ack


def test_message_to_gateway_callsign_routes_by_conversation_history(
    tmp_path, fake_connection_manager, running_event_loop
):
    # A reply from an RF correspondent is always addressed to the
    # gateway's own callsign (mesh->RF frames never carry the individual
    # sender's callsign as source), so conversation history -- not
    # registration -- is what routes it back to the right mesh node.
    bridge, conn, _sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.set_conversation_node(conn, "N0CALL-10", "!node0001")

    frame = _build_rf_frame("W4BRD-13", "hi there", msgno="900", source="N0CALL-10")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    sent = fake_connection_manager.sent[0]
    assert sent["destinationId"] == "!node0001"
    assert sent["text"] == "N0CALL-10: hi there"


def test_conversation_history_takes_priority_over_coincidental_registration(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")  # coincidental registration
    registry.set_conversation_node(conn, "N0CALL-10", "!node0001")  # actual conversation

    frame = _build_rf_frame("W4BRD-13", "hi there", msgno="900", source="N0CALL-10")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert fake_connection_manager.sent[0]["destinationId"] == "!node0001"


def test_ack_for_unknown_msgno_does_not_raise_or_deliver(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, _conn, _sent, ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    ack_info = aprs_message.build_ack("W4BRD-13", "999")
    ack_frame = ax25.build_ui_frame("APZ019", "WU2Z", ["WIDE1-1", "WIDE2-1"], ack_info)
    bridge.on_ax25_frame(ack_frame)  # must not raise
    time.sleep(0.1)
    assert fake_connection_manager.sent == []


def test_rate_limit_exceeded_drops_delivery(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, _sent, _ack_tracker = _make_bridge(
        tmp_path,
        fake_connection_manager,
        running_event_loop,
        rate_limit_per_min=6000.0,
        rate_limit_burst=1000.0,
        per_callsign_rate_limit_per_min=60.0,
        per_callsign_rate_limit_burst=1.0,  # exactly one message allowed
    )
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    bridge.on_ax25_frame(_build_rf_frame("WU2Z", "first", source="N0CALL-1"))
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)

    bridge.on_ax25_frame(_build_rf_frame("WU2Z", "second", source="N0CALL-2"))
    time.sleep(0.2)
    assert len(fake_connection_manager.sent) == 1  # second delivery rate-limited


def test_message_reaches_mesh_node_by_short_name_when_not_a_registered_callsign(
    tmp_path, fake_connection_manager, running_event_loop
):
    # Third-party relay model: an RF sender can reach an unlicensed mesh
    # user directly by their mesh short name, not just a registered
    # callsign -- the gateway callsign is unaffected either way.
    bridge, _conn, _sent, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop,
        mesh_nodes={"!aabbccdd": {"user": {"shortName": "PGR"}}},
    )

    frame = _build_rf_frame("PGR", "hello there")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    sent = fake_connection_manager.sent[0]
    assert sent["destinationId"] == "!aabbccdd"
    assert sent["text"] == "N0CALL-10: hello there"


def test_registered_callsign_takes_priority_over_short_name_match(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _sent, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop,
        mesh_nodes={"!zzzzzzzz": {"user": {"shortName": "WU2Z"}}},  # coincidental name collision
    )
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "hello")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert fake_connection_manager.sent[0]["destinationId"] == "!aabbccdd"  # registered node, not the name match


def test_message_reaches_mesh_node_by_node_id_code_when_unnamed(
    tmp_path, fake_connection_manager, running_event_loop
):
    # A node with no short name set (or one an RF sender can't type --
    # Meshtastic short names can be arbitrary unicode) can still be
    # reached by the last 4 hex chars of its node id, the same fallback
    # code shown as mesh->RF attribution for unnamed nodes.
    bridge, _conn, _sent, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop,
        mesh_nodes={"!aabbccdd": {}},
    )

    frame = _build_rf_frame("CCDD", "hello there")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    sent = fake_connection_manager.sent[0]
    assert sent["destinationId"] == "!aabbccdd"
    assert sent["text"] == "N0CALL-10: hello there"


def test_short_name_match_takes_priority_over_node_id_code(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, _conn, _sent, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop,
        mesh_nodes={
            "!aabbccdd": {},  # code CCDD
            "!11ccdd22": {"user": {"shortName": "CCDD"}},  # name happens to equal that code
        },
    )

    frame = _build_rf_frame("CCDD", "hello there")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert fake_connection_manager.sent[0]["destinationId"] == "!11ccdd22"  # short-name match wins


