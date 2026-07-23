import logging
import time
from types import SimpleNamespace

from aprs_bridge import registry
from aprs_bridge.ack_tracker import AckTracker, MsgnoGenerator
from aprs_bridge.config import BridgeConfig
from aprs_bridge.mesh_bridge import MeshToRfBridge
from aprs_bridge.protocol import ax25, aprs_message, kiss
from aprs_bridge.protocol.dedupe import DedupeCache
from aprs_bridge.protocol.ratelimit import RateLimiter

LOCAL_ID = "!local0001"


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


def _make_bridge(tmp_path, fake_connection_manager, running_event_loop, **cfg_overrides):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    cfg = _make_config(registry_db_path=str(tmp_path / "reg.db"), **cfg_overrides)
    sent_rf_frames = []

    def transport_send(data: bytes) -> bool:
        sent_rf_frames.append(data)
        return True

    meshtastic_data = SimpleNamespace(local_node_id=LOCAL_ID)
    dedupe = DedupeCache(ttl_seconds=cfg.dedupe_ttl_sec)
    ack_tracker = AckTracker(
        transport_send=transport_send,
        logger=logging.getLogger("test.mesh_bridge.ack"),
        retry_intervals=cfg.ack_retry_intervals_sec,
        max_attempts=cfg.ack_max_attempts,
    )
    rate_limiter = RateLimiter(
        direction_rate_per_sec=cfg.rate_limit_per_min / 60.0,
        direction_capacity=cfg.rate_limit_burst,
        per_callsign_rate_per_sec=cfg.per_callsign_rate_limit_per_min / 60.0,
        per_callsign_capacity=cfg.per_callsign_rate_limit_burst,
    )

    bridge = MeshToRfBridge(
        cfg=cfg,
        registry_conn=conn,
        connection_manager=fake_connection_manager,
        meshtastic_data=meshtastic_data,
        event_loop=running_event_loop,
        logger=logging.getLogger("test.mesh_bridge"),
        transport_send=transport_send,
        dedupe=dedupe,
        ack_tracker=ack_tracker,
        rate_limiter=rate_limiter,
        msgno_generator=MsgnoGenerator(),
    )
    return bridge, conn, sent_rf_frames, ack_tracker


def _dm_packet(from_id: str, text: str, channel: int = 0, to_id: str = LOCAL_ID) -> dict:
    # channel is included because real packets carry it, but the bridge no
    # longer inspects it for DMs -- confirmed on real hardware that
    # Meshtastic DMs don't carry usable channel-encryption metadata (see
    # mesh_bridge.py's docstring and CLAUDE.md), so it can't gate anything.
    return {
        "fromId": from_id,
        "toId": to_id,
        "channel": channel,
        "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": text},
    }


def _decode_last_rf_frame(sent_rf_frames):
    assert len(sent_rf_frames) == 1
    _port, _cmd, ax25_bytes = kiss.decode_frame(sent_rf_frames[0])
    parsed = ax25.parse_ui_frame(ax25_bytes)
    return parsed, aprs_message.decode_message(parsed.info)


def test_register_command_creates_registration_and_replies(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)

    bridge.on_mesh_packet(_dm_packet("!node0001", "!register W4BRD-13", channel=0))

    assert registry.lookup_callsign_for_node(conn, "!node0001") == "W4BRD-13"
    assert sent_rf_frames == []  # registration never touches RF
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Registered W4BRD-13" in fake_connection_manager.sent[0]["text"]
    assert fake_connection_manager.sent[0]["destinationId"] == "!node0001"


def test_register_command_ignores_channel_field(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_mesh_packet(_dm_packet("!node0001", "!register W4BRD-13", channel=7))
    assert registry.lookup_callsign_for_node(conn, "!node0001") == "W4BRD-13"


def test_register_command_malformed_replies_with_error(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_mesh_packet(_dm_packet("!node0001", "!register not-valid-at-all", channel=0))

    assert registry.lookup_callsign_for_node(conn, "!node0001") is None
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Register failed" in fake_connection_manager.sent[0]["text"]


def test_unregister_command_removes_registration(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "!unregister", channel=0))

    assert registry.lookup_callsign_for_node(conn, "!node0001") is None
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Unregistered W4BRD-13" in fake_connection_manager.sent[0]["text"]


def test_unregister_command_when_not_registered(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, _conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_mesh_packet(_dm_packet("!node0001", "!unregister", channel=0))
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "no active registration" in fake_connection_manager.sent[0]["text"]


def test_unregistered_sender_cannot_reach_rf(tmp_path, fake_connection_manager, running_event_loop):
    bridge, _conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2))

    time.sleep(0.2)
    assert sent_rf_frames == []
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Not registered" in fake_connection_manager.sent[0]["text"]


def test_registered_sender_with_explicit_addressee_reaches_rf(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: Testing 123", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    parsed, message = _decode_last_rf_frame(sent_rf_frames)
    assert parsed.source == "W4BRD-13"  # gateway callsign, not the mesh user's
    assert parsed.destination == "APZBRD"
    assert message.addressee == "WU2Z"
    assert message.text == "W4BRD-13: Testing 123"  # user callsign embedded in payload


def test_registered_sender_reaches_rf_on_channel_0(tmp_path, fake_connection_manager, running_event_loop):
    # Regression guard: real Meshtastic DMs are always tagged channel 0
    # regardless of the sender's active channel context (confirmed on real
    # hardware). A registered sender's DM must still reach RF -- channel
    # value must never gate the DM path.
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=0))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)


def test_last_correspondent_used_when_no_explicit_addressee(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")
    registry.set_last_correspondent(conn, "W4BRD-13", "WU2Z")

    bridge.on_mesh_packet(_dm_packet("!node0001", "just a reply, no callsign prefix", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _parsed, message = _decode_last_rf_frame(sent_rf_frames)
    assert message.addressee == "WU2Z"
    assert message.text == "W4BRD-13: just a reply, no callsign prefix"


def test_no_addressee_and_no_last_correspondent_replies_with_error(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "no addressee here", channel=2))

    time.sleep(0.2)
    assert sent_rf_frames == []
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "No recipient" in fake_connection_manager.sent[0]["text"]


def test_sending_updates_last_correspondent(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: first message", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    assert registry.get_last_correspondent(conn, "W4BRD-13") == "WU2Z"


def test_broadcast_and_non_dm_traffic_is_ignored(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    # A broadcast, not a DM to us.
    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2, to_id="^all"))
    # A non-text packet.
    bridge.on_mesh_packet(
        {
            "fromId": "!node0001",
            "toId": LOCAL_ID,
            "channel": 2,
            "decoded": {"portnum": "POSITION_APP"},
        }
    )

    time.sleep(0.2)
    assert sent_rf_frames == []
    assert fake_connection_manager.sent == []


def test_malformed_packet_does_not_raise(tmp_path, fake_connection_manager, running_event_loop):
    bridge, _conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_mesh_packet({})  # missing everything
    bridge.on_mesh_packet({"decoded": {}})


def test_outbound_message_carries_a_msgno_and_is_tracked_for_ack(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: Testing 123", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _parsed, message = _decode_last_rf_frame(sent_rf_frames)
    assert message.msgno is not None
    assert ack_tracker.pending_count() == 1


def test_duplicate_mesh_packet_id_sent_only_once(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    packet = _dm_packet("!node0001", "WU2Z: hello", channel=2)
    packet["id"] = 12345
    bridge.on_mesh_packet(packet)
    bridge.on_mesh_packet(dict(packet))  # identical packet id, as if pubsub fired twice

    time.sleep(0.2)
    assert len(sent_rf_frames) == 1


def test_rate_limit_exceeded_drops_send_and_replies(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path,
        fake_connection_manager,
        running_event_loop,
        per_callsign_rate_limit_per_min=60.0,
        per_callsign_rate_limit_burst=1.0,  # exactly one message allowed
    )
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: first", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: second", channel=2))
    time.sleep(0.2)
    assert len(sent_rf_frames) == 1  # second send rate-limited
    # Successful sends generate no mesh reply; only the rate-limit error does.
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Rate limit" in fake_connection_manager.sent[-1]["text"]
