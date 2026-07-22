import asyncio
import logging
import time

from aprs_bridge import registry
from aprs_bridge.bridge import RfToMeshBridge
from aprs_bridge.config import BridgeConfig
from aprs_bridge.protocol import ax25, aprs_message, kiss


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
        allowed_mesh_channels=(),
    )
    defaults.update(overrides)
    return BridgeConfig(**defaults)


def _make_bridge(tmp_path, fake_connection_manager, running_event_loop, sent_rf_frames=None):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    cfg = _make_config(registry_db_path=str(tmp_path / "reg.db"))
    sent_rf_frames = sent_rf_frames if sent_rf_frames is not None else []

    def transport_send(data: bytes) -> bool:
        sent_rf_frames.append(data)
        return True

    bridge = RfToMeshBridge(
        cfg=cfg,
        registry_conn=conn,
        connection_manager=fake_connection_manager,
        event_loop=running_event_loop,
        logger=logging.getLogger("test.bridge"),
        transport_send=transport_send,
    )
    return bridge, conn, sent_rf_frames


def _build_rf_frame(addressee: str, text: str, msgno=None, source="N0CALL-10") -> bytes:
    info = aprs_message.encode_message(addressee, text, msgno)
    ax25_frame = ax25.build_ui_frame("APZ019", source, ["WIDE1-1", "WIDE2-1"], info)
    return ax25_frame


def test_message_to_registered_callsign_is_delivered_to_mesh(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _ = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "Testing", msgno="003")
    bridge.on_ax25_frame(frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    sent = fake_connection_manager.sent[0]
    assert sent["destinationId"] == "!aabbccdd"
    assert sent["text"] == "Testing"
    assert sent["channelIndex"] == 0


def test_message_to_unregistered_callsign_is_dropped(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, _conn, sent_rf_frames = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)

    frame = _build_rf_frame("NOBODY", "hi there")
    bridge.on_ax25_frame(frame)

    time.sleep(0.2)  # give the (nonexistent) delivery a moment to have fired if it were going to
    assert fake_connection_manager.sent == []
    assert sent_rf_frames == []


def test_message_with_msgno_triggers_rf_ack(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
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
    bridge, conn, sent_rf_frames = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
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
    bridge, conn, _ = _make_bridge(tmp_path, not_ready_cm, running_event_loop)
    registry.add_registration(conn, "WU2Z", "!aabbccdd")

    frame = _build_rf_frame("WU2Z", "hello")
    bridge.on_ax25_frame(frame)

    time.sleep(0.2)
    assert not_ready_cm.sent == []


def test_non_message_ax25_frame_is_ignored(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
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
    bridge, _conn, _sent = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_ax25_frame(b"\x00\x01\x02not a real ax25 frame")  # must not raise
