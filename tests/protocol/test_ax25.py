import pytest

from aprs_bridge.protocol import ax25
from aprs_bridge.protocol.errors import Ax25FrameError

FIXTURE_INFO = b":WU2Z     :Testing{003"
FIXTURE_HEX = (
    "82a0b4606272609c608682989874ae92888a624062ae92888a"
    "64406303f03a5755325a20202020203a54657374696e677b303033"
)


def test_build_ui_frame_matches_known_good_hex():
    frame = ax25.build_ui_frame("APZ019", "N0CALL-10", ["WIDE1-1", "WIDE2-1"], FIXTURE_INFO)
    assert frame == bytes.fromhex(FIXTURE_HEX)


def test_parse_ui_frame_matches_known_good_hex():
    parsed = ax25.parse_ui_frame(bytes.fromhex(FIXTURE_HEX))
    assert parsed.destination == "APZ019"
    assert parsed.source == "N0CALL-10"
    assert parsed.path == ("WIDE1-1", "WIDE2-1")
    assert parsed.info == FIXTURE_INFO


@pytest.mark.parametrize(
    "destination,source,path",
    [
        ("A", "N0CALL", []),
        ("APZ019", "N0CALL", ["WIDE1-1"]),
        ("ABCDEF", "N0CALL-15", ["WIDE1-1", "WIDE2-1"]),
    ],
)
def test_build_parse_round_trip_address_edge_cases(destination, source, path):
    info = b":TEST     :hello"
    frame = ax25.build_ui_frame(destination, source, path, info)
    parsed = ax25.parse_ui_frame(frame)
    assert parsed.destination == destination
    assert parsed.source == source
    assert parsed.path == tuple(path)
    assert parsed.info == info


def test_repeated_digi_marker_not_last_in_path_decodes_correctly():
    # ax253.Address can't *encode* a repeated marker on a non-last address
    # (see the comment in ax25.py), so this fixture is built by taking a
    # clean "WIDE1-1,WIDE2-1" frame and setting the AX.25 has-been-repeated
    # bit (0x80) directly on WIDE1-1's raw SSID byte -- exactly what a real
    # digipeater does on the air. This is the most common APRS path shape.
    frame = bytearray(
        ax25.build_ui_frame("APZ019", "N0CALL", ["WIDE1-1", "WIDE2-1"], b":TEST     :hi")
    )
    wide1_ssid_byte_offset = 7 * 2 + 6  # dest(7) + src(7) + 6 bytes into WIDE1-1's address
    frame[wide1_ssid_byte_offset] |= 0x80
    parsed = ax25.parse_ui_frame(bytes(frame))
    assert parsed.path == ("WIDE1-1*", "WIDE2-1")


def test_repeated_digi_marker_when_last_in_path_decodes_correctly():
    frame = ax25.build_ui_frame("APZ019", "N0CALL", ["WIDE1-1*"], b":TEST     :hi")
    parsed = ax25.parse_ui_frame(frame)
    assert parsed.path == ("WIDE1-1*",)


def test_parse_ui_frame_rejects_non_ui_control():
    frame = bytearray(ax25.build_ui_frame("APZ019", "N0CALL", ["WIDE1-1"], b":TEST     :hi"))
    # Control byte sits right after the two 7-byte address fields.
    control_offset = 7 * 3
    frame[control_offset] = 0x3F  # not the UI control value (0x03)
    with pytest.raises(Ax25FrameError):
        ax25.parse_ui_frame(bytes(frame))


def test_parse_ui_frame_rejects_wrong_pid():
    frame = bytearray(ax25.build_ui_frame("APZ019", "N0CALL", ["WIDE1-1"], b":TEST     :hi"))
    pid_offset = 7 * 3 + 1
    frame[pid_offset] = 0x00  # not the APRS PID (0xF0)
    with pytest.raises(Ax25FrameError):
        ax25.parse_ui_frame(bytes(frame))
