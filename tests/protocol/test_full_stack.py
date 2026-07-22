from aprs_bridge.protocol import ax25, aprs_message, kiss

FULL_STACK_KISS_HEX = (
    "c00082a0b4606272609c608682989874ae92888a624062ae92888a"
    "64406303f03a5755325a20202020203a54657374696e677b303033c0"
)


def test_full_stack_encode_matches_known_good_hex():
    info = aprs_message.encode_message("WU2Z", "Testing", "003")
    ax25_frame = ax25.build_ui_frame("APZ019", "N0CALL-10", ["WIDE1-1", "WIDE2-1"], info)
    kiss_frame = kiss.encode_frame(ax25_frame, port=0, command=0)
    assert kiss_frame == bytes.fromhex(FULL_STACK_KISS_HEX)


def test_full_stack_decode_reconstructs_message():
    kiss_frame = bytes.fromhex(FULL_STACK_KISS_HEX)

    port, command, ax25_bytes = kiss.decode_frame(kiss_frame)
    assert (port, command) == (0, 0)

    parsed_frame = ax25.parse_ui_frame(ax25_bytes)
    assert parsed_frame.destination == "APZ019"
    assert parsed_frame.source == "N0CALL-10"
    assert parsed_frame.path == ("WIDE1-1", "WIDE2-1")

    message = aprs_message.decode_message(parsed_frame.info)
    assert message.addressee == "WU2Z"
    assert message.text == "Testing"
    assert message.msgno == "003"
