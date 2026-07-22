import pytest

from aprs_bridge import commands
from aprs_bridge.commands import CommandError


def test_is_valid_callsign_accepts_typical_forms():
    assert commands.is_valid_callsign("W4BRD-13")
    assert commands.is_valid_callsign("w4brd-13")
    assert commands.is_valid_callsign("WU2Z")
    assert commands.is_valid_callsign("N0CALL-0")
    assert commands.is_valid_callsign("N0CALL-15")


def test_is_valid_callsign_rejects_bad_forms():
    assert not commands.is_valid_callsign("AB")  # too short
    assert not commands.is_valid_callsign("TOOLONGCALL")  # too long
    assert not commands.is_valid_callsign("W4BRD-16")  # SSID out of range
    assert not commands.is_valid_callsign("W4 BRD")  # space
    assert not commands.is_valid_callsign("")


def test_parse_register_command_valid():
    assert commands.parse_register_command("!register W4BRD-13") == "W4BRD-13"
    assert commands.parse_register_command("!REGISTER w4brd-13") == "W4BRD-13"
    assert commands.parse_register_command("  !register W4BRD-13  ") == "W4BRD-13"


def test_parse_register_command_not_a_register_command():
    assert commands.parse_register_command("hello world") is None
    assert commands.parse_register_command("!unregister") is None
    assert commands.parse_register_command("WU2Z: hi") is None


def test_parse_register_command_malformed_raises():
    with pytest.raises(CommandError):
        commands.parse_register_command("!register")
    with pytest.raises(CommandError):
        commands.parse_register_command("!register not-a-callsign-at-all")


def test_is_unregister_command():
    assert commands.is_unregister_command("!unregister")
    assert commands.is_unregister_command("!UNREGISTER")
    assert commands.is_unregister_command("  !unregister  ")
    assert not commands.is_unregister_command("!unregister please")
    assert not commands.is_unregister_command("hello")


def test_parse_outbound_request_with_addressee():
    addressee, text = commands.parse_outbound_request("WU2Z: Testing 123")
    assert addressee == "WU2Z"
    assert text == "Testing 123"


def test_parse_outbound_request_with_ssid_addressee():
    addressee, text = commands.parse_outbound_request("n0call-10: hey there")
    assert addressee == "N0CALL-10"
    assert text == "hey there"


def test_parse_outbound_request_bare_message_falls_back():
    addressee, text = commands.parse_outbound_request("just a plain message")
    assert addressee is None
    assert text == "just a plain message"


def test_parse_outbound_request_colon_but_not_a_callsign_falls_back():
    addressee, text = commands.parse_outbound_request("note to self: buy milk")
    assert addressee is None
    assert text == "note to self: buy milk"
