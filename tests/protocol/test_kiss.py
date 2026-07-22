import socket

import pytest

from aprs_bridge.protocol import kiss
from aprs_bridge.protocol.errors import KissFramingError

ESCAPE_FIXTURE_PAYLOAD = bytes([0x01, 0xC0, 0x02, 0xDB, 0x03])
ESCAPE_FIXTURE_KISS_FRAME = bytes.fromhex("c00001dbdc02dbdd03c0")


def test_encode_frame_empty_payload():
    frame = kiss.encode_frame(b"", port=0, command=0)
    assert frame == bytes.fromhex("c000c0")


def test_decode_frame_empty_payload():
    port, command, payload = kiss.decode_frame(bytes.fromhex("c000c0"))
    assert (port, command, payload) == (0, 0, b"")


def test_encode_frame_escaping_fixture():
    frame = kiss.encode_frame(ESCAPE_FIXTURE_PAYLOAD, port=0, command=0)
    assert frame == ESCAPE_FIXTURE_KISS_FRAME


def test_decode_frame_escaping_fixture():
    port, command, payload = kiss.decode_frame(ESCAPE_FIXTURE_KISS_FRAME)
    assert (port, command, payload) == (0, 0, ESCAPE_FIXTURE_PAYLOAD)


def test_encode_frame_command_byte_collides_with_fend():
    # port=0x0C, command=0x00 -> cmd_byte == 0xC0 == FEND, must be escaped.
    frame = kiss.encode_frame(b"\x01", port=0x0C, command=0x00)
    assert frame == bytes.fromhex("c0dbdc01c0")
    port, command, payload = kiss.decode_frame(frame)
    assert (port, command, payload) == (0x0C, 0x00, b"\x01")


def test_decode_frame_missing_leading_fend():
    with pytest.raises(KissFramingError):
        kiss.decode_frame(bytes.fromhex("0000c0"))


def test_decode_frame_missing_trailing_fend():
    with pytest.raises(KissFramingError):
        kiss.decode_frame(bytes.fromhex("c00000"))


def test_decode_frame_dangling_fesc_at_end():
    with pytest.raises(KissFramingError):
        kiss.decode_frame(bytes.fromhex("c000db") + b"\xc0")


def test_decode_frame_invalid_escape_sequence():
    # FESC followed by neither TFEND nor TFESC.
    with pytest.raises(KissFramingError):
        kiss.decode_frame(bytes.fromhex("c000db01c0"))


class TestKissStreamDecoder:
    def test_single_frame_delivered_byte_by_byte(self):
        decoder = kiss.KissStreamDecoder()
        frame = ESCAPE_FIXTURE_KISS_FRAME
        results = []
        for i in range(len(frame)):
            results.extend(decoder.feed(frame[i : i + 1]))
        assert len(results) == 1
        assert results[0].port == 0
        assert results[0].command == 0
        assert results[0].payload == ESCAPE_FIXTURE_PAYLOAD

    def test_two_frames_sharing_a_single_fend(self):
        decoder = kiss.KissStreamDecoder()
        frame_a = kiss.encode_frame(b"\xaa", port=0, command=0)
        frame_b = kiss.encode_frame(b"\xbb", port=0, command=0)
        # frame_a's trailing FEND doubles as frame_b's leading FEND.
        combined = frame_a[:-1] + frame_b
        results = decoder.feed(combined)
        assert [r.payload for r in results] == [b"\xaa", b"\xbb"]

    def test_back_to_back_fends_yield_no_frames(self):
        decoder = kiss.KissStreamDecoder()
        results = decoder.feed(bytes([0xC0, 0xC0, 0xC0]))
        assert results == []

    def test_leading_garbage_is_discarded(self):
        decoder = kiss.KissStreamDecoder()
        frame = kiss.encode_frame(b"\xaa", port=0, command=0)
        results = decoder.feed(b"\xff\xff" + frame)
        assert len(results) == 1
        assert results[0].payload == b"\xaa"


def test_kiss_stream_decoder_over_real_tcp_socket(kiss_tcp_server):
    client = socket.create_connection((kiss_tcp_server.host, kiss_tcp_server.port), timeout=5)
    try:
        frame_a = kiss.encode_frame(b"\xaa\xbb", port=0, command=0)
        frame_b = kiss.encode_frame(ESCAPE_FIXTURE_PAYLOAD, port=0, command=0)
        combined = frame_a + frame_b

        # Deliver one byte at a time over the wire to genuinely exercise
        # partial-frame buffering across real TCP reads, not just feed().
        kiss_tcp_server.send_to_client(combined, chunk_size=1)

        decoder = kiss.KissStreamDecoder()
        decoded = []
        client.settimeout(5)
        while len(decoded) < 2:
            chunk = client.recv(1)
            if not chunk:
                break
            decoded.extend(decoder.feed(chunk))

        assert [d.payload for d in decoded] == [b"\xaa\xbb", ESCAPE_FIXTURE_PAYLOAD]
    finally:
        client.close()
