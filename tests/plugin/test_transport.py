import logging
import time

from aprs_bridge.protocol import kiss
from aprs_bridge.transport import TncTransport


def _wait_until(predicate, timeout=5, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_transport_receives_and_decodes_frames(kiss_tcp_server):
    received_payloads = []
    transport = TncTransport(
        host=kiss_tcp_server.host,
        port=kiss_tcp_server.port,
        on_frame=received_payloads.append,
        logger=logging.getLogger("test.transport"),
        reconnect_delay=0.1,
        recv_timeout=0.1,
    )
    transport.start()
    try:
        frame = kiss.encode_frame(b"\xaa\xbb\xcc", port=0, command=0)
        # send_to_client() itself waits for the client to connect first.
        kiss_tcp_server.send_to_client(frame, chunk_size=3)

        assert _wait_until(lambda: len(received_payloads) == 1)
        assert received_payloads[0] == b"\xaa\xbb\xcc"
    finally:
        transport.stop()


def test_transport_send_writes_kiss_bytes_to_server(kiss_tcp_server):
    transport = TncTransport(
        host=kiss_tcp_server.host,
        port=kiss_tcp_server.port,
        on_frame=lambda payload: None,
        logger=logging.getLogger("test.transport"),
        reconnect_delay=0.1,
        recv_timeout=0.1,
    )
    transport.start()
    try:
        outbound = kiss.encode_frame(b"\x01\x02\x03", port=0, command=0)
        assert _wait_until(lambda: transport.send(outbound))

        received = kiss_tcp_server.wait_until_received(len(outbound))
        assert outbound in received
    finally:
        transport.stop()
