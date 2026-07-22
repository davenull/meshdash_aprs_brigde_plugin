import asyncio
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import List

import pytest


@dataclass
class KissTcpServer:
    """A background TCP server for testing KISS transport code without
    hardware. A test drives it by calling send_to_client() with arbitrary
    chunking, and reads whatever the client wrote back via received()."""

    host: str
    port: int
    _server_socket: socket.socket
    _conn: socket.socket = field(init=False, default=None)
    _received: bytearray = field(init=False, default_factory=bytearray)
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock)
    _stop: threading.Event = field(init=False, default_factory=threading.Event)
    _thread: threading.Thread = field(init=False, default=None)

    def _accept_and_pump(self) -> None:
        self._server_socket.settimeout(5)
        try:
            conn, _ = self._server_socket.accept()
        except socket.timeout:
            return
        conn.settimeout(0.2)
        self._conn = conn
        while not self._stop.is_set():
            try:
                chunk = conn.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if not chunk:
                break
            with self._lock:
                self._received.extend(chunk)
        try:
            conn.close()
        except OSError:
            pass

    def start(self) -> None:
        self._thread = threading.Thread(target=self._accept_and_pump, daemon=True)
        self._thread.start()

    def send_to_client(self, data: bytes, chunk_size: int = None) -> None:
        """Write bytes to the connected client, optionally split into
        chunk_size pieces to exercise partial-frame buffering."""
        deadline = time.monotonic() + 5
        while self._conn is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert self._conn is not None, "no client connected to mock KISS server"
        if chunk_size is None:
            self._conn.sendall(data)
            return
        for i in range(0, len(data), chunk_size):
            self._conn.sendall(data[i : i + chunk_size])

    def received(self) -> bytes:
        with self._lock:
            return bytes(self._received)

    def wait_until_received(self, min_bytes: int, timeout: float = 5) -> bytes:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            data = self.received()
            if len(data) >= min_bytes:
                return data
            time.sleep(0.01)
        return self.received()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._server_socket.close()
        except OSError:
            pass
        if self._conn is not None:
            try:
                self._conn.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2)


@pytest.fixture
def kiss_tcp_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    host, port = sock.getsockname()

    server = KissTcpServer(host=host, port=port, _server_socket=sock)
    server.start()
    try:
        yield server
    finally:
        server.stop()


class FakeConnectionManager:
    """Lightweight stand-in for MeshDash's MeshtasticConnectionManager, per
    CLAUDE.md's testing strategy. is_ready mirrors the real threading.Event
    API; sendText records calls instead of touching a radio."""

    def __init__(self, ready: bool = True) -> None:
        self.is_ready = threading.Event()
        if ready:
            self.is_ready.set()
        self.sent: List[dict] = []
        self.raise_on_send: BaseException = None

    async def sendText(self, text, destinationId, channelIndex=0, wantAck=False):
        if self.raise_on_send is not None:
            raise self.raise_on_send
        self.sent.append(
            {
                "text": text,
                "destinationId": destinationId,
                "channelIndex": channelIndex,
                "wantAck": wantAck,
            }
        )


@pytest.fixture
def fake_connection_manager():
    return FakeConnectionManager()


@pytest.fixture
def running_event_loop():
    """A real asyncio event loop running in a background thread, so code
    under test can genuinely use asyncio.run_coroutine_threadsafe(coro,
    loop) the same way the plugin does against MeshDash's uvicorn loop."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        def _cancel_all_tasks():
            for task in asyncio.all_tasks(loop=loop):
                task.cancel()

        loop.call_soon_threadsafe(_cancel_all_tasks)
        time.sleep(0.05)  # let cancellation callbacks run before stopping the loop
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2)
        loop.close()
