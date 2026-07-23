from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Callable, Optional

from .protocol.kiss import KissStreamDecoder


class TncTransport:
    """Owns a TCP connection to a KISS-over-TCP TNC (e.g. Direwolf port
    8001). Runs a daemon thread that connects, reconnects on failure, and
    feeds received bytes through a KissStreamDecoder, invoking
    on_frame(payload) with each decoded KISS frame's payload (raw AX.25
    bytes)."""

    def __init__(
        self,
        host: str,
        port: int,
        on_frame: Callable[[bytes], None],
        logger: logging.Logger,
        reconnect_delay: float = 5.0,
        recv_timeout: float = 1.0,
    ) -> None:
        self._host = host
        self._port = port
        self._on_frame = on_frame
        self._logger = logger
        self._reconnect_delay = reconnect_delay
        self._recv_timeout = recv_timeout
        self._sock: Optional[socket.socket] = None
        self._sock_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="aprs-bridge-tnc-rx")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._sock_lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass

    def is_connected(self) -> bool:
        with self._sock_lock:
            return self._sock is not None

    def send(self, data: bytes) -> bool:
        with self._sock_lock:
            if self._sock is None:
                return False
            try:
                self._sock.sendall(data)
                return True
            except OSError as exc:
                self._logger.warning("TNC send failed: %s", exc)
                return False

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._connect_and_pump()
            except OSError as exc:
                self._logger.warning("TNC connection error: %s", exc)
            if self._stop.is_set():
                return
            time.sleep(self._reconnect_delay)

    def _connect_and_pump(self) -> None:
        self._logger.info("Connecting to TNC at %s:%d", self._host, self._port)
        sock = socket.create_connection((self._host, self._port), timeout=10)
        sock.settimeout(self._recv_timeout)
        with self._sock_lock:
            self._sock = sock
        self._logger.info("Connected to TNC at %s:%d", self._host, self._port)

        decoder = KissStreamDecoder()
        try:
            while not self._stop.is_set():
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    self._logger.warning("TNC connection closed by peer")
                    break
                for frame in decoder.feed(chunk):
                    try:
                        self._on_frame(frame.payload)
                    except Exception:
                        self._logger.exception("aprs_bridge: on_frame callback raised")
        finally:
            with self._sock_lock:
                self._sock = None
            try:
                sock.close()
            except OSError:
                pass
