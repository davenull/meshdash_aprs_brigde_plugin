from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from kiss import util as kiss_util

from .errors import KissFramingError

FEND = 0xC0
FESC = 0xDB
TFEND = 0xDC
TFESC = 0xDD


def _unescape_strict(data: bytes) -> bytes:
    out = bytearray()
    i = 0
    n = len(data)
    while i < n:
        b = data[i]
        if b == FESC:
            if i + 1 >= n:
                raise KissFramingError("dangling FESC at end of frame")
            nxt = data[i + 1]
            if nxt == TFEND:
                out.append(FEND)
            elif nxt == TFESC:
                out.append(FESC)
            else:
                raise KissFramingError(
                    "invalid KISS escape sequence: FESC followed by 0x%02x" % nxt
                )
            i += 2
        else:
            out.append(b)
            i += 1
    return bytes(out)


def encode_frame(payload: bytes, port: int = 0, command: int = 0x00) -> bytes:
    if not 0 <= port <= 0x0F:
        raise ValueError("port must be in 0..15")
    if not 0 <= command <= 0x0F:
        raise ValueError("command must be in 0..15")
    cmd_byte = ((port & 0x0F) << 4) | (command & 0x0F)
    body = bytes([cmd_byte]) + payload
    escaped = kiss_util.escape_special_codes(body)
    return bytes([FEND]) + escaped + bytes([FEND])


def decode_frame(frame: bytes) -> Tuple[int, int, bytes]:
    if len(frame) < 2 or frame[0] != FEND or frame[-1] != FEND:
        raise KissFramingError("frame must start and end with FEND")
    body = _unescape_strict(frame[1:-1])
    if len(body) < 1:
        raise KissFramingError("frame has no command byte")
    cmd_byte = body[0]
    port = (cmd_byte >> 4) & 0x0F
    command = cmd_byte & 0x0F
    payload = body[1:]
    return port, command, payload


@dataclass(frozen=True)
class DecodedKissFrame:
    port: int
    command: int
    payload: bytes


class KissStreamDecoder:
    """Stateful decoder for a raw byte stream that may deliver partial
    frames across reads, at arbitrary chunk boundaries."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> List[DecodedKissFrame]:
        self._buffer.extend(data)
        frames: List[DecodedKissFrame] = []
        while True:
            while self._buffer and self._buffer[0] != FEND:
                del self._buffer[0]

            # Back-to-back FENDs are not an empty frame; collapse them.
            while len(self._buffer) >= 2 and self._buffer[0] == FEND and self._buffer[1] == FEND:
                del self._buffer[0]

            if not self._buffer or self._buffer[0] != FEND:
                break

            end_idx = None
            for i in range(1, len(self._buffer)):
                if self._buffer[i] == FEND:
                    end_idx = i
                    break
            if end_idx is None:
                break  # incomplete frame; wait for more data

            frame_bytes = bytes(self._buffer[: end_idx + 1])
            # Leave the trailing FEND in place: it doubles as the next
            # frame's leading FEND when frames are back-to-back on the wire.
            del self._buffer[:end_idx]

            try:
                port, command, payload = decode_frame(frame_bytes)
            except KissFramingError:
                continue
            frames.append(DecodedKissFrame(port=port, command=command, payload=payload))
        return frames
