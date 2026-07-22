from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

from ax253 import Address, Frame

from .errors import Ax25FrameError

UI_CONTROL = b"\x03"
UI_PID = b"\xf0"

# ax253.Address conflates the AX.25 "has-been-repeated" bit with the
# "last address in the chain" (extension) bit: Address.__bytes__ only sets
# the repeated bit when a7_hldc is also True, and Address.from_bytes only
# reports digi=True when hldc is also True. That makes the single most
# common real-world APRS path shape -- a digipeated hop that ISN'T the last
# address, e.g. "WIDE1-1*,WIDE2-1" -- unrepresentable through the library's
# public API in either direction. We never need to *encode* a repeated
# marker ourselves (we originate frames, we don't relay them as a
# digipeater), so build_ui_frame is unaffected. For *decoding*, we read the
# repeated bit straight off the raw bytes instead of trusting Address.digi.


@dataclass(frozen=True)
class ParsedUiFrame:
    destination: str
    source: str
    path: Tuple[str, ...]
    info: bytes


def build_ui_frame(destination: str, source: str, path: Sequence[str], info: bytes) -> bytes:
    frame = Frame.ui(destination=destination, source=source, path=list(path), info=info)
    return bytes(frame)


def _path_repeated_flags(data: bytes, path_len: int) -> Tuple[bool, ...]:
    flags = []
    for i in range(path_len):
        ssid_byte_offset = 7 * (2 + i) + 6
        flags.append(bool(data[ssid_byte_offset] & 0x80))
    return tuple(flags)


def _format_path_address(addr: Address, repeated: bool) -> str:
    call = addr.callsign.decode("latin1")
    ssid_part = "-%d" % addr.ssid if addr.ssid else ""
    return "%s%s%s" % (call, ssid_part, "*" if repeated else "")


def parse_ui_frame(data: bytes) -> ParsedUiFrame:
    try:
        frame = Frame.from_bytes(data)
    except Exception as exc:
        raise Ax25FrameError(f"malformed AX.25 frame: {exc}") from exc

    if bytes(frame.control) != UI_CONTROL:
        raise Ax25FrameError(f"not a UI frame: control={bytes(frame.control)!r}")
    if frame.pid != UI_PID:
        raise Ax25FrameError(f"unexpected PID for APRS UI frame: pid={frame.pid!r}")

    repeated_flags = _path_repeated_flags(data, len(frame.path))
    path = tuple(
        _format_path_address(addr, repeated)
        for addr, repeated in zip(frame.path, repeated_flags)
    )

    return ParsedUiFrame(
        destination=str(frame.destination),
        source=str(frame.source),
        path=path,
        info=frame.info,
    )
