from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .errors import AprsMessageError

_FORBIDDEN_TEXT_CHARS = ("|", "~", "{")
_ADDRESSEE_WIDTH = 9
_MAX_TEXT_LEN = 67


@dataclass(frozen=True)
class AprsMessage:
    addressee: str
    text: str
    msgno: Optional[str] = None


def _validate_addressee(addressee: str) -> str:
    if len(addressee) > _ADDRESSEE_WIDTH:
        raise AprsMessageError(
            f"addressee {addressee!r} exceeds {_ADDRESSEE_WIDTH} characters"
        )
    return addressee


def _validate_text(text: str) -> str:
    if not 1 <= len(text) <= _MAX_TEXT_LEN:
        raise AprsMessageError(
            f"text must be 1..{_MAX_TEXT_LEN} characters, got {len(text)}"
        )
    for ch in _FORBIDDEN_TEXT_CHARS:
        if ch in text:
            raise AprsMessageError(f"text contains forbidden character {ch!r}")
    return text


def _validate_msgno(msgno: Optional[str]) -> Optional[str]:
    if msgno is None:
        return None
    if not 1 <= len(msgno) <= 5:
        raise AprsMessageError(f"msgno must be 1..5 characters, got {len(msgno)!r}")
    return msgno


def encode_message(addressee: str, text: str, msgno: Optional[str] = None) -> bytes:
    addressee = _validate_addressee(addressee)
    text = _validate_text(text)
    msgno = _validate_msgno(msgno)

    parts = [":", addressee.ljust(_ADDRESSEE_WIDTH), ":", text]
    if msgno is not None:
        parts.append("{" + msgno)
    return "".join(parts).encode("ascii")


def is_message(info: bytes) -> bool:
    return info[0:1] == b":"


def decode_message(info: bytes) -> AprsMessage:
    if info[0:1] != b":":
        raise AprsMessageError("info field does not start with ':'")
    if len(info) < _ADDRESSEE_WIDTH + 2 or info[_ADDRESSEE_WIDTH + 1 : _ADDRESSEE_WIDTH + 2] != b":":
        raise AprsMessageError("malformed addressee framing (expected ':' at byte 10)")

    addressee = info[1 : _ADDRESSEE_WIDTH + 1].decode("ascii").strip()
    raw_tail = info[_ADDRESSEE_WIDTH + 2 :]

    text = raw_tail
    msgno: Optional[bytes] = None
    tail_window = raw_tail[-6:]
    if b"{" in tail_window:
        idx = raw_tail.rfind(b"{")
        candidate = raw_tail[idx + 1 :]
        if 1 <= len(candidate) <= 5:
            text = raw_tail[:idx]
            msgno = candidate

    return AprsMessage(
        addressee=addressee,
        text=text.decode("ascii"),
        msgno=msgno.decode("ascii") if msgno is not None else None,
    )


def build_ack(addressee: str, msgno: str) -> bytes:
    return encode_message(addressee, "ack" + msgno, msgno=None)


def parse_ack(msg: AprsMessage) -> Optional[str]:
    if msg.msgno is not None:
        return None
    if not msg.text.startswith("ack"):
        return None
    candidate = msg.text[3:]
    if 1 <= len(candidate) <= 5:
        return candidate
    return None
