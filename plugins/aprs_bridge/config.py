from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Tuple


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class BridgeConfig:
    tnc_mode: str
    tnc_host: str
    tnc_port: int
    kiss_port: int
    gateway_callsign: str
    aprs_tocall: str
    digi_path: Tuple[str, ...]
    mesh_channel_index: int
    registry_db_path: str
    dedupe_ttl_sec: float
    rate_limit_per_min: float
    rate_limit_burst: float
    per_callsign_rate_limit_per_min: float
    per_callsign_rate_limit_burst: float
    ack_retry_intervals_sec: Tuple[float, ...]
    ack_max_attempts: int
    mesh_fanout_delay_sec: float


_REQUIRED_FIELDS = {"tnc_mode", "tnc_host", "tnc_port", "gateway_callsign"}
_SUPPORTED_TNC_MODES = {"kiss_tcp"}


def load_config(path: str) -> BridgeConfig:
    if not os.path.exists(path):
        raise ConfigError(f"config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        try:
            raw = json.load(f)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"config file is not valid JSON: {exc}") from exc

    missing = _REQUIRED_FIELDS - raw.keys()
    if missing:
        raise ConfigError(f"config missing required fields: {sorted(missing)}")

    if raw["tnc_mode"] not in _SUPPORTED_TNC_MODES:
        raise ConfigError(
            f"unsupported tnc_mode {raw['tnc_mode']!r}; supported: {sorted(_SUPPORTED_TNC_MODES)}"
        )

    try:
        tnc_port = int(raw["tnc_port"])
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"tnc_port must be an integer: {raw['tnc_port']!r}") from exc

    gateway_callsign = str(raw["gateway_callsign"]).strip().upper()
    if not gateway_callsign:
        raise ConfigError("gateway_callsign must not be empty")

    plugin_dir = os.path.dirname(os.path.abspath(path))
    registry_db_path = raw.get("registry_db_path") or os.path.join(plugin_dir, "registrations.db")

    ack_max_attempts = int(raw.get("ack_max_attempts", 4))
    if ack_max_attempts < 1:
        raise ConfigError("ack_max_attempts must be >= 1")

    return BridgeConfig(
        tnc_mode=raw["tnc_mode"],
        tnc_host=str(raw["tnc_host"]),
        tnc_port=tnc_port,
        kiss_port=int(raw.get("kiss_port", 0)),
        gateway_callsign=gateway_callsign,
        aprs_tocall=str(raw.get("aprs_tocall", "APZBRD")).strip().upper(),
        digi_path=tuple(raw.get("digi_path", ["WIDE1-1", "WIDE2-1"])),
        mesh_channel_index=int(raw.get("mesh_channel_index", 0)),
        registry_db_path=registry_db_path,
        # Dedupe / rate-limit / retry defaults: conservative enough for a
        # single-station APRS/mesh gateway. 30s loop/dupe TTL comfortably
        # covers a digipeated repeat of the same message arriving shortly
        # after the direct copy (observed live via the user's own
        # W4BRD-1 digipeater).
        dedupe_ttl_sec=float(raw.get("dedupe_ttl_sec", 30.0)),
        rate_limit_per_min=float(raw.get("rate_limit_per_min", 20.0)),
        rate_limit_burst=float(raw.get("rate_limit_burst", 10.0)),
        per_callsign_rate_limit_per_min=float(raw.get("per_callsign_rate_limit_per_min", 6.0)),
        per_callsign_rate_limit_burst=float(raw.get("per_callsign_rate_limit_burst", 3.0)),
        ack_retry_intervals_sec=tuple(raw.get("ack_retry_intervals_sec", [30, 60, 120])),
        ack_max_attempts=ack_max_attempts,
        # Gap between sequential RF->mesh deliveries when more than one
        # device is targeted (fan-out / "!ALL"). Confirmed live that a
        # too-short gap (originally a hardcoded 0.5s) let a second
        # delivery silently fail to reach its device even with
        # sendText(wantAck=True) requesting Meshtastic's own delivery
        # confirmation -- the first send's ack-wait/retry cycle appears
        # to still be occupying the radio when the second one goes out
        # too soon after it. Configurable since the right value is a
        # property of the radio/mesh, not something to hardcode.
        mesh_fanout_delay_sec=float(raw.get("mesh_fanout_delay_sec", 2.0)),
    )
