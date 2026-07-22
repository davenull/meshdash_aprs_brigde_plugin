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
    allowed_mesh_channels: Tuple[int, ...]


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
        # No default here on purpose: Meshtastic's default channel is
        # AES-encrypted, and CLAUDE.md's hard invariant is that encrypted
        # content must never reach RF. Mesh->RF gating stays off (empty
        # allowlist) until the operator explicitly names a vetted,
        # non-default channel index here.
        allowed_mesh_channels=tuple(raw.get("allowed_mesh_channels", ())),
    )
