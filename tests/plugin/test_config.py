import json

import pytest

from aprs_bridge.config import ConfigError, load_config


def _write_config(tmp_path, data):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(data))
    return str(path)


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(str(tmp_path / "does_not_exist.json"))


def test_invalid_json_raises(tmp_path):
    path = tmp_path / "config.json"
    path.write_text("{not valid json")
    with pytest.raises(ConfigError):
        load_config(str(path))


def test_missing_required_field_raises(tmp_path):
    path = _write_config(tmp_path, {"tnc_mode": "kiss_tcp", "tnc_host": "127.0.0.1"})
    with pytest.raises(ConfigError):
        load_config(path)


def test_unsupported_tnc_mode_raises(tmp_path):
    path = _write_config(
        tmp_path,
        {
            "tnc_mode": "agw",
            "tnc_host": "127.0.0.1",
            "tnc_port": 8000,
            "gateway_callsign": "W4BRD-13",
        },
    )
    with pytest.raises(ConfigError):
        load_config(path)


def test_empty_gateway_callsign_raises(tmp_path):
    path = _write_config(
        tmp_path,
        {
            "tnc_mode": "kiss_tcp",
            "tnc_host": "127.0.0.1",
            "tnc_port": 8001,
            "gateway_callsign": "   ",
        },
    )
    with pytest.raises(ConfigError):
        load_config(path)


def test_valid_minimal_config_applies_defaults(tmp_path):
    path = _write_config(
        tmp_path,
        {
            "tnc_mode": "kiss_tcp",
            "tnc_host": "192.168.2.39",
            "tnc_port": 8001,
            "gateway_callsign": "w4brd-13",
        },
    )
    cfg = load_config(path)
    assert cfg.tnc_host == "192.168.2.39"
    assert cfg.tnc_port == 8001
    assert cfg.gateway_callsign == "W4BRD-13"  # normalized uppercase
    assert cfg.kiss_port == 0
    assert cfg.aprs_tocall == "APZBRD"
    assert cfg.digi_path == ("WIDE1-1", "WIDE2-1")
    assert cfg.mesh_channel_index == 0
    assert cfg.registry_db_path == str(tmp_path / "registrations.db")
    assert cfg.allowed_mesh_channels == ()  # mesh->RF gating off by default


def test_allowed_mesh_channels_defaults_to_empty_not_the_default_channel(tmp_path):
    # Regression guard: this must never silently default to (0,) or any
    # other channel, since channel 0 is typically Meshtastic's
    # AES-encrypted default -- CLAUDE.md's hard invariant is that
    # encrypted-channel content must never reach RF.
    path = _write_config(
        tmp_path,
        {
            "tnc_mode": "kiss_tcp",
            "tnc_host": "127.0.0.1",
            "tnc_port": 8001,
            "gateway_callsign": "W4BRD-13",
        },
    )
    cfg = load_config(path)
    assert cfg.allowed_mesh_channels == ()


def test_full_config_overrides_all_defaults(tmp_path):
    path = _write_config(
        tmp_path,
        {
            "tnc_mode": "kiss_tcp",
            "tnc_host": "10.0.0.5",
            "tnc_port": 9001,
            "kiss_port": 2,
            "gateway_callsign": "n0call-5",
            "aprs_tocall": "apzfoo",
            "digi_path": ["WIDE2-2"],
            "mesh_channel_index": 3,
            "registry_db_path": "/tmp/custom_registry.db",
            "allowed_mesh_channels": [3],
        },
    )
    cfg = load_config(path)
    assert cfg.tnc_host == "10.0.0.5"
    assert cfg.tnc_port == 9001
    assert cfg.kiss_port == 2
    assert cfg.gateway_callsign == "N0CALL-5"
    assert cfg.aprs_tocall == "APZFOO"
    assert cfg.digi_path == ("WIDE2-2",)
    assert cfg.mesh_channel_index == 3
    assert cfg.registry_db_path == "/tmp/custom_registry.db"
    assert cfg.allowed_mesh_channels == (3,)
