import importlib.util
import json
import logging
import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from aprs_bridge import registry
from aprs_bridge.ack_tracker import AckTracker
from aprs_bridge.config import BridgeConfig

PLUGIN_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "plugins",
    "aprs_bridge",
)
MAIN_PY = os.path.join(PLUGIN_DIR, "main.py")


def _load_main_module():
    """Same dynamic-import mechanism the real MeshDash core loader uses
    (importlib.util.spec_from_file_location), so a regression in that
    context (e.g. a relative import creeping back in) is caught here too."""
    spec = importlib.util.spec_from_file_location("plugin_aprs_bridge_routes", MAIN_PY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_config(**overrides):
    defaults = dict(
        tnc_mode="kiss_tcp",
        tnc_host="127.0.0.1",
        tnc_port=8001,
        kiss_port=0,
        gateway_callsign="W4BRD-13",
        aprs_tocall="APZBRD",
        digi_path=("WIDE1-1", "WIDE2-1"),
        mesh_channel_index=0,
        registry_db_path=":memory:",
        dedupe_ttl_sec=30.0,
        rate_limit_per_min=20.0,
        rate_limit_burst=10.0,
        per_callsign_rate_limit_per_min=6.0,
        per_callsign_rate_limit_burst=3.0,
        ack_retry_intervals_sec=(30.0, 60.0, 120.0),
        ack_max_attempts=4,
        mesh_fanout_delay_sec=0.05,  # tiny in tests; production default is 2.0s
    )
    defaults.update(overrides)
    return BridgeConfig(**defaults)


class _FakeTransport:
    def __init__(self, connected=True):
        self._connected = connected

    def is_connected(self):
        return self._connected


@pytest.fixture
def client_module(tmp_path):
    """A fresh main.py module instance with plugin_router mounted on a
    throwaway FastAPI app, config.json pointed at tmp_path so /config
    tests never touch the real repo file."""
    module = _load_main_module()
    module.CONFIG_PATH = str(tmp_path / "config.json")

    config_dict = {
        "tnc_mode": "kiss_tcp",
        "tnc_host": "127.0.0.1",
        "tnc_port": 8001,
        "gateway_callsign": "W4BRD-13",
    }
    with open(module.CONFIG_PATH, "w") as f:
        json.dump(config_dict, f)

    app = FastAPI()
    app.include_router(module.plugin_router)
    client = TestClient(app)
    return module, client


def _populate_ready_state(module, tmp_path, **cfg_overrides):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    cfg = _make_config(registry_db_path=str(tmp_path / "reg.db"), **cfg_overrides)
    ack_tracker = AckTracker(
        transport_send=lambda data: True,
        logger=logging.getLogger("test.api.ack"),
    )
    module._state["cfg"] = cfg
    module._state["registry_conn"] = conn
    module._state["transport"] = _FakeTransport(connected=True)
    module._state["ack_tracker"] = ack_tracker
    return conn, cfg, ack_tracker


# --- /status ---


def test_status_before_bootstrap_reports_starting(client_module):
    _module, client = client_module
    resp = client.get("/status")
    assert resp.status_code == 200
    assert resp.json() == {"status": "starting"}


def test_status_after_bootstrap_reports_running(client_module, tmp_path):
    module, client = client_module
    _populate_ready_state(module, tmp_path)

    resp = client.get("/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "running"
    assert body["tnc_connected"] is True
    assert body["gateway_callsign"] == "W4BRD-13"
    assert body["registration_count"] == 0
    assert body["pending_acks"] == 0


# --- /registrations ---


def test_registrations_not_ready_returns_503(client_module):
    _module, client = client_module
    resp = client.get("/registrations")
    assert resp.status_code == 503


def test_add_list_remove_registration_round_trip(client_module, tmp_path):
    module, client = client_module
    _populate_ready_state(module, tmp_path)

    resp = client.post("/registrations", json={"callsign": "wu2z", "node_id": "!aabbccdd"})
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "callsign": "WU2Z", "node_id": "!aabbccdd"}

    resp = client.get("/registrations")
    assert resp.status_code == 200
    regs = resp.json()["registrations"]
    assert len(regs) == 1
    assert regs[0]["callsign"] == "WU2Z"
    assert regs[0]["node_id"] == "!aabbccdd"

    resp = client.delete("/registrations/by-node/!aabbccdd")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "callsign": "WU2Z", "node_id": "!aabbccdd"}

    resp = client.get("/registrations")
    assert resp.json()["registrations"] == []


def test_remove_registration_by_node_404_when_not_registered(client_module, tmp_path):
    module, client = client_module
    _populate_ready_state(module, tmp_path)

    resp = client.delete("/registrations/by-node/!doesnotexist")
    assert resp.status_code == 404


def test_multiple_devices_can_register_under_the_same_callsign(client_module, tmp_path):
    module, client = client_module
    _populate_ready_state(module, tmp_path)

    client.post("/registrations", json={"callsign": "W4BRD-13", "node_id": "!11111111"})
    client.post("/registrations", json={"callsign": "W4BRD-13", "node_id": "!22222222"})

    resp = client.get("/registrations")
    regs = resp.json()["registrations"]
    assert len(regs) == 2
    assert {r["node_id"] for r in regs} == {"!11111111", "!22222222"}
    assert all(r["callsign"] == "W4BRD-13" for r in regs)

    # Removing one device leaves the other registered under the same callsign.
    client.delete("/registrations/by-node/!11111111")
    resp = client.get("/registrations")
    regs = resp.json()["registrations"]
    assert len(regs) == 1
    assert regs[0]["node_id"] == "!22222222"


def test_add_registration_rejects_invalid_callsign(client_module, tmp_path):
    module, client = client_module
    _populate_ready_state(module, tmp_path)

    resp = client.post("/registrations", json={"callsign": "not valid!!", "node_id": "!aabbccdd"})
    assert resp.status_code == 400


def test_add_registration_rejects_node_id_without_bang(client_module, tmp_path):
    module, client = client_module
    _populate_ready_state(module, tmp_path)

    resp = client.post("/registrations", json={"callsign": "WU2Z", "node_id": "aabbccdd"})
    assert resp.status_code == 400


# --- /mesh-nodes ---


class _FakeMeshtasticData:
    def __init__(self, nodes, local_node_id=None):
        self.nodes = nodes
        self.local_node_id = local_node_id


def test_mesh_nodes_before_bootstrap_returns_empty(client_module):
    _module, client = client_module
    resp = client.get("/mesh-nodes")
    assert resp.status_code == 200
    assert resp.json() == {"nodes": []}


def test_mesh_nodes_lists_known_nodes_excluding_local(client_module):
    module, client = client_module
    module._state["meshtastic_data"] = _FakeMeshtasticData(
        nodes={
            "!local0001": {"user": {"longName": "Gateway Radio"}, "lastHeard": 100},
            "!aabbccdd": {"user": {"longName": "David's Pager", "shortName": "PGR"}, "lastHeard": 200},
            "!11223344": {"long_name": "Base Station", "last_heard": 50},
        },
        local_node_id="!local0001",
    )

    resp = client.get("/mesh-nodes")
    assert resp.status_code == 200
    nodes = resp.json()["nodes"]
    node_ids = {n["node_id"] for n in nodes}
    assert node_ids == {"!aabbccdd", "!11223344"}  # local node excluded
    pager = next(n for n in nodes if n["node_id"] == "!aabbccdd")
    assert pager["long_name"] == "David's Pager"
    assert pager["short_name"] == "PGR"
    # Sorted newest-heard first.
    assert nodes[0]["node_id"] == "!aabbccdd"


def test_mesh_nodes_falls_back_to_node_id_when_unnamed(client_module):
    module, client = client_module
    module._state["meshtastic_data"] = _FakeMeshtasticData(
        nodes={"!aabbccdd": {}},
        local_node_id=None,
    )
    resp = client.get("/mesh-nodes")
    nodes = resp.json()["nodes"]
    assert nodes[0]["long_name"] == "!aabbccdd"
    assert nodes[0]["short_name"] == "ccdd"


# --- /config ---


def test_get_config_before_bootstrap_has_no_running_section(client_module):
    _module, client = client_module
    resp = client.get("/config")
    assert resp.status_code == 200
    body = resp.json()
    assert body["running"] is None
    assert body["restart_required"] is False
    assert body["saved"]["gateway_callsign"] == "W4BRD-13"


def test_get_config_flags_restart_required_when_saved_differs_from_running(
    client_module, tmp_path
):
    module, client = client_module
    _populate_ready_state(module, tmp_path)  # running cfg has tnc_host=127.0.0.1

    # Simulate an on-disk edit that hasn't been picked up by a restart yet.
    with open(module.CONFIG_PATH, "w") as f:
        json.dump({"tnc_mode": "kiss_tcp", "tnc_host": "10.0.0.5", "tnc_port": 8001, "gateway_callsign": "W4BRD-13"}, f)

    resp = client.get("/config")
    body = resp.json()
    assert body["restart_required"] is True


def test_get_config_no_restart_required_when_saved_matches_running(client_module, tmp_path):
    module, client = client_module
    _populate_ready_state(module, tmp_path)

    with open(module.CONFIG_PATH, "w") as f:
        json.dump({"tnc_mode": "kiss_tcp", "tnc_host": "127.0.0.1", "tnc_port": 8001, "gateway_callsign": "W4BRD-13"}, f)

    resp = client.get("/config")
    assert resp.json()["restart_required"] is False


def test_post_config_valid_writes_file_and_reports_restart_required(client_module):
    module, client = client_module

    resp = client.post(
        "/config",
        json={
            "tnc_mode": "kiss_tcp",
            "tnc_host": "192.168.2.39",
            "tnc_port": 8001,
            "gateway_callsign": "w4brd-14",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["restart_required"] is True

    with open(module.CONFIG_PATH) as f:
        saved = json.load(f)
    assert saved["tnc_host"] == "192.168.2.39"
    assert saved["gateway_callsign"] == "w4brd-14"  # raw JSON keeps caller's case; load_config normalizes on load


def test_post_config_invalid_tnc_mode_rejected_and_file_untouched(client_module):
    module, client = client_module
    original = open(module.CONFIG_PATH).read()

    resp = client.post(
        "/config",
        json={
            "tnc_mode": "not_a_real_mode",
            "tnc_host": "192.168.2.39",
            "tnc_port": 8001,
            "gateway_callsign": "W4BRD-13",
        },
    )
    assert resp.status_code == 400
    # Original file must be untouched -- no partial/invalid write survives.
    assert open(module.CONFIG_PATH).read() == original
    assert not os.path.exists(module.CONFIG_PATH + ".tmp")


def test_post_config_empty_gateway_callsign_rejected(client_module):
    _module, client = client_module
    resp = client.post(
        "/config",
        json={
            "tnc_mode": "kiss_tcp",
            "tnc_host": "192.168.2.39",
            "tnc_port": 8001,
            "gateway_callsign": "   ",
        },
    )
    assert resp.status_code == 400


def test_post_config_missing_required_field_rejected_by_pydantic(client_module):
    _module, client = client_module
    resp = client.post("/config", json={"tnc_mode": "kiss_tcp"})
    assert resp.status_code == 422  # FastAPI/Pydantic validation error, before our own load_config check
