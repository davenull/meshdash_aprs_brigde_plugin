from aprs_bridge import registry


def test_lookup_unregistered_callsign_returns_none(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    assert registry.lookup_node_for_callsign(conn, "W4BRD-13") is None


def test_add_and_lookup_registration(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "w4brd-13", "!aabbccdd")
    assert registry.lookup_node_for_callsign(conn, "W4BRD-13") == "!aabbccdd"
    # Lookup is case-insensitive / whitespace-tolerant.
    assert registry.lookup_node_for_callsign(conn, " w4brd-13 ") == "!aabbccdd"


def test_add_registration_overwrites_existing(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!11111111")
    registry.add_registration(conn, "W4BRD-13", "!22222222")
    assert registry.lookup_node_for_callsign(conn, "W4BRD-13") == "!22222222"


def test_remove_registration(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    registry.remove_registration(conn, "W4BRD-13")
    assert registry.lookup_node_for_callsign(conn, "W4BRD-13") is None


def test_registrations_persist_across_connections(tmp_path):
    db_path = str(tmp_path / "reg.db")
    conn1 = registry.init_db(db_path)
    registry.add_registration(conn1, "W4BRD-13", "!aabbccdd")

    conn2 = registry.init_db(db_path)
    assert registry.lookup_node_for_callsign(conn2, "W4BRD-13") == "!aabbccdd"
