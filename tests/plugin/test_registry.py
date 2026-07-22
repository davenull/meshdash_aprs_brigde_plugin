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


def test_lookup_callsign_for_node(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    assert registry.lookup_callsign_for_node(conn, "!aabbccdd") is None
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    assert registry.lookup_callsign_for_node(conn, "!aabbccdd") == "W4BRD-13"


def test_add_registration_enforces_one_callsign_per_node(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    # Same node re-registers under a different callsign -- the old mapping
    # must not linger and create an ambiguous reverse lookup.
    registry.add_registration(conn, "W4BRD-14", "!aabbccdd")
    assert registry.lookup_callsign_for_node(conn, "!aabbccdd") == "W4BRD-14"
    assert registry.lookup_node_for_callsign(conn, "W4BRD-13") is None
    assert registry.lookup_node_for_callsign(conn, "W4BRD-14") == "!aabbccdd"


def test_remove_registration_by_node(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    removed = registry.remove_registration_by_node(conn, "!aabbccdd")
    assert removed == "W4BRD-13"
    assert registry.lookup_node_for_callsign(conn, "W4BRD-13") is None


def test_remove_registration_by_node_when_none_exists(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    assert registry.remove_registration_by_node(conn, "!aabbccdd") is None


def test_last_correspondent_round_trip(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    assert registry.get_last_correspondent(conn, "W4BRD-13") is None
    registry.set_last_correspondent(conn, "w4brd-13", "wu2z")
    assert registry.get_last_correspondent(conn, "W4BRD-13") == "WU2Z"
    registry.set_last_correspondent(conn, "W4BRD-13", "N0CALL-10")
    assert registry.get_last_correspondent(conn, "W4BRD-13") == "N0CALL-10"
