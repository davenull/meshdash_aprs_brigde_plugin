import sqlite3

from aprs_bridge import registry


def test_lookup_unregistered_callsign_returns_no_nodes(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-13") == []


def test_add_and_lookup_registration(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "w4brd-13", "!aabbccdd")
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-13") == ["!aabbccdd"]
    # Lookup is case-insensitive / whitespace-tolerant.
    assert registry.lookup_nodes_for_callsign(conn, " w4brd-13 ") == ["!aabbccdd"]


def test_re_registering_same_node_overwrites_its_callsign(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    registry.add_registration(conn, "W4BRD-14", "!aabbccdd")  # same device, new callsign
    assert registry.lookup_callsign_for_node(conn, "!aabbccdd") == "W4BRD-14"
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-13") == []
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-14") == ["!aabbccdd"]


def test_one_callsign_can_have_multiple_registered_devices(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!11111111")
    registry.add_registration(conn, "W4BRD-13", "!22222222")
    nodes = registry.lookup_nodes_for_callsign(conn, "W4BRD-13")
    assert set(nodes) == {"!11111111", "!22222222"}
    # Each device still resolves back to the one shared callsign.
    assert registry.lookup_callsign_for_node(conn, "!11111111") == "W4BRD-13"
    assert registry.lookup_callsign_for_node(conn, "!22222222") == "W4BRD-13"


def test_remove_registration_removes_all_devices_for_callsign(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!11111111")
    registry.add_registration(conn, "W4BRD-13", "!22222222")
    registry.remove_registration(conn, "W4BRD-13")
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-13") == []


def test_registrations_persist_across_connections(tmp_path):
    db_path = str(tmp_path / "reg.db")
    conn1 = registry.init_db(db_path)
    registry.add_registration(conn1, "W4BRD-13", "!aabbccdd")

    conn2 = registry.init_db(db_path)
    assert registry.lookup_nodes_for_callsign(conn2, "W4BRD-13") == ["!aabbccdd"]


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
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-13") == []
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-14") == ["!aabbccdd"]


def test_remove_registration_by_node(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    removed = registry.remove_registration_by_node(conn, "!aabbccdd")
    assert removed == "W4BRD-13"
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-13") == []


def test_remove_registration_by_node_only_removes_that_device(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!11111111")
    registry.add_registration(conn, "W4BRD-13", "!22222222")
    registry.remove_registration_by_node(conn, "!11111111")
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-13") == ["!22222222"]


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


def test_last_active_node_round_trip(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    assert registry.get_last_active_node(conn, "W4BRD-13") is None
    registry.set_last_active_node(conn, "w4brd-13", "!node0001")
    assert registry.get_last_active_node(conn, "W4BRD-13") == "!node0001"
    registry.set_last_active_node(conn, "W4BRD-13", "!node0002")
    assert registry.get_last_active_node(conn, "W4BRD-13") == "!node0002"


def test_conversation_node_round_trip(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    assert registry.get_conversation_node(conn, "WU2Z") is None
    registry.set_conversation_node(conn, "wu2z", "!node0001")
    assert registry.get_conversation_node(conn, "WU2Z") == "!node0001"
    registry.set_conversation_node(conn, "WU2Z", "!node0002")
    assert registry.get_conversation_node(conn, "WU2Z") == "!node0002"


def test_list_registrations_empty(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    assert registry.list_registrations(conn) == []


def test_list_registrations_returns_all_newest_first(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    registry.add_registration(conn, "WU2Z", "!11223344")

    rows = registry.list_registrations(conn)
    assert [r.callsign for r in rows] == ["WU2Z", "W4BRD-13"]
    assert rows[0].node_id == "!11223344"
    assert isinstance(rows[0].created_at, float)


def test_list_registrations_excludes_removed(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    registry.remove_registration(conn, "W4BRD-13")
    assert registry.list_registrations(conn) == []


def test_list_registrations_includes_all_devices_for_a_callsign(tmp_path):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    registry.add_registration(conn, "W4BRD-13", "!11111111")
    registry.add_registration(conn, "W4BRD-13", "!22222222")
    rows = registry.list_registrations(conn)
    assert len(rows) == 2
    assert {r.node_id for r in rows} == {"!11111111", "!22222222"}
    assert all(r.callsign == "W4BRD-13" for r in rows)


def test_init_db_migrates_old_callsign_primary_key_schema(tmp_path):
    # Simulate a pre-migration database (callsign PRIMARY KEY, the old
    # 1:1-only schema) with one existing registration, then confirm
    # init_db migrates it in place without losing data.
    db_path = str(tmp_path / "reg.db")
    old_conn = sqlite3.connect(db_path)
    old_conn.execute(
        """
        CREATE TABLE callsign_registry (
            callsign TEXT PRIMARY KEY,
            node_id TEXT NOT NULL,
            created_at REAL NOT NULL
        )
        """
    )
    old_conn.execute(
        "INSERT INTO callsign_registry (callsign, node_id, created_at) VALUES (?, ?, ?)",
        ("W4BRD-13", "!16d7f598", 1700000000.0),
    )
    old_conn.commit()
    old_conn.close()

    conn = registry.init_db(db_path)
    assert registry.lookup_nodes_for_callsign(conn, "W4BRD-13") == ["!16d7f598"]
    assert registry.lookup_callsign_for_node(conn, "!16d7f598") == "W4BRD-13"

    # And the new schema now actually supports a second device.
    registry.add_registration(conn, "W4BRD-13", "!aabbccdd")
    assert set(registry.lookup_nodes_for_callsign(conn, "W4BRD-13")) == {"!16d7f598", "!aabbccdd"}


def test_init_db_is_idempotent_on_already_migrated_schema(tmp_path):
    db_path = str(tmp_path / "reg.db")
    conn1 = registry.init_db(db_path)
    registry.add_registration(conn1, "W4BRD-13", "!aabbccdd")

    conn2 = registry.init_db(db_path)  # re-open; must not re-migrate or error
    assert registry.lookup_nodes_for_callsign(conn2, "W4BRD-13") == ["!aabbccdd"]
