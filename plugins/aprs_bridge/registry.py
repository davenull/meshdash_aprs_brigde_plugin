from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Registration:
    callsign: str
    node_id: str
    created_at: float


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Older versions of this table used callsign as the primary key,
    strictly limiting a callsign to at most one registered device.
    node_id is now the primary key instead -- still exactly one callsign
    per device, but a callsign can now have many devices (plenty of
    operators run more than one mesh node). Detects the old schema and
    migrates existing data in place rather than dropping it."""
    cols = conn.execute("PRAGMA table_info(callsign_registry)").fetchall()
    if not cols:
        return  # table doesn't exist yet; the CREATE TABLE below handles it
    pk_col = next((c[1] for c in cols if c[5] == 1), None)  # column name where pk=1
    if pk_col == "node_id":
        return  # already on the new schema
    conn.execute("ALTER TABLE callsign_registry RENAME TO callsign_registry_old")
    conn.execute(
        """
        CREATE TABLE callsign_registry (
            node_id TEXT PRIMARY KEY,
            callsign TEXT NOT NULL,
            created_at REAL NOT NULL
        )
        """
    )
    conn.execute(
        "INSERT INTO callsign_registry (node_id, callsign, created_at) "
        "SELECT node_id, callsign, created_at FROM callsign_registry_old"
    )
    conn.execute("DROP TABLE callsign_registry_old")
    conn.commit()


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    _migrate_schema(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS callsign_registry (
            node_id TEXT PRIMARY KEY,
            callsign TEXT NOT NULL,
            created_at REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS last_correspondent (
            callsign TEXT PRIMARY KEY,
            addressee TEXT NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _normalize(callsign: str) -> str:
    return callsign.strip().upper()


def add_registration(conn: sqlite3.Connection, callsign: str, node_id: str) -> None:
    """A device (node_id) maps to exactly one callsign at a time --
    re-registering it under a different callsign moves it (INSERT OR
    REPLACE on the node_id primary key handles this). A callsign may have
    many registered devices: registering a second device under an
    already-registered callsign just adds another row."""
    conn.execute(
        "INSERT OR REPLACE INTO callsign_registry (node_id, callsign, created_at) VALUES (?, ?, ?)",
        (node_id, _normalize(callsign), time.time()),
    )
    conn.commit()


def remove_registration(conn: sqlite3.Connection, callsign: str) -> None:
    """Removes every device registered under callsign. For removing a
    single device, use remove_registration_by_node instead."""
    conn.execute("DELETE FROM callsign_registry WHERE callsign = ?", (_normalize(callsign),))
    conn.commit()


def remove_registration_by_node(conn: sqlite3.Connection, node_id: str) -> Optional[str]:
    """Removes whatever registration exists for a single device (used by
    !unregister and the UI's per-row remove button). Returns the callsign
    that was removed, or None if the node had no registration."""
    row = conn.execute(
        "SELECT callsign FROM callsign_registry WHERE node_id = ?", (node_id,)
    ).fetchone()
    if row is None:
        return None
    conn.execute("DELETE FROM callsign_registry WHERE node_id = ?", (node_id,))
    conn.commit()
    return row[0]


def lookup_nodes_for_callsign(conn: sqlite3.Connection, callsign: str) -> List[str]:
    rows = conn.execute(
        "SELECT node_id FROM callsign_registry WHERE callsign = ? ORDER BY created_at",
        (_normalize(callsign),),
    ).fetchall()
    return [r[0] for r in rows]


def list_registrations(conn: sqlite3.Connection) -> List[Registration]:
    rows = conn.execute(
        "SELECT callsign, node_id, created_at FROM callsign_registry ORDER BY created_at DESC"
    ).fetchall()
    return [Registration(callsign=r[0], node_id=r[1], created_at=r[2]) for r in rows]


def lookup_callsign_for_node(conn: sqlite3.Connection, node_id: str) -> Optional[str]:
    row = conn.execute(
        "SELECT callsign FROM callsign_registry WHERE node_id = ?", (node_id,)
    ).fetchone()
    return row[0] if row else None


def set_last_correspondent(conn: sqlite3.Connection, callsign: str, addressee: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO last_correspondent (callsign, addressee, updated_at) VALUES (?, ?, ?)",
        (_normalize(callsign), _normalize(addressee), time.time()),
    )
    conn.commit()


def get_last_correspondent(conn: sqlite3.Connection, callsign: str) -> Optional[str]:
    row = conn.execute(
        "SELECT addressee FROM last_correspondent WHERE callsign = ?",
        (_normalize(callsign),),
    ).fetchone()
    return row[0] if row else None
