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


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS callsign_registry (
            callsign TEXT PRIMARY KEY,
            node_id TEXT NOT NULL,
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
    """Enforces a 1:1 mapping: registering a callsign to a node drops any
    other callsign previously registered to that same node (a licensed
    operator has exactly one callsign per node), on top of the PRIMARY KEY
    already enforcing at most one node per callsign."""
    callsign = _normalize(callsign)
    conn.execute(
        "DELETE FROM callsign_registry WHERE node_id = ? AND callsign != ?",
        (node_id, callsign),
    )
    conn.execute(
        "INSERT OR REPLACE INTO callsign_registry (callsign, node_id, created_at) VALUES (?, ?, ?)",
        (callsign, node_id, time.time()),
    )
    conn.commit()


def remove_registration(conn: sqlite3.Connection, callsign: str) -> None:
    conn.execute("DELETE FROM callsign_registry WHERE callsign = ?", (_normalize(callsign),))
    conn.commit()


def remove_registration_by_node(conn: sqlite3.Connection, node_id: str) -> Optional[str]:
    """Removes whatever registration exists for a node (used by
    !unregister). Returns the callsign that was removed, or None if the
    node had no registration."""
    row = conn.execute(
        "SELECT callsign FROM callsign_registry WHERE node_id = ?", (node_id,)
    ).fetchone()
    if row is None:
        return None
    conn.execute("DELETE FROM callsign_registry WHERE node_id = ?", (node_id,))
    conn.commit()
    return row[0]


def lookup_node_for_callsign(conn: sqlite3.Connection, callsign: str) -> Optional[str]:
    row = conn.execute(
        "SELECT node_id FROM callsign_registry WHERE callsign = ?",
        (_normalize(callsign),),
    ).fetchone()
    return row[0] if row else None


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
