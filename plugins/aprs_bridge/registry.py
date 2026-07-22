from __future__ import annotations

import sqlite3
import time
from typing import Optional


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
    conn.commit()
    return conn


def _normalize(callsign: str) -> str:
    return callsign.strip().upper()


def add_registration(conn: sqlite3.Connection, callsign: str, node_id: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO callsign_registry (callsign, node_id, created_at) VALUES (?, ?, ?)",
        (_normalize(callsign), node_id, time.time()),
    )
    conn.commit()


def remove_registration(conn: sqlite3.Connection, callsign: str) -> None:
    conn.execute("DELETE FROM callsign_registry WHERE callsign = ?", (_normalize(callsign),))
    conn.commit()


def lookup_node_for_callsign(conn: sqlite3.Connection, callsign: str) -> Optional[str]:
    row = conn.execute(
        "SELECT node_id FROM callsign_registry WHERE callsign = ?",
        (_normalize(callsign),),
    ).fetchone()
    return row[0] if row else None
