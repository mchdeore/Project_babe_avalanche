from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from typing import Iterable

import yaml

CONFIG_PATH = "config.yaml"
SCHEMA_PATH = "schema.sql"


def load_config(path: str = CONFIG_PATH) -> dict:
    """Load YAML config from disk and return a dict."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def init_db(db_path: str, schema_path: str = SCHEMA_PATH) -> sqlite3.Connection:
    """Initialize the SQLite DB and apply the schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_team(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def canonical_game_id(league: str, team_a: str, team_b: str, date_str: str) -> str:
    teams = sorted([normalize_team(team_a), normalize_team(team_b)])
    return f"{date_str}_{league}_{teams[0]}_{teams[1]}"


def sanitize_column(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def get_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}


def ensure_book_columns(
    conn: sqlite3.Connection,
    books: Iterable[str],
    table: str = "game_market_current",
) -> dict[str, dict[str, str]]:
    """Ensure per-book odds/line columns exist in the current snapshot table.

    Returns a mapping of book -> column name mapping.
    """
    existing = get_table_columns(conn, table)
    book_map: dict[str, dict[str, str]] = {}

    for book in books:
        safe = sanitize_column(book)
        cols = {
            "home_odds": f"home_odds_{safe}",
            "away_odds": f"away_odds_{safe}",
            "home_line": f"home_line_{safe}",
            "away_line": f"away_line_{safe}",
            "over_odds": f"over_odds_{safe}",
            "under_odds": f"under_odds_{safe}",
            "total_line": f"total_line_{safe}",
        }
        for col in cols.values():
            if col not in existing:
                conn.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" REAL;')
                existing.add(col)
        book_map[book] = cols

    conn.commit()
    return book_map


def upsert_rows(
    conn: sqlite3.Connection,
    table: str,
    key_cols: list[str],
    update_cols: list[str],
    rows: Iterable[dict],
) -> None:
    rows = list(rows)
    if not rows:
        return

    cols = []
    for col in key_cols + update_cols:
        if col not in cols:
            cols.append(col)

    quoted_cols = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    conflict_cols = ", ".join([f'"{c}"' for c in key_cols])
    update_set = ", ".join(
        [f'"{c}"=excluded."{c}"' for c in update_cols if c not in key_cols]
    )

    sql = (
        f"INSERT INTO {table} ({quoted_cols}) VALUES ({placeholders}) "
        f"ON CONFLICT({conflict_cols}) DO UPDATE SET {update_set};"
    )

    data = []
    for row in rows:
        data.append([row.get(c) for c in cols])

    conn.executemany(sql, data)
