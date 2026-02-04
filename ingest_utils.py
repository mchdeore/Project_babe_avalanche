from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta, timezone
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


def parse_iso_utc(value: str) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def within_bettable_window(
    commence_time: str,
    window_days: int,
    now: datetime | None = None,
) -> bool:
    if not commence_time:
        return False
    now = now or datetime.now(timezone.utc)

    # If the string is date-only (e.g., "2026-02-04"), compare by date
    # so we don't exclude same-day games due to missing time info.
    if len(commence_time.strip()) <= 10:
        try:
            game_date = datetime.fromisoformat(commence_time.strip()).date()
        except ValueError:
            return False
        today = now.date()
        return today <= game_date <= (today + timedelta(days=window_days))

    dt = parse_iso_utc(commence_time)
    if dt is None:
        return False
    if dt < now:
        return False
    return dt <= now + timedelta(days=window_days)


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


def insert_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    rows: Iterable[tuple],
) -> None:
    rows = list(rows)
    if not rows:
        return
    quoted_cols = ", ".join([f'"{c}"' for c in columns])
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO {table} ({quoted_cols}) VALUES ({placeholders});"
    conn.executemany(sql, rows)
