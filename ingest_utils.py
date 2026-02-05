"""Shared utilities for odds ingestion."""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Iterable

import yaml

CONFIG_PATH = "config.yaml"
SCHEMA_PATH = "schema.sql"


def load_config(path: str = CONFIG_PATH) -> dict:
    """Load YAML config."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def init_db(db_path: str, schema_path: str = SCHEMA_PATH) -> sqlite3.Connection:
    """Initialize SQLite DB and apply schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    with open(schema_path, encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    return conn


def utc_now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def normalize_team(name: str) -> str:
    """Normalize team name for matching."""
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def canonical_game_id(league: str, team_a: str, team_b: str, date_str: str) -> str:
    """Build deterministic game_id from date, league, and teams."""
    teams = sorted([normalize_team(team_a), normalize_team(team_b)])
    return f"{date_str}_{league}_{teams[0]}_{teams[1]}"


def parse_iso_utc(value: str) -> datetime | None:
    """Parse ISO-8601 timestamp into UTC datetime."""
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


def within_bettable_window(commence_time: str, window_days: int) -> bool:
    """Return True if game is within the bettable window."""
    if not commence_time:
        return False
    now = datetime.now(timezone.utc)

    # Handle date-only strings (e.g., "2026-02-04")
    if len(commence_time.strip()) <= 10:
        try:
            game_date = datetime.fromisoformat(commence_time.strip()).date()
        except ValueError:
            return False
        today = now.date()
        return today <= game_date <= (today + timedelta(days=window_days))

    dt = parse_iso_utc(commence_time)
    if dt is None or dt < now:
        return False
    return dt <= now + timedelta(days=window_days)


def upsert_rows(
    conn: sqlite3.Connection,
    table: str,
    key_cols: list[str],
    update_cols: list[str],
    rows: Iterable[dict],
) -> None:
    """Insert or update rows using SQLite ON CONFLICT upserts."""
    rows = list(rows)
    if not rows:
        return

    cols = list(dict.fromkeys(key_cols + update_cols))  # preserve order, dedupe
    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["?"] * len(cols))
    conflict_cols = ", ".join(f'"{c}"' for c in key_cols)
    update_set = ", ".join(f'"{c}"=excluded."{c}"' for c in update_cols if c not in key_cols)

    sql = f"INSERT INTO {table} ({quoted_cols}) VALUES ({placeholders}) "
    sql += f"ON CONFLICT({conflict_cols}) DO UPDATE SET {update_set};" if update_set else f"ON CONFLICT({conflict_cols}) DO NOTHING;"

    conn.executemany(sql, [[row.get(c) for c in cols] for row in rows])


def implied_prob(price: float | None) -> float | None:
    """Convert decimal odds to implied probability."""
    if price is None or price <= 0:
        return None
    return 1.0 / price
