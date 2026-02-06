"""
Shared Utilities
================
Database operations, probability functions, and helpers.
"""
from __future__ import annotations

import json
import re
import sqlite3
import yaml
from datetime import datetime, timedelta, timezone
from typing import Iterable


# =============================================================================
# CONFIG & DATABASE
# =============================================================================

def load_config(path: str = "config.yaml") -> dict:
    """Load YAML configuration file."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def init_db(db_path: str, schema_path: str = "schema.sql") -> sqlite3.Connection:
    """Initialize database with schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    with open(schema_path, encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    return conn


# =============================================================================
# TIME & NORMALIZATION
# =============================================================================

def utc_now_iso() -> str:
    """Current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def normalize_team(name: str) -> str:
    """
    Normalize team name for matching.
    'Los Angeles Lakers' → 'losangeleslakers'
    """
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def canonical_game_id(league: str, team_a: str, team_b: str, date_str: str) -> str:
    """
    Generate consistent game ID across sources.
    Teams are sorted alphabetically for consistency.
    """
    teams = sorted([normalize_team(team_a), normalize_team(team_b)])
    return f"{date_str}_{league}_{teams[0]}_{teams[1]}"


def within_window(commence_time: str, window_days: int) -> bool:
    """Check if game is within bettable window."""
    if not commence_time:
        return False

    now = datetime.now(timezone.utc)

    # Handle date-only strings
    if len(commence_time.strip()) <= 10:
        try:
            game_date = datetime.fromisoformat(commence_time.strip()).date()
            return now.date() <= game_date <= (now.date() + timedelta(days=window_days))
        except ValueError:
            return False

    # Handle full ISO timestamps
    try:
        value = commence_time.replace("Z", "+00:00") if commence_time.endswith("Z") else commence_time
        dt = datetime.fromisoformat(value)
        dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return now <= dt <= now + timedelta(days=window_days)
    except ValueError:
        return False


def safe_json(val):
    """Parse JSON string or return as-is."""
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            return []
    return val if val else []


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def _quote(col: str) -> str:
    """Quote column name for SQL."""
    return f'"{col}"'


def upsert_rows(
    conn: sqlite3.Connection,
    table: str,
    keys: list[str],
    updates: list[str],
    rows: Iterable[dict],
) -> None:
    """Insert or update rows (upsert)."""
    rows = list(rows)
    if not rows:
        return

    cols = list(dict.fromkeys(keys + updates))
    placeholders = ", ".join(["?"] * len(cols))
    key_clause = ", ".join(_quote(c) for c in keys)
    update_clause = ", ".join(
        f"{_quote(c)}=excluded.{_quote(c)}"
        for c in updates if c not in keys
    )

    sql = (
        f"INSERT INTO {table} ({', '.join(_quote(c) for c in cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT({key_clause}) DO UPDATE SET {update_clause};"
    )

    conn.executemany(sql, [[row.get(c) for c in cols] for row in rows])


def insert_history(conn: sqlite3.Connection, rows: Iterable[dict]) -> None:
    """Append rows to history table (no upsert, always insert)."""
    rows = list(rows)
    if not rows:
        return

    cols = [
        "game_id", "market", "side", "line", "source", "provider",
        "price", "implied_prob", "devigged_prob", "provider_updated_at",
        "snapshot_time", "source_event_id", "source_market_id", "outcome",
    ]
    placeholders = ", ".join(["?"] * len(cols))

    sql = f"INSERT INTO market_history ({', '.join(_quote(c) for c in cols)}) VALUES ({placeholders});"
    conn.executemany(sql, [[row.get(c) for c in cols] for row in rows])


# =============================================================================
# PROBABILITY FUNCTIONS
# =============================================================================

def odds_to_prob(price: float | None) -> float | None:
    """
    Convert decimal odds to implied probability.
    Example: 2.0 → 50%
    """
    if price and price > 0:
        return 1.0 / price
    return None


def devig(probs: list[float]) -> list[float]:
    """
    Remove vig by normalizing probabilities to sum to 1.
    
    Example:
        [0.55, 0.55] → [0.50, 0.50]  (10% vig removed)
    """
    if not probs or any(p is None or p <= 0 for p in probs):
        return probs

    total = sum(probs)
    if total > 0:
        return [p / total for p in probs]
    return probs


def devig_market(rows: list[dict]) -> list[dict]:
    """
    Apply de-vigging to a group of market outcomes.
    Polymarket has no vig, so prices are used directly.
    """
    if not rows:
        return []

    # Polymarket prices ARE fair probabilities
    if rows[0].get("source") == "polymarket":
        for row in rows:
            row["devigged_prob"] = row.get("implied_prob")
        return rows

    # De-vig sportsbook odds
    probs = [row.get("implied_prob", 0) for row in rows]
    devigged = devig(probs)

    for row, dv in zip(rows, devigged):
        row["devigged_prob"] = dv

    return rows
