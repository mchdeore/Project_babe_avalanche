"""Shared utilities for odds ingestion and analysis."""
from __future__ import annotations
import re, sqlite3, yaml, json
from datetime import datetime, timedelta, timezone
from typing import Iterable

def load_config(path: str = "config.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def init_db(db_path: str, schema_path: str = "schema.sql") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    with open(schema_path, encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    return conn

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_team(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower()) if name else ""

def canonical_game_id(league: str, team_a: str, team_b: str, date_str: str) -> str:
    teams = sorted([normalize_team(team_a), normalize_team(team_b)])
    return f"{date_str}_{league}_{teams[0]}_{teams[1]}"

def parse_iso_utc(value: str) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

def within_window(commence_time: str, window_days: int) -> bool:
    if not commence_time:
        return False
    now = datetime.now(timezone.utc)
    if len(commence_time.strip()) <= 10:
        try:
            game_date = datetime.fromisoformat(commence_time.strip()).date()
            return now.date() <= game_date <= (now.date() + timedelta(days=window_days))
        except ValueError:
            return False
    dt = parse_iso_utc(commence_time)
    return dt is not None and now <= dt <= now + timedelta(days=window_days)

def _quote(c): return f'"{c}"'

def upsert_rows(conn: sqlite3.Connection, table: str, keys: list[str], updates: list[str], rows: Iterable[dict]) -> None:
    rows = list(rows)
    if not rows:
        return
    cols = list(dict.fromkeys(keys + updates))
    sql = f"INSERT INTO {table} ({', '.join(_quote(c) for c in cols)}) VALUES ({', '.join(['?'] * len(cols))}) "
    sql += f"ON CONFLICT({', '.join(_quote(c) for c in keys)}) DO UPDATE SET {', '.join(f'{_quote(c)}=excluded.{_quote(c)}' for c in updates if c not in keys)};"
    conn.executemany(sql, [[row.get(c) for c in cols] for row in rows])

def insert_history(conn: sqlite3.Connection, rows: Iterable[dict]) -> None:
    rows = list(rows)
    if not rows:
        return
    cols = ["game_id", "market", "side", "line", "source", "provider", "price", "implied_prob", "devigged_prob", "provider_updated_at", "snapshot_time", "source_event_id", "source_market_id", "outcome"]
    conn.executemany(f"INSERT INTO market_history ({', '.join(_quote(c) for c in cols)}) VALUES ({', '.join(['?'] * len(cols))});", [[row.get(c) for c in cols] for row in rows])

# === Probability Functions ===

def odds_to_prob(price: float | None) -> float | None:
    """Decimal odds → probability: 2.0 → 50%"""
    return 1.0 / price if price and price > 0 else None

def prob_from_price(price: float | None) -> float | None:
    """Polymarket price is already probability: 0.54 → 54%"""
    return price if price is not None and 0 <= price <= 1 else None

def devig(probs: list[float]) -> list[float]:
    """Remove vig by normalizing probabilities to sum to 1."""
    if not probs or any(p <= 0 for p in probs):
        return probs
    total = sum(probs)
    return [p / total for p in probs] if total > 0 else probs

def devig_market(rows: list[dict]) -> list[dict]:
    """Apply de-vigging to market rows. Polymarket has no vig."""
    if not rows:
        return []
    if rows[0].get("source") == "polymarket":
        for row in rows:
            row["devigged_prob"] = row.get("implied_prob")
        return rows
    probs = [row.get("implied_prob", 0) for row in rows]
    devigged = devig(probs)
    for row, dv in zip(rows, devigged):
        row["devigged_prob"] = dv
    return rows

def safe_json(val):
    """Parse JSON string or return as-is."""
    if isinstance(val, str):
        try:
            return json.loads(val)
        except:
            return []
    return val if val else []
