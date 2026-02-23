"""Shared helpers for source ingestion."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable
import sqlite3
import time

import requests

from utils import DEFAULT_RETRIES, DEFAULT_TIMEOUT, devig, insert_history, upsert_rows

NO_VIG_SOURCES = {"polymarket", "kalshi", "stx"}


def api_request(
    session: requests.Session,
    url: str,
    params: dict | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> tuple[dict | list | None, int]:
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)

            if resp.status_code == 200:
                try:
                    return resp.json(), 200
                except ValueError:
                    return None, 200

            if 400 <= resp.status_code < 500:
                if resp.status_code == 429 and attempt < retries:
                    time.sleep(5 * (attempt + 1))
                    continue
                return None, resp.status_code

            if resp.status_code >= 500:
                if attempt < retries:
                    time.sleep(1.5 ** attempt)
                    continue
                return None, resp.status_code

            return None, resp.status_code

        except requests.exceptions.RequestException:
            pass

        if attempt < retries:
            time.sleep(1.5 ** attempt)

    return None, 0


def apply_devig(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows

    groups: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (
            row.get("source"),
            row.get("provider"),
            row.get("game_id"),
            row.get("market"),
            row.get("line"),
            row.get("player", ""),
        )
        groups[key].append(row)

    for group in groups.values():
        source = group[0].get("source", "")
        if source in NO_VIG_SOURCES:
            for row in group:
                row["devigged_prob"] = row.get("implied_prob")
            continue

        probs = [row.get("implied_prob") for row in group]
        devigged = devig(probs)
        for row, dv in zip(group, devigged):
            row["devigged_prob"] = dv

    return rows


def save_to_db(
    conn: sqlite3.Connection,
    games: dict[str, dict[str, Any]],
    rows: Iterable[dict[str, Any]],
) -> None:
    rows = list(rows)
    if games:
        upsert_rows(
            conn,
            "games",
            ["game_id"],
            ["league", "commence_time", "home_team", "away_team", "last_refreshed"],
            games.values(),
        )

    if rows:
        upsert_rows(
            conn,
            "market_latest",
            ["game_id", "market", "side", "line", "source", "provider", "player"],
            [
                "price",
                "implied_prob",
                "devigged_prob",
                "provider_updated_at",
                "last_refreshed",
                "source_event_id",
                "source_market_id",
                "outcome",
            ],
            rows,
        )
        insert_history(conn, rows)

    conn.commit()
