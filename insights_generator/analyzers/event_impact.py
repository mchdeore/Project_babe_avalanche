"""Event -> Market impact analyzer."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from utils import parse_iso_timestamp, utc_now_iso, upsert_rows


def compute_event_impacts(
    conn: sqlite3.Connection,
    pre_window_minutes: int,
    post_window_minutes: int,
    max_event_age_hours: int = 72,
    min_snapshot_count: int = 1,
) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=max_event_age_hours)

    query = """
        SELECT
            se.id as event_id,
            se.event_type,
            se.team,
            se.player,
            se.extracted_at,
            nh.game_id,
            nh.published_at,
            nh.scraped_at
        FROM structured_events se
        JOIN news_headlines nh ON se.headline_id = nh.id
        WHERE nh.game_id IS NOT NULL
        AND nh.scraped_at >= ?
        ORDER BY nh.scraped_at DESC
    """

    cursor = conn.execute(query, (cutoff.isoformat(),))
    events = cursor.fetchall()
    if not events:
        return []

    impacts: list[dict[str, Any]] = []
    config_json = json.dumps({
        "pre_window_minutes": pre_window_minutes,
        "post_window_minutes": post_window_minutes,
        "max_event_age_hours": max_event_age_hours,
        "min_snapshot_count": min_snapshot_count,
    })

    for event in events:
        event_time = _select_event_time(event)
        if not event_time:
            continue

        pre_start = event_time - timedelta(minutes=pre_window_minutes)
        post_end = event_time + timedelta(minutes=post_window_minutes)

        snapshots = _load_snapshots(
            conn,
            event["game_id"],
            pre_start.isoformat(),
            post_end.isoformat(),
        )

        if not snapshots:
            continue

        grouped = _group_snapshots(snapshots)
        for key, rows in grouped.items():
            baseline = _find_baseline(rows, event_time)
            if not baseline:
                continue

            post_rows = [r for r in rows if r["time"] > event_time]
            if len(post_rows) < min_snapshot_count:
                continue

            impact = _find_impact(baseline, post_rows)
            if not impact:
                continue

            market, side, line, provider = key
            impacts.append({
                "event_id": event["event_id"],
                "game_id": event["game_id"],
                "market": market,
                "side": side,
                "line": line,
                "provider": provider,
                "baseline_prob": baseline["prob"],
                "baseline_time": baseline["time"].isoformat(),
                "max_prob": impact["max_prob"],
                "min_prob": impact["min_prob"],
                "impact_prob": impact["impact_prob"],
                "impact_delta": impact["impact_delta"],
                "impact_direction": impact["impact_direction"],
                "impact_time": impact["impact_time"].isoformat(),
                "snapshot_count": impact["snapshot_count"],
                "computed_at": utc_now_iso(),
                "config_json": config_json,
            })

    if impacts:
        _store_impacts(conn, impacts)

    return impacts


def _select_event_time(event_row: sqlite3.Row) -> datetime | None:
    for field in ("published_at", "scraped_at", "extracted_at"):
        dt = parse_iso_timestamp(event_row[field])
        if dt is not None:
            return dt
    return None


def _load_snapshots(
    conn: sqlite3.Connection,
    game_id: str,
    start_iso: str,
    end_iso: str,
) -> list[dict[str, Any]]:
    query = """
        SELECT market, side, line, provider, devigged_prob, implied_prob, snapshot_time
        FROM market_history
        WHERE game_id = ?
        AND snapshot_time >= ?
        AND snapshot_time <= ?
        ORDER BY snapshot_time ASC
    """
    cursor = conn.execute(query, (game_id, start_iso, end_iso))
    rows = []
    for row in cursor.fetchall():
        prob = row["devigged_prob"] if row["devigged_prob"] is not None else row["implied_prob"]
        time = parse_iso_timestamp(row["snapshot_time"])
        if prob is None or time is None:
            continue
        rows.append({
            "market": row["market"],
            "side": row["side"],
            "line": row["line"],
            "provider": row["provider"],
            "prob": float(prob),
            "time": time,
        })
    return rows


def _group_snapshots(rows: list[dict[str, Any]]) -> dict[tuple[str, str, float, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, float, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["market"], row["side"], float(row["line"]), row["provider"])
        grouped.setdefault(key, []).append(row)
    return grouped


def _find_baseline(rows: list[dict[str, Any]], event_time: datetime) -> dict[str, Any] | None:
    baseline = None
    for row in rows:
        if row["time"] <= event_time:
            baseline = row
        else:
            break
    return baseline


def _find_impact(baseline: dict[str, Any], post_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not post_rows:
        return None

    max_row = max(post_rows, key=lambda r: r["prob"])
    min_row = min(post_rows, key=lambda r: r["prob"])

    delta_max = max_row["prob"] - baseline["prob"]
    delta_min = min_row["prob"] - baseline["prob"]

    if abs(delta_max) >= abs(delta_min):
        impact_row = max_row
        impact_delta = delta_max
    else:
        impact_row = min_row
        impact_delta = delta_min

    if impact_delta > 0.01:
        direction = "up"
    elif impact_delta < -0.01:
        direction = "down"
    else:
        direction = "stable"

    return {
        "max_prob": max_row["prob"],
        "min_prob": min_row["prob"],
        "impact_prob": impact_row["prob"],
        "impact_delta": impact_delta,
        "impact_direction": direction,
        "impact_time": impact_row["time"],
        "snapshot_count": len(post_rows),
    }


def _store_impacts(conn: sqlite3.Connection, impacts: list[dict[str, Any]]) -> None:
    upsert_rows(
        conn,
        "event_market_impacts",
        ["event_id", "provider", "market", "side", "line"],
        [
            "game_id",
            "baseline_prob",
            "baseline_time",
            "max_prob",
            "min_prob",
            "impact_prob",
            "impact_delta",
            "impact_direction",
            "impact_time",
            "snapshot_count",
            "computed_at",
            "config_json",
        ],
        impacts,
    )
    conn.commit()
