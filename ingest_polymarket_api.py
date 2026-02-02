from __future__ import annotations

"""Pull Polymarket sports data and store snapshot rows in SQLite."""

import json
import re
import time
from typing import Any, Iterable

import requests

from ingest_utils import (
    canonical_game_id,
    load_config,
    normalize_team,
    init_db,
    upsert_rows,
    utc_now_iso,
)

API_BASE = "https://gamma-api.polymarket.com"
CONFIG_PATH = "config.yaml"
SCHEMA_PATH = "schema.sql"
USER_AGENT = "Mozilla/5.0"


def fetch_json(session: requests.Session, path: str, params: dict | None = None) -> Any:
    url = f"{API_BASE}{path}"
    resp = session.get(url, params=params, timeout=20, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return resp.json()


def parse_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return []


def match_side(outcome_name: str, home_team: str, away_team: str) -> str | None:
    outcome_norm = normalize_team(outcome_name)
    if outcome_norm == normalize_team(home_team):
        return "home"
    if outcome_norm == normalize_team(away_team):
        return "away"
    return None


def has_vs(text: str) -> bool:
    text_lower = text.lower()
    if " @ " in text_lower:
        return True
    if re.search(r"\bvs\.?\b", text_lower):
        return True
    if " v " in text_lower:
        return True
    return False


def parse_matchup(text: str) -> tuple[str, str] | None:
    text = text.strip()
    for sep in [" vs. ", " vs ", " @ ", " v "]:
        if sep in text:
            left, right = text.split(sep, 1)
            left = left.strip()
            right = right.strip()
            if left and right:
                return left, right
    return None


def chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def ingest() -> None:
    config = load_config(CONFIG_PATH)
    pm_cfg = config.get("polymarket", {})
    sports = pm_cfg.get("sports", [])
    if not sports:
        print("Polymarket ingest skipped: no sports configured.")
        return

    db_path = config["storage"]["database"]
    reset_snapshot = config.get("storage", {}).get("reset_snapshot", False)
    now = utc_now_iso()
    tag_id = pm_cfg.get("tag_id", 100639)
    limit = int(pm_cfg.get("limit", 200))
    delay_seconds = pm_cfg.get("request_delay_seconds", 0)
    league_aliases = pm_cfg.get("league_aliases", {})

    conn = init_db(db_path, SCHEMA_PATH)
    try:
        cur = conn.cursor()
        pm_columns = [
            "pm_home_price",
            "pm_away_price",
            "pm_over_price",
            "pm_under_price",
            "pm_market_id",
            "pm_event_id",
            "pm_updated_at",
        ]
        if reset_snapshot:
            cur.execute(
                "UPDATE game_market_current SET "
                + ", ".join([f'"{col}"=NULL' for col in pm_columns])
            )

        games_rows: dict[str, dict] = {}
        market_rows: dict[tuple[str, str], dict] = {}

        total_events = 0
        matched_markets = 0
        market_row_count = 0
        skipped_yes_no = 0
        skipped_non_matchup = 0
        with requests.Session() as session:
            sports_meta = fetch_json(session, "/sports")
            meta_by_code = {item.get("sport"): item for item in sports_meta}

            for sport_code in sports:
                meta = meta_by_code.get(sport_code)
                if not meta:
                    print(f"Polymarket: unknown sport code '{sport_code}', skipping.")
                    continue

                series_id = meta.get("series")
                ordering = meta.get("ordering")
                league_key = league_aliases.get(sport_code, sport_code)

                offset = 0
                while True:
                    params = {
                        "series_id": series_id,
                        "tag_id": tag_id,
                        "active": True,
                        "closed": False,
                        "limit": limit,
                        "offset": offset,
                    }
                    events = fetch_json(session, "/events", params=params)
                    if not events:
                        break
                    total_events += len(events)

                    for event in events:
                        start_date = event.get("startDate")
                        if not start_date:
                            continue
                        date_str = start_date[:10]

                        markets = event.get("markets", [])
                        for market in markets:
                            outcomes = parse_list(market.get("outcomes"))
                            prices = parse_list(market.get("outcomePrices"))
                            if len(outcomes) != len(prices) or len(outcomes) < 2:
                                continue

                            outcome_lowers = {str(o).strip().lower() for o in outcomes}
                            if outcome_lowers == {"yes", "no"}:
                                skipped_yes_no += 1
                                continue

                            title = event.get("title") or ""
                            question = market.get("question") or ""
                            if not (has_vs(question) or has_vs(title)):
                                skipped_non_matchup += 1
                                continue

                            # Detect totals markets (Over/Under). Default to h2h.
                            if outcome_lowers == {"over", "under"}:
                                market_key = "totals"
                            else:
                                market_key = "h2h"

                            # Only handle 2-outcome markets for now.
                            if len(outcomes) != 2:
                                continue

                            if market_key == "totals":
                                matchup = parse_matchup(question) or parse_matchup(title)
                                if not matchup:
                                    continue
                                team_a, team_b = matchup
                            else:
                                team_a, team_b = outcomes[0], outcomes[1]
                            if ordering == "away":
                                home_team, away_team = team_b, team_a
                            else:
                                home_team, away_team = team_a, team_b

                            game_id = canonical_game_id(
                                league_key, home_team, away_team, date_str
                            )

                            games_rows[game_id] = {
                                "game_id": game_id,
                                "league": league_key,
                                "polymarket_event_id": event.get("id"),
                                "commence_time": start_date,
                                "home_team": home_team,
                                "away_team": away_team,
                                "last_updated": now,
                            }

                            row_key = (game_id, market_key)
                            if row_key not in market_rows:
                                market_row_count += 1
                                market_rows[row_key] = {
                                    "game_id": game_id,
                                    "market": market_key,
                                    "pm_market_id": market.get("id"),
                                    "pm_event_id": event.get("id"),
                                    "pm_updated_at": market.get("updatedAt")
                                    or event.get("updatedAt")
                                    or now,
                                }
                            row = market_rows[row_key]
                            matched_markets += 1

                            for outcome, price in zip(outcomes, prices):
                                try:
                                    price_val = float(price)
                                except (TypeError, ValueError):
                                    continue

                                if market_key == "totals":
                                    outcome_lower = str(outcome).strip().lower()
                                    if outcome_lower == "over":
                                        row["pm_over_price"] = price_val
                                    elif outcome_lower == "under":
                                        row["pm_under_price"] = price_val
                                else:
                                    side = match_side(outcome, home_team, away_team)
                                    if side == "home":
                                        row["pm_home_price"] = price_val
                                    elif side == "away":
                                        row["pm_away_price"] = price_val

                    if len(events) < limit:
                        break
                    offset += limit
                    if delay_seconds:
                        time.sleep(delay_seconds)

        if games_rows:
            existing_ids = list(games_rows.keys())
            for chunk in chunked(existing_ids, 900):
                placeholders = ",".join(["?"] * len(chunk))
                rows = conn.execute(
                    f"""
                    SELECT game_id, league, commence_time, home_team, away_team
                    FROM games_current
                    WHERE game_id IN ({placeholders})
                    """,
                    chunk,
                ).fetchall()
                for game_id, league, commence_time, home_team, away_team in rows:
                    row = games_rows.get(game_id)
                    if not row:
                        continue
                    if league is not None:
                        row["league"] = league
                    if commence_time is not None:
                        row["commence_time"] = commence_time
                    if home_team is not None:
                        row["home_team"] = home_team
                    if away_team is not None:
                        row["away_team"] = away_team

        upsert_rows(
            conn,
            table="games_current",
            key_cols=["game_id"],
            update_cols=[
                "league",
                "polymarket_event_id",
                "commence_time",
                "home_team",
                "away_team",
                "last_updated",
            ],
            rows=games_rows.values(),
        )

        upsert_rows(
            conn,
            table="game_market_current",
            key_cols=["game_id", "market"],
            update_cols=pm_columns,
            rows=market_rows.values(),
        )

        conn.commit()
    finally:
        conn.close()

    print("Polymarket summary:")
    print(f"  total events fetched: {total_events}")
    print(f"  matched markets: {matched_markets}")
    print(f"  market rows written: {market_row_count}")
    print(f"  skipped yes/no markets: {skipped_yes_no}")
    print(f"  skipped non-matchup markets: {skipped_non_matchup}")
    print("Polymarket ingestion complete.")


if __name__ == "__main__":
    ingest()
