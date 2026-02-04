from __future__ import annotations

"""Pull Polymarket sports data and upsert latest rows into SQLite."""

import json
import re
import time
from typing import Any

import requests

from ingest_utils import (
    canonical_game_id,
    implied_prob_from_price,
    init_db,
    load_config,
    normalize_team,
    upsert_rows,
    utc_now_iso,
    within_bettable_window,
)

API_BASE = "https://gamma-api.polymarket.com"
CONFIG_PATH = "config.yaml"
SCHEMA_PATH = "schema.sql"
USER_AGENT = "Mozilla/5.0"
POLYMARKET_PROVIDER = "polymarket"


def fetch_json(session: requests.Session, path: str, params: dict | None = None) -> Any:
    """Fetch JSON from the Polymarket Gamma API."""
    url = f"{API_BASE}{path}"
    resp = session.get(url, params=params, timeout=20, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return resp.json()


def parse_list(value: Any) -> list:
    """Parse list-ish API fields that may be JSON-encoded strings."""
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
    """Map an outcome label to home/away/draw."""
    outcome_norm = normalize_team(outcome_name)
    home_norm = normalize_team(home_team)
    away_norm = normalize_team(away_team)
    if outcome_norm == home_norm or home_norm in outcome_norm:
        return "home"
    if outcome_norm == away_norm or away_norm in outcome_norm:
        return "away"
    if outcome_norm in {"draw", "tie", "x"}:
        return "draw"
    return None


def parse_matchup(text: str) -> tuple[str, str] | None:
    """Parse a matchup string into two teams."""
    text = text.strip()
    for sep in [" vs. ", " vs ", " @ ", " v "]:
        if sep in text:
            left, right = text.split(sep, 1)
            left = left.strip()
            right = right.strip()
            if left and right:
                return left, right
    return None


def parse_line_from_outcome(text: str, require_sign: bool = False) -> float | None:
    """Extract a numeric line from an outcome label."""
    pattern = r"([+-]\d+(?:\.\d+)?)" if require_sign else r"(\d+(?:\.\d+)?)"
    match = re.search(pattern, text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def normalize_line_for_market(market_type: str, line: float | None) -> float | None:
    """Ensure the line value is usable as a primary key component."""
    if market_type == "h2h":
        return 0.0
    return line


def strip_line_from_outcome(text: str) -> str:
    """Remove spread/total line values from an outcome label."""
    cleaned = re.sub(r"[+-]\d+(?:\.\d+)?", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def classify_market(outcomes: list[str]) -> str:
    """Classify market type based on outcomes."""
    outcome_lowers = {str(o).strip().lower() for o in outcomes}
    if outcome_lowers == {"over", "under"}:
        return "totals"
    if outcome_lowers == {"yes", "no"}:
        return "binary"
    if any(parse_line_from_outcome(str(o), require_sign=True) is not None for o in outcomes):
        return "spreads"
    return "h2h"


def infer_matchup(title: str, question: str, outcomes: list[str]) -> tuple[str, str] | None:
    """Infer team names from title/question or outcome labels."""
    matchup = parse_matchup(question) or parse_matchup(title)
    if matchup:
        return matchup

    candidates: list[str] = []
    for outcome in outcomes:
        outcome_text = str(outcome).strip()
        outcome_lower = outcome_text.lower()
        if outcome_lower in {"over", "under", "yes", "no", "draw", "tie", "x"}:
            continue
        cleaned = strip_line_from_outcome(outcome_text)
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)
    if len(candidates) >= 2:
        return candidates[0], candidates[1]
    return None


def ingest() -> None:
    """Fetch Polymarket markets and upsert the latest rows into SQLite."""
    config = load_config(CONFIG_PATH)
    pm_cfg = config.get("polymarket", {})
    sports = pm_cfg.get("sports", [])
    if not sports:
        print("Polymarket ingest skipped: no sports configured.")
        return

    db_path = config["storage"]["database"]
    now = utc_now_iso()
    window_days = int(config.get("bettable_window_days", 5))
    debug = bool(pm_cfg.get("debug", False))
    use_date_only = bool(pm_cfg.get("use_date_only", True))
    tag_id = pm_cfg.get("tag_id", 100639)
    limit = int(pm_cfg.get("limit", 200))
    delay_seconds = pm_cfg.get("request_delay_seconds", 0)
    league_aliases = pm_cfg.get("league_aliases", {})
    allowed_markets = set(pm_cfg.get("markets") or config.get("markets", []) or [])

    conn = init_db(db_path, SCHEMA_PATH)
    try:
        games_rows: dict[str, dict] = {}
        pm_rows: list[dict] = []

        total_events = 0
        matched_markets = 0
        price_row_count = 0
        skipped_yes_no = 0
        skipped_non_matchup = 0
        skipped_outside_window = 0
        missing_start_date = 0
        skipped_inactive_market = 0
        skipped_disallowed_market = 0
        sample_start_dates: list[str] = []
        min_start: str | None = None
        max_start: str | None = None
        with requests.Session() as session:
            sports_meta = fetch_json(session, "/sports")
            meta_by_code = {item.get("sport"): item for item in sports_meta}
            if debug:
                print(f"Polymarket debug: now={now} window_days={window_days}")
                print(f"Polymarket debug: sports={sports}")
                if allowed_markets:
                    print(f"Polymarket debug: allowed_markets={sorted(allowed_markets)}")

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
                            missing_start_date += 1
                            continue
                        effective_time = start_date
                        if use_date_only:
                            effective_time = start_date[:10]
                        if debug and len(sample_start_dates) < 5:
                            sample_start_dates.append(start_date)
                        if debug:
                            if min_start is None or start_date < min_start:
                                min_start = start_date
                            if max_start is None or start_date > max_start:
                                max_start = start_date
                        if not within_bettable_window(effective_time, window_days):
                            skipped_outside_window += 1
                            continue
                        date_str = start_date[:10]

                        markets = event.get("markets", [])
                        for market in markets:
                            if market.get("active") is False or market.get("closed") is True:
                                skipped_inactive_market += 1
                                continue
                            outcomes = parse_list(market.get("outcomes"))
                            prices = parse_list(market.get("outcomePrices"))
                            if len(outcomes) != len(prices) or len(outcomes) < 2:
                                continue

                            market_type = classify_market([str(o) for o in outcomes])
                            if market_type == "binary":
                                skipped_yes_no += 1
                                continue
                            if allowed_markets and market_type not in allowed_markets:
                                skipped_disallowed_market += 1
                                continue

                            title = event.get("title") or ""
                            question = market.get("question") or ""
                            matchup = infer_matchup(title, question, outcomes)
                            if not matchup:
                                skipped_non_matchup += 1
                                continue
                            team_a, team_b = matchup
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
                                "commence_time": start_date,
                                "home_team": home_team,
                                "away_team": away_team,
                                "last_refreshed": now,
                            }
                            matched_markets += 1

                            provider_updated_at = (
                                market.get("updatedAt")
                                or event.get("updatedAt")
                                or now
                            )

                            for outcome, price in zip(outcomes, prices):
                                try:
                                    price_val = float(price)
                                except (TypeError, ValueError):
                                    continue

                                implied_prob = implied_prob_from_price(price_val)
                                outcome_text = str(outcome)

                                if market_type == "totals":
                                    outcome_lower = outcome_text.strip().lower()
                                    if outcome_lower not in {"over", "under"}:
                                        continue
                                    side = outcome_lower
                                    line = parse_line_from_outcome(outcome_text)
                                elif market_type == "spreads":
                                    side = match_side(outcome_text, home_team, away_team)
                                    if side not in {"home", "away"}:
                                        continue
                                    line = parse_line_from_outcome(outcome_text, require_sign=True)
                                else:
                                    side = match_side(outcome_text, home_team, away_team)
                                    if side is None:
                                        continue
                                    line = 0.0

                                line = normalize_line_for_market(market_type, line)
                                if line is None:
                                    continue

                                pm_rows.append(
                                    {
                                        "game_id": game_id,
                                        "market": market_type,
                                        "side": side,
                                        "line": line,
                                        "source": "polymarket",
                                        "provider": POLYMARKET_PROVIDER,
                                        "price": price_val,
                                        "implied_prob": implied_prob,
                                        "provider_updated_at": provider_updated_at,
                                        "last_refreshed": now,
                                        "source_event_id": event.get("id"),
                                        "source_market_id": market.get("id"),
                                        "outcome": outcome_text,
                                    }
                                )
                                price_row_count += 1

                    if len(events) < limit:
                        break
                    offset += limit
                    if delay_seconds:
                        time.sleep(delay_seconds)

        upsert_rows(
            conn,
            table="games",
            key_cols=["game_id"],
            update_cols=[
                "league",
                "commence_time",
                "home_team",
                "away_team",
                "last_refreshed",
            ],
            rows=games_rows.values(),
        )

        upsert_rows(
            conn,
            table="market_latest",
            key_cols=["game_id", "market", "side", "line", "source", "provider"],
            update_cols=[
                "price",
                "implied_prob",
                "provider_updated_at",
                "last_refreshed",
                "source_event_id",
                "source_market_id",
                "outcome",
            ],
            rows=pm_rows,
        )

        conn.commit()
    finally:
        conn.close()

    print("Polymarket summary:")
    print(f"  total events fetched: {total_events}")
    print(f"  matched markets: {matched_markets}")
    print(f"  price rows written: {price_row_count}")
    print(f"  skipped outside window: {skipped_outside_window}")
    print(f"  skipped yes/no markets: {skipped_yes_no}")
    print(f"  skipped non-matchup markets: {skipped_non_matchup}")
    print(f"  skipped inactive markets: {skipped_inactive_market}")
    print(f"  skipped disallowed markets: {skipped_disallowed_market}")
    print(f"  missing startDate: {missing_start_date}")
    if debug:
        print(f"  sample startDate values: {sample_start_dates}")
        print(f"  min startDate: {min_start}")
        print(f"  max startDate: {max_start}")
    print("Polymarket ingestion complete.")


if __name__ == "__main__":
    ingest()
