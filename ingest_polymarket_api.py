"""Pull Polymarket sports data and upsert latest rows into SQLite."""
from __future__ import annotations

import json
import re
import time

import requests

from ingest_utils import (
    canonical_game_id,
    implied_prob,
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


def fetch_json(session: requests.Session, path: str, params: dict | None = None):
    """Fetch JSON from Polymarket Gamma API."""
    resp = session.get(f"{API_BASE}{path}", params=params, timeout=20,
                       headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    return resp.json()


def parse_list(value) -> list:
    """Parse API fields that may be JSON-encoded strings."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return []


def parse_matchup(text: str) -> tuple[str, str] | None:
    """Parse 'Team A vs Team B' into two teams."""
    for sep in [" vs. ", " vs ", " @ ", " v "]:
        if sep in text:
            left, right = text.split(sep, 1)
            if left.strip() and right.strip():
                return left.strip(), right.strip()
    return None


def extract_line(text: str, require_sign: bool = False) -> float | None:
    """Extract numeric line from outcome text."""
    pattern = r"([+-]\d+(?:\.\d+)?)" if require_sign else r"(\d+(?:\.\d+)?)"
    match = re.search(pattern, text)
    return float(match.group(1)) if match else None


def classify_market(outcomes: list[str]) -> str:
    """Classify market type from outcomes."""
    lowers = {str(o).strip().lower() for o in outcomes}
    if lowers == {"over", "under"}:
        return "totals"
    if lowers == {"yes", "no"}:
        return "binary"
    if any(extract_line(str(o), require_sign=True) is not None for o in outcomes):
        return "spreads"
    return "h2h"


def infer_matchup(title: str, question: str, outcomes: list[str]) -> tuple[str, str] | None:
    """Infer team names from title/question or outcomes."""
    matchup = parse_matchup(question) or parse_matchup(title)
    if matchup:
        return matchup

    # Try to extract teams from outcome labels
    candidates = []
    skip = {"over", "under", "yes", "no", "draw", "tie", "x"}
    for outcome in outcomes:
        text = str(outcome).strip()
        if text.lower() in skip:
            continue
        cleaned = re.sub(r"[+-]\d+(?:\.\d+)?", "", text).strip()
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)
    return (candidates[0], candidates[1]) if len(candidates) >= 2 else None


def match_side(outcome: str, home: str, away: str) -> str | None:
    """Map outcome to home/away/draw."""
    o = normalize_team(outcome)
    h, a = normalize_team(home), normalize_team(away)
    if o == h or h in o:
        return "home"
    if o == a or a in o:
        return "away"
    if o in {"draw", "tie", "x"}:
        return "draw"
    return None


def ingest() -> None:
    """Fetch Polymarket markets and upsert into SQLite."""
    config = load_config(CONFIG_PATH)
    pm_cfg = config.get("polymarket", {})
    sports = pm_cfg.get("sports", [])
    if not sports:
        print("Polymarket: no sports configured, skipping.")
        return

    db_path = config["storage"]["database"]
    window_days = config.get("bettable_window_days", 30)
    tag_id = pm_cfg.get("tag_id", 100639)
    limit = pm_cfg.get("limit", 200)
    delay = pm_cfg.get("request_delay_seconds", 0)
    league_aliases = pm_cfg.get("league_aliases", {})
    allowed_markets = set(pm_cfg.get("markets") or config.get("markets", []))
    now = utc_now_iso()

    conn = init_db(db_path, SCHEMA_PATH)
    games_rows, pm_rows = {}, []

    try:
        with requests.Session() as session:
            sports_meta = {s.get("sport"): s for s in fetch_json(session, "/sports")}

            for sport_code in sports:
                meta = sports_meta.get(sport_code)
                if not meta:
                    print(f"Polymarket: unknown sport '{sport_code}'")
                    continue

                league_key = league_aliases.get(sport_code, sport_code)
                ordering = meta.get("ordering")
                offset = 0

                while True:
                    events = fetch_json(session, "/events", {
                        "series_id": meta.get("series"),
                        "tag_id": tag_id,
                        "active": True,
                        "closed": False,
                        "limit": limit,
                        "offset": offset,
                    })
                    if not events:
                        break

                    for event in events:
                        start_date = event.get("startDate")
                        if not start_date:
                            continue
                        if not within_bettable_window(start_date[:10], window_days):
                            continue

                        for market in event.get("markets", []):
                            if market.get("active") is False or market.get("closed") is True:
                                continue

                            outcomes = parse_list(market.get("outcomes"))
                            prices = parse_list(market.get("outcomePrices"))
                            if len(outcomes) != len(prices) or len(outcomes) < 2:
                                continue

                            market_type = classify_market(outcomes)
                            if market_type == "binary":
                                continue
                            if allowed_markets and market_type not in allowed_markets:
                                continue

                            matchup = infer_matchup(event.get("title", ""),
                                                    market.get("question", ""), outcomes)
                            if not matchup:
                                continue

                            team_a, team_b = matchup
                            home, away = (team_b, team_a) if ordering == "away" else (team_a, team_b)
                            game_id = canonical_game_id(league_key, home, away, start_date[:10])

                            games_rows[game_id] = {
                                "game_id": game_id,
                                "league": league_key,
                                "commence_time": start_date,
                                "home_team": home,
                                "away_team": away,
                                "last_refreshed": now,
                            }

                            updated_at = market.get("updatedAt") or event.get("updatedAt") or now

                            for outcome_name, price_str in zip(outcomes, prices):
                                try:
                                    price = float(price_str)
                                except (TypeError, ValueError):
                                    continue

                                outcome_text = str(outcome_name)

                                if market_type == "totals":
                                    side = outcome_text.strip().lower()
                                    if side not in {"over", "under"}:
                                        continue
                                    line = extract_line(outcome_text)
                                elif market_type == "spreads":
                                    side = match_side(outcome_text, home, away)
                                    if side not in {"home", "away"}:
                                        continue
                                    line = extract_line(outcome_text, require_sign=True)
                                else:
                                    side = match_side(outcome_text, home, away)
                                    if not side:
                                        continue
                                    line = 0.0

                                if line is None:
                                    continue

                                pm_rows.append({
                                    "game_id": game_id,
                                    "market": market_type,
                                    "side": side,
                                    "line": float(line),
                                    "source": "polymarket",
                                    "provider": "polymarket",
                                    "price": price,
                                    "implied_prob": implied_prob(price),
                                    "provider_updated_at": updated_at,
                                    "last_refreshed": now,
                                    "source_event_id": event.get("id"),
                                    "source_market_id": market.get("id"),
                                    "outcome": outcome_text,
                                })

                    if len(events) < limit:
                        break
                    offset += limit
                    if delay:
                        time.sleep(delay)

        upsert_rows(conn, "games", ["game_id"],
                    ["league", "commence_time", "home_team", "away_team", "last_refreshed"],
                    games_rows.values())
        upsert_rows(conn, "market_latest",
                    ["game_id", "market", "side", "line", "source", "provider"],
                    ["price", "implied_prob", "provider_updated_at", "last_refreshed",
                     "source_event_id", "source_market_id", "outcome"],
                    pm_rows)
        conn.commit()
    finally:
        conn.close()

    print(f"Polymarket: {len(games_rows)} games, {len(pm_rows)} price rows")


if __name__ == "__main__":
    ingest()
