"""Pull Odds API data and upsert latest rows into SQLite."""
from __future__ import annotations

import os
import time

import requests
from dotenv import load_dotenv

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

API_BASE = "https://api.the-odds-api.com/v4/sports"
CONFIG_PATH = "config.yaml"
SCHEMA_PATH = "schema.sql"


def fetch_odds(session: requests.Session, api_key: str, config: dict, sport: str, market: str):
    """Fetch odds for a sport/market from The Odds API."""
    url = f"{API_BASE}/{sport}/odds"
    params = {
        "apiKey": api_key,
        "regions": ",".join(config["regions"]),
        "markets": market,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    resp = session.get(url, params=params, timeout=20)
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code in {404, 422}:
        return []
    if resp.status_code == 429:
        wait = int(resp.headers.get("Retry-After", 30))
        time.sleep(wait)
        resp = session.get(url, params=params, timeout=20)
        if resp.status_code == 200:
            return resp.json()
    raise SystemExit(f"Odds API error {resp.status_code}: {resp.text.strip()}")


def match_side(outcome_name: str, home: str, away: str) -> str | None:
    """Map outcome name to home/away/draw."""
    outcome = normalize_team(outcome_name)
    home_n, away_n = normalize_team(home), normalize_team(away)
    if outcome == home_n or home_n in outcome:
        return "home"
    if outcome == away_n or away_n in outcome:
        return "away"
    if outcome in {"draw", "tie", "x"}:
        return "draw"
    return None


def ingest() -> None:
    """Fetch odds and upsert into SQLite."""
    load_dotenv()
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        raise SystemExit("Missing ODDS_API_KEY in .env")

    config = load_config(CONFIG_PATH)
    db_path = config["storage"]["database"]
    window_days = config.get("bettable_window_days", 30)
    delay = config.get("polling", {}).get("request_delay_seconds", 0)
    allowed_books = set(config["books"])
    now = utc_now_iso()

    conn = init_db(db_path, SCHEMA_PATH)
    games_rows, odds_rows = {}, []

    try:
        with requests.Session() as session:
            for sport in config["sports"]:
                for market_key in config["markets"]:
                    print(f"Fetching {sport}/{market_key}...")
                    games = fetch_odds(session, api_key, config, sport, market_key)
                    print(f"  {len(games)} games")
                    if delay:
                        time.sleep(delay)

                    for game in games:
                        home, away = game.get("home_team"), game.get("away_team")
                        commence = game.get("commence_time")
                        if not all([home, away, commence]):
                            continue
                        if not within_bettable_window(commence, window_days):
                            continue

                        game_id = canonical_game_id(game["sport_key"], home, away, commence[:10])
                        games_rows[game_id] = {
                            "game_id": game_id,
                            "league": game["sport_key"],
                            "commence_time": commence,
                            "home_team": home,
                            "away_team": away,
                            "last_refreshed": now,
                        }

                        for book in game.get("bookmakers", []):
                            if book["key"] not in allowed_books:
                                continue
                            for mkt in book.get("markets", []):
                                if mkt["key"] not in config["markets"]:
                                    continue
                                for outcome in mkt.get("outcomes", []):
                                    name, price = outcome.get("name"), outcome.get("price")
                                    if name is None or price is None:
                                        continue

                                    # Determine side and line
                                    if mkt["key"] == "totals":
                                        side = name.strip().lower()
                                        if side not in {"over", "under"}:
                                            continue
                                        line = outcome.get("point")
                                    else:
                                        side = match_side(name, home, away)
                                        if not side:
                                            continue
                                        line = outcome.get("point", 0.0) if mkt["key"] == "spreads" else 0.0

                                    if line is None:
                                        continue

                                    odds_rows.append({
                                        "game_id": game_id,
                                        "market": mkt["key"],
                                        "side": side,
                                        "line": float(line),
                                        "source": "odds",
                                        "provider": book["key"],
                                        "price": price,
                                        "implied_prob": implied_prob(price),
                                        "provider_updated_at": book.get("last_update", now),
                                        "last_refreshed": now,
                                        "source_event_id": game.get("id"),
                                        "source_market_id": None,
                                        "outcome": name,
                                    })

        upsert_rows(conn, "games", ["game_id"],
                    ["league", "commence_time", "home_team", "away_team", "last_refreshed"],
                    games_rows.values())
        upsert_rows(conn, "market_latest",
                    ["game_id", "market", "side", "line", "source", "provider"],
                    ["price", "implied_prob", "provider_updated_at", "last_refreshed",
                     "source_event_id", "source_market_id", "outcome"],
                    odds_rows)
        conn.commit()
    finally:
        conn.close()

    print(f"Odds API: {len(games_rows)} games, {len(odds_rows)} price rows")


if __name__ == "__main__":
    ingest()
