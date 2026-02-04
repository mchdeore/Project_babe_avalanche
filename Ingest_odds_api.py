from __future__ import annotations

"""Pull Odds API data and upsert latest rows into SQLite."""

import os
import signal
import time

import requests
from dotenv import load_dotenv

from ingest_utils import (
    canonical_game_id,
    implied_prob_from_price,
    load_config,
    normalize_team,
    init_db,
    upsert_rows,
    utc_now_iso,
    within_bettable_window,
)

# ------------------
# Constants
# ------------------
API_URL_BASE = "https://api.the-odds-api.com/v4/sports"
API_KEY_ENV = "ODDS_API_KEY"
CONFIG_PATH = "config.yaml"
SCHEMA_PATH = "schema.sql"


def fetch_odds(
    session: requests.Session,
    api_key: str,
    config: dict,
    sport: str,
    market_key: str,
):
    """Fetch odds for a single sport/market; returns [] if unsupported."""
    url = f"{API_URL_BASE}/{sport}/odds"
    params = {
        "apiKey": api_key,
        "regions": ",".join(config["regions"]),
        "markets": market_key,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    for attempt in range(2):
        resp = session.get(url, params=params, timeout=20)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in {404, 422}:
            return []
        if resp.status_code == 429 and attempt == 0:
            retry_after = resp.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else 30
            time.sleep(wait_seconds)
            continue

        body = resp.text.strip()
        raise SystemExit(
            f"Odds API error {resp.status_code} for {sport}/{market_key}: {body}"
        )

    raise SystemExit("Odds API rate-limited; try again later.")


def match_side(outcome_name: str, home_team: str, away_team: str) -> str | None:
    """Map outcome names to home/away/draw sides."""
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


def line_value(market_key: str, point: float | None) -> float | None:
    """Normalize line values so the primary key is never NULL."""
    if market_key == "h2h":
        return 0.0
    if point is None:
        return None
    try:
        return float(point)
    except (TypeError, ValueError):
        return None


def ingest() -> None:
    """Fetch odds and upsert the latest rows into SQLite."""
    load_dotenv()
    api_key = os.getenv(API_KEY_ENV)
    if not api_key:
        raise SystemExit(f"Missing env var: {API_KEY_ENV}")

    config = load_config(CONFIG_PATH)
    db_path = config["storage"]["database"]
    polling_cfg = config.get("polling", {})
    now = utc_now_iso()
    window_days = int(config.get("bettable_window_days", 5))

    if polling_cfg.get("ignore_sigint"):
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    # ------------------
    # Ingest
    # ------------------
    conn = init_db(db_path, SCHEMA_PATH)
    try:
        allowed_books = set(config["books"])
        allowed_markets = set(config["markets"])

        games_rows: dict[str, dict] = {}
        odds_rows: list[dict] = []
        total_games = 0
        total_requests = len(config["sports"]) * len(config["markets"])
        request_count = 0
        seen_books: set[str] = set()
        matched_books: set[str] = set()
        price_row_count = 0
        skipped_outside_window = 0

        with requests.Session() as session:
            delay_seconds = polling_cfg.get("request_delay_seconds", 0)

            for sport in config["sports"]:
                for requested_market in config["markets"]:
                    request_count += 1
                    if polling_cfg.get("show_progress", True):
                        print(
                            f"Request {request_count}/{total_requests}: "
                            f"sport={sport} market={requested_market}"
                        )
                    games = fetch_odds(session, api_key, config, sport, requested_market)
                    if polling_cfg.get("show_progress", True):
                        print(f"  -> games returned: {len(games)}")
                    total_games += len(games)
                    if delay_seconds:
                        time.sleep(delay_seconds)

                    for game in games:
                        home_team = game.get("home_team")
                        away_team = game.get("away_team")
                        commence_time = game.get("commence_time")
                        event_id = game.get("id")
                        league = game.get("sport_key")

                        if not home_team or not away_team or not commence_time or not league:
                            continue

                        if not within_bettable_window(commence_time, window_days):
                            skipped_outside_window += 1
                            continue

                        date_str = commence_time[:10]
                        game_id = canonical_game_id(league, home_team, away_team, date_str)

                        games_rows[game_id] = {
                            "game_id": game_id,
                            "league": league,
                            "commence_time": commence_time,
                            "home_team": home_team,
                            "away_team": away_team,
                            "last_refreshed": now,
                        }

                        for book in game.get("bookmakers", []):
                            sportsbook = book.get("key")
                            if sportsbook:
                                seen_books.add(sportsbook)
                            if sportsbook not in allowed_books:
                                continue
                            matched_books.add(sportsbook)
                            provider_updated_at = book.get("last_update") or now

                            for market in book.get("markets", []):
                                market_key = market.get("key")
                                if market_key not in allowed_markets:
                                    continue

                                for outcome in market.get("outcomes", []):
                                    outcome_name = outcome.get("name")
                                    odds = outcome.get("price")
                                    point = outcome.get("point")

                                    if outcome_name is None or odds is None:
                                        continue
                                    implied_prob = implied_prob_from_price(odds)

                                    line = line_value(market_key, point)
                                    if line is None:
                                        continue

                                    if market_key in {"h2h", "spreads"}:
                                        side = match_side(outcome_name, home_team, away_team)
                                        if side == "home":
                                            odds_rows.append(
                                                {
                                                    "game_id": game_id,
                                                    "market": market_key,
                                                    "side": "home",
                                                    "line": line,
                                                    "source": "odds",
                                                    "provider": sportsbook,
                                                    "price": odds,
                                                    "implied_prob": implied_prob,
                                                    "provider_updated_at": provider_updated_at,
                                                    "last_refreshed": now,
                                                    "source_event_id": event_id,
                                                    "source_market_id": None,
                                                    "outcome": outcome_name,
                                                }
                                            )
                                            price_row_count += 1
                                        elif side == "away":
                                            odds_rows.append(
                                                {
                                                    "game_id": game_id,
                                                    "market": market_key,
                                                    "side": "away",
                                                    "line": line,
                                                    "source": "odds",
                                                    "provider": sportsbook,
                                                    "price": odds,
                                                    "implied_prob": implied_prob,
                                                    "provider_updated_at": provider_updated_at,
                                                    "last_refreshed": now,
                                                    "source_event_id": event_id,
                                                    "source_market_id": None,
                                                    "outcome": outcome_name,
                                                }
                                            )
                                            price_row_count += 1
                                        elif side == "draw":
                                            odds_rows.append(
                                                {
                                                    "game_id": game_id,
                                                    "market": market_key,
                                                    "side": "draw",
                                                    "line": line,
                                                    "source": "odds",
                                                    "provider": sportsbook,
                                                    "price": odds,
                                                    "implied_prob": implied_prob,
                                                    "provider_updated_at": provider_updated_at,
                                                    "last_refreshed": now,
                                                    "source_event_id": event_id,
                                                    "source_market_id": None,
                                                    "outcome": outcome_name,
                                                }
                                            )
                                            price_row_count += 1
                                    elif market_key == "totals":
                                        outcome_lower = str(outcome_name).strip().lower()
                                        if outcome_lower == "over":
                                            odds_rows.append(
                                                {
                                                    "game_id": game_id,
                                                    "market": market_key,
                                                    "side": "over",
                                                    "line": line,
                                                    "source": "odds",
                                                    "provider": sportsbook,
                                                    "price": odds,
                                                    "implied_prob": implied_prob,
                                                    "provider_updated_at": provider_updated_at,
                                                    "last_refreshed": now,
                                                    "source_event_id": event_id,
                                                    "source_market_id": None,
                                                    "outcome": outcome_name,
                                                }
                                            )
                                            price_row_count += 1
                                        elif outcome_lower == "under":
                                            odds_rows.append(
                                                {
                                                    "game_id": game_id,
                                                    "market": market_key,
                                                    "side": "under",
                                                    "line": line,
                                                    "source": "odds",
                                                    "provider": sportsbook,
                                                    "price": odds,
                                                    "implied_prob": implied_prob,
                                                    "provider_updated_at": provider_updated_at,
                                                    "last_refreshed": now,
                                                    "source_event_id": event_id,
                                                    "source_market_id": None,
                                                    "outcome": outcome_name,
                                                }
                                            )
                                            price_row_count += 1

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
            rows=odds_rows,
        )

        conn.commit()
    finally:
        conn.close()

    print("Odds API summary:")
    print(f"  total games: {total_games}")
    print(f"  price rows written: {price_row_count}")
    print(f"  skipped outside window: {skipped_outside_window}")
    print(f"  books seen: {sorted(seen_books)}")
    print(f"  books matched config: {sorted(matched_books)}")
    print("Odds API ingestion complete.")


if __name__ == "__main__":
    ingest()
