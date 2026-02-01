from __future__ import annotations

"""
Pull odds data from The Odds API and store normalized rows in SQLite.
"""

import os
import signal
import sqlite3
import time
from datetime import datetime, timezone

import requests
import yaml
from dotenv import load_dotenv

# ------------------
# Constants
# ------------------
API_URL_BASE = "https://api.the-odds-api.com/v4/sports"
API_KEY_ENV = "ODDS_API_KEY"
CONFIG_PATH = "config.yaml"
SCHEMA_PATH = "schema.sql"


def load_config(path: str) -> dict:
    """Load YAML config from disk and return a dict."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def init_db(db_path: str, schema_path: str) -> sqlite3.Connection:
    """Initialize the SQLite DB and apply the schema."""
    conn = sqlite3.connect(db_path)
    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    return conn


def fetch_odds(
    session: requests.Session,
    api_key: str,
    config: dict,
    sport: str,
    market_key: str,
):
    """Fetch odds for a single sport/market. Returns [] if the combo is unsupported."""
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


def make_bet_id(game, market_key, outcome_name):
    """Build a stable bet identifier for a market outcome."""
    date = game["commence_time"][:10]
    league = game["sport_key"]
    outcome = outcome_name.lower().replace(" ", "_")
    return f"{date}_{league}_{outcome}_{market_key}"


def iter_atomic_records(games, allowed_books: set, allowed_markets: set):
    """Yield atomic bet + odds rows from a list of games."""
    for game in games:
        event_id = game["id"]
        event_date = game["commence_time"][:10]
        league = game["sport_key"]

        for book in game.get("bookmakers", []):
            sportsbook = book["key"]
            if sportsbook not in allowed_books:
                continue

            last_update = book.get("last_update")

            for market in book.get("markets", []):
                market_key = market["key"]
                if market_key not in allowed_markets:
                    continue

                for outcome in market.get("outcomes", []):
                    outcome_name = outcome["name"]
                    odds = outcome["price"]
                    bet_id = make_bet_id(game, market_key, outcome_name)

                    bet_row = (
                        bet_id,
                        league,
                        event_id,
                        market_key,
                        outcome_name,
                        event_date,
                    )
                    odds_row = (bet_id, sportsbook, odds, last_update)
                    yield bet_row, odds_row


def ingest() -> None:
    """Main ingest routine: fetch odds and upsert into SQLite."""
    load_dotenv()
    api_key = os.getenv(API_KEY_ENV)
    if not api_key:
        raise SystemExit(f"Missing env var: {API_KEY_ENV}")

    config = load_config(CONFIG_PATH)
    db_path = config["storage"]["database"]
    now = datetime.now(timezone.utc).isoformat()
    polling_cfg = config.get("polling", {})

    if polling_cfg.get("ignore_sigint"):
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    # ------------------
    # Ingest
    # ------------------
    conn = init_db(db_path, SCHEMA_PATH)
    try:
        cur = conn.cursor()
        # Snapshot semantics: clear current_odds once per ingest run.
        cur.execute("DELETE FROM current_odds;")
        allowed_books = set(config["books"])
        allowed_markets = set(config["markets"])
        seen_current = set()

        with requests.Session() as session:
            delay_seconds = polling_cfg.get("request_delay_seconds", 0)
            total_requests = len(config["sports"]) * len(config["markets"])
            request_count = 0

            for sport in config["sports"]:
                for requested_market in config["markets"]:
                    request_count += 1
                    if polling_cfg.get("show_progress", True):
                        print(
                            f"Request {request_count}/{total_requests}: "
                            f"sport={sport} market={requested_market}"
                        )
                    games = fetch_odds(session, api_key, config, sport, requested_market)
                    if delay_seconds:
                        time.sleep(delay_seconds)

                    bet_rows = []
                    odds_rows = []

                    for bet_row, odds_row in iter_atomic_records(
                        games, allowed_books, allowed_markets
                    ):
                        bet_rows.append(bet_row)
                        current_key = (odds_row[0], odds_row[1])
                        if current_key in seen_current:
                            continue
                        seen_current.add(current_key)
                        odds_rows.append(odds_row)

                    if bet_rows:
                        cur.executemany("""
                            INSERT OR IGNORE INTO bets
                            (bet_id, league, event_id, market, outcome, event_date)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, bet_rows)

                    if odds_rows:
                        cur.executemany("""
                            INSERT INTO current_odds
                            (bet_id, sportsbook, odds, last_updated)
                            VALUES (?, ?, ?, ?)
                        """, odds_rows)

        cur.execute("""
            INSERT INTO odds_timeseries (bet_id, best_odds, observed_at)
            SELECT bet_id, MAX(odds), ?
            FROM current_odds
            GROUP BY bet_id
        """, (now,))

        conn.commit()
    finally:
        conn.close()

    print("Ingestion complete.")


if __name__ == "__main__":
    ingest()
