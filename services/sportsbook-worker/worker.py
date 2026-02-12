"""Sportsbook Worker Service - Polls Odds API for sportsbook odds."""
from __future__ import annotations

import logging
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils import (
    canonical_game_id, devig, devig_market, get_source_config, init_db,
    insert_history, load_config, normalize_player, normalize_team,
    odds_to_prob, update_source_metadata, upsert_rows, utc_now_iso, within_window,
)

SERVICE_NAME = "sportsbook-worker"
SOURCE_NAME = "odds_api"
DEFAULT_POLL_INTERVAL = 300

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(SERVICE_NAME)


def api_request(session, url, params=None, timeout=20, retries=3, context=""):
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json(), 200
            if 400 <= resp.status_code < 500:
                return None, resp.status_code
            if attempt < retries:
                time.sleep(1.5 ** attempt)
        except Exception:
            if attempt < retries:
                time.sleep(1.5 ** attempt)
    return None, 0


def fetch_games(session, api_key, config):
    games, groups, now = {}, defaultdict(list), utc_now_iso()
    source_cfg = get_source_config(config, SOURCE_NAME)
    delay = source_cfg.get("request_delay_seconds", 0.5)
    
    for sport in config.get("sports", []):
        for market_type in config.get("markets", []):
            url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
            params = {
                "apiKey": api_key, "regions": ",".join(config.get("regions", ["us"])),
                "markets": market_type, "oddsFormat": "decimal", "dateFormat": "iso",
            }
            data, status = api_request(session, url, params=params, context=f"{sport}/{market_type}")
            data = data if data and status == 200 else []
            logger.info(f"  {sport}/{market_type}: {len(data)} games")
            time.sleep(delay)
            
            for game in data:
                home, away, commence = game.get("home_team"), game.get("away_team"), game.get("commence_time")
                if not all([home, away, commence]) or not within_window(commence, config.get("bettable_window_days", 14)):
                    continue
                game_id = canonical_game_id(game["sport_key"], home, away, commence[:10])
                games[game_id] = {"game_id": game_id, "league": game["sport_key"], "commence_time": commence,
                                  "home_team": home, "away_team": away, "last_refreshed": now}
                
                for book in game.get("bookmakers", []):
                    if book["key"] not in config.get("books", []):
                        continue
                    for mkt in book.get("markets", []):
                        if mkt["key"] != market_type:
                            continue
                        for outcome in mkt.get("outcomes", []):
                            row = _parse_outcome(outcome, mkt, game, book, game_id, home, away, now)
                            if row:
                                groups[(game_id, market_type, row["line"], row["provider"])].append(row)
    
    return games, [r for group in groups.values() for r in devig_market(group)]


def _parse_outcome(outcome, market, game, book, game_id, home, away, now):
    name, price = outcome.get("name"), outcome.get("price")
    if name is None or price is None:
        return None
    
    market_key = market["key"]
    if market_key == "totals":
        side, line = name.strip().lower(), outcome.get("point")
        if side not in {"over", "under"}:
            return None
    else:
        normalized = normalize_team(name)
        if normalized == normalize_team(home) or normalize_team(home) in normalized:
            side = "home"
        elif normalized == normalize_team(away) or normalize_team(away) in normalized:
            side = "away"
        elif normalized in {"draw", "tie", "x"}:
            side = "draw"
        else:
            return None
        line = outcome.get("point", 0.0) if market_key == "spreads" else 0.0
    
    if line is None:
        return None
    
    return {"game_id": game_id, "market": market_key, "side": side, "line": float(line),
            "source": SOURCE_NAME, "provider": book["key"], "player": "", "price": price,
            "implied_prob": odds_to_prob(price), "provider_updated_at": book.get("last_update", now),
            "last_refreshed": now, "snapshot_time": now, "source_event_id": game.get("id"),
            "source_market_id": None, "outcome": name}


def run_once():
    load_dotenv()
    config = load_config()
    
    source_cfg = get_source_config(config, SOURCE_NAME)
    if not source_cfg.get("enabled", True):
        logger.info("Odds API disabled, skipping")
        return {"status": "disabled"}
    
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        logger.error("ODDS_API_KEY not found")
        return {"status": "error", "message": "Missing API key"}
    
    conn = init_db()
    api_calls = 0
    
    logger.info("=" * 60)
    logger.info("SPORTSBOOK WORKER - Starting poll")
    
    with requests.Session() as session:
        games, rows = fetch_games(session, api_key, config)
        api_calls += len(config.get("sports", [])) * len(config.get("markets", []))
        
        if games:
            for g in games.values():
                g["last_refreshed"] = utc_now_iso()
            upsert_rows(conn, "games", ["game_id"], 
                       ["league", "commence_time", "home_team", "away_team", "last_refreshed"], games.values())
        
        if rows:
            upsert_rows(conn, "market_latest",
                   ["game_id", "market", "side", "line", "source", "provider", "player"],
                   ["price", "implied_prob", "devigged_prob", "provider_updated_at", "last_refreshed", "source_event_id", "source_market_id", "outcome"],
                   rows)
            insert_history(conn, rows)
    
    update_source_metadata(conn, SOURCE_NAME, api_calls)
    conn.close()
    
    logger.info(f"COMPLETE: {len(games)} games, {len(rows)} rows, {api_calls} API calls")
    return {"status": "success", "games": len(games), "rows": len(rows), "api_calls": api_calls}


def run_daemon():
    load_dotenv()
    config = load_config()
    interval = get_source_config(config, SOURCE_NAME).get("poll_interval_seconds", DEFAULT_POLL_INTERVAL)
    logger.info(f"Starting daemon with {interval}s interval")
    
    while True:
        try:
            run_once()
        except Exception as e:
            logger.error(f"Poll failed: {e}")
        logger.info(f"Sleeping {interval}s...")
        time.sleep(interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", "-d", action="store_true")
    parser.add_argument("--once", "-1", action="store_true")
    args = parser.parse_args()
    run_daemon() if args.daemon else run_once()
