"""Open Market Worker Service - Polls Polymarket and Kalshi with dynamic polling."""
from __future__ import annotations

import datetime
import logging
import os
import re
import sys
import time
from datetime import timezone
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils import (
    get_source_config, init_db, insert_history, load_config, normalize_player,
    normalize_team, update_source_metadata, upsert_rows, utc_now_iso, safe_json,
)

SERVICE_NAME = "openmarket-worker"
DEFAULT_POLL_INTERVAL = 60
LIVE_POLL_INTERVAL = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(SERVICE_NAME)

TEAM_ABBREVS = {
    "hawks": "atl", "celtics": "bos", "nets": "bkn", "hornets": "cha", "bulls": "chi",
    "cavaliers": "cle", "mavericks": "dal", "nuggets": "den", "pistons": "det",
    "warriors": "gsw", "rockets": "hou", "pacers": "ind", "clippers": "lac", "lakers": "lal",
    "grizzlies": "mem", "heat": "mia", "bucks": "mil", "timberwolves": "min",
    "pelicans": "nop", "knicks": "nyk", "thunder": "okc", "magic": "orl", "76ers": "phi",
    "suns": "phx", "trail blazers": "por", "blazers": "por", "kings": "sac", "spurs": "sas",
    "raptors": "tor", "jazz": "uta", "wizards": "was",
}
SPORT_MAP = {"basketball_nba": "nba", "icehockey_nhl": "nhl", "americanfootball_nfl": "nfl"}


def api_request(session, url, params=None, timeout=15, retries=3, context=""):
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


def get_live_games(conn):
    now = datetime.datetime.now(timezone.utc)
    cursor = conn.execute("SELECT game_id, league, commence_time, home_team, away_team FROM games WHERE commence_time IS NOT NULL")
    live = []
    for row in cursor:
        game_id, league, commence_time, home_team, away_team = row
        try:
            if "T" in commence_time:
                dt = datetime.datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
            else:
                dt = datetime.datetime.fromisoformat(commence_time + "T00:00:00+00:00")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            hours_since = (now - dt).total_seconds() / 3600
            if 0 <= hours_since <= 4:
                live.append({"game_id": game_id, "league": league, "home_team": home_team, "away_team": away_team})
        except (ValueError, TypeError):
            continue
    return live


def fetch_polymarket_futures(session, config):
    games, rows, now = {}, [], utc_now_iso()
    delay = get_source_config(config, "polymarket").get("request_delay_seconds", 0.2)
    
    all_markets = []
    for offset in range(0, 500, 100):
        data, status = api_request(session, "https://gamma-api.polymarket.com/markets",
                                   params={"closed": "false", "limit": 100, "offset": offset})
        if status != 200 or not data:
            break
        all_markets.extend(data)
        time.sleep(delay)
    
    logger.info(f"  Fetched {len(all_markets)} Polymarket markets")
    
    FUTURES = {"futures_basketball_nba_championship_winner": ("nba", "NBA Championship")}
    
    for futures_id, (phrase, name) in FUTURES.items():
        games[futures_id] = {"game_id": futures_id, "league": futures_id.replace("futures_", ""),
                           "commence_time": "", "home_team": name, "away_team": "", "last_refreshed": now}
        for market in all_markets:
            question = (market.get("question") or "").lower()
            if phrase in question and "win" in question:
                match = re.search(r"will (?:the )?(.+?) win", question)
                if not match:
                    continue
                team = normalize_team(match.group(1))
                outcomes = safe_json(market.get("outcomes"))
                prices = safe_json(market.get("outcomePrices"))
                for i, out in enumerate(outcomes):
                    if str(out).lower() == "yes" and i < len(prices):
                        try:
                            price = float(prices[i])
                            rows.append({"game_id": futures_id, "market": "futures", "side": team,
                                        "line": 0.0, "source": "polymarket", "provider": "polymarket",
                                        "player": "", "price": price, "implied_prob": price, "devigged_prob": price,
                                        "provider_updated_at": now, "last_refreshed": now, "snapshot_time": now})
                        except (ValueError, TypeError):
                            pass
    return games, rows


def fetch_polymarket_games(session, config, existing_games):
    games, rows, now = {}, [], utc_now_iso()
    delay = get_source_config(config, "polymarket").get("request_delay_seconds", 0.2)
    
    def get_abbrev(team):
        return TEAM_ABBREVS.get(team.lower().strip())
    
    slugs = []
    if existing_games:
        for game in existing_games.values():
            sport = SPORT_MAP.get(game.get("league", ""))
            if not sport:
                continue
            commence = game.get("commence_time", "")
            if not commence:
                continue
            date_str = commence.split("T")[0] if "T" in commence else commence[:10]
            away_abbrev = get_abbrev(game.get("away_team", ""))
            home_abbrev = get_abbrev(game.get("home_team", ""))
            if away_abbrev and home_abbrev:
                slugs.append((f"{sport}-{away_abbrev}-{home_abbrev}-{date_str}",
                             game.get("away_team"), game.get("home_team")))
    
    logger.info(f"  Trying {len(slugs)} Polymarket game slugs...")
    
    for slug, away_team, home_team in slugs[:50]:
        data, status = api_request(session, "https://gamma-api.polymarket.com/events",
                                   params={"slug": slug}, retries=2)
        if status == 200 and data and isinstance(data, list) and len(data) > 0:
            event = data[0]
            game_id = f"poly_{slug}"
            league = {"nba": "basketball_nba"}.get(slug.split("-")[0], "unknown")
            
            games[game_id] = {"game_id": game_id, "league": league,
                            "commence_time": "-".join(slug.split("-")[-3:]),
                            "home_team": home_team, "away_team": away_team, "last_refreshed": now}
            
            for market in event.get("markets", []):
                rows.extend(_parse_polymarket_market(market, game_id, home_team, away_team, now))
        time.sleep(delay)
    
    logger.info(f"  Polymarket: {len(games)} games, {len(rows)} markets")
    return games, rows


def _parse_polymarket_market(market, game_id, home_team, away_team, now):
    rows = []
    question = market.get("question", "")
    outcomes = safe_json(market.get("outcomes"))
    prices = safe_json(market.get("outcomePrices"))
    
    if not outcomes or not prices or len(outcomes) != len(prices):
        return []
    
    market_type, line, player = None, 0.0, ""
    
    if re.search(r"^spread:", question, re.IGNORECASE):
        spread_match = re.search(r"\(([+-]?\d+\.?\d*)\)", question)
        if spread_match:
            market_type, line = "spreads", float(spread_match.group(1))
    elif re.search(r":\s*o/u\s*(\d+\.?\d*)", question, re.IGNORECASE):
        total_match = re.search(r":\s*o/u\s*(\d+\.?\d*)", question, re.IGNORECASE)
        market_type, line = "totals", float(total_match.group(1))
    elif re.match(r"^[^:]+:\s*(points|rebounds|assists)\s+o/u", question, re.IGNORECASE):
        prop_match = re.match(r"^([^:]+):\s*(points|rebounds|assists)\s+o/u\s*(\d+\.?\d*)", question, re.IGNORECASE)
        if prop_match:
            player = normalize_player(prop_match.group(1).strip())
            prop_type = prop_match.group(2).lower()
            line = float(prop_match.group(3))
            market_type = {"points": "player_points", "rebounds": "player_rebounds", "assists": "player_assists"}.get(prop_type)
    elif re.match(r"^[^:]+\s+vs\.?\s+[^:]+$", question, re.IGNORECASE):
        market_type = "h2h"
    
    if not market_type:
        return []
    
    for i, outcome in enumerate(outcomes):
        if i >= len(prices):
            break
        try:
            price = float(prices[i])
        except (ValueError, TypeError):
            continue
        
        outcome_str = str(outcome).strip()
        outcome_lower = outcome_str.lower()
        
        if market_type in ("h2h", "spreads"):
            outcome_norm = normalize_team(outcome_str)
            if outcome_norm == normalize_team(home_team):
                side = "home"
            elif outcome_norm == normalize_team(away_team):
                side = "away"
            else:
                side = outcome_norm
        elif market_type == "totals" or market_type.startswith("player_"):
            side = "over" if outcome_lower in ("over", "yes") else "under" if outcome_lower in ("under", "no") else None
            if side is None:
                continue
        else:
            side = outcome_lower
        
        rows.append({"game_id": game_id, "market": market_type, "side": side, "line": line,
                    "source": "polymarket", "provider": "polymarket", "player": player,
                    "price": price, "implied_prob": price, "devigged_prob": price,
                    "provider_updated_at": now, "last_refreshed": now, "snapshot_time": now})
    return rows


def fetch_kalshi(session, config):
    games, rows, now = {}, [], utc_now_iso()
    delay = get_source_config(config, "kalshi").get("request_delay_seconds", 0.3)
    
    logger.info("  Fetching Kalshi parlay markets...")
    all_markets, cursor = [], None
    for page in range(10):
        params = {"limit": 100, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        data, status = api_request(session, "https://api.elections.kalshi.com/trade-api/v2/markets", params=params)
        if status != 200 or not data:
            break
        all_markets.extend(data.get("markets", []))
        cursor = data.get("cursor")
        time.sleep(delay * 0.5)
        if not cursor or len(data.get("markets", [])) < 100:
            break
    
    leg_tickers = set()
    for market in all_markets:
        for leg in market.get("mve_selected_legs", []):
            ticker = leg.get("market_ticker", "")
            if ticker and ticker.startswith("KX"):
                leg_tickers.add(ticker)
    
    logger.info(f"  Found {len(leg_tickers)} leg tickers")
    
    SPORTS_PREFIXES = {"KXNBA", "KXNFL", "KXNHL"}
    sports_tickers = [t for t in leg_tickers if any(t.startswith(p) for p in SPORTS_PREFIXES)]
    logger.info(f"  Sports tickers: {len(sports_tickers)}")
    
    for ticker in sports_tickers[:100]:
        data, status = api_request(session, f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}", retries=1)
        if status == 200 and data:
            result = _parse_kalshi_market(ticker, data.get("market", {}), now)
            if result:
                game_record, row = result
                games[game_record["game_id"]] = game_record
                rows.append(row)
        time.sleep(delay * 0.2)
    
    logger.info(f"  Kalshi: {len(games)} games, {len(rows)} markets")
    return games, rows


def _parse_kalshi_market(ticker, market, now):
    MONTHS = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
              "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"}
    
    parts = ticker.split("-")
    if len(parts) < 2:
        return None
    
    prefix = parts[0]
    date_teams_match = re.match(r"(\d{2})([A-Z]{3})(\d{2})([A-Z]{3,6})$", parts[1])
    if not date_teams_match:
        return None
    
    year_short, month_abbr, day, teams_str = date_teams_match.groups()
    date = f"20{year_short}-{MONTHS.get(month_abbr, '01')}-{day}"
    away_team, home_team = teams_str[:3], teams_str[3:6] if len(teams_str) >= 6 else teams_str[3:]
    
    if "NBA" in prefix:
        league = "basketball_nba"
    elif "NFL" in prefix:
        league = "americanfootball_nfl"
    elif "NHL" in prefix:
        league = "icehockey_nhl"
    else:
        return None
    
    game_id = f"kalshi_{date}_{away_team}_{home_team}"
    market_type, side, line, player = "", "", 0.0, ""
    
    yes_bid = market.get("yes_bid", 0) or 0
    yes_ask = market.get("yes_ask", 0) or 0
    price = ((yes_bid + yes_ask) / 2) / 100 if (yes_bid or yes_ask) else None
    
    selection = parts[2] if len(parts) > 2 else ""
    
    if "GAME" in prefix:
        market_type, side = "h2h", "away" if selection == away_team else "home"
    elif "SPREAD" in prefix:
        market_type = "spreads"
        spread_match = re.match(r"([A-Z]{2,4})(\d+\.?\d*)", selection)
        if spread_match:
            spread_team, spread_val = spread_match.groups()
            line = -float(spread_val)
            side = "away" if spread_team == away_team else "home"
        else:
            return None
    elif "TOTAL" in prefix:
        market_type = "totals"
        try:
            line, side = float(selection), "over"
        except ValueError:
            return None
    elif "PTS" in prefix:
        market_type, side = "player_points", "over"
        if len(parts) >= 4:
            player = normalize_player(re.sub(r"\d+$", "", parts[2][3:]))
            try:
                line = float(parts[3])
            except ValueError:
                return None
    else:
        return None
    
    if not market_type:
        return None
    
    game_record = {"game_id": game_id, "league": league, "commence_time": date,
                  "home_team": home_team, "away_team": away_team, "last_refreshed": now}
    row = {"game_id": game_id, "market": market_type, "side": side, "line": line,
          "source": "kalshi", "provider": "kalshi", "player": player, "price": price,
          "implied_prob": price, "devigged_prob": price,
          "provider_updated_at": now, "last_refreshed": now, "snapshot_time": now}
    return game_record, row


def run_once(existing_games=None):
    load_dotenv()
    config = load_config()
    conn = init_db()
    
    if existing_games is None:
        cursor = conn.execute("SELECT game_id, league, commence_time, home_team, away_team FROM games")
        existing_games = {r[0]: {"game_id": r[0], "league": r[1], "commence_time": r[2],
                                "home_team": r[3], "away_team": r[4]} for r in cursor}
    
    all_games, all_rows = {}, []
    
    logger.info("=" * 60)
    logger.info("OPEN MARKET WORKER - Starting poll")
    
    with requests.Session() as session:
        if get_source_config(config, "polymarket").get("enabled", True):
            logger.info("Fetching Polymarket...")
            fut_games, fut_rows = fetch_polymarket_futures(session, config)
            all_games.update(fut_games)
            all_rows.extend(fut_rows)
            
            game_games, game_rows = fetch_polymarket_games(session, config, existing_games)
            all_games.update(game_games)
            all_rows.extend(game_rows)
        
        if get_source_config(config, "kalshi").get("enabled", True):
            logger.info("Fetching Kalshi...")
            kalshi_games, kalshi_rows = fetch_kalshi(session, config)
            all_games.update(kalshi_games)
            all_rows.extend(kalshi_rows)
    
    if all_games:
        for g in all_games.values():
            g["last_refreshed"] = utc_now_iso()
        upsert_rows(conn, "games", ["game_id"],
                   ["league", "commence_time", "home_team", "away_team", "last_refreshed"], all_games.values())
    
    if all_rows:
        upsert_rows(conn, "market_latest",
                   ["game_id", "market", "side", "line", "source", "provider", "player"],
                   ["price", "implied_prob", "devigged_prob", "provider_updated_at", "last_refreshed", "source_event_id", "source_market_id", "outcome"],
                   all_rows)
        insert_history(conn, all_rows)
    
    update_source_metadata(conn, "polymarket", 0)
    update_source_metadata(conn, "kalshi", 0)
    conn.close()
    
    logger.info(f"COMPLETE: {len(all_games)} games, {len(all_rows)} rows")
    return {"status": "success", "games": len(all_games), "rows": len(all_rows)}


def run_daemon():
    load_dotenv()
    config = load_config()
    logger.info("Starting daemon with dynamic polling...")
    
    while True:
        try:
            conn = init_db()
            live_games = get_live_games(conn)
            interval = LIVE_POLL_INTERVAL if live_games else DEFAULT_POLL_INTERVAL
            if live_games:
                logger.info(f"🔴 {len(live_games)} live games - fast polling ({interval}s)")
            conn.close()
            run_once()
        except Exception as e:
            logger.error(f"Poll failed: {e}")
            interval = DEFAULT_POLL_INTERVAL
        
        logger.info(f"Sleeping {interval}s...")
        time.sleep(interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", "-d", action="store_true")
    parser.add_argument("--once", "-1", action="store_true")
    args = parser.parse_args()
    run_daemon() if args.daemon else run_once()
