"""
Unified Odds Ingestion
======================
Fetches data from all sources and stores in SQLite.

Sources:
    - Odds API: Sportsbook odds (games + futures)
    - Polymarket: Prediction market futures
    - Kalshi: Game references from parlays
"""
from __future__ import annotations

import os
import re
import time
from collections import defaultdict

import requests
from dotenv import load_dotenv

from utils import (
    canonical_game_id,
    devig,
    devig_market,
    init_db,
    insert_history,
    load_config,
    normalize_team,
    odds_to_prob,
    safe_json,
    upsert_rows,
    utc_now_iso,
    within_window,
)


# =============================================================================
# ODDS API
# =============================================================================

def fetch_odds_api_games(session: requests.Session, api_key: str, config: dict):
    """
    Fetch game-by-game odds from all configured bookmakers.
    
    Returns:
        tuple: (games dict, market rows list)
    """
    games = {}
    groups = defaultdict(list)
    now = utc_now_iso()

    for sport in config["sports"]:
        for market_type in config["markets"]:
            # Fetch from API
            url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
            params = {
                "apiKey": api_key,
                "regions": ",".join(config["regions"]),
                "markets": market_type,
                "oddsFormat": "decimal",
                "dateFormat": "iso",
            }
            resp = session.get(url, params=params, timeout=20)
            data = resp.json() if resp.status_code == 200 else []
            print(f"  {sport}/{market_type}: {len(data)} games")
            time.sleep(config.get("request_delay_seconds", 0))

            # Process each game
            for game in data:
                home = game.get("home_team")
                away = game.get("away_team")
                commence = game.get("commence_time")

                if not all([home, away, commence]):
                    continue
                if not within_window(commence, config.get("bettable_window_days", 30)):
                    continue

                game_id = canonical_game_id(game["sport_key"], home, away, commence[:10])
                games[game_id] = {
                    "game_id": game_id,
                    "league": game["sport_key"],
                    "commence_time": commence,
                    "home_team": home,
                    "away_team": away,
                    "last_refreshed": now,
                }

                # Process each bookmaker
                for book in game.get("bookmakers", []):
                    if book["key"] not in config["books"]:
                        continue

                    for mkt in book.get("markets", []):
                        if mkt["key"] not in config["markets"]:
                            continue

                        for outcome in mkt.get("outcomes", []):
                            row = _parse_outcome(outcome, mkt, game, book, game_id, home, away, now)
                            if row:
                                groups[(game_id, mkt["key"], row["line"], book["key"])].append(row)

    # Apply de-vigging to each market group
    rows = [r for group in groups.values() for r in devig_market(group)]
    return games, rows


def _parse_outcome(outcome, market, game, book, game_id, home, away, now):
    """Parse a single outcome into a market row."""
    name = outcome.get("name")
    price = outcome.get("price")

    if name is None or price is None:
        return None

    # Determine side and line based on market type
    if market["key"] == "totals":
        side = name.strip().lower()
        line = outcome.get("point")
        if side not in {"over", "under"}:
            return None
    else:
        # h2h or spreads
        normalized = normalize_team(name)
        home_norm = normalize_team(home)
        away_norm = normalize_team(away)

        if normalized == home_norm or home_norm in normalized:
            side = "home"
        elif normalized == away_norm or away_norm in normalized:
            side = "away"
        elif normalized in {"draw", "tie", "x"}:
            side = "draw"
        else:
            return None

        line = outcome.get("point", 0.0) if market["key"] == "spreads" else 0.0

    if line is None:
        return None

    return {
        "game_id": game_id,
        "market": market["key"],
        "side": side,
        "line": float(line),
        "source": "odds_api",
        "provider": book["key"],
        "price": price,
        "implied_prob": odds_to_prob(price),
        "provider_updated_at": book.get("last_update", now),
        "last_refreshed": now,
        "snapshot_time": now,
        "source_event_id": game.get("id"),
        "source_market_id": None,
        "outcome": name,
    }


def fetch_odds_api_futures(session: requests.Session, api_key: str, config: dict):
    """
    Fetch championship futures from all configured bookmakers.
    
    Returns:
        tuple: (games dict, market rows list)
    """
    games = {}
    rows = []
    now = utc_now_iso()

    FUTURES = {
        "basketball_nba_championship_winner": "NBA Championship",
        "icehockey_nhl_championship_winner": "NHL Stanley Cup",
    }

    for sport_key, name in FUTURES.items():
        print(f"  {name}...", end=" ")

        resp = session.get(
            f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds",
            params={"apiKey": api_key, "regions": "us", "oddsFormat": "decimal"},
            timeout=15,
        )

        if resp.status_code != 200:
            print("failed")
            continue

        futures_id = f"futures_{sport_key}"
        games[futures_id] = {
            "game_id": futures_id,
            "league": sport_key,
            "commence_time": "",
            "home_team": name,
            "away_team": "",
            "last_refreshed": now,
        }

        book_count = 0
        for event in resp.json():
            for book in event.get("bookmakers", []):
                if book["key"] not in config["books"]:
                    continue

                book_count += 1
                for mkt in book.get("markets", []):
                    outcomes = mkt.get("outcomes", [])
                    probs = [odds_to_prob(o.get("price", 0)) for o in outcomes]
                    devigged = devig(probs)

                    for i, out in enumerate(outcomes):
                        team = normalize_team(out.get("name", ""))
                        rows.append({
                            "game_id": futures_id,
                            "market": "futures",
                            "side": team,
                            "line": 0.0,
                            "source": "odds_api",
                            "provider": book["key"],
                            "price": out.get("price"),
                            "implied_prob": probs[i],
                            "devigged_prob": devigged[i],
                            "provider_updated_at": book.get("last_update", now),
                            "last_refreshed": now,
                            "snapshot_time": now,
                            "source_event_id": None,
                            "source_market_id": None,
                            "outcome": team,
                        })

        print(f"{book_count} books")

    return games, rows


# =============================================================================
# POLYMARKET
# =============================================================================

def fetch_polymarket(session: requests.Session):
    """
    Fetch futures from Polymarket.
    Polymarket prices ARE probabilities (no vig).
    
    Returns:
        tuple: (games dict, market rows list)
    """
    games = {}
    rows = []
    now = utc_now_iso()

    # Fetch all open markets
    all_markets = []
    for offset in range(0, 1000, 100):
        resp = session.get(
            "https://gamma-api.polymarket.com/markets",
            params={"closed": "false", "limit": 100, "offset": offset},
            timeout=15,
        )
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        all_markets.extend(data)

    print(f"  Fetched {len(all_markets)} markets")

    # Match to our tracked futures
    FUTURES = {
        "futures_basketball_nba_championship_winner": ("nba", "NBA Championship"),
        "futures_icehockey_nhl_championship_winner": ("stanley cup", "NHL Stanley Cup"),
    }

    for futures_id, (phrase, name) in FUTURES.items():
        games[futures_id] = {
            "game_id": futures_id,
            "league": futures_id.replace("futures_", ""),
            "commence_time": "",
            "home_team": name,
            "away_team": "",
            "last_refreshed": now,
        }

        teams_found = []
        for market in all_markets:
            question = (market.get("question") or "").lower()

            if phrase in question and "win" in question:
                # Extract team name from question
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
                            teams_found.append(team)
                            rows.append({
                                "game_id": futures_id,
                                "market": "futures",
                                "side": team,
                                "line": 0.0,
                                "source": "polymarket",
                                "provider": "polymarket",
                                "price": price,
                                "implied_prob": price,  # Price IS probability
                                "devigged_prob": price,  # No vig to remove
                                "provider_updated_at": now,
                                "last_refreshed": now,
                                "snapshot_time": now,
                                "source_event_id": market.get("id"),
                                "source_market_id": None,
                                "outcome": team,
                            })
                        except (ValueError, TypeError):
                            pass
                        break

        print(f"  {phrase}: {len(teams_found)} teams")

    return games, rows


# =============================================================================
# KALSHI
# =============================================================================

def fetch_kalshi(session: requests.Session):
    """
    Extract game references from Kalshi parlay markets.
    
    Returns:
        tuple: (games dict, market rows list)
    """
    games = {}
    rows = []
    now = utc_now_iso()

    # Fetch all open markets with pagination
    all_markets = []
    cursor = None

    for _ in range(20):
        params = {"limit": 100, "status": "open"}
        if cursor:
            params["cursor"] = cursor

        resp = session.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params=params,
            timeout=15,
        )

        if resp.status_code != 200:
            break

        data = resp.json()
        all_markets.extend(data.get("markets", []))
        cursor = data.get("cursor")

        if not cursor or len(data.get("markets", [])) < 100:
            break

    print(f"  Fetched {len(all_markets)} markets")

    # Extract game references from parlay legs
    MONTHS = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }

    seen = set()
    for market in all_markets:
        for leg in market.get("mve_selected_legs", []):
            ticker = leg.get("market_ticker", "")
            parts = ticker.split("-")

            if len(parts) < 2:
                continue

            match = re.match(r"(\d{2})([A-Z]{3})(\d{2})([A-Z]+)", parts[1])
            if not match or "GAME" not in parts[0]:
                continue

            year, month, day, teams = match.groups()
            date = f"20{year}-{MONTHS.get(month, '01')}-{day}"

            team1 = teams[:3] if len(teams) >= 3 else teams
            team2 = teams[3:6] if len(teams) >= 6 else ""
            selection = parts[2] if len(parts) > 2 else ""

            key = (date, team1, team2)
            if key in seen:
                continue
            seen.add(key)

            # Determine league
            if "NBA" in parts[0]:
                league = "NBA"
            elif "NFL" in parts[0]:
                league = "NFL"
            else:
                league = "SPORT"

            game_id = f"kalshi_{date}_{team1}_{team2}"

            games[game_id] = {
                "game_id": game_id,
                "league": league,
                "commence_time": date,
                "home_team": team1,
                "away_team": team2,
                "last_refreshed": now,
            }

            rows.append({
                "game_id": game_id,
                "market": "h2h",
                "side": normalize_team(selection),
                "line": 0.0,
                "source": "kalshi",
                "provider": "kalshi",
                "price": None,
                "implied_prob": None,
                "devigged_prob": None,
                "provider_updated_at": now,
                "last_refreshed": now,
                "snapshot_time": now,
                "source_event_id": ticker,
                "source_market_id": market.get("ticker"),
                "outcome": selection,
            })

    print(f"  Extracted {len(games)} games")
    return games, rows


# =============================================================================
# MAIN
# =============================================================================

def ingest():
    """Run full ingestion from all sources."""
    load_dotenv()

    api_key = os.getenv("ODDS_API_KEY")
    config = load_config()
    conn = init_db(config["storage"]["database"])

    all_games = {}
    all_rows = []

    try:
        with requests.Session() as session:
            # Odds API - Games
            print("\n[ODDS API - Games]")
            if api_key:
                g, r = fetch_odds_api_games(session, api_key, config)
                all_games.update(g)
                all_rows.extend(r)
                print(f"  → {len(g)} games, {len(r)} rows")
            else:
                print("  ⚠️  Missing ODDS_API_KEY")

            # Odds API - Futures
            print("\n[ODDS API - Futures]")
            if api_key:
                g, r = fetch_odds_api_futures(session, api_key, config)
                all_games.update(g)
                all_rows.extend(r)
                print(f"  → {len(g)} futures, {len(r)} rows")

            # Polymarket
            print("\n[POLYMARKET]")
            g, r = fetch_polymarket(session)
            all_games.update(g)
            all_rows.extend(r)
            print(f"  → {len(r)} rows")

            # Kalshi
            print("\n[KALSHI]")
            g, r = fetch_kalshi(session)
            all_games.update(g)
            all_rows.extend(r)
            print(f"  → {len(g)} games, {len(r)} rows")

        # Save to database
        upsert_rows(
            conn, "games", ["game_id"],
            ["league", "commence_time", "home_team", "away_team", "last_refreshed"],
            all_games.values(),
        )
        upsert_rows(
            conn, "market_latest",
            ["game_id", "market", "side", "line", "source", "provider"],
            ["price", "implied_prob", "devigged_prob", "provider_updated_at",
             "last_refreshed", "source_event_id", "source_market_id", "outcome"],
            all_rows,
        )
        insert_history(conn, all_rows)
        conn.commit()

    finally:
        conn.close()

    print(f"\n✅ {len(all_games)} markets, {len(all_rows)} rows")


if __name__ == "__main__":
    ingest()
