"""
Data Ingestion Module
=====================

Fetches odds and market data from multiple sources and stores in SQLite.

This module provides fetch functions for each supported data source:
    - Odds API: Aggregated sportsbook odds (DraftKings, FanDuel, etc.)
    - Polymarket: Prediction market prices (no vig)
    - Kalshi: US-regulated prediction market

Each fetch function returns a standardized tuple of:
    - games: Dict of game/event records keyed by game_id
    - rows: List of market price records

Architecture:
    1. Each source has its own fetch_* function
    2. The ingest() function orchestrates all fetches
    3. Data is normalized to common schema before storage
    4. De-vigging is applied to sportsbook odds

Usage:
    from ingest import ingest
    ingest()  # Run full ingestion from all sources

    # Or fetch from specific source
    from ingest import fetch_odds_api_games
    with requests.Session() as session:
        games, rows = fetch_odds_api_games(session, api_key, config)

Dependencies:
    - requests: HTTP client for API calls
    - python-dotenv: Environment variable loading

Author: Arbitrage Detection System
"""
from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from typing import Any, Optional

import requests
from dotenv import load_dotenv

from utils import (
    canonical_game_id,
    devig,
    devig_market,
    get_source_config,
    init_db,
    insert_history,
    load_config,
    normalize_team,
    odds_to_prob,
    safe_json,
    update_source_metadata,
    upsert_rows,
    utc_now_iso,
    within_window,
)


# =============================================================================
# TYPE ALIASES
# =============================================================================

# Game record: metadata about a sporting event
GameRecord = dict[str, Any]

# Market row: a single price/odds record
MarketRow = dict[str, Any]

# Standard return type for fetch functions
FetchResult = tuple[dict[str, GameRecord], list[MarketRow]]


# =============================================================================
# ODDS API - SPORTSBOOK ODDS
# =============================================================================

def fetch_odds_api_games(
    session: requests.Session,
    api_key: str,
    config: dict[str, Any],
) -> FetchResult:
    """
    Fetch game-by-game odds from all configured sportsbooks via Odds API.

    Iterates through configured sports and market types, fetching odds
    from all bookmakers and normalizing to our schema.

    API Call Count: len(sports) × len(markets) calls per invocation.

    Args:
        session: Active requests.Session for connection pooling.
        api_key: Odds API key (from environment).
        config: Full configuration dict with sports, markets, books.

    Returns:
        Tuple of (games_dict, market_rows):
            - games_dict: Game records keyed by canonical game_id
            - market_rows: List of price records (one per book/market/outcome)

    Example:
        >>> with requests.Session() as session:
        ...     games, rows = fetch_odds_api_games(session, 'API_KEY', config)
        ...     print(f"Found {len(games)} games, {len(rows)} price rows")
    """
    games: dict[str, GameRecord] = {}
    groups: dict[tuple, list[MarketRow]] = defaultdict(list)
    now = utc_now_iso()

    # Get source-specific config
    source_cfg = get_source_config(config, "odds_api")
    delay = source_cfg.get("request_delay_seconds", 0.5)

    sports = config.get("sports", [])
    markets = config.get("markets", [])
    regions = config.get("regions", ["us"])
    books = config.get("books", [])

    for sport in sports:
        for market_type in markets:
            # Build API request
            url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
            params = {
                "apiKey": api_key,
                "regions": ",".join(regions),
                "markets": market_type,
                "oddsFormat": "decimal",
                "dateFormat": "iso",
            }

            # Make request with error handling
            try:
                resp = session.get(url, params=params, timeout=20)
                data = resp.json() if resp.status_code == 200 else []
            except (requests.RequestException, ValueError) as e:
                print(f"  ⚠️  {sport}/{market_type}: {e}")
                data = []

            print(f"  {sport}/{market_type}: {len(data)} games")
            time.sleep(delay)

            # Process each game
            for game in data:
                result = _process_odds_api_game(
                    game, market_type, now, config, books
                )
                if result:
                    game_record, game_rows = result
                    game_id = game_record["game_id"]
                    games[game_id] = game_record

                    # Group rows by (game_id, market, line, book) for de-vigging
                    for row in game_rows:
                        key = (game_id, market_type, row["line"], row["provider"])
                        groups[key].append(row)

    # Apply de-vigging to each market group
    rows = [r for group in groups.values() for r in devig_market(group)]
    return games, rows


def _process_odds_api_game(
    game: dict[str, Any],
    market_type: str,
    now: str,
    config: dict[str, Any],
    books: list[str],
) -> Optional[tuple[GameRecord, list[MarketRow]]]:
    """
    Process a single game from Odds API response.

    Extracts game metadata and iterates through bookmakers to build
    market rows for each outcome.

    Args:
        game: Single game object from Odds API.
        market_type: Type of market (h2h, spreads, totals).
        now: Current ISO timestamp.
        config: Configuration dict.
        books: List of bookmaker keys to include.

    Returns:
        Tuple of (game_record, rows) or None if game should be skipped.
    """
    home = game.get("home_team")
    away = game.get("away_team")
    commence = game.get("commence_time")

    # Skip invalid or out-of-window games
    if not all([home, away, commence]):
        return None

    window_days = config.get("bettable_window_days", 14)
    if not within_window(commence, window_days):
        return None

    # Generate canonical game ID
    game_id = canonical_game_id(game["sport_key"], home, away, commence[:10])

    game_record: GameRecord = {
        "game_id": game_id,
        "league": game["sport_key"],
        "commence_time": commence,
        "home_team": home,
        "away_team": away,
        "last_refreshed": now,
    }

    rows: list[MarketRow] = []

    # Process each bookmaker
    for book in game.get("bookmakers", []):
        if book["key"] not in books:
            continue

        for mkt in book.get("markets", []):
            if mkt["key"] != market_type:
                continue

            for outcome in mkt.get("outcomes", []):
                row = _parse_outcome(
                    outcome=outcome,
                    market=mkt,
                    game=game,
                    book=book,
                    game_id=game_id,
                    home=home,
                    away=away,
                    now=now,
                )
                if row:
                    rows.append(row)

    return game_record, rows


def _parse_outcome(
    outcome: dict[str, Any],
    market: dict[str, Any],
    game: dict[str, Any],
    book: dict[str, Any],
    game_id: str,
    home: str,
    away: str,
    now: str,
) -> Optional[MarketRow]:
    """
    Parse a single outcome into a standardized market row.

    Handles different market types:
        - h2h: Maps outcome name to home/away/draw
        - spreads: Includes point spread line
        - totals: Maps to over/under with point total

    Args:
        outcome: Single outcome object from API.
        market: Parent market object.
        game: Parent game object.
        book: Parent bookmaker object.
        game_id: Canonical game ID.
        home: Home team name.
        away: Away team name.
        now: Current timestamp.

    Returns:
        MarketRow dict or None if outcome is invalid.
    """
    name = outcome.get("name")
    price = outcome.get("price")

    if name is None or price is None:
        return None

    market_key = market["key"]

    # Determine side and line based on market type
    if market_key == "totals":
        # Over/under markets
        side = name.strip().lower()
        line = outcome.get("point")
        if side not in {"over", "under"}:
            return None
    else:
        # H2H or spreads - map team name to side
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

        line = outcome.get("point", 0.0) if market_key == "spreads" else 0.0

    if line is None:
        return None

    return {
        "game_id": game_id,
        "market": market_key,
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


def fetch_odds_api_futures(
    session: requests.Session,
    api_key: str,
    config: dict[str, Any],
) -> FetchResult:
    """
    Fetch championship futures from all configured bookmakers.

    Futures are long-term bets on season outcomes (e.g., who wins NBA title).
    These are stored with special game_ids prefixed with 'futures_'.

    Args:
        session: Active requests.Session.
        api_key: Odds API key.
        config: Full configuration dict.

    Returns:
        Tuple of (games_dict, market_rows) for futures markets.

    Example:
        >>> games, rows = fetch_odds_api_futures(session, api_key, config)
        >>> 'futures_basketball_nba_championship_winner' in games
        True
    """
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    books = config.get("books", [])

    # Championship futures to fetch
    FUTURES = {
        "basketball_nba_championship_winner": "NBA Championship",
        "icehockey_nhl_championship_winner": "NHL Stanley Cup",
    }

    for sport_key, name in FUTURES.items():
        print(f"  {name}...", end=" ")

        try:
            resp = session.get(
                f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds",
                params={"apiKey": api_key, "regions": "us", "oddsFormat": "decimal"},
                timeout=15,
            )
        except requests.RequestException as e:
            print(f"failed: {e}")
            continue

        if resp.status_code != 200:
            print("failed")
            continue

        # Create futures game record
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
                if book["key"] not in books:
                    continue

                book_count += 1
                for mkt in book.get("markets", []):
                    outcomes = mkt.get("outcomes", [])
                    # De-vig all outcomes together
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
                            "devigged_prob": devigged[i] if i < len(devigged) else None,
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
# POLYMARKET - PREDICTION MARKET
# =============================================================================

def fetch_polymarket(session: requests.Session, config: dict[str, Any]) -> FetchResult:
    """
    Fetch futures/prediction markets from Polymarket.

    Polymarket prices ARE probabilities - there is no vig to remove.
    Currently focuses on championship futures that can match Odds API.

    Note: Polymarket may not have active game-by-game sports markets.
    Most sports coverage is through futures (e.g., "Will Lakers win NBA?").

    Args:
        session: Active requests.Session.
        config: Full configuration dict.

    Returns:
        Tuple of (games_dict, market_rows) for matched futures.

    Example:
        >>> games, rows = fetch_polymarket(session, config)
        >>> any('nba' in g for g in games.keys())
        True
    """
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    # Get source-specific config
    source_cfg = get_source_config(config, "polymarket")
    delay = source_cfg.get("request_delay_seconds", 0.2)

    # Fetch all open markets with pagination
    all_markets: list[dict] = []
    for offset in range(0, 1000, 100):
        try:
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
            time.sleep(delay)
        except (requests.RequestException, ValueError) as e:
            print(f"  ⚠️  Polymarket page {offset}: {e}")
            break

    print(f"  Fetched {len(all_markets)} markets")

    # Match to our tracked futures
    # Maps futures_id -> (search_phrase, display_name)
    FUTURES = {
        "futures_basketball_nba_championship_winner": ("nba", "NBA Championship"),
        "futures_icehockey_nhl_championship_winner": ("stanley cup", "NHL Stanley Cup"),
    }

    for futures_id, (phrase, name) in FUTURES.items():
        # Create game record for this futures market
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

            # Match "Will [team] win [championship]?" pattern
            if phrase in question and "win" in question:
                # Extract team name from question
                match = re.search(r"will (?:the )?(.+?) win", question)
                if not match:
                    continue

                team = normalize_team(match.group(1))
                outcomes = safe_json(market.get("outcomes"))
                prices = safe_json(market.get("outcomePrices"))

                # Find "Yes" outcome price
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
# KALSHI - US REGULATED PREDICTION MARKET
# =============================================================================

def fetch_kalshi(session: requests.Session, config: dict[str, Any]) -> FetchResult:
    """
    Extract game references from Kalshi markets.

    Kalshi provides sports exposure through parlay-style markets.
    This function extracts underlying game references from market tickers.

    Note: Kalshi may have limited direct sports betting; most exposure
    is through derivative or event markets.

    Args:
        session: Active requests.Session.
        config: Full configuration dict.

    Returns:
        Tuple of (games_dict, market_rows) for extracted games.

    Example:
        >>> games, rows = fetch_kalshi(session, config)
        >>> print(f"Found {len(games)} game references")
    """
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    # Get source-specific config
    source_cfg = get_source_config(config, "kalshi")
    delay = source_cfg.get("request_delay_seconds", 0.3)

    # Fetch all open markets with cursor pagination
    all_markets: list[dict] = []
    cursor: Optional[str] = None

    for _ in range(20):  # Max 2000 markets
        params: dict[str, Any] = {"limit": 100, "status": "open"}
        if cursor:
            params["cursor"] = cursor

        try:
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
            time.sleep(delay)

            if not cursor or len(data.get("markets", [])) < 100:
                break

        except (requests.RequestException, ValueError) as e:
            print(f"  ⚠️  Kalshi: {e}")
            break

    print(f"  Fetched {len(all_markets)} markets")

    # Month name to number mapping
    MONTHS = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }

    # Extract game references from parlay legs
    seen: set[tuple] = set()
    for market in all_markets:
        for leg in market.get("mve_selected_legs", []):
            result = _parse_kalshi_leg(leg, market, MONTHS, seen, now)
            if result:
                game_record, row = result
                games[game_record["game_id"]] = game_record
                rows.append(row)

    print(f"  Extracted {len(games)} games")
    return games, rows


def _parse_kalshi_leg(
    leg: dict[str, Any],
    market: dict[str, Any],
    months: dict[str, str],
    seen: set[tuple],
    now: str,
) -> Optional[tuple[GameRecord, MarketRow]]:
    """
    Parse a single Kalshi parlay leg into game and market records.

    Kalshi tickers follow patterns like:
        NBAGAME-26FEB10LALNYC-LAL
        (NBA game on Feb 10, 2026, LA Lakers vs NYC, selection: Lakers)

    Args:
        leg: Parlay leg object from Kalshi API.
        market: Parent market object.
        months: Month name to number mapping.
        seen: Set of already-processed (date, team1, team2) tuples.
        now: Current timestamp.

    Returns:
        Tuple of (game_record, market_row) or None if invalid.
    """
    ticker = leg.get("market_ticker", "")
    parts = ticker.split("-")

    if len(parts) < 2:
        return None

    # Parse ticker format: PREFIX-DDMMMYYTEAMS-SELECTION
    match = re.match(r"(\d{2})([A-Z]{3})(\d{2})([A-Z]+)", parts[1])
    if not match or "GAME" not in parts[0]:
        return None

    year, month, day, teams = match.groups()
    date = f"20{year}-{months.get(month, '01')}-{day}"

    # Extract team abbreviations (3 chars each)
    team1 = teams[:3] if len(teams) >= 3 else teams
    team2 = teams[3:6] if len(teams) >= 6 else ""
    selection = parts[2] if len(parts) > 2 else ""

    # Skip duplicates
    key = (date, team1, team2)
    if key in seen:
        return None
    seen.add(key)

    # Determine league from ticker prefix
    if "NBA" in parts[0]:
        league = "basketball_nba"
    elif "NFL" in parts[0]:
        league = "americanfootball_nfl"
    elif "NHL" in parts[0]:
        league = "icehockey_nhl"
    elif "MLB" in parts[0]:
        league = "baseball_mlb"
    else:
        league = "unknown"

    game_id = f"kalshi_{date}_{team1}_{team2}"

    game_record: GameRecord = {
        "game_id": game_id,
        "league": league,
        "commence_time": date,
        "home_team": team1,
        "away_team": team2,
        "last_refreshed": now,
    }

    row: MarketRow = {
        "game_id": game_id,
        "market": "h2h",
        "side": normalize_team(selection),
        "line": 0.0,
        "source": "kalshi",
        "provider": "kalshi",
        "price": None,  # Kalshi doesn't expose prices in parlay legs
        "implied_prob": None,
        "devigged_prob": None,
        "provider_updated_at": now,
        "last_refreshed": now,
        "snapshot_time": now,
        "source_event_id": ticker,
        "source_market_id": market.get("ticker"),
        "outcome": selection,
    }

    return game_record, row


# =============================================================================
# MAIN INGESTION ORCHESTRATOR
# =============================================================================

def ingest(
    sources: Optional[list[str]] = None,
    skip_disabled: bool = True,
) -> dict[str, int]:
    """
    Run full ingestion from all (or specified) sources.

    Orchestrates data fetching from all enabled sources, normalizes
    the data, and stores in the SQLite database.

    Args:
        sources: List of source names to ingest. If None, ingest all enabled.
        skip_disabled: If True, skip sources with enabled=False in config.

    Returns:
        Dictionary with counts: {'games': N, 'rows': M, 'sources': X}

    Example:
        >>> result = ingest()
        >>> print(f"Ingested {result['games']} games, {result['rows']} rows")

        >>> # Ingest only specific sources
        >>> result = ingest(sources=['polymarket', 'kalshi'])
    """
    load_dotenv()

    api_key = os.getenv("ODDS_API_KEY")
    config = load_config()
    conn = init_db(config["storage"]["database"])

    all_games: dict[str, GameRecord] = {}
    all_rows: list[MarketRow] = []
    api_calls = 0

    try:
        with requests.Session() as session:
            # Determine which sources to ingest
            source_configs = config.get("sources", {})
            sources_to_run = sources or list(source_configs.keys())

            for source_name in sources_to_run:
                source_cfg = source_configs.get(source_name, {})

                # Skip disabled sources
                if skip_disabled and not source_cfg.get("enabled", True):
                    print(f"\n[{source_name.upper()}] Skipped (disabled)")
                    continue

                print(f"\n[{source_name.upper()}]")

                try:
                    if source_name == "odds_api":
                        if not api_key:
                            print("  ⚠️  Missing ODDS_API_KEY")
                            continue

                        # Games (per-game odds)
                        print("  Games:")
                        g, r = fetch_odds_api_games(session, api_key, config)
                        all_games.update(g)
                        all_rows.extend(r)
                        calls = len(config.get("sports", [])) * len(config.get("markets", []))
                        api_calls += calls
                        print(f"  → {len(g)} games, {len(r)} rows ({calls} API calls)")

                        # Futures (championship odds)
                        print("  Futures:")
                        g, r = fetch_odds_api_futures(session, api_key, config)
                        all_games.update(g)
                        all_rows.extend(r)
                        api_calls += 2  # NBA + NHL futures
                        print(f"  → {len(g)} futures, {len(r)} rows")

                        update_source_metadata(conn, "odds_api", success=True, calls_made=api_calls)

                    elif source_name == "polymarket":
                        g, r = fetch_polymarket(session, config)
                        all_games.update(g)
                        all_rows.extend(r)
                        print(f"  → {len(r)} rows")
                        update_source_metadata(conn, "polymarket", success=True)

                    elif source_name == "kalshi":
                        g, r = fetch_kalshi(session, config)
                        all_games.update(g)
                        all_rows.extend(r)
                        print(f"  → {len(g)} games, {len(r)} rows")
                        update_source_metadata(conn, "kalshi", success=True)

                except Exception as e:
                    print(f"  ❌ Error: {e}")
                    update_source_metadata(conn, source_name, success=False, error=str(e))

            # Save to database
            print("\n[SAVING TO DATABASE]")

            games_saved = upsert_rows(
                conn, "games", ["game_id"],
                ["league", "commence_time", "home_team", "away_team", "last_refreshed"],
                all_games.values(),
            )

            rows_saved = upsert_rows(
                conn, "market_latest",
                ["game_id", "market", "side", "line", "source", "provider"],
                ["price", "implied_prob", "devigged_prob", "provider_updated_at",
                 "last_refreshed", "source_event_id", "source_market_id", "outcome"],
                all_rows,
            )

            history_saved = insert_history(conn, all_rows)
            conn.commit()

            print(f"  Games: {games_saved}")
            print(f"  Market rows: {rows_saved}")
            print(f"  History rows: {history_saved}")

    finally:
        conn.close()

    print(f"\n✅ Complete: {len(all_games)} games, {len(all_rows)} rows")

    return {
        "games": len(all_games),
        "rows": len(all_rows),
        "api_calls": api_calls,
    }


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    ingest()
