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

import logging
import requests
from dotenv import load_dotenv

from utils import (
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    canonical_game_id,
    devig,
    devig_market,
    get_source_config,
    init_db,
    insert_history,
    load_config,
    logger,
    normalize_player,
    normalize_team,
    odds_to_prob,
    safe_json,
    setup_logging,
    update_source_metadata,
    upsert_rows,
    utc_now_iso,
    within_window,
)

# Initialize logging for this module
ingest_logger = logging.getLogger(__name__)


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
# API REQUEST HELPER
# =============================================================================

def api_request(
    session: requests.Session,
    url: str,
    params: Optional[dict] = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    context: str = "",
) -> tuple[Optional[dict | list], int]:
    """
    Make an API request with automatic retry and comprehensive error handling.

    Features:
        - Automatic retries with exponential backoff for transient failures
        - Detailed logging for debugging API issues
        - Consistent error handling across all API calls
        - Returns both data and status code for caller flexibility

    Args:
        session: requests.Session for connection pooling.
        url: The API endpoint URL.
        params: Optional query parameters.
        timeout: Request timeout in seconds.
        retries: Maximum number of retry attempts.
        context: Description of request for logging (e.g., "Odds API NBA games").

    Returns:
        Tuple of (data, status_code):
            - data: JSON response (dict or list), or None on failure
            - status_code: HTTP status code, or 0 if request failed entirely

    Example:
        >>> data, status = api_request(session, url, params={"key": "value"}, context="Polymarket")
        >>> if status == 200 and data:
        ...     process(data)
    """
    last_error: Optional[Exception] = None
    context_str = f" [{context}]" if context else ""

    for attempt in range(retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)

            # Success
            if resp.status_code == 200:
                try:
                    return resp.json(), 200
                except ValueError as e:
                    ingest_logger.warning(f"Invalid JSON response{context_str}: {e}")
                    return None, 200

            # Client errors (don't retry)
            if 400 <= resp.status_code < 500:
                if resp.status_code == 401:
                    ingest_logger.error(f"Unauthorized{context_str}: Check API key")
                elif resp.status_code == 429:
                    ingest_logger.warning(f"Rate limited{context_str}: {url}")
                    # Rate limit - wait longer before retry
                    if attempt < retries:
                        time.sleep(5 * (attempt + 1))
                        continue
                else:
                    ingest_logger.warning(
                        f"Client error {resp.status_code}{context_str}: {url}"
                    )
                return None, resp.status_code

            # Server errors (retry)
            if resp.status_code >= 500:
                ingest_logger.warning(
                    f"Server error {resp.status_code}{context_str} "
                    f"(attempt {attempt + 1}/{retries + 1})"
                )
                if attempt < retries:
                    time.sleep(1.5 ** attempt)
                    continue
                return None, resp.status_code

            return None, resp.status_code

        except requests.exceptions.Timeout as e:
            last_error = e
            ingest_logger.warning(
                f"Timeout{context_str} (attempt {attempt + 1}/{retries + 1}): {url}"
            )
        except requests.exceptions.ConnectionError as e:
            last_error = e
            ingest_logger.warning(
                f"Connection error{context_str} (attempt {attempt + 1}/{retries + 1})"
            )
        except requests.exceptions.RequestException as e:
            last_error = e
            ingest_logger.warning(
                f"Request failed{context_str} (attempt {attempt + 1}/{retries + 1}): {e}"
            )

        # Wait before retry (exponential backoff)
        if attempt < retries:
            time.sleep(1.5 ** attempt)

    ingest_logger.error(
        f"All {retries + 1} attempts failed{context_str}: {last_error}"
    )
    return None, 0


def validate_api_key(api_key: Optional[str], source_name: str) -> bool:
    """
    Validate that an API key is present and properly formatted.

    Args:
        api_key: The API key to validate.
        source_name: Name of the API source for error messages.

    Returns:
        True if valid, False otherwise.
    """
    if not api_key:
        ingest_logger.error(f"{source_name}: API key not configured")
        return False
    if len(api_key) < 10:
        ingest_logger.warning(f"{source_name}: API key looks invalid (too short)")
        return False
    return True


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

            # Make request with automatic retry
            data, status = api_request(
                session, url, params=params, timeout=20,
                context=f"Odds API {sport}/{market_type}"
            )
            data = data if data and status == 200 else []

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
        "player": "",  # Empty for game lines
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

        data, status = api_request(
            session,
            f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds",
            params={"apiKey": api_key, "regions": "us", "oddsFormat": "decimal"},
            timeout=15,
            context=f"Odds API futures {name}"
        )

        if status != 200 or not data:
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
        for event in data:
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
                            "player": "",  # Empty for futures
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


def fetch_odds_api_player_props(
    session: requests.Session,
    api_key: str,
    config: dict[str, Any],
    existing_games: dict[str, GameRecord],
) -> list[MarketRow]:
    """
    Fetch player props from Odds API for existing games.

    Uses the /v4/sports/{sport}/events/{event_id}/odds endpoint with
    player prop markets (player_points, player_rebounds, etc.).

    Note: This uses additional API calls - one per game.

    Args:
        session: Active requests.Session.
        api_key: Odds API key.
        config: Full configuration dict.
        existing_games: Dict of game records from previous fetch.

    Returns:
        List of player prop market rows.

    Example:
        >>> rows = fetch_odds_api_player_props(session, api_key, config, games)
        >>> print(f"Found {len(rows)} player props")
    """
    rows: list[MarketRow] = []
    now = utc_now_iso()

    # Check if player props are enabled in config
    player_props_cfg = config.get("player_props", {})
    if not player_props_cfg.get("enabled", False):
        return []

    # Player prop markets to fetch
    prop_markets = player_props_cfg.get("markets", [
        "player_points",
        "player_rebounds",
        "player_assists",
        "player_threes",
    ])

    # Get source-specific config
    source_cfg = get_source_config(config, "odds_api")
    delay = source_cfg.get("request_delay_seconds", 0.5)
    books = config.get("books", [])

    # Group games by sport to batch API calls
    games_by_sport: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for game_id, game in existing_games.items():
        if game_id.startswith("futures_"):
            continue
        sport = game.get("league", "")
        if sport:
            games_by_sport[sport].append((game_id, game))

    print(f"  Fetching props for {sum(len(g) for g in games_by_sport.values())} games...")

    # Limit to avoid using too many API calls
    max_games = player_props_cfg.get("max_games_per_run", 10)
    games_processed = 0

    for sport, game_list in games_by_sport.items():
        for game_id, game in game_list[:max_games]:
            if games_processed >= max_games:
                break

            # Need the original Odds API event ID to fetch props
            # We'll search for it in existing rows or try to reconstruct
            # For now, use a search approach with team names and date
            event_id = _find_odds_api_event_id(
                session, api_key, sport, game, delay
            )

            if not event_id:
                continue

            # Fetch player props for this event
            for prop_market in prop_markets:
                url = f"https://api.the-odds-api.com/v4/sports/{sport}/events/{event_id}/odds"
                params = {
                    "apiKey": api_key,
                    "regions": "us",
                    "markets": prop_market,
                    "oddsFormat": "decimal",
                }

                data, status = api_request(
                    session, url, params=params, timeout=15,
                    context=f"Odds API props {game_id}/{prop_market}"
                )
                time.sleep(delay)

                if status != 200 or not data:
                    continue

                prop_rows = _parse_player_props(
                    data, game_id, prop_market, books, now
                )
                rows.extend(prop_rows)

            games_processed += 1

    print(f"  → {len(rows)} player prop rows")
    return rows


def _find_odds_api_event_id(
    session: requests.Session,
    api_key: str,
    sport: str,
    game: dict[str, Any],
    delay: float,
) -> Optional[str]:
    """
    Find the Odds API event ID for a game.

    Fetches upcoming events and matches by team names.

    Args:
        session: Active requests.Session.
        api_key: Odds API key.
        sport: Sport key (e.g., basketball_nba).
        game: Game record dict with home_team and away_team.
        delay: Request delay in seconds.

    Returns:
        Event ID string or None if not found.
    """
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
    params = {"apiKey": api_key}

    events, status = api_request(
        session, url, params=params, timeout=15,
        context=f"Odds API events {sport}"
    )
    time.sleep(delay)

    if status != 200 or not events:
        return None

    home = normalize_team(game.get("home_team", ""))
    away = normalize_team(game.get("away_team", ""))

    for event in events:
        event_home = normalize_team(event.get("home_team", ""))
        event_away = normalize_team(event.get("away_team", ""))

        if event_home == home and event_away == away:
            return event.get("id")

    return None


def _parse_player_props(
    data: dict[str, Any],
    game_id: str,
    prop_market: str,
    books: list[str],
    now: str,
) -> list[MarketRow]:
    """
    Parse player props response from Odds API.

    Args:
        data: API response dict.
        game_id: Canonical game ID.
        prop_market: Market type (e.g., player_points).
        books: List of bookmaker keys to include.
        now: Current timestamp.

    Returns:
        List of MarketRow dicts.
    """
    rows: list[MarketRow] = []

    for book in data.get("bookmakers", []):
        if book["key"] not in books:
            continue

        for mkt in book.get("markets", []):
            if mkt["key"] != prop_market:
                continue

            for outcome in mkt.get("outcomes", []):
                name = outcome.get("name", "")  # Player name
                description = outcome.get("description", "")  # Over/Under
                point = outcome.get("point", 0.0)  # Line
                price = outcome.get("price", 0.0)

                # Determine side from description
                desc_lower = description.lower()
                if "over" in desc_lower:
                    side = "over"
                elif "under" in desc_lower:
                    side = "under"
                else:
                    continue

                implied_prob = odds_to_prob(price)

                rows.append({
                    "game_id": game_id,
                    "market": prop_market,
                    "side": side,
                    "line": float(point),
                    "source": "odds_api",
                    "provider": book["key"],
                    "player": normalize_player(name),
                    "price": price,
                    "implied_prob": implied_prob,
                    "devigged_prob": implied_prob,  # TODO: de-vig pairs
                    "provider_updated_at": book.get("last_update", now),
                    "last_refreshed": now,
                    "snapshot_time": now,
                    "source_event_id": data.get("id"),
                    "source_market_id": None,
                    "outcome": f"{name} {description}",
                })

    return rows


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
        data, status = api_request(
            session,
            "https://gamma-api.polymarket.com/markets",
            params={"closed": "false", "limit": 100, "offset": offset},
            timeout=15,
            context=f"Polymarket markets page {offset // 100}"
        )
        if status != 200 or not data:
            break
        all_markets.extend(data)
        time.sleep(delay)

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
                                "player": "",  # Empty for futures
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


def fetch_polymarket_games(
    session: requests.Session,
    config: dict[str, Any],
    existing_games: Optional[dict[str, GameRecord]] = None,
) -> FetchResult:
    """
    Fetch game-by-game markets from Polymarket (moneyline, spreads, totals, player props).

    Polymarket organizes sports by events with slugs like:
        'nba-was-cle-2026-02-11' (NBA: Wizards @ Cavaliers on Feb 11, 2026)

    These events are NOT returned by the general /events endpoint.
    We must query by specific slug patterns.

    Strategy:
        1. If existing_games provided (from Odds API), construct slugs from those
        2. Otherwise, try known team abbreviation combinations for today's date

    Each event contains multiple markets:
        - Moneyline: "Wizards vs. Cavaliers"
        - Spreads: "Spread: Cavaliers (-17.5)"
        - Totals: "Wizards vs. Cavaliers: O/U 238.5"
        - Player Props: "Donovan Mitchell: Points O/U 27.5"

    Polymarket prices ARE probabilities - there is no vig to remove.

    Args:
        session: Active requests.Session.
        config: Full configuration dict.
        existing_games: Optional dict of games from Odds API to cross-reference.

    Returns:
        Tuple of (games_dict, market_rows) including all market types.

    Example:
        >>> games, rows = fetch_polymarket_games(session, config)
        >>> print(f"Found {len(games)} games, {len(rows)} markets")
    """
    import datetime

    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    # Get source-specific config
    source_cfg = get_source_config(config, "polymarket")
    delay = source_cfg.get("request_delay_seconds", 0.2)

    # Team name to abbreviation mapping
    TEAM_ABBREVS = {
        # NBA
        "hawks": "atl", "atlanta hawks": "atl",
        "celtics": "bos", "boston celtics": "bos",
        "nets": "bkn", "brooklyn nets": "bkn",
        "hornets": "cha", "charlotte hornets": "cha",
        "bulls": "chi", "chicago bulls": "chi",
        "cavaliers": "cle", "cleveland cavaliers": "cle",
        "mavericks": "dal", "dallas mavericks": "dal",
        "nuggets": "den", "denver nuggets": "den",
        "pistons": "det", "detroit pistons": "det",
        "warriors": "gsw", "golden state warriors": "gsw",
        "rockets": "hou", "houston rockets": "hou",
        "pacers": "ind", "indiana pacers": "ind",
        "clippers": "lac", "la clippers": "lac", "los angeles clippers": "lac",
        "lakers": "lal", "la lakers": "lal", "los angeles lakers": "lal",
        "grizzlies": "mem", "memphis grizzlies": "mem",
        "heat": "mia", "miami heat": "mia",
        "bucks": "mil", "milwaukee bucks": "mil",
        "timberwolves": "min", "minnesota timberwolves": "min",
        "pelicans": "nop", "new orleans pelicans": "nop",
        "knicks": "nyk", "new york knicks": "nyk",
        "thunder": "okc", "oklahoma city thunder": "okc",
        "magic": "orl", "orlando magic": "orl",
        "76ers": "phi", "philadelphia 76ers": "phi",
        "suns": "phx", "phoenix suns": "phx",
        "trail blazers": "por", "portland trail blazers": "por", "blazers": "por",
        "kings": "sac", "sacramento kings": "sac",
        "spurs": "sas", "san antonio spurs": "sas",
        "raptors": "tor", "toronto raptors": "tor",
        "jazz": "uta", "utah jazz": "uta",
        "wizards": "was", "washington wizards": "was",
        # NHL (subset)
        "bruins": "bos", "boston bruins": "bos",
        "sabres": "buf", "buffalo sabres": "buf",
        "flames": "cgy", "calgary flames": "cgy",
        "hurricanes": "car", "carolina hurricanes": "car",
        "blackhawks": "chi", "chicago blackhawks": "chi",
        "avalanche": "col", "colorado avalanche": "col",
        "blue jackets": "cbj", "columbus blue jackets": "cbj",
        "stars": "dal", "dallas stars": "dal",
        "red wings": "det", "detroit red wings": "det",
        "oilers": "edm", "edmonton oilers": "edm",
        "panthers": "fla", "florida panthers": "fla",
        "wild": "min", "minnesota wild": "min",
        "canadiens": "mtl", "montreal canadiens": "mtl",
        "predators": "nsh", "nashville predators": "nsh",
        "devils": "nj", "new jersey devils": "nj",
        "islanders": "nyi", "new york islanders": "nyi",
        "rangers": "nyr", "new york rangers": "nyr",
        "senators": "ott", "ottawa senators": "ott",
        "flyers": "phi", "philadelphia flyers": "phi",
        "penguins": "pit", "pittsburgh penguins": "pit",
        "sharks": "sj", "san jose sharks": "sj",
        "kraken": "sea", "seattle kraken": "sea",
        "blues": "stl", "st. louis blues": "stl",
        "lightning": "tb", "tampa bay lightning": "tb",
        "maple leafs": "tor", "toronto maple leafs": "tor",
        "canucks": "van", "vancouver canucks": "van",
        "golden knights": "vgk", "vegas golden knights": "vgk",
        "capitals": "was", "washington capitals": "was",
        "jets": "wpg", "winnipeg jets": "wpg",
    }

    SPORT_MAP = {
        "basketball_nba": "nba",
        "icehockey_nhl": "nhl",
        "americanfootball_nfl": "nfl",
        "baseball_mlb": "mlb",
    }

    def get_abbrev(team_name: str) -> Optional[str]:
        """Get team abbreviation from full name."""
        key = team_name.lower().strip()
        return TEAM_ABBREVS.get(key)

    # Build list of slugs to try
    slugs_to_try: list[tuple[str, str, str]] = []  # (slug, away_team, home_team)

    if existing_games:
        # Use existing games from Odds API
        for game in existing_games.values():
            league = game.get("league", "")
            sport = SPORT_MAP.get(league)
            if not sport:
                continue

            # Parse date from commence_time
            commence = game.get("commence_time", "")
            if not commence:
                continue

            try:
                if "T" in commence:
                    date_str = commence.split("T")[0]
                else:
                    date_str = commence[:10]
            except (IndexError, AttributeError):
                continue

            away = game.get("away_team", "")
            home = game.get("home_team", "")

            away_abbrev = get_abbrev(away)
            home_abbrev = get_abbrev(home)

            if away_abbrev and home_abbrev:
                slug = f"{sport}-{away_abbrev}-{home_abbrev}-{date_str}"
                slugs_to_try.append((slug, away, home))

        print(f"  Trying {len(slugs_to_try)} games from Odds API...")

    else:
        # Fallback: try today's date with common matchups
        today = datetime.date.today()
        print(f"  No existing games, trying today ({today})...")

    # Fetch events by slug
    events_found = 0
    for slug, away_team, home_team in slugs_to_try:
        data, status = api_request(
            session,
            "https://gamma-api.polymarket.com/events",
            params={"slug": slug},
            timeout=10,
            retries=2,  # Fewer retries for game lookups
            context=f"Polymarket event {slug}"
        )

        if status == 200 and data and isinstance(data, list) and len(data) > 0:
            event = data[0]
            events_found += 1

            # Parse the event
            title = event.get("title", "")
            markets = event.get("markets", [])

            # Extract team names from title if different
            title_match = re.match(
                r"(.+?)\s+(?:vs\.?|@)\s+(.+)",
                title,
                re.IGNORECASE,
            )
            if title_match:
                poly_away = title_match.group(1).strip()
                poly_home = title_match.group(2).strip()
            else:
                poly_away = away_team
                poly_home = home_team

            game_id = f"poly_{slug}"

            # Parse date from slug
            date_str = "-".join(slug.split("-")[-3:])

            league_prefix = slug.split("-")[0]
            league = {
                "nba": "basketball_nba",
                "nhl": "icehockey_nhl",
                "nfl": "americanfootball_nfl",
                "mlb": "baseball_mlb",
            }.get(league_prefix, "unknown")

            games[game_id] = {
                "game_id": game_id,
                "league": league,
                "commence_time": date_str,
                "home_team": poly_home,
                "away_team": poly_away,
                "last_refreshed": now,
            }

            for market in markets:
                market_rows = _parse_polymarket_game_market(
                    market, game_id, poly_home, poly_away, now
                )
                rows.extend(market_rows)

            print(f"    ✓ {slug}: {len(markets)} markets")

        time.sleep(delay)

    print(f"  Games: {events_found}, Markets: {len(rows)}")
    return games, rows


def _parse_polymarket_game_market(
    market: dict[str, Any],
    game_id: str,
    home_team: str,
    away_team: str,
    now: str,
) -> list[MarketRow]:
    """
    Parse a single Polymarket market into standardized rows.

    Actual question formats from Polymarket:
        - Moneyline: "Wizards vs. Cavaliers"
        - Spread: "Spread: Cavaliers (-18.5)" or "1H Spread: Cavaliers (-10.5)"
        - Total: "Wizards vs. Cavaliers: O/U 238.5" or "1H O/U 122.5"
        - Player Props: "Donovan Mitchell: Points O/U 27.5"
                       "James Harden: Assists O/U 7.5"
                       "Jarrett Allen: Rebounds O/U 10.5"

    Args:
        market: Market object from Polymarket event.
        game_id: Canonical game ID.
        home_team: Home team name.
        away_team: Away team name.
        now: Current timestamp.

    Returns:
        List of MarketRow dicts.
    """
    rows: list[MarketRow] = []
    question = market.get("question", "")
    slug = market.get("slug", "")
    outcomes = safe_json(market.get("outcomes"))
    prices = safe_json(market.get("outcomePrices"))

    if not outcomes or not prices or len(outcomes) != len(prices):
        return []

    # Determine market type from question
    market_type = None
    line = 0.0
    player = ""

    question_lower = question.lower()

    # 1H Spread: "1H Spread: Cavaliers (-10.5)"
    match_1h_spread = re.search(r"1h\s+spread:.*\(([+-]?\d+\.?\d*)\)", question, re.IGNORECASE)
    if match_1h_spread:
        market_type = "spreads_1h"
        line = float(match_1h_spread.group(1))

    # 1H Total: "Wizards vs. Cavaliers: 1H O/U 122.5"
    elif re.search(r"1h\s+o/u\s*(\d+\.?\d*)", question, re.IGNORECASE):
        match = re.search(r"1h\s+o/u\s*(\d+\.?\d*)", question, re.IGNORECASE)
        market_type = "totals_1h"
        line = float(match.group(1))

    # 1H Moneyline: "Wizards vs. Cavaliers: 1H Moneyline"
    elif "1h moneyline" in question_lower:
        market_type = "h2h_1h"

    # Spread: "Spread: Cavaliers (-18.5)"
    elif re.search(r"^spread:", question, re.IGNORECASE):
        spread_match = re.search(r"\(([+-]?\d+\.?\d*)\)", question)
        if spread_match:
            market_type = "spreads"
            line = float(spread_match.group(1))

    # Total: "Team vs. Team: O/U 238.5"
    elif re.search(r":\s*o/u\s*(\d+\.?\d*)", question, re.IGNORECASE):
        total_match = re.search(r":\s*o/u\s*(\d+\.?\d*)", question, re.IGNORECASE)
        market_type = "totals"
        line = float(total_match.group(1))

    # Player Props: "Player Name: Stat O/U X.5"
    # Examples: "Donovan Mitchell: Points O/U 27.5", "James Harden: Assists O/U 7.5"
    elif re.match(r"^[^:]+:\s*(points|rebounds|assists)\s+o/u", question, re.IGNORECASE):
        prop_match = re.match(
            r"^([^:]+):\s*(points|rebounds|assists)\s+o/u\s*(\d+\.?\d*)",
            question,
            re.IGNORECASE,
        )
        if prop_match:
            player_name = prop_match.group(1).strip()
            prop_type = prop_match.group(2).lower()
            line = float(prop_match.group(3))
            player = normalize_player(player_name)

            # Map prop type to market name
            prop_map = {
                "points": "player_points",
                "rebounds": "player_rebounds",
                "assists": "player_assists",
            }
            market_type = prop_map.get(prop_type, "player_points")

    # Moneyline (simple team vs team without any special markers)
    # Example: "Wizards vs. Cavaliers"
    elif re.match(r"^[^:]+\s+vs\.?\s+[^:]+$", question, re.IGNORECASE):
        market_type = "h2h"

    if not market_type:
        return []

    # Parse outcomes and create rows
    for i, outcome in enumerate(outcomes):
        if i >= len(prices):
            break

        try:
            price = float(prices[i])
        except (ValueError, TypeError):
            continue

        outcome_str = str(outcome).strip()
        outcome_lower = outcome_str.lower()

        # Determine side based on market type
        if market_type in ("h2h", "h2h_1h", "spreads", "spreads_1h"):
            # Map outcome to home/away
            outcome_norm = normalize_team(outcome_str)
            if outcome_norm == normalize_team(home_team):
                side = "home"
            elif outcome_norm == normalize_team(away_team):
                side = "away"
            else:
                # Check if it's a substring match
                if outcome_lower in home_team.lower():
                    side = "home"
                elif outcome_lower in away_team.lower():
                    side = "away"
                else:
                    side = outcome_norm

        elif market_type in ("totals", "totals_1h"):
            if outcome_lower in ("over", "yes"):
                side = "over"
            elif outcome_lower in ("under", "no"):
                side = "under"
            else:
                continue

        elif market_type.startswith("player_"):
            if outcome_lower in ("yes", "over"):
                side = "over"
            elif outcome_lower in ("no", "under"):
                side = "under"
            else:
                continue

        else:
            side = outcome_lower

        rows.append({
            "game_id": game_id,
            "market": market_type,
            "side": side,
            "line": line,
            "source": "polymarket",
            "provider": "polymarket",
            "player": player,
            "price": price,
            "implied_prob": price,  # Polymarket prices ARE probabilities
            "devigged_prob": price,  # No vig to remove
            "provider_updated_at": now,
            "last_refreshed": now,
            "snapshot_time": now,
            "source_event_id": market.get("id"),
            "source_market_id": slug,
            "outcome": outcome_str,
        })

    return rows


# =============================================================================
# KALSHI - US REGULATED PREDICTION MARKET
# =============================================================================

def fetch_kalshi(session: requests.Session, config: dict[str, Any]) -> FetchResult:
    """
    Fetch sports markets from Kalshi (h2h, spreads, totals, player props).

    Kalshi provides sports exposure through parlay-style markets, but the
    individual leg markets can be fetched directly with prices.

    Ticker patterns:
        - KXNBAGAME-26FEB11ATLCHA-ATL       (h2h: ATL to win vs CHA)
        - KXNBASPREAD-26FEB11ATLCHA-ATL6    (spread: ATL -6)
        - KXNBATOTAL-26FEB11ATLCHA-228      (total: O/U 228)
        - KXNBAPTS-26FEB11ATLCHA-ATLCMCCOLLUM3-15  (player points: 15+)
        - KXNBAREB-26FEB11ATLCHA-...        (player rebounds)
        - KXNBAAST-26FEB11ATLCHA-...        (player assists)
        - KXNBA3PT-26FEB11ATLCHA-...        (player 3-pointers)

    Args:
        session: Active requests.Session.
        config: Full configuration dict.

    Returns:
        Tuple of (games_dict, market_rows) with prices.

    Example:
        >>> games, rows = fetch_kalshi(session, config)
        >>> print(f"Found {len(games)} games, {len(rows)} market rows")
    """
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    # Get source-specific config
    source_cfg = get_source_config(config, "kalshi")
    delay = source_cfg.get("request_delay_seconds", 0.3)

    # Fetch all open markets with cursor pagination to get leg tickers
    print("  Fetching parlay markets...")
    all_markets: list[dict] = []
    cursor: Optional[str] = None

    for page in range(10):  # Max 1000 markets
        params: dict[str, Any] = {"limit": 100, "status": "open"}
        if cursor:
            params["cursor"] = cursor

        data, status = api_request(
            session,
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params=params,
            timeout=15,
            context=f"Kalshi markets page {page}"
        )
        if status != 200 or not data:
            break

        all_markets.extend(data.get("markets", []))
        cursor = data.get("cursor")
        time.sleep(delay * 0.5)

        if not cursor or len(data.get("markets", [])) < 100:
            break

    print(f"  Fetched {len(all_markets)} parlay markets")

    # Extract unique leg tickers
    leg_tickers: set[str] = set()
    for market in all_markets:
        for leg in market.get("mve_selected_legs", []):
            ticker = leg.get("market_ticker", "")
            if ticker and ticker.startswith("KX"):
                leg_tickers.add(ticker)

    print(f"  Found {len(leg_tickers)} unique leg tickers")

    # Filter to sports leagues we care about
    SPORTS_PREFIXES = {"KXNBA", "KXNFL", "KXNHL", "KXMLB", "KXNCAAMB"}
    sports_tickers = [t for t in leg_tickers if any(t.startswith(p) for p in SPORTS_PREFIXES)]
    print(f"  Sports tickers: {len(sports_tickers)}")

    # Fetch individual markets with prices
    fetched = 0
    for ticker in sports_tickers:
        data, status = api_request(
            session,
            f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}",
            timeout=10,
            retries=1,  # Single retry for individual markets
            context=f"Kalshi market {ticker}"
        )
        if status != 200 or not data:
            continue

        market_data = data.get("market", {})
        result = _parse_kalshi_market(ticker, market_data, now)

        if result:
            game_record, row = result
            games[game_record["game_id"]] = game_record
            rows.append(row)
            fetched += 1

        time.sleep(delay * 0.2)

    print(f"  Fetched {fetched} markets with prices")
    return games, rows


def _parse_kalshi_market(
    ticker: str,
    market: dict[str, Any],
    now: str,
) -> Optional[tuple[GameRecord, MarketRow]]:
    """
    Parse a Kalshi market ticker and response into game and market records.

    Ticker patterns:
        - KXNBAGAME-26FEB11ATLCHA-ATL       (h2h)
        - KXNBASPREAD-26FEB11ATLCHA-ATL6    (spread: team -6)
        - KXNBATOTAL-26FEB11ATLCHA-228      (total: O/U 228)
        - KXNBAPTS-26FEB11ATLCHA-ATLCMCCOLLUM3-15  (player points: 15+)
        - KXNBAREB-26FEB11ATLCHA-ATLPLAYER-4      (player rebounds: 4+)
        - KXNBAAST-26FEB11ATLCHA-ATLPLAYER-4      (player assists: 4+)
        - KXNBA3PT-26FEB11ATLCHA-ATLPLAYER-2      (player 3PT: 2+)

    Args:
        ticker: Full market ticker string.
        market: Market data from Kalshi API.
        now: Current timestamp.

    Returns:
        Tuple of (game_record, market_row) or None if invalid.
    """
    # Month name to number mapping
    MONTHS = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }

    parts = ticker.split("-")
    if len(parts) < 2:
        return None

    prefix = parts[0]  # e.g., KXNBAGAME, KXNBASPREAD, KXNBAPTS

    # Parse date and teams from middle part: 26FEB11ATLCHA
    date_teams_match = re.match(r"(\d{2})([A-Z]{3})(\d{2})([A-Z]{3,6})$", parts[1])
    if not date_teams_match:
        return None

    year_short, month_abbr, day, teams_str = date_teams_match.groups()
    date = f"20{year_short}-{MONTHS.get(month_abbr, '01')}-{day}"

    # Extract team abbreviations (3 chars each)
    away_team = teams_str[:3]
    home_team = teams_str[3:6] if len(teams_str) >= 6 else teams_str[3:]

    # Determine league from prefix
    if "NBA" in prefix:
        league = "basketball_nba"
    elif "NFL" in prefix:
        league = "americanfootball_nfl"
    elif "NHL" in prefix:
        league = "icehockey_nhl"
    elif "MLB" in prefix:
        league = "baseball_mlb"
    elif "NCAAMB" in prefix:
        league = "basketball_ncaab"
    else:
        return None

    game_id = f"kalshi_{date}_{away_team}_{home_team}"

    # Determine market type and parse selection
    market_type = ""
    side = ""
    line = 0.0
    player = ""

    # Get prices from market data (Kalshi uses cents, 0-100)
    yes_bid = market.get("yes_bid", 0) or 0
    yes_ask = market.get("yes_ask", 0) or 0
    # Use midpoint as price, convert from cents to probability
    price = ((yes_bid + yes_ask) / 2) / 100 if (yes_bid or yes_ask) else None

    selection = parts[2] if len(parts) > 2 else ""

    if "GAME" in prefix:
        # h2h: KXNBAGAME-26FEB11ATLCHA-ATL
        market_type = "h2h"
        side = "away" if selection == away_team else "home"

    elif "SPREAD" in prefix:
        # spread: KXNBASPREAD-26FEB11ATLCHA-ATL6
        market_type = "spreads"
        # Parse team and spread from selection (e.g., ATL6 = ATL -6)
        spread_match = re.match(r"([A-Z]{2,4})(\d+\.?\d*)", selection)
        if spread_match:
            spread_team, spread_val = spread_match.groups()
            line = -float(spread_val)  # Kalshi shows favorite spread as positive
            side = "away" if spread_team == away_team else "home"
        else:
            return None

    elif "TOTAL" in prefix:
        # total: KXNBATOTAL-26FEB11ATLCHA-228
        market_type = "totals"
        try:
            line = float(selection)
            side = "over"  # Kalshi "Yes" = over
        except ValueError:
            return None

    elif "PTS" in prefix:
        # player points: KXNBAPTS-26FEB11ATLCHA-ATLCMCCOLLUM3-15
        # parts[2] = team+player+jersey, parts[3] = line
        market_type = "player_points"
        if len(parts) >= 4:
            player = _parse_kalshi_player_name(parts[2])
            try:
                line = float(parts[3])
            except ValueError:
                return None
        side = "over"

    elif "REB" in prefix:
        # player rebounds
        market_type = "player_rebounds"
        if len(parts) >= 4:
            player = _parse_kalshi_player_name(parts[2])
            try:
                line = float(parts[3])
            except ValueError:
                return None
        side = "over"

    elif "AST" in prefix:
        # player assists
        market_type = "player_assists"
        if len(parts) >= 4:
            player = _parse_kalshi_player_name(parts[2])
            try:
                line = float(parts[3])
            except ValueError:
                return None
        side = "over"

    elif "3PT" in prefix:
        # player 3-pointers
        market_type = "player_threes"
        if len(parts) >= 4:
            player = _parse_kalshi_player_name(parts[2])
            try:
                line = float(parts[3])
            except ValueError:
                return None
        side = "over"

    else:
        return None

    if not market_type:
        return None

    game_record: GameRecord = {
        "game_id": game_id,
        "league": league,
        "commence_time": date,
        "home_team": home_team,
        "away_team": away_team,
        "last_refreshed": now,
    }

    row: MarketRow = {
        "game_id": game_id,
        "market": market_type,
        "side": side,
        "line": line,
        "source": "kalshi",
        "provider": "kalshi",
        "player": player,
        "price": price,
        "implied_prob": price,  # Kalshi prices are probabilities
        "devigged_prob": price,  # No vig in prediction markets
        "provider_updated_at": now,
        "last_refreshed": now,
        "snapshot_time": now,
        "source_event_id": ticker,
        "source_market_id": market.get("ticker", ticker),
        "outcome": market.get("title", selection),
    }

    return game_record, row


def _parse_kalshi_player_name(player_part: str) -> str:
    """
    Parse player name from Kalshi ticker part.

    Format: {TEAM}{PLAYERNAME}{JERSEY}
    Examples:
        - ATLCMCCOLLUM3 -> "cmccollum"
        - ORLDBANE3 -> "dbane"
        - SASVWEMBANYAMA1 -> "vwembanyama"

    Args:
        player_part: Team+player+jersey string (e.g., "ATLCMCCOLLUM3")

    Returns:
        Normalized player name string.
    """
    if len(player_part) <= 3:
        return normalize_player(player_part)

    # Remove team prefix (first 3 chars)
    name_part = player_part[3:]
    # Remove trailing digits (jersey number)
    name_cleaned = re.sub(r"\d+$", "", name_part)
    return normalize_player(name_cleaned)


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

                        # Player props (if enabled)
                        player_props_cfg = config.get("player_props", {})
                        if player_props_cfg.get("enabled", False):
                            print("  Player Props:")
                            prop_rows = fetch_odds_api_player_props(
                                session, api_key, config, all_games
                            )
                            all_rows.extend(prop_rows)
                            # Estimate API calls (events + props per game)
                            api_calls += len(all_games) * 2

                        update_source_metadata(conn, "odds_api", success=True, calls_made=api_calls)

                    elif source_name == "polymarket":
                        # Futures (championship odds)
                        print("  Futures:")
                        g, r = fetch_polymarket(session, config)
                        all_games.update(g)
                        all_rows.extend(r)
                        print(f"  → {len(r)} futures rows")

                        # Game-by-game markets (moneyline, spreads, totals, props)
                        # Pass existing games to cross-reference with Polymarket slugs
                        print("  Games:")
                        g, r = fetch_polymarket_games(session, config, all_games)
                        all_games.update(g)
                        all_rows.extend(r)
                        print(f"  → {len(g)} games, {len(r)} market rows")

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
                ["game_id", "market", "side", "line", "source", "provider", "player"],
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
