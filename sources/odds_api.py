"""Odds API ingestion."""
from __future__ import annotations

import os
import time
from collections import defaultdict
from typing import Any, Optional

import requests

from sources.common import api_request
from utils import (
    canonical_game_id,
    get_source_config,
    normalize_player,
    normalize_team,
    odds_to_prob,
    utc_now_iso,
    within_window,
)

GameRecord = dict[str, Any]
MarketRow = dict[str, Any]
FetchResult = tuple[dict[str, GameRecord], list[MarketRow]]


def fetch(session: requests.Session, config: dict[str, Any]) -> FetchResult:
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key or len(api_key) < 10:
        return {}, []

    games, rows = _fetch_games(session, api_key, config)

    futures_games, futures_rows = _fetch_futures(session, api_key, config)
    games.update(futures_games)
    rows.extend(futures_rows)

    props_cfg = config.get("player_props", {})
    if props_cfg.get("enabled", False):
        rows.extend(_fetch_player_props(session, api_key, config, games))

    return games, rows


def _fetch_games(
    session: requests.Session,
    api_key: str,
    config: dict[str, Any],
) -> FetchResult:
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    source_cfg = get_source_config(config, "odds_api")
    delay = source_cfg.get("request_delay_seconds", 0.5)

    sports = config.get("sports", [])
    markets = config.get("markets", [])
    regions = config.get("regions", ["us"])
    books = config.get("books", [])

    for sport in sports:
        for market_type in markets:
            url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
            params = {
                "apiKey": api_key,
                "regions": ",".join(regions),
                "markets": market_type,
                "oddsFormat": "decimal",
                "dateFormat": "iso",
            }

            data, status = api_request(session, url, params=params, timeout=20)
            data = data if data and status == 200 else []

            time.sleep(delay)

            for game in data:
                result = _process_game(game, market_type, now, config, books)
                if result:
                    game_record, game_rows = result
                    games[game_record["game_id"]] = game_record
                    rows.extend(game_rows)

    return games, rows


def _fetch_futures(
    session: requests.Session,
    api_key: str,
    config: dict[str, Any],
) -> FetchResult:
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    source_cfg = get_source_config(config, "odds_api")
    delay = source_cfg.get("request_delay_seconds", 0.5)

    books = config.get("books", [])

    futures = {
        "basketball_nba_championship_winner": "NBA Championship",
        "icehockey_nhl_championship_winner": "NHL Stanley Cup",
    }

    for sport_key, name in futures.items():
        data, status = api_request(
            session,
            f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds",
            params={"apiKey": api_key, "regions": "us", "oddsFormat": "decimal"},
            timeout=15,
        )

        if status != 200 or not data:
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

        for event in data:
            for book in event.get("bookmakers", []):
                if book["key"] not in books:
                    continue

                for mkt in book.get("markets", []):
                    outcomes = mkt.get("outcomes", [])
                    for out in outcomes:
                        team = normalize_team(out.get("name", ""))
                        price = out.get("price", 0)
                        implied_prob = odds_to_prob(price)
                        rows.append({
                            "game_id": futures_id,
                            "market": "futures",
                            "side": team,
                            "line": 0.0,
                            "source": "odds_api",
                            "provider": book["key"],
                            "player": "",
                            "price": price,
                            "implied_prob": implied_prob,
                            "devigged_prob": implied_prob,
                            "provider_updated_at": book.get("last_update", now),
                            "last_refreshed": now,
                            "snapshot_time": now,
                            "source_event_id": event.get("id"),
                            "source_market_id": None,
                            "outcome": out.get("name", ""),
                        })
        time.sleep(delay)

    return games, rows


def _fetch_player_props(
    session: requests.Session,
    api_key: str,
    config: dict[str, Any],
    existing_games: dict[str, GameRecord],
) -> list[MarketRow]:
    rows: list[MarketRow] = []
    now = utc_now_iso()

    props_cfg = config.get("player_props", {})
    prop_markets = props_cfg.get("markets", [
        "player_points",
        "player_rebounds",
        "player_assists",
        "player_threes",
    ])

    source_cfg = get_source_config(config, "odds_api")
    delay = source_cfg.get("request_delay_seconds", 0.5)
    books = config.get("books", [])

    games_by_sport: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for game_id, game in existing_games.items():
        if game_id.startswith("futures_"):
            continue
        sport = game.get("league", "")
        if sport:
            games_by_sport[sport].append((game_id, game))

    max_games = props_cfg.get("max_games_per_run", 10)
    games_processed = 0

    for sport, game_list in games_by_sport.items():
        for game_id, game in game_list[:max_games]:
            if games_processed >= max_games:
                break

            event_id = _find_odds_api_event_id(session, api_key, sport, game, delay)
            if not event_id:
                continue

            for prop_market in prop_markets:
                url = f"https://api.the-odds-api.com/v4/sports/{sport}/events/{event_id}/odds"
                params = {
                    "apiKey": api_key,
                    "regions": "us",
                    "markets": prop_market,
                    "oddsFormat": "decimal",
                }

                data, status = api_request(session, url, params=params, timeout=15)
                time.sleep(delay)

                if status != 200 or not data:
                    continue

                rows.extend(_parse_player_props(data, game_id, prop_market, books, now))

            games_processed += 1

    return rows


def _find_odds_api_event_id(
    session: requests.Session,
    api_key: str,
    sport: str,
    game: dict[str, Any],
    delay: float,
) -> Optional[str]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
    params = {"apiKey": api_key}

    events, status = api_request(session, url, params=params, timeout=15)
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
    rows: list[MarketRow] = []

    for book in data.get("bookmakers", []):
        if book["key"] not in books:
            continue

        for mkt in book.get("markets", []):
            if mkt["key"] != prop_market:
                continue

            for outcome in mkt.get("outcomes", []):
                name = outcome.get("name", "")
                description = outcome.get("description", "")
                point = outcome.get("point", 0.0)
                price = outcome.get("price", 0.0)

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
                    "devigged_prob": implied_prob,
                    "provider_updated_at": book.get("last_update", now),
                    "last_refreshed": now,
                    "snapshot_time": now,
                    "source_event_id": data.get("id"),
                    "source_market_id": None,
                    "outcome": f"{name} {description}",
                })

    return rows


def _process_game(
    game: dict[str, Any],
    market_type: str,
    now: str,
    config: dict[str, Any],
    books: list[str],
) -> Optional[tuple[GameRecord, list[MarketRow]]]:
    home = game.get("home_team")
    away = game.get("away_team")
    commence = game.get("commence_time")

    if not all([home, away, commence]):
        return None

    window_days = config.get("bettable_window_days", 14)
    if not within_window(commence, window_days):
        return None

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
    name = outcome.get("name")
    price = outcome.get("price")

    if name is None or price is None:
        return None

    market_key = market["key"]

    if market_key == "totals":
        side = name.strip().lower()
        line = outcome.get("point")
        if side not in {"over", "under"}:
            return None
    else:
        normalized = normalize_team(name)
        home_norm = normalize_team(home)
        away_norm = normalize_team(away)

        if normalized == home_norm or home_norm in normalized:
            side = "home"
        elif normalized == away_norm or away_norm in normalized:
            side = "away"
        else:
            side = normalized

        line = outcome.get("point", 0.0)

    implied_prob = odds_to_prob(price)

    return {
        "game_id": game_id,
        "market": market_key,
        "side": side,
        "line": float(line) if line is not None else 0.0,
        "source": "odds_api",
        "provider": book["key"],
        "player": "",
        "price": price,
        "implied_prob": implied_prob,
        "devigged_prob": implied_prob,
        "provider_updated_at": book.get("last_update", now),
        "last_refreshed": now,
        "snapshot_time": now,
        "source_event_id": game.get("id"),
        "source_market_id": None,
        "outcome": name,
    }
