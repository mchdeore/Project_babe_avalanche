"""Polymarket ingestion."""
from __future__ import annotations

import re
import time
from typing import Any, Optional

import requests

from sources.common import api_request
from utils import get_source_config, normalize_player, normalize_team, safe_json, utc_now_iso

GameRecord = dict[str, Any]
MarketRow = dict[str, Any]
FetchResult = tuple[dict[str, GameRecord], list[MarketRow]]

TEAM_ABBREVS = {
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


def fetch(
    session: requests.Session,
    config: dict[str, Any],
    existing_games: Optional[dict[str, GameRecord]] = None,
) -> FetchResult:
    games, rows = _fetch_futures(session, config)

    game_games, game_rows = _fetch_games(session, config, existing_games or {})
    games.update(game_games)
    rows.extend(game_rows)

    return games, rows


def _fetch_futures(session: requests.Session, config: dict[str, Any]) -> FetchResult:
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    source_cfg = get_source_config(config, "polymarket")
    delay = source_cfg.get("request_delay_seconds", 0.2)

    all_markets: list[dict[str, Any]] = []
    for offset in range(0, 500, 100):
        data, status = api_request(
            session,
            "https://gamma-api.polymarket.com/markets",
            params={"closed": "false", "limit": 100, "offset": offset},
        )
        if status != 200 or not data:
            break
        all_markets.extend(data)
        time.sleep(delay)

    futures = {
        "futures_basketball_nba_championship_winner": ("nba", "NBA Championship"),
        "futures_icehockey_nhl_championship_winner": ("stanley cup", "NHL Stanley Cup"),
    }

    for futures_id, (phrase, name) in futures.items():
        games[futures_id] = {
            "game_id": futures_id,
            "league": futures_id.replace("futures_", ""),
            "commence_time": "",
            "home_team": name,
            "away_team": "",
            "last_refreshed": now,
        }

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
                        except (ValueError, TypeError):
                            break

                        rows.append({
                            "game_id": futures_id,
                            "market": "futures",
                            "side": team,
                            "line": 0.0,
                            "source": "polymarket",
                            "provider": "polymarket",
                            "player": "",
                            "price": price,
                            "implied_prob": price,
                            "devigged_prob": price,
                            "provider_updated_at": now,
                            "last_refreshed": now,
                            "snapshot_time": now,
                            "source_event_id": market.get("id"),
                            "source_market_id": None,
                            "outcome": team,
                        })
                        break

    return games, rows


def _fetch_games(
    session: requests.Session,
    config: dict[str, Any],
    existing_games: dict[str, GameRecord],
) -> FetchResult:
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    source_cfg = get_source_config(config, "polymarket")
    delay = source_cfg.get("request_delay_seconds", 0.2)

    if not existing_games:
        return games, rows

    slugs: list[tuple[str, str, str]] = []
    for game in existing_games.values():
        league = game.get("league", "")
        sport = SPORT_MAP.get(league)
        if not sport:
            continue

        commence = game.get("commence_time", "")
        if not commence:
            continue

        date_str = commence.split("T")[0] if "T" in commence else commence[:10]
        away_abbrev = _get_abbrev(game.get("away_team", ""))
        home_abbrev = _get_abbrev(game.get("home_team", ""))
        if away_abbrev and home_abbrev:
            slugs.append((
                f"{sport}-{away_abbrev}-{home_abbrev}-{date_str}",
                game.get("away_team", ""),
                game.get("home_team", ""),
            ))

    for slug, away_team, home_team in slugs[:50]:
        data, status = api_request(
            session,
            "https://gamma-api.polymarket.com/events",
            params={"slug": slug},
            retries=2,
        )
        if status == 200 and data and isinstance(data, list) and len(data) > 0:
            event = data[0]
            game_id = f"poly_{slug}"
            league = {"nba": "basketball_nba", "nfl": "americanfootball_nfl", "nhl": "icehockey_nhl"}.get(
                slug.split("-")[0], "unknown"
            )

            games[game_id] = {
                "game_id": game_id,
                "league": league,
                "commence_time": "-".join(slug.split("-")[-3:]),
                "home_team": home_team,
                "away_team": away_team,
                "last_refreshed": now,
            }

            for market in event.get("markets", []):
                rows.extend(_parse_market(market, game_id, home_team, away_team, now))

        time.sleep(delay)

    return games, rows


def _parse_market(market: dict[str, Any], game_id: str, home_team: str, away_team: str, now: str) -> list[MarketRow]:
    rows: list[MarketRow] = []
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
        if total_match:
            market_type, line = "totals", float(total_match.group(1))
    elif re.match(r"^[^:]+:\s*(points|rebounds|assists)\s+o/u", question, re.IGNORECASE):
        prop_match = re.match(r"^([^:]+):\s*(points|rebounds|assists)\s+o/u\s*(\d+\.?\d*)", question, re.IGNORECASE)
        if prop_match:
            player = normalize_player(prop_match.group(1).strip())
            prop_type = prop_match.group(2).lower()
            line = float(prop_match.group(3))
            market_type = {
                "points": "player_points",
                "rebounds": "player_rebounds",
                "assists": "player_assists",
            }.get(prop_type)
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

        rows.append({
            "game_id": game_id,
            "market": market_type,
            "side": side,
            "line": line,
            "source": "polymarket",
            "provider": "polymarket",
            "player": player,
            "price": price,
            "implied_prob": price,
            "devigged_prob": price,
            "provider_updated_at": now,
            "last_refreshed": now,
            "snapshot_time": now,
        })

    return rows


def _get_abbrev(team: str) -> Optional[str]:
    key = team.lower().strip()
    return TEAM_ABBREVS.get(key)
