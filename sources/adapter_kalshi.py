"""Kalshi ingestion."""
from __future__ import annotations

import re
import time
from typing import Any, Optional

import requests

from sources.adapter_common import api_request
from utils import get_source_config, normalize_player, utc_now_iso

GameRecord = dict[str, Any]
MarketRow = dict[str, Any]
FetchResult = tuple[dict[str, GameRecord], list[MarketRow]]

MONTHS = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
    "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def fetch(session: requests.Session, config: dict[str, Any]) -> FetchResult:
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    delay = get_source_config(config, "kalshi").get("request_delay_seconds", 0.3)

    all_markets: list[dict[str, Any]] = []
    cursor = None
    for page in range(10):
        params = {"limit": 100, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        data, status = api_request(
            session,
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params=params,
        )
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

    sports_prefixes = {"KXNBA", "KXNFL", "KXNHL"}
    sports_tickers = [t for t in leg_tickers if any(t.startswith(p) for p in sports_prefixes)]

    for ticker in sports_tickers[:100]:
        data, status = api_request(
            session,
            f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}",
            retries=1,
        )
        if status == 200 and data:
            result = _parse_market(ticker, data.get("market", {}), now)
            if result:
                game_record, row = result
                games[game_record["game_id"]] = game_record
                rows.append(row)
        time.sleep(delay * 0.2)

    return games, rows


def _parse_market(ticker: str, market: dict[str, Any], now: str) -> Optional[tuple[GameRecord, MarketRow]]:
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

    if price is None:
        return None

    game_record = {
        "game_id": game_id,
        "league": league,
        "commence_time": date,
        "home_team": home_team,
        "away_team": away_team,
        "last_refreshed": now,
    }
    row = {
        "game_id": game_id,
        "market": market_type,
        "side": side,
        "line": line,
        "source": "kalshi",
        "provider": "kalshi",
        "player": player,
        "price": price,
        "implied_prob": price,
        "devigged_prob": price,
        "provider_updated_at": now,
        "last_refreshed": now,
        "snapshot_time": now,
    }
    return game_record, row
