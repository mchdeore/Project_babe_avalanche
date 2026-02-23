"""STX ingestion via GraphQL login + refresh."""
from __future__ import annotations

import os
import time
import uuid
from typing import Any, Optional

import requests

from utils import (
    canonical_game_id,
    get_source_config,
    normalize_team,
    utc_now_iso,
    within_window,
)

DEFAULT_GRAPHQL_URL = "https://api.stx.ca/graphql"
DEFAULT_TIMEOUT_SECONDS = 15

GameRecord = dict[str, Any]
MarketRow = dict[str, Any]
FetchResult = tuple[dict[str, GameRecord], list[MarketRow]]

_DEVICE_ID_CACHE: Optional[str] = None


class STXClient:
    def __init__(
        self,
        session: requests.Session,
        graphql_url: str,
        email: str,
        password: str,
        device_id: str,
    ) -> None:
        self.session = session
        self.graphql_url = graphql_url
        self.email = email
        self.password = password
        self.device_id = device_id
        self.token: Optional[str] = None
        self.refresh_token: Optional[str] = None

    def login(self) -> bool:
        if not self.email or not self.password:
            return False

        query = (
            "mutation login($input: LoginCredentials!) {"
            "  login(credentials: $input) {"
            "    token"
            "    refreshToken"
            "    userId"
            "    sessionId"
            "  }"
            "}"
        )
        variables = {
            "input": {
                "email": self.email,
                "password": self.password,
                "deviceInfo": {"deviceId": self.device_id},
            }
        }
        result, status = _graphql_post(self.session, self.graphql_url, query, variables)
        if status != 200 or not result:
            return False

        if result.get("errors"):
            return False

        login_data = (result.get("data") or {}).get("login") or {}
        token = login_data.get("token")
        refresh_token = login_data.get("refreshToken")

        if not token:
            return False

        self.token = token
        self.refresh_token = refresh_token
        return True

    def refresh(self) -> bool:
        if not self.refresh_token:
            return False

        escaped = _graphql_escape(self.refresh_token)
        query = (
            "mutation refreshToken {"
            f"  newToken(refreshToken: \"{escaped}\") {{"
            "    token"
            "    refreshToken"
            "  }"
            "}"
        )

        result, status = _graphql_post(self.session, self.graphql_url, query, None)
        if status != 200 or not result:
            return False

        if result.get("errors"):
            return False

        new_token = (result.get("data") or {}).get("newToken") or {}
        token = new_token.get("token")
        refresh_token = new_token.get("refreshToken")

        if not token:
            return False

        self.token = token
        if refresh_token:
            self.refresh_token = refresh_token
        return True

    def graphql(self, query: str, variables: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
        if not self.token and not self.login():
            return None

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        result, status = _graphql_post(
            self.session,
            self.graphql_url,
            query,
            variables,
            headers=headers,
        )

        if _is_auth_error(status, result):
            if self.refresh():
                headers["Authorization"] = f"Bearer {self.token}"
                result, status = _graphql_post(
                    self.session,
                    self.graphql_url,
                    query,
                    variables,
                    headers=headers,
                )
            elif self.login():
                headers["Authorization"] = f"Bearer {self.token}"
                result, status = _graphql_post(
                    self.session,
                    self.graphql_url,
                    query,
                    variables,
                    headers=headers,
                )

        if status != 200 or not result:
            return None

        if result.get("errors"):
            return None

        return result


def fetch(session: requests.Session, config: dict[str, Any]) -> FetchResult:
    games: dict[str, GameRecord] = {}
    rows: list[MarketRow] = []
    now = utc_now_iso()

    source_cfg = get_source_config(config, "stx")
    delay = source_cfg.get("request_delay_seconds", 0.2)

    graphql_url = os.getenv("STX_GRAPHQL_URL", DEFAULT_GRAPHQL_URL)
    email = os.getenv("STX_EMAIL", "")
    password = os.getenv("STX_PASSWORD", "")
    device_id = _get_device_id()

    client = STXClient(session, graphql_url, email, password, device_id)
    if not client.login():
        return games, rows

    events_query = """
    query GetSportsEvents($sportType: String, $limit: Int) {
        events(sportType: $sportType, limit: $limit, status: "open") {
            id
            name
            sportType
            league
            startTime
            homeTeam { id name abbreviation }
            awayTeam { id name abbreviation }
            markets {
                id
                type
                line
                outcomes {
                    id
                    name
                    side
                    bestBid
                    bestAsk
                    lastPrice
                }
            }
        }
    }
    """

    sport_types = {
        "basketball_nba": "basketball",
        "icehockey_nhl": "hockey",
        "americanfootball_nfl": "football",
        "baseball_mlb": "baseball",
    }

    config_sports = config.get("sports", [])
    stx_sports = [sport_types.get(s) for s in config_sports if sport_types.get(s)]
    if not stx_sports:
        stx_sports = ["basketball", "hockey", "football"]

    allowed_markets = set(config.get("markets", []))
    props_cfg = config.get("player_props", {})
    props_enabled = props_cfg.get("enabled", False)
    allowed_props = set(props_cfg.get("markets", [])) if props_enabled else set()

    for sport_type in stx_sports:
        variables = {"sportType": sport_type, "limit": 100}
        result = client.graphql(events_query, variables)
        if not result:
            continue

        events = (result.get("data") or {}).get("events", [])
        for event in events:
            parsed = _parse_event(event, now, config, allowed_markets, allowed_props)
            if parsed:
                game_record, event_rows = parsed
                games[game_record["game_id"]] = game_record
                rows.extend(event_rows)

        time.sleep(delay)

    return games, rows


def _parse_event(
    event: dict[str, Any],
    now: str,
    config: dict[str, Any],
    allowed_markets: set[str],
    allowed_props: set[str],
) -> Optional[tuple[GameRecord, list[MarketRow]]]:
    event_id = event.get("id")
    sport_type = event.get("sportType", "")
    start_time = event.get("startTime", "")

    home_team_data = event.get("homeTeam", {}) or {}
    away_team_data = event.get("awayTeam", {}) or {}
    home_team = home_team_data.get("name", "")
    away_team = away_team_data.get("name", "")

    if not all([event_id, home_team, away_team]):
        return None

    league_map = {
        "basketball": "basketball_nba",
        "hockey": "icehockey_nhl",
        "football": "americanfootball_nfl",
        "baseball": "baseball_mlb",
    }
    our_league = league_map.get(str(sport_type).lower(), str(sport_type))

    window_days = config.get("bettable_window_days", 14)
    if start_time and not within_window(start_time, window_days):
        return None

    date_str = start_time[:10] if start_time else now[:10]
    game_id = canonical_game_id(our_league, home_team, away_team, date_str)

    game_record = {
        "game_id": game_id,
        "league": our_league,
        "commence_time": start_time,
        "home_team": home_team,
        "away_team": away_team,
        "last_refreshed": now,
    }

    rows: list[MarketRow] = []
    for market in event.get("markets", []) or []:
        rows.extend(
            _parse_market(market, game_id, home_team, away_team, now, allowed_markets, allowed_props)
        )

    return game_record, rows


def _parse_market(
    market: dict[str, Any],
    game_id: str,
    home_team: str,
    away_team: str,
    now: str,
    allowed_markets: set[str],
    allowed_props: set[str],
) -> list[MarketRow]:
    rows: list[MarketRow] = []

    market_type = (market.get("type") or "").lower()
    line = market.get("line")
    outcomes = market.get("outcomes", []) or []

    if not outcomes:
        return []

    type_map = {
        "moneyline": "h2h",
        "money_line": "h2h",
        "h2h": "h2h",
        "spread": "spreads",
        "point_spread": "spreads",
        "spreads": "spreads",
        "total": "totals",
        "over_under": "totals",
        "totals": "totals",
        "player_points": "player_points",
        "player_rebounds": "player_rebounds",
        "player_assists": "player_assists",
        "player_threes": "player_threes",
    }

    our_market_type = type_map.get(market_type, market_type)
    if not our_market_type:
        return []

    is_prop = our_market_type.startswith("player_")
    if is_prop:
        if our_market_type not in allowed_props:
            return []
    else:
        if our_market_type not in allowed_markets:
            return []

    for outcome in outcomes:
        outcome_id = outcome.get("id")
        outcome_name = outcome.get("name", "")
        outcome_side = (outcome.get("side") or "").lower()

        best_bid = outcome.get("bestBid")
        best_ask = outcome.get("bestAsk")
        last_price = outcome.get("lastPrice")

        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2
            price = mid / 100 if mid > 1 else mid
        elif last_price is not None:
            price = last_price / 100 if last_price > 1 else last_price
        else:
            continue

        if our_market_type in ("h2h", "spreads"):
            outcome_norm = normalize_team(outcome_name)
            home_norm = normalize_team(home_team)
            away_norm = normalize_team(away_team)
            if outcome_norm == home_norm or home_norm in outcome_norm:
                side = "home"
            elif outcome_norm == away_norm or away_norm in outcome_norm:
                side = "away"
            else:
                side = outcome_side or outcome_norm
        elif our_market_type == "totals":
            if outcome_side in ("over", "o") or "over" in outcome_name.lower():
                side = "over"
            elif outcome_side in ("under", "u") or "under" in outcome_name.lower():
                side = "under"
            else:
                continue
        elif is_prop:
            if outcome_side in ("over", "o", "yes"):
                side = "over"
            elif outcome_side in ("under", "u", "no"):
                side = "under"
            else:
                continue
        else:
            side = outcome_side or outcome_name.lower()

        rows.append({
            "game_id": game_id,
            "market": our_market_type,
            "side": side,
            "line": float(line) if line is not None else 0.0,
            "source": "stx",
            "provider": "stx",
            "player": "",
            "price": price,
            "implied_prob": price,
            "devigged_prob": price,
            "provider_updated_at": now,
            "last_refreshed": now,
            "snapshot_time": now,
            "source_event_id": str(market.get("id") or ""),
            "source_market_id": str(outcome_id or ""),
            "outcome": outcome_name,
        })

    return rows


def _graphql_post(
    session: requests.Session,
    graphql_url: str,
    query: str,
    variables: Optional[dict[str, Any]],
    headers: Optional[dict[str, str]] = None,
) -> tuple[Optional[dict[str, Any]], int]:
    payload: dict[str, Any] = {"query": query}
    if variables is not None:
        payload["variables"] = variables

    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)

    try:
        resp = session.post(
            graphql_url,
            json=payload,
            headers=req_headers,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except requests.exceptions.RequestException:
        return None, 0

    status = resp.status_code
    try:
        data = resp.json()
    except ValueError:
        return None, status

    return data, status


def _is_auth_error(status: int, result: Optional[dict[str, Any]]) -> bool:
    if status == 401:
        return True
    if not result:
        return False
    errors = result.get("errors") or []
    for err in errors:
        message = str(err.get("message", "")).lower()
        if any(token in message for token in ("unauthorized", "forbidden", "token", "jwt", "auth")):
            return True
    return False


def _graphql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', "\\\"")


def _get_device_id() -> str:
    global _DEVICE_ID_CACHE
    if _DEVICE_ID_CACHE:
        return _DEVICE_ID_CACHE

    env_id = os.getenv("STX_DEVICE_ID")
    _DEVICE_ID_CACHE = env_id or str(uuid.uuid4())
    return _DEVICE_ID_CACHE
