"""Roster cache helpers for player/team linking."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from aliases import canonical_team
from insights_generator import MODULE_ROOT
from insights_generator.config import get_api_config, get_espn_config
from utils import normalize_player, utc_now_iso

CACHE_DIR = MODULE_ROOT / "cache"
LEAGUE_MAP = {
    "nba": "basketball_nba",
    "nfl": "americanfootball_nfl",
    "nhl": "icehockey_nhl",
    "mlb": "baseball_mlb",
}
LEAGUE_REVERSE_MAP = {value: key for key, value in LEAGUE_MAP.items()}


def _cache_path(league: str) -> Path:
    return CACHE_DIR / f"espn_rosters_{league}.json"


def _is_cache_fresh(path: Path, max_age_hours: int) -> bool:
    if not path.exists():
        return False
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return False
    age_hours = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600
    return age_hours <= max_age_hours


def _fetch_json(session: requests.Session, url: str, timeout: int, user_agent: str) -> dict[str, Any] | None:
    headers = {"User-Agent": user_agent}
    try:
        resp = session.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def _extract_team_items(payload: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        if isinstance(payload.get("teams"), list):
            for item in payload["teams"]:
                if isinstance(item, dict):
                    items.append(item)
        for value in payload.values():
            items.extend(_extract_team_items(value))
    elif isinstance(payload, list):
        for value in payload:
            items.extend(_extract_team_items(value))
    return items


def _extract_athletes(payload: Any) -> list[dict[str, Any]]:
    athletes: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        if isinstance(payload.get("athletes"), list):
            for athlete in payload["athletes"]:
                if isinstance(athlete, dict):
                    athletes.append(athlete)
        for value in payload.values():
            athletes.extend(_extract_athletes(value))
    elif isinstance(payload, list):
        for value in payload:
            athletes.extend(_extract_athletes(value))
    return athletes


def ensure_roster_cache(
    session: requests.Session,
    sport: str,
    league: str,
) -> dict[str, Any] | None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    espn_cfg = get_espn_config()
    api_cfg = get_api_config()
    cache_hours = int(espn_cfg.get("cache_hours", 24))

    path = _cache_path(league)
    if _is_cache_fresh(path, cache_hours):
        return load_roster_cache(league)

    base = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}"
    teams_url = f"{base}/teams"

    payload = _fetch_json(
        session,
        teams_url,
        timeout=int(api_cfg.get("request_timeout_seconds", 15)),
        user_agent=api_cfg.get("user_agent", "insights-generator/0.1"),
    )
    if not payload:
        return None

    team_items = _extract_team_items(payload)
    teams: list[dict[str, Any]] = []

    league_key = LEAGUE_MAP.get(league, league)

    for item in team_items:
        team = item.get("team") if isinstance(item, dict) else None
        if team is None and isinstance(item, dict) and "id" in item:
            team = item
        if not isinstance(team, dict):
            continue

        team_id = str(team.get("id") or "")
        team_name = team.get("displayName") or team.get("name") or ""
        team_key = canonical_team(team_name, league_key)
        if not team_id or not team_name:
            continue

        roster_url = f"{base}/teams/{team_id}/roster"
        roster_payload = _fetch_json(
            session,
            roster_url,
            timeout=int(api_cfg.get("request_timeout_seconds", 15)),
            user_agent=api_cfg.get("user_agent", "insights-generator/0.1"),
        )
        if not roster_payload:
            players = []
        else:
            athletes = _extract_athletes(roster_payload)
            players = []
            for athlete in athletes:
                name = athlete.get("displayName") or athlete.get("fullName")
                if name:
                    players.append(name)

        teams.append({
            "team_id": team_id,
            "team_name": team_name,
            "team_key": team_key,
            "players": sorted(set(players)),
        })

    cache_data = {
        "league": league,
        "sport": sport,
        "updated_at": utc_now_iso(),
        "teams": teams,
    }

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache_data, f)
    except OSError:
        return None

    return cache_data


def load_roster_cache(league: str) -> dict[str, Any] | None:
    path = _cache_path(league)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def build_player_index(leagues: list[str]) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}

    for league in leagues:
        espn_code = LEAGUE_REVERSE_MAP.get(league, league)
        cache = load_roster_cache(espn_code)
        if not cache:
            continue

        for team in cache.get("teams", []) or []:
            team_key = team.get("team_key", "")
            for player in team.get("players", []) or []:
                norm = normalize_player(player)
                if not norm:
                    continue
                index[norm] = {
                    "player": player,
                    "team_key": team_key,
                }

    return index
