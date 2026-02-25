"""API-based scrapers for the insights generator."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

import requests

from aliases import canonical_player, canonical_team, get_team_records
from insights_generator.config import get_api_config
from insights_generator.rosters import LEAGUE_MAP, ensure_roster_cache
from utils import parse_iso_timestamp, utc_now_iso


def scrape_api(conn: sqlite3.Connection, source: dict[str, Any]) -> int:
    api_type = source.get("api_type")
    if not api_type:
        print(f"Warning: API source missing api_type: {source.get('name')}")
        return 0

    if api_type == "reddit":
        return _scrape_reddit(conn, source)
    if api_type == "weather":
        return _scrape_weather(conn, source)
    if api_type == "espn_injuries":
        return _scrape_espn_injuries(conn, source)
    if api_type == "espn_lineups":
        return _scrape_espn_lineups(conn, source)

    print(f"Warning: Unknown api_type '{api_type}'")
    return 0


def _insert_headline(
    conn: sqlite3.Connection,
    source_name: str,
    source_type: str,
    headline: str,
    summary: str,
    url: str,
    published_at: str | None,
    game_id: str | None,
    matched_teams: list[str] | None,
    processed: int = 0,
    relevance_score: float | None = None,
) -> int | None:
    if not headline or not url:
        return None

    url_hash = hashlib.sha256(url.encode()).hexdigest()
    cursor = conn.execute(
        "SELECT id FROM news_headlines WHERE url_hash = ?",
        (url_hash,),
    )
    if cursor.fetchone():
        return None

    now = utc_now_iso()
    matched_json = json.dumps(matched_teams) if matched_teams else None

    try:
        cursor = conn.execute(
            """
            INSERT INTO news_headlines (
                source, source_type, headline, summary, url, url_hash,
                published_at, scraped_at, game_id, matched_teams,
                processed, relevance_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_name,
                source_type,
                headline,
                summary[:1000] if summary else None,
                url,
                url_hash,
                published_at,
                now,
                game_id,
                matched_json,
                processed,
                relevance_score,
            ),
        )
        return cursor.lastrowid
    except sqlite3.Error as e:
        print(f"Warning: Failed to insert headline: {e}")
        return None


def _insert_structured_event(
    conn: sqlite3.Connection,
    headline_id: int,
    event_type: str,
    team: str | None,
    player: str | None,
    opponent_team: str | None,
    severity: int | None,
    position_importance: int | None,
    starter_status: str | None,
    injury_type: str | None,
    expected_absence: str | None,
    weather_condition: str | None,
    weather_severity: int | None,
    trade_status: str | None,
    confidence: float | None,
    raw_response: dict[str, Any] | None,
    model: str = "api",
) -> None:
    now = utc_now_iso()
    try:
        conn.execute(
            """
            INSERT INTO structured_events (
                headline_id,
                event_type,
                player,
                team,
                opponent_team,
                severity,
                position_importance,
                starter_status,
                injury_type,
                expected_absence,
                weather_condition,
                weather_severity,
                trade_status,
                confidence,
                extracted_at,
                ollama_model,
                raw_response
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                headline_id,
                event_type,
                player,
                team,
                opponent_team,
                severity,
                position_importance,
                starter_status,
                injury_type,
                expected_absence,
                weather_condition,
                weather_severity,
                trade_status,
                confidence,
                now,
                model,
                json.dumps(raw_response or {}),
            ),
        )
    except sqlite3.Error as e:
        print(f"Warning: Failed to store structured event: {e}")


def _scrape_reddit(conn: sqlite3.Connection, source: dict[str, Any]) -> int:
    subreddit = source.get("subreddit")
    if not subreddit:
        print("Warning: reddit source missing subreddit")
        return 0

    api_cfg = get_api_config()
    limit = int(source.get("limit", 100))
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"

    headers = {"User-Agent": api_cfg.get("user_agent", "insights-generator/0.1")}
    try:
        resp = requests.get(url, headers=headers, timeout=int(api_cfg.get("request_timeout_seconds", 15)))
    except requests.RequestException as e:
        print(f"Warning: Reddit request failed: {e}")
        return 0

    if resp.status_code != 200:
        print(f"Warning: Reddit returned {resp.status_code} for r/{subreddit}")
        return 0

    try:
        payload = resp.json()
    except ValueError:
        return 0

    posts = (((payload or {}).get("data") or {}).get("children") or [])
    inserted = 0
    for post in posts:
        data = (post or {}).get("data") or {}
        title = (data.get("title") or "").strip()
        selftext = (data.get("selftext") or "").strip()
        permalink = data.get("permalink") or ""
        created = data.get("created_utc")

        if not title or not permalink:
            continue

        published_at = None
        if created:
            try:
                published_at = datetime.fromtimestamp(created, tz=timezone.utc).isoformat()
            except (TypeError, ValueError):
                published_at = None

        url = f"https://www.reddit.com{permalink}"
        headline_id = _insert_headline(
            conn,
            source.get("name", f"reddit_{subreddit}"),
            "api",
            title,
            selftext,
            url,
            published_at,
            None,
            None,
            processed=0,
        )
        if headline_id:
            inserted += 1

    conn.commit()
    return inserted


def _scrape_weather(conn: sqlite3.Connection, source: dict[str, Any]) -> int:
    league = source.get("league")
    if not league:
        print("Warning: weather source missing league")
        return 0

    api_cfg = get_api_config()
    hours_ahead = int(source.get("hours_ahead", 72))
    now = datetime.now(timezone.utc)

    teams = [
        record for record in get_team_records().values()
        if record.get("league") == league
    ]

    inserted = 0

    for team in teams:
        team_key = team.get("key")
        lat = team.get("lat")
        lon = team.get("lon")
        if lat is None or lon is None:
            continue

        game = _find_next_game(conn, league, team_key, hours_ahead)
        if not game:
            continue

        forecast = _fetch_weather(lat, lon, api_cfg)
        if not forecast:
            continue

        game_time = parse_iso_timestamp(game.get("commence_time", ""))
        if not game_time:
            continue

        weather = _extract_weather_at(forecast, game_time)
        if not weather:
            continue

        condition, severity = _classify_weather(weather)
        if not condition:
            continue

        opponent = game.get("away_team") if game.get("home_key") == team_key else game.get("home_team")
        opponent_key = canonical_team(opponent, league) if opponent else None

        headline = f"Weather watch: {team.get('name')} upcoming game"
        summary = f"Wind {weather.get('wind_speed', 0)} mph, precip {weather.get('precipitation', 0)}"
        url = f"weather:{team_key}:{game.get('game_id')}:{game.get('commence_time')}"

        headline_id = _insert_headline(
            conn,
            source.get("name", "weather"),
            "api",
            headline,
            summary,
            url,
            game.get("commence_time"),
            game.get("game_id"),
            [team_key, opponent_key] if opponent_key else [team_key],
            processed=1,
            relevance_score=0.7,
        )
        if not headline_id:
            continue

        _insert_structured_event(
            conn,
            headline_id,
            event_type="weather",
            team=team_key,
            player=None,
            opponent_team=opponent_key,
            severity=severity,
            position_importance=None,
            starter_status=None,
            injury_type=None,
            expected_absence=None,
            weather_condition=condition,
            weather_severity=severity,
            trade_status=None,
            confidence=0.8,
            raw_response={"weather": weather},
            model="api_weather",
        )
        inserted += 1

    conn.commit()
    return inserted


def _fetch_weather(lat: float, lon: float, api_cfg: dict[str, Any]) -> dict[str, Any] | None:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,precipitation,wind_speed_10m"
        "&timezone=UTC"
    )
    try:
        resp = requests.get(url, timeout=int(api_cfg.get("request_timeout_seconds", 15)))
        if resp.status_code != 200:
            return None
        return resp.json()
    except requests.RequestException:
        return None


def _extract_weather_at(payload: dict[str, Any], target_time: datetime) -> dict[str, Any] | None:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return None

    target_iso = target_time.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00")
    try:
        idx = times.index(target_iso)
    except ValueError:
        idx = None

    if idx is None:
        return None

    def _get(series_name: str, default: float = 0.0) -> float:
        series = hourly.get(series_name) or []
        if idx < len(series):
            try:
                return float(series[idx])
            except (TypeError, ValueError):
                return default
        return default

    return {
        "time": target_iso,
        "temperature": _get("temperature_2m"),
        "precipitation": _get("precipitation"),
        "wind_speed": _get("wind_speed_10m"),
    }


def _classify_weather(weather: dict[str, Any]) -> tuple[str | None, int | None]:
    precip = weather.get("precipitation", 0.0)
    wind = weather.get("wind_speed", 0.0)

    if precip >= 10:
        return "rain", 4
    if precip >= 2:
        return "rain", 3
    if wind >= 25:
        return "wind", 4
    if wind >= 15:
        return "wind", 3

    return None, None


def _find_next_game(conn: sqlite3.Connection, league: str, team_key: str, hours_ahead: int) -> dict[str, Any] | None:
    query = """
        SELECT game_id, league, commence_time, home_team, away_team
        FROM games
        WHERE league = ?
        AND commence_time >= datetime('now')
        AND commence_time <= datetime('now', ?)
        ORDER BY commence_time ASC
    """

    cursor = conn.execute(query, (league, f"+{hours_ahead} hours"))
    rows = cursor.fetchall()

    for row in rows:
        home_key = canonical_team(row["home_team"], league)
        away_key = canonical_team(row["away_team"], league)
        if team_key in (home_key, away_key):
            return {
                "game_id": row["game_id"],
                "commence_time": row["commence_time"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "home_key": home_key,
                "away_key": away_key,
            }
    return None


def _scrape_espn_injuries(conn: sqlite3.Connection, source: dict[str, Any]) -> int:
    sport = source.get("sport")
    league = source.get("league")
    if not sport or not league:
        print("Warning: espn_injuries missing sport/league")
        return 0

    api_cfg = get_api_config()
    inserted = 0

    league_key = LEAGUE_MAP.get(league, league)

    with requests.Session() as session:
        roster_cache = ensure_roster_cache(session, sport, league)
        if not roster_cache:
            return 0

        teams = roster_cache.get("teams", [])
        base = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}"

        for team in teams:
            team_id = team.get("team_id")
            team_key = team.get("team_key")
            team_name = team.get("team_name")
            if not team_id or not team_key:
                continue

            injuries_url = f"{base}/teams/{team_id}"
            payload = _fetch_json(session, injuries_url, api_cfg)
            injuries = _extract_injuries(payload)
            if not injuries:
                continue

            game = _find_next_game(conn, league_key, team_key, hours_ahead=168)
            opponent_key = None
            game_id = None
            if game:
                game_id = game.get("game_id")
                opponent = game.get("away_team") if game.get("home_key") == team_key else game.get("home_team")
                opponent_key = canonical_team(opponent, league_key) if opponent else None

            for injury in injuries:
                player_name = injury.get("player")
                status = injury.get("status") or "unknown"
                description = injury.get("description") or ""

                severity = _severity_from_status(status)
                expected_absence = _absence_from_status(status)

                headline = f"{team_name} injury update: {player_name}"
                url = f"espn_injury:{league}:{team_id}:{canonical_player(player_name)}:{status}"

                headline_id = _insert_headline(
                    conn,
                    source.get("name", "espn_injuries"),
                    "api",
                    headline,
                    description,
                    url,
                    utc_now_iso(),
                    game_id,
                    [team_key, opponent_key] if opponent_key else [team_key],
                    processed=1,
                    relevance_score=0.8,
                )
                if not headline_id:
                    continue

                _insert_structured_event(
                    conn,
                    headline_id,
                    event_type="injury",
                    team=team_key,
                    player=canonical_player(player_name),
                    opponent_team=opponent_key,
                    severity=severity,
                    position_importance=None,
                    starter_status=injury.get("starter_status"),
                    injury_type=injury.get("injury_type"),
                    expected_absence=expected_absence,
                    weather_condition=None,
                    weather_severity=None,
                    trade_status=None,
                    confidence=0.8,
                    raw_response=injury,
                    model="api_espn",
                )
                inserted += 1

    conn.commit()
    return inserted


def _scrape_espn_lineups(conn: sqlite3.Connection, source: dict[str, Any]) -> int:
    sport = source.get("sport")
    league = source.get("league")
    if not sport or not league:
        print("Warning: espn_lineups missing sport/league")
        return 0

    api_cfg = get_api_config()
    inserted = 0
    league_key = LEAGUE_MAP.get(league, league)

    with requests.Session() as session:
        roster_cache = ensure_roster_cache(session, sport, league)
        if not roster_cache:
            return 0

        teams = roster_cache.get("teams", [])
        base = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}"

        for team in teams:
            team_id = team.get("team_id")
            team_key = team.get("team_key")
            team_name = team.get("team_name")
            if not team_id or not team_key:
                continue

            depth_url = f"{base}/teams/{team_id}/depthchart"
            payload = _fetch_json(session, depth_url, api_cfg)
            starters = _extract_depth_chart_starters(payload)
            if not starters:
                continue

            game = _find_next_game(conn, league_key, team_key, hours_ahead=168)
            opponent_key = None
            game_id = None
            if game:
                game_id = game.get("game_id")
                opponent = game.get("away_team") if game.get("home_key") == team_key else game.get("home_team")
                opponent_key = canonical_team(opponent, league_key) if opponent else None

            headline = f"{team_name} depth chart update"
            url = f"espn_lineup:{league}:{team_id}:{utc_now_iso()}"
            headline_id = _insert_headline(
                conn,
                source.get("name", "espn_lineups"),
                "api",
                headline,
                ", ".join(starters[:10]),
                url,
                utc_now_iso(),
                game_id,
                [team_key, opponent_key] if opponent_key else [team_key],
                processed=1,
                relevance_score=0.7,
            )
            if not headline_id:
                continue

            _insert_structured_event(
                conn,
                headline_id,
                event_type="lineup",
                team=team_key,
                player=None,
                opponent_team=opponent_key,
                severity=2,
                position_importance=None,
                starter_status="starter",
                injury_type=None,
                expected_absence=None,
                weather_condition=None,
                weather_severity=None,
                trade_status=None,
                confidence=0.6,
                raw_response={"starters": starters},
                model="api_espn",
            )
            inserted += 1

    conn.commit()
    return inserted


def _fetch_json(session: requests.Session, url: str, api_cfg: dict[str, Any]) -> dict[str, Any] | None:
    headers = {"User-Agent": api_cfg.get("user_agent", "insights-generator/0.1")}
    try:
        resp = session.get(url, headers=headers, timeout=int(api_cfg.get("request_timeout_seconds", 15)))
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def _extract_injuries(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not payload:
        return []

    injuries: list[dict[str, Any]] = []

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if isinstance(obj.get("injuries"), list):
                for item in obj["injuries"]:
                    parsed = _parse_injury(item)
                    if parsed:
                        injuries.append(parsed)
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)

    walk(payload)
    return injuries


def _parse_injury(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None

    athlete = item.get("athlete") or item.get("player") or {}
    if isinstance(athlete, dict):
        player_name = athlete.get("displayName") or athlete.get("fullName")
    else:
        player_name = None

    status = item.get("status") or item.get("type") or item.get("injuryStatus")
    description = item.get("description") or item.get("details") or ""

    if not player_name:
        return None

    return {
        "player": player_name,
        "status": str(status) if status else "unknown",
        "description": description,
        "injury_type": item.get("injuryType") or item.get("type"),
        "starter_status": item.get("starterStatus") or None,
    }


def _extract_depth_chart_starters(payload: dict[str, Any] | None) -> list[str]:
    if not payload:
        return []

    starters: list[str] = []

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if isinstance(obj.get("positions"), list):
                for pos in obj["positions"]:
                    if not isinstance(pos, dict):
                        continue
                    depth = pos.get("athletes") or []
                    if depth:
                        athlete = depth[0]
                        if isinstance(athlete, dict):
                            name = athlete.get("displayName") or athlete.get("fullName")
                            if name:
                                starters.append(name)
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)

    walk(payload)
    return starters


def _severity_from_status(status: str) -> int:
    status_lower = status.lower()
    if "out" in status_lower:
        return 4
    if "doubt" in status_lower:
        return 3
    if "question" in status_lower:
        return 2
    return 1


def _absence_from_status(status: str) -> str:
    status_lower = status.lower()
    if "out" in status_lower:
        return "game"
    if "doubt" in status_lower:
        return "game"
    if "question" in status_lower:
        return "unknown"
    return "unknown"
