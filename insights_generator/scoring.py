"""
AI Scoring System
=================

Produces a composite score for each upcoming game by combining signals from
structured events (injuries, weather, lineups), news volume, market momentum,
and cross-provider lag detection.

Each dimension is normalised to 0.0-1.0 and combined via configurable weights
into a single ``composite_score``.  Scores are stored in ``game_scores`` for
historical tracking and fed into the ML feature pipeline.

Usage:
------
    from insights_generator.scoring import score_all_upcoming

    scores = score_all_upcoming(conn)
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from insights_generator.config import get_scoring_config
from utils import utc_now_iso


# =============================================================================
# DATA MODEL
# =============================================================================

@dataclass
class GameScore:
    game_id: str
    scored_at: str

    injury_score: float = 0.0
    weather_score: float = 0.0
    news_momentum_score: float = 0.0
    market_momentum_score: float = 0.0
    provider_lag_score: float = 0.0
    lineup_score: float = 0.0

    composite_score: float = 0.0
    config_json: str = ""

    league: str = ""
    home_team: str = ""
    away_team: str = ""
    commence_time: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# =============================================================================
# PUBLIC API
# =============================================================================

def score_game(conn: sqlite3.Connection, game_id: str) -> GameScore | None:
    """Score a single game across all dimensions."""
    cursor = conn.execute(
        "SELECT game_id, league, home_team, away_team, commence_time "
        "FROM games WHERE game_id = ?",
        (game_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None

    cfg = get_scoring_config()
    now = utc_now_iso()

    gs = GameScore(
        game_id=row["game_id"],
        scored_at=now,
        league=row["league"] or "",
        home_team=row["home_team"] or "",
        away_team=row["away_team"] or "",
        commence_time=row["commence_time"] or "",
    )

    lookback_h = cfg.get("lookback_hours", 72)
    max_sev = cfg.get("max_severity", 5)
    outdoor = cfg.get("outdoor_leagues", [])

    gs.injury_score = _compute_injury_score(conn, gs, lookback_h, max_sev)
    gs.weather_score = _compute_weather_score(conn, gs, lookback_h, max_sev, outdoor)
    gs.news_momentum_score = _compute_news_momentum(conn, gs, lookback_h)
    gs.market_momentum_score = _compute_market_momentum(conn, gs)
    gs.provider_lag_score = _compute_provider_lag(conn, gs)
    gs.lineup_score = _compute_lineup_score(conn, gs, lookback_h)
    gs.composite_score = _weighted_composite(gs, cfg)
    gs.config_json = json.dumps(cfg)

    return gs


def score_all_upcoming(conn: sqlite3.Connection) -> list[GameScore]:
    """Score every game that hasn't commenced yet."""
    cursor = conn.execute(
        "SELECT game_id FROM games "
        "WHERE commence_time >= datetime('now') "
        "ORDER BY commence_time ASC"
    )
    scores = []
    for row in cursor.fetchall():
        gs = score_game(conn, row["game_id"])
        if gs:
            scores.append(gs)

    scores.sort(key=lambda s: s.composite_score, reverse=True)

    if scores:
        _store_scores(conn, scores)

    return scores


def get_score_features(conn: sqlite3.Connection, game_id: str) -> dict[str, float]:
    """
    Return the most recent scoring dimensions for *game_id* as a flat dict
    suitable for merging into the ML feature vector.
    """
    cursor = conn.execute(
        "SELECT * FROM game_scores WHERE game_id = ? "
        "ORDER BY scored_at DESC LIMIT 1",
        (game_id,),
    )
    row = cursor.fetchone()
    if not row:
        return {
            "score_injury": 0.0,
            "score_weather": 0.0,
            "score_news_momentum": 0.0,
            "score_market_momentum": 0.0,
            "score_provider_lag": 0.0,
            "score_lineup": 0.0,
            "score_composite": 0.0,
        }
    return {
        "score_injury": row["injury_score"],
        "score_weather": row["weather_score"],
        "score_news_momentum": row["news_momentum_score"],
        "score_market_momentum": row["market_momentum_score"],
        "score_provider_lag": row["provider_lag_score"],
        "score_lineup": row["lineup_score"],
        "score_composite": row["composite_score"],
    }


# =============================================================================
# DIMENSION CALCULATORS
# =============================================================================

def _compute_injury_score(
    conn: sqlite3.Connection, gs: GameScore, lookback_h: int, max_sev: int,
) -> float:
    """Aggregate injury severity across both teams, normalised to 0-1."""
    query = """
        SELECT COALESCE(SUM(se.severity), 0) as total,
               COALESCE(MAX(se.severity), 0) as peak
        FROM structured_events se
        JOIN news_headlines nh ON se.headline_id = nh.id
        WHERE se.event_type = 'injury'
        AND (nh.game_id = ? OR LOWER(se.team) IN (?, ?))
        AND nh.scraped_at >= datetime('now', ?)
    """
    try:
        cursor = conn.execute(query, (
            gs.game_id,
            gs.home_team.lower(),
            gs.away_team.lower(),
            f"-{lookback_h} hours",
        ))
        row = cursor.fetchone()
        if not row:
            return 0.0
        total = row["total"] or 0
        # Cap at ~4 severity-5 injuries (20) as the practical max
        return min(1.0, total / (max_sev * 4))
    except sqlite3.Error:
        return 0.0


def _compute_weather_score(
    conn: sqlite3.Connection, gs: GameScore, lookback_h: int,
    max_sev: int, outdoor_leagues: list[str],
) -> float:
    """Weather severity — only non-zero for outdoor leagues."""
    if gs.league not in outdoor_leagues:
        return 0.0

    query = """
        SELECT COALESCE(MAX(se.weather_severity), 0) as peak
        FROM structured_events se
        JOIN news_headlines nh ON se.headline_id = nh.id
        WHERE se.event_type = 'weather'
        AND (nh.game_id = ? OR LOWER(se.team) IN (?, ?))
        AND nh.scraped_at >= datetime('now', ?)
    """
    try:
        cursor = conn.execute(query, (
            gs.game_id,
            gs.home_team.lower(),
            gs.away_team.lower(),
            f"-{lookback_h} hours",
        ))
        row = cursor.fetchone()
        if not row:
            return 0.0
        return min(1.0, (row["peak"] or 0) / max_sev)
    except sqlite3.Error:
        return 0.0


def _compute_news_momentum(
    conn: sqlite3.Connection, gs: GameScore, lookback_h: int,
) -> float:
    """
    News volume normalised to 0-1.  10+ headlines in the lookback window
    saturates the score.
    """
    query = """
        SELECT COUNT(*) as cnt
        FROM news_headlines
        WHERE (game_id = ? OR matched_teams LIKE ? OR matched_teams LIKE ?)
        AND scraped_at >= datetime('now', ?)
    """
    try:
        cursor = conn.execute(query, (
            gs.game_id,
            f"%{gs.home_team}%",
            f"%{gs.away_team}%",
            f"-{lookback_h} hours",
        ))
        row = cursor.fetchone()
        if not row:
            return 0.0
        return min(1.0, (row["cnt"] or 0) / 10.0)
    except sqlite3.Error:
        return 0.0


def _compute_market_momentum(conn: sqlite3.Connection, gs: GameScore) -> float:
    """
    Max absolute price velocity over the last 60 minutes for any provider
    on this game, normalised so a 5 pp/min move saturates.
    """
    query = """
        SELECT provider, devigged_prob, snapshot_time
        FROM market_history
        WHERE game_id = ?
        AND snapshot_time >= datetime('now', '-60 minutes')
        AND devigged_prob IS NOT NULL
        ORDER BY provider, snapshot_time
    """
    try:
        cursor = conn.execute(query, (gs.game_id,))
        rows = cursor.fetchall()
    except sqlite3.Error:
        return 0.0

    if len(rows) < 2:
        return 0.0

    # Group by provider, compute velocity per provider
    providers: dict[str, list[tuple[float, str]]] = {}
    for r in rows:
        providers.setdefault(r["provider"], []).append(
            (r["devigged_prob"], r["snapshot_time"])
        )

    max_vel = 0.0
    for snapshots in providers.values():
        if len(snapshots) < 2:
            continue
        first_prob, first_t = snapshots[0]
        last_prob, last_t = snapshots[-1]
        try:
            dt_min = (
                datetime.fromisoformat(last_t) - datetime.fromisoformat(first_t)
            ).total_seconds() / 60.0
        except (ValueError, TypeError):
            continue
        if dt_min > 0:
            vel = abs(last_prob - first_prob) / dt_min
            max_vel = max(max_vel, vel)

    return min(1.0, max_vel / 0.05)


def _compute_provider_lag(conn: sqlite3.Connection, gs: GameScore) -> float:
    """
    If any recent lag signal exists for this game, score by signal strength.
    Normalised so a strength of 0.1 saturates.
    """
    query = """
        SELECT MAX(signal_strength) as peak
        FROM market_lag_signals
        WHERE game_id = ?
        AND detected_at >= datetime('now', '-60 minutes')
    """
    try:
        cursor = conn.execute(query, (gs.game_id,))
        row = cursor.fetchone()
        if not row or row["peak"] is None:
            return 0.0
        return min(1.0, row["peak"] / 0.1)
    except sqlite3.Error:
        return 0.0


def _compute_lineup_score(
    conn: sqlite3.Connection, gs: GameScore, lookback_h: int,
) -> float:
    """Count recent lineup events; 2+ events saturates."""
    query = """
        SELECT COUNT(*) as cnt
        FROM structured_events se
        JOIN news_headlines nh ON se.headline_id = nh.id
        WHERE se.event_type = 'lineup'
        AND (nh.game_id = ? OR LOWER(se.team) IN (?, ?))
        AND nh.scraped_at >= datetime('now', ?)
    """
    try:
        cursor = conn.execute(query, (
            gs.game_id,
            gs.home_team.lower(),
            gs.away_team.lower(),
            f"-{lookback_h} hours",
        ))
        row = cursor.fetchone()
        if not row:
            return 0.0
        return min(1.0, (row["cnt"] or 0) / 2.0)
    except sqlite3.Error:
        return 0.0


# =============================================================================
# COMPOSITE + STORAGE
# =============================================================================

def _weighted_composite(gs: GameScore, cfg: dict[str, Any]) -> float:
    weights = cfg.get("weights", {})
    total = (
        gs.injury_score * weights.get("injury", 0.25)
        + gs.weather_score * weights.get("weather", 0.10)
        + gs.news_momentum_score * weights.get("news_momentum", 0.15)
        + gs.market_momentum_score * weights.get("market_momentum", 0.20)
        + gs.provider_lag_score * weights.get("provider_lag", 0.20)
        + gs.lineup_score * weights.get("lineup", 0.10)
    )
    return round(min(1.0, total), 4)


def _store_scores(conn: sqlite3.Connection, scores: list[GameScore]) -> None:
    for gs in scores:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO game_scores (
                    game_id, scored_at,
                    injury_score, weather_score, news_momentum_score,
                    market_momentum_score, provider_lag_score, lineup_score,
                    composite_score, config_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                gs.game_id, gs.scored_at,
                gs.injury_score, gs.weather_score, gs.news_momentum_score,
                gs.market_momentum_score, gs.provider_lag_score, gs.lineup_score,
                gs.composite_score, gs.config_json,
            ))
        except sqlite3.Error as e:
            print(f"Warning: failed to store score for {gs.game_id}: {e}")
    conn.commit()
