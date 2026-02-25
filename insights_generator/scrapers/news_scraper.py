"""
News Scraper
============

Scrapes sports news headlines from RSS feeds and other sources.
Headlines are stored in the news_headlines table for later NLP processing.

Supported source types:
- rss: Standard RSS/Atom feeds (ESPN, etc.)
- api: Direct API integrations (Reddit, weather, ESPN)

Usage:
------
    from insights_generator.scrapers.news_scraper import scrape_all_sources
    
    results = scrape_all_sources(conn, sources)
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from aliases import canonical_team, get_team_aliases_by_league
from insights_generator.rosters import build_player_index
from insights_generator.scrapers.api_scraper import scrape_api
from utils import normalize_player, normalize_team, parse_iso_timestamp

try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False
    print("Warning: feedparser not installed. RSS scraping will not work.")
    print("Install with: pip install feedparser")


MIN_ALIAS_LENGTH = 3


def scrape_all_sources(
    conn: sqlite3.Connection,
    sources: list[dict[str, str]],
) -> dict[str, int]:
    """
    Scrape headlines from all configured sources.
    
    Args:
        conn: Database connection
        sources: List of source configs with 'name', 'url', 'type' keys
        
    Returns:
        dict: Map of source_name -> number of new headlines scraped
    """
    results = {}
    
    for source in sources:
        source_name = source.get("name", "unknown")
        source_url = source.get("url", "")
        source_type = source.get("type", "rss")

        if source_type == "rss":
            count = scrape_rss(conn, source_name, source_url)
        elif source_type == "api":
            count = scrape_api(conn, source)
        else:
            print(f"Warning: Unknown source type '{source_type}' for {source_name}")
            count = 0

        results[source_name] = count
    
    return results


def scrape_news(
    conn: sqlite3.Connection,
    source_name: str,
    source_url: str,
    source_type: str = "rss",
    source_config: dict[str, Any] | None = None,
) -> int:
    """
    Scrape news from a single source.
    
    Args:
        conn: Database connection
        source_name: Name identifier for the source
        source_url: URL to scrape
        source_type: Type of source ('rss', 'api', 'scrape')
        
    Returns:
        int: Number of new headlines scraped
    """
    if source_type == "rss":
        return scrape_rss(conn, source_name, source_url)
    if source_type == "api":
        cfg = source_config or {"name": source_name, "type": "api"}
        return scrape_api(conn, cfg)

    print(f"Warning: Source type '{source_type}' not implemented")
    return 0


def scrape_rss(
    conn: sqlite3.Connection,
    source_name: str,
    feed_url: str,
) -> int:
    """
    Scrape headlines from an RSS feed.
    
    Args:
        conn: Database connection
        source_name: Name identifier for the source
        feed_url: RSS feed URL
        
    Returns:
        int: Number of new headlines added
    """
    if not FEEDPARSER_AVAILABLE:
        print("ERROR: feedparser not available. Install with: pip install feedparser")
        return 0
    
    if not feed_url:
        print(f"ERROR: No URL provided for source {source_name}")
        return 0
    
    # Parse feed
    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        print(f"ERROR: Failed to parse feed {feed_url}: {e}")
        return 0
    
    if not feed.entries:
        print(f"Warning: No entries found in feed {feed_url}")
        return 0
    
    now = datetime.now(timezone.utc).isoformat()
    new_count = 0
    
    for entry in feed.entries:
        # Extract headline data
        headline = entry.get("title", "").strip()
        summary = entry.get("summary", entry.get("description", "")).strip()
        url = entry.get("link", "").strip()
        
        if not headline or not url:
            continue
        
        # Parse published date
        published_at = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                published_at = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
            except (ValueError, TypeError):
                pass
        
        # Generate URL hash for deduplication
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        
        # Check if already exists
        cursor = conn.execute(
            "SELECT id FROM news_headlines WHERE url_hash = ?",
            (url_hash,)
        )
        if cursor.fetchone():
            continue
        
        # Match teams/players in headline
        matched_teams, _matched_players = _extract_entities(headline + " " + summary)
        matched_teams_json = json.dumps(sorted(matched_teams)) if matched_teams else None

        # Try to match to a game (optional, can be null)
        game_id = _match_to_game(conn, matched_teams, published_at)
        
        # Insert headline
        try:
            conn.execute("""
                INSERT INTO news_headlines (
                    source, source_type, headline, summary, url, url_hash,
                    published_at, scraped_at, game_id, matched_teams,
                    processed, relevance_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
            """, (
                source_name,
                "rss",
                headline,
                summary[:1000] if summary else None,  # Truncate long summaries
                url,
                url_hash,
                published_at,
                now,
                game_id,
                matched_teams_json,
            ))
            new_count += 1
        except sqlite3.Error as e:
            print(f"Warning: Failed to insert headline: {e}")
    
    conn.commit()
    return new_count


def _extract_entities(text: str) -> tuple[set[str], set[str]]:
    """
    Extract canonical teams and players mentioned in text.

    Returns:
        tuple: (matched_team_keys, matched_players)
    """
    matched_teams: set[str] = set()
    matched_players: set[str] = set()

    text_team_norm = normalize_team(text)
    team_aliases = get_team_aliases_by_league()

    for league, alias_map in team_aliases.items():
        for alias_norm, keys in alias_map.items():
            if len(alias_norm) < MIN_ALIAS_LENGTH:
                continue
            if alias_norm in text_team_norm:
                matched_teams.update(keys)

    # Player matching from cached rosters (if available)
    leagues = list(team_aliases.keys())
    player_index = build_player_index(leagues)
    text_player_norm = normalize_player(text)

    for player_norm, info in player_index.items():
        if len(player_norm) < 6:
            continue
        if player_norm in text_player_norm:
            player_name = info.get("player") or player_norm
            matched_players.add(player_name)
            team_key = info.get("team_key")
            if team_key:
                matched_teams.add(team_key)

    return matched_teams, matched_players


def _match_to_game(
    conn: sqlite3.Connection,
    teams: set[str],
    published_at: str | None,
) -> str | None:
    """
    Try to match canonical team keys to an upcoming game.
    """
    if not teams:
        return None

    # Look for games in the near future (next 7 days)
    query = """
        SELECT game_id, league, home_team, away_team, commence_time
        FROM games
        WHERE commence_time >= datetime('now')
        AND commence_time <= datetime('now', '+7 days')
        ORDER BY commence_time ASC
    """

    try:
        cursor = conn.execute(query)
        rows = cursor.fetchall()
    except sqlite3.Error:
        return None

    event_time = parse_iso_timestamp(published_at) if published_at else None
    if event_time is None:
        event_time = datetime.now(timezone.utc)

    best_game = None
    best_score = (-1, float("inf"))

    for row in rows:
        league = row["league"]
        home_key = canonical_team(row["home_team"], league)
        away_key = canonical_team(row["away_team"], league)

        score = 0
        if home_key in teams:
            score += 1
        if away_key in teams:
            score += 1
        if score == 0:
            continue

        commence = parse_iso_timestamp(row["commence_time"]) if row["commence_time"] else None
        if commence is None:
            time_delta = float("inf")
        else:
            time_delta = abs((commence - event_time).total_seconds())

        candidate_score = (score, time_delta)
        if candidate_score > best_score:
            best_score = candidate_score
            best_game = row["game_id"]

    return best_game


def get_unprocessed_headlines(
    conn: sqlite3.Connection,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Get headlines that haven't been processed by NLP yet.
    
    Args:
        conn: Database connection
        limit: Maximum headlines to return
        
    Returns:
        list: List of headline dictionaries
    """
    query = """
        SELECT *
        FROM news_headlines
        WHERE processed = 0
        ORDER BY scraped_at DESC
        LIMIT ?
    """
    
    cursor = conn.execute(query, (limit,))
    return [dict(row) for row in cursor.fetchall()]


def mark_processed(
    conn: sqlite3.Connection,
    headline_ids: list[int],
    relevance_scores: dict[int, float] | None = None,
) -> int:
    """
    Mark headlines as processed by NLP.
    
    Args:
        conn: Database connection
        headline_ids: List of headline IDs to mark
        relevance_scores: Optional map of id -> relevance_score
        
    Returns:
        int: Number of headlines updated
    """
    if not headline_ids:
        return 0
    
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    
    for hid in headline_ids:
        relevance = relevance_scores.get(hid) if relevance_scores else None
        
        conn.execute("""
            UPDATE news_headlines
            SET processed = 1, processed_at = ?, relevance_score = ?
            WHERE id = ?
        """, (now, relevance, hid))
        count += 1
    
    conn.commit()
    return count
