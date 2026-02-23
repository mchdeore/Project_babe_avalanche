"""
News Scraper
============

Scrapes sports news headlines from RSS feeds and other sources.
Headlines are stored in the news_headlines table for later NLP processing.

Supported source types:
- rss: Standard RSS/Atom feeds (ESPN, Rotoworld, etc.)

Future source types (not yet implemented):
- api: Direct API integrations (Twitter, official league APIs)
- scrape: Web scraping for sites without feeds

Usage:
------
    from insights_generator.scrapers.news_scraper import scrape_all_sources
    
    results = scrape_all_sources(conn, sources)
"""

import hashlib
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False
    print("Warning: feedparser not installed. RSS scraping will not work.")
    print("Install with: pip install feedparser")


# =============================================================================
# TEAM NAME PATTERNS
# =============================================================================
# Used to match headlines to games

NBA_TEAMS = {
    "hawks", "celtics", "nets", "hornets", "bulls", "cavaliers", "cavs",
    "mavericks", "mavs", "nuggets", "pistons", "warriors", "rockets",
    "pacers", "clippers", "lakers", "grizzlies", "heat", "bucks", "timberwolves",
    "wolves", "pelicans", "knicks", "thunder", "magic", "76ers", "sixers",
    "suns", "blazers", "trail blazers", "kings", "spurs", "raptors", "jazz", "wizards",
    "atlanta", "boston", "brooklyn", "charlotte", "chicago", "cleveland",
    "dallas", "denver", "detroit", "golden state", "houston", "indiana",
    "los angeles", "la", "memphis", "miami", "milwaukee", "minnesota",
    "new orleans", "new york", "oklahoma city", "okc", "orlando", "philadelphia",
    "philly", "phoenix", "portland", "sacramento", "san antonio", "toronto",
    "utah", "washington",
}

NFL_TEAMS = {
    "cardinals", "falcons", "ravens", "bills", "panthers", "bears",
    "bengals", "browns", "cowboys", "broncos", "lions", "packers",
    "texans", "colts", "jaguars", "chiefs", "raiders", "chargers",
    "rams", "dolphins", "vikings", "patriots", "saints", "giants",
    "jets", "eagles", "steelers", "49ers", "niners", "seahawks",
    "buccaneers", "bucs", "titans", "commanders", "redskins",
    "arizona", "atlanta", "baltimore", "buffalo", "carolina", "chicago",
    "cincinnati", "cleveland", "dallas", "denver", "detroit", "green bay",
    "houston", "indianapolis", "jacksonville", "kansas city", "las vegas",
    "los angeles", "la", "miami", "minnesota", "new england", "new orleans",
    "new york", "ny", "philadelphia", "philly", "pittsburgh", "san francisco",
    "seattle", "tampa bay", "tampa", "tennessee", "washington",
}

ALL_TEAMS = NBA_TEAMS | NFL_TEAMS


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
    else:
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
        
        # Match teams in headline
        matched_teams = _extract_teams(headline + " " + summary)
        matched_teams_json = str(list(matched_teams)) if matched_teams else None
        
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


def _extract_teams(text: str) -> set[str]:
    """
    Extract team names mentioned in text.
    
    Args:
        text: Text to search for team names
        
    Returns:
        set: Set of matched team names (lowercase)
    """
    text_lower = text.lower()
    matched = set()
    
    for team in ALL_TEAMS:
        # Use word boundaries to avoid partial matches
        pattern = r'\b' + re.escape(team) + r'\b'
        if re.search(pattern, text_lower):
            matched.add(team)
    
    return matched


def _match_to_game(
    conn: sqlite3.Connection,
    teams: set[str],
    published_at: str | None,
) -> str | None:
    """
    Try to match teams to an upcoming game.
    
    Args:
        conn: Database connection
        teams: Set of team names mentioned
        published_at: When the article was published
        
    Returns:
        str | None: game_id if matched, None otherwise
    """
    if not teams or len(teams) < 1:
        return None
    
    # Build query to find games with matching teams
    # This is a simple heuristic - match if any team name appears in home/away
    team_patterns = [f"%{team}%" for team in teams]
    
    placeholders = " OR ".join(
        ["(LOWER(home_team) LIKE ? OR LOWER(away_team) LIKE ?)"] * len(team_patterns)
    )
    
    params = []
    for pattern in team_patterns:
        params.extend([pattern, pattern])
    
    # Look for games in the near future (next 7 days)
    query = f"""
        SELECT game_id, home_team, away_team, commence_time
        FROM games
        WHERE ({placeholders})
        AND commence_time >= datetime('now')
        AND commence_time <= datetime('now', '+7 days')
        ORDER BY commence_time ASC
        LIMIT 1
    """
    
    try:
        cursor = conn.execute(query, params)
        row = cursor.fetchone()
        if row:
            return row["game_id"]
    except sqlite3.Error:
        pass
    
    return None


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
