"""
NLP Processor
=============

Processes news headlines through Ollama to extract structured features.
Converts unstructured text into categorized events with severity scores.

Event Types:
- injury: Player injury reports
- weather: Weather conditions affecting games
- trade: Player trades (confirmed)
- rumor: Trade rumors and speculation
- lineup: Starting lineup changes
- other: Miscellaneous news

Usage:
------
    from insights_generator.analyzers.nlp_processor import process_headlines
    
    results = process_headlines(
        conn,
        model="llama3.2",
        host="http://localhost:11434",
        batch_size=10,
    )
"""

import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

import requests

from aliases import canonical_player, canonical_team

from insights_generator.scrapers.news_scraper import get_unprocessed_headlines, mark_processed


# =============================================================================
# OLLAMA PROMPT TEMPLATE
# =============================================================================

EXTRACTION_PROMPT = """You are a sports betting analyst. Extract structured information from this sports headline.

Headline: "{headline}"
{summary_section}

Extract the following information and return ONLY a valid JSON object (no explanation):

{{
    "event_type": "injury|weather|trade|rumor|lineup|other",
    "player": "player name or null",
    "team": "team name or null",
    "opponent_team": "opponent team name or null",
    "severity": 1-5 (1=minor, 5=critical impact on game),
    "position_importance": 1-5 (1=bench player, 5=star/MVP),
    "starter_status": "starter|bench|unknown",
    "injury_type": "ankle|knee|illness|rest|concussion|other or null",
    "expected_absence": "game|week|season|unknown or null",
    "weather_condition": "rain|snow|wind|extreme_heat|other or null",
    "weather_severity": 1-5 or null,
    "trade_status": "confirmed|rumor|speculation or null",
    "confidence": 0.0-1.0 (how confident are you in this extraction),
    "relevance_to_betting": 0.0-1.0 (how relevant is this to sports betting)
}}

Rules:
- event_type is REQUIRED
- For injuries: include injury_type, expected_absence, starter_status
- For weather: include weather_condition, weather_severity  
- For trades: include trade_status
- severity should reflect impact on betting lines (5 = major line movement expected)
- Return ONLY the JSON object, no other text
"""


def process_headlines(
    conn: sqlite3.Connection,
    model: str = "llama3.2",
    host: str = "http://localhost:11434",
    batch_size: int = 10,
) -> dict[str, int]:
    """
    Process unprocessed headlines through Ollama NLP.
    
    Args:
        conn: Database connection
        model: Ollama model name
        host: Ollama API host URL
        batch_size: Maximum headlines to process in this run
        
    Returns:
        dict: Results with 'processed', 'events_created', 'errors' counts
    """
    # Get unprocessed headlines
    headlines = get_unprocessed_headlines(conn, limit=batch_size)
    
    if not headlines:
        return {"processed": 0, "events_created": 0, "errors": 0}
    
    results = {
        "processed": 0,
        "events_created": 0,
        "errors": 0,
    }
    
    processed_ids = []
    relevance_scores = {}
    
    for headline_row in headlines:
        headline_id = headline_row["id"]
        headline_text = headline_row["headline"]
        summary = headline_row.get("summary", "")
        
        # Extract structured data via Ollama
        extracted = extract_structured_features(
            headline_text,
            summary=summary,
            model=model,
            host=host,
        )
        
        if extracted is None:
            results["errors"] += 1
            continue
        
        # Store extracted event
        event_id = _store_event(conn, headline_id, extracted, model)
        
        if event_id:
            results["events_created"] += 1
        
        # Track for marking processed
        processed_ids.append(headline_id)
        relevance_scores[headline_id] = extracted.get("relevance_to_betting", 0.5)
        results["processed"] += 1
    
    # Mark headlines as processed
    mark_processed(conn, processed_ids, relevance_scores)
    
    return results


def extract_structured_features(
    headline: str,
    summary: str = "",
    model: str = "llama3.2",
    host: str = "http://localhost:11434",
) -> dict[str, Any] | None:
    """
    Extract structured features from a headline using Ollama.
    
    Args:
        headline: The headline text
        summary: Optional article summary
        model: Ollama model name
        host: Ollama API host URL
        
    Returns:
        dict: Extracted features, or None if extraction failed
    """
    # Build prompt
    summary_section = f'Summary: "{summary}"' if summary else ""
    prompt = EXTRACTION_PROMPT.format(
        headline=headline,
        summary_section=summary_section,
    )
    
    # Call Ollama
    try:
        response = _call_ollama(prompt, model, host)
    except Exception as e:
        print(f"ERROR: Ollama call failed: {e}")
        return None
    
    if not response:
        return None
    
    # Parse JSON from response
    extracted = _parse_json_response(response)
    
    if not extracted:
        print(f"Warning: Failed to parse Ollama response for: {headline[:50]}...")
        return None
    
    # Validate required fields
    if "event_type" not in extracted:
        extracted["event_type"] = "other"

    # Canonicalize entities
    if extracted.get("team"):
        extracted["team"] = canonical_team(str(extracted.get("team")))
    if extracted.get("opponent_team"):
        extracted["opponent_team"] = canonical_team(str(extracted.get("opponent_team")))
    if extracted.get("player"):
        extracted["player"] = canonical_player(str(extracted.get("player")))
    
    return extracted


def _call_ollama(
    prompt: str,
    model: str,
    host: str,
    timeout: int = 60,
) -> str | None:
    """
    Call Ollama API to generate a response.
    
    Args:
        prompt: The prompt to send
        model: Model name
        host: Ollama host URL
        timeout: Request timeout in seconds
        
    Returns:
        str: Generated response text, or None if failed
    """
    url = f"{host.rstrip('/')}/api/generate"
    
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.1,  # Low temperature for consistent extraction
        },
    }
    
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        return data.get("response", "")
        
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Cannot connect to Ollama at {host}")
        print("Make sure Ollama is running: ollama serve")
        return None
        
    except requests.exceptions.Timeout:
        print(f"ERROR: Ollama request timed out after {timeout}s")
        return None
        
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: Ollama HTTP error: {e}")
        return None
        
    except Exception as e:
        print(f"ERROR: Unexpected error calling Ollama: {e}")
        return None


def _parse_json_response(response: str) -> dict[str, Any] | None:
    """
    Parse JSON from Ollama response.
    
    Handles cases where the model includes extra text around the JSON.
    
    Args:
        response: Raw response text from Ollama
        
    Returns:
        dict: Parsed JSON object, or None if parsing failed
    """
    if not response:
        return None
    
    # Try direct parse first
    try:
        return json.loads(response.strip())
    except json.JSONDecodeError:
        pass
    
    # Try to extract JSON from response
    # Look for JSON object pattern
    json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
    
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    # Try to find JSON with nested objects
    json_match = re.search(r'\{.*\}', response, re.DOTALL)
    
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    
    return None


def _store_event(
    conn: sqlite3.Connection,
    headline_id: int,
    extracted: dict[str, Any],
    model: str,
) -> int | None:
    """
    Store extracted event in the structured_events table.
    
    Args:
        conn: Database connection
        headline_id: ID of the source headline
        extracted: Extracted feature dictionary
        model: Name of Ollama model used
        
    Returns:
        int: ID of inserted event, or None if failed
    """
    now = datetime.now(timezone.utc).isoformat()
    
    try:
        cursor = conn.execute("""
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
        """, (
            headline_id,
            extracted.get("event_type", "other"),
            extracted.get("player"),
            extracted.get("team"),
            extracted.get("opponent_team"),
            extracted.get("severity"),
            extracted.get("position_importance"),
            extracted.get("starter_status"),
            extracted.get("injury_type"),
            extracted.get("expected_absence"),
            extracted.get("weather_condition"),
            extracted.get("weather_severity"),
            extracted.get("trade_status"),
            extracted.get("confidence"),
            now,
            model,
            json.dumps(extracted),
        ))
        
        conn.commit()
        return cursor.lastrowid
        
    except sqlite3.Error as e:
        print(f"Warning: Failed to store event: {e}")
        return None


def get_events_for_game(
    conn: sqlite3.Connection,
    game_id: str,
    event_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Get structured events related to a specific game.
    
    Args:
        conn: Database connection
        game_id: Game ID to look up
        event_types: Optional filter for event types
        
    Returns:
        list: List of event dictionaries
    """
    query = """
        SELECT se.*, nh.headline, nh.published_at
        FROM structured_events se
        JOIN news_headlines nh ON se.headline_id = nh.id
        WHERE nh.game_id = ?
    """
    params = [game_id]
    
    if event_types:
        placeholders = ",".join("?" * len(event_types))
        query += f" AND se.event_type IN ({placeholders})"
        params.extend(event_types)
    
    query += " ORDER BY nh.published_at DESC"
    
    cursor = conn.execute(query, params)
    return [dict(row) for row in cursor.fetchall()]


def get_team_injury_severity(
    conn: sqlite3.Connection,
    team: str,
    hours_back: int = 72,
) -> dict[str, Any]:
    """
    Calculate aggregate injury severity for a team.
    
    Args:
        conn: Database connection
        team: Team name to look up
        hours_back: How many hours of history to consider
        
    Returns:
        dict: Injury severity summary with total_severity, injury_count, avg_position_importance
    """
    query = """
        SELECT 
            COUNT(*) as injury_count,
            SUM(severity) as total_severity,
            AVG(position_importance) as avg_position_importance,
            MAX(severity) as max_severity
        FROM structured_events se
        JOIN news_headlines nh ON se.headline_id = nh.id
        WHERE se.event_type = 'injury'
        AND LOWER(se.team) LIKE ?
        AND nh.published_at >= datetime('now', ?)
    """
    
    cursor = conn.execute(query, (f"%{team.lower()}%", f"-{hours_back} hours"))
    row = cursor.fetchone()
    
    if row and row["injury_count"]:
        return {
            "injury_count": row["injury_count"],
            "total_severity": row["total_severity"] or 0,
            "avg_position_importance": row["avg_position_importance"] or 0,
            "max_severity": row["max_severity"] or 0,
        }
    
    return {
        "injury_count": 0,
        "total_severity": 0,
        "avg_position_importance": 0,
        "max_severity": 0,
    }
