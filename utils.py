"""
Shared Utilities Module
=======================

Core utility functions used across the arbitrage detection system.

This module provides:
    - Configuration loading and validation
    - Database initialization and operations (upsert, insert, queries)
    - Time and date utilities (ISO formatting, window checking)
    - String normalization (team names, game IDs)
    - Probability functions (odds conversion, de-vigging)

Usage:
    from utils import (
        load_config, init_db, upsert_rows,
        canonical_game_id, normalize_team,
        odds_to_prob, devig, devig_market
    )

Dependencies:
    - yaml: Configuration file parsing
    - sqlite3: Database operations
    - json: JSON string parsing

Author: Arbitrage Detection System
"""
from __future__ import annotations

import functools
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Optional, TypeVar

import yaml


# =============================================================================
# LOGGING SETUP
# =============================================================================

# Configure module logger
logger = logging.getLogger(__name__)

def setup_logging(
    level: int = logging.INFO,
    format_str: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
) -> None:
    """
    Configure logging for the application.

    Args:
        level: Logging level (default: INFO).
        format_str: Log message format string.

    Example:
        >>> setup_logging(logging.DEBUG)
    """
    logging.basicConfig(level=level, format=format_str)


# =============================================================================
# CONSTANTS
# =============================================================================

# Default configuration file path
DEFAULT_CONFIG_PATH: str = "config.yaml"

# Default schema file path
DEFAULT_SCHEMA_PATH: str = "schema.sql"

# Default database path
DEFAULT_DB_PATH: str = "odds.db"

# API request defaults
DEFAULT_TIMEOUT: int = 30  # seconds
DEFAULT_RETRIES: int = 3
DEFAULT_RETRY_DELAY: float = 1.0  # seconds


# =============================================================================
# RETRY DECORATOR
# =============================================================================

T = TypeVar("T")


def retry_on_failure(
    max_retries: int = DEFAULT_RETRIES,
    delay: float = DEFAULT_RETRY_DELAY,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that retries a function on failure with exponential backoff.

    Useful for API calls that may fail due to transient network issues.

    Args:
        max_retries: Maximum number of retry attempts.
        delay: Initial delay between retries (seconds).
        backoff: Multiplier for delay after each retry.
        exceptions: Tuple of exception types to catch and retry.

    Returns:
        Decorated function that retries on failure.

    Example:
        >>> @retry_on_failure(max_retries=3, delay=1.0)
        ... def fetch_data():
        ...     return requests.get(url)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )

            raise last_exception  # type: ignore

        return wrapper
    return decorator


def safe_request(
    method: str,
    url: str,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    **kwargs: Any,
) -> Optional[dict]:
    """
    Make an HTTP request with automatic retry and error handling.

    Args:
        method: HTTP method ('get', 'post', etc.).
        url: Request URL.
        session: Optional requests.Session for connection pooling.
        timeout: Request timeout in seconds.
        retries: Number of retry attempts.
        **kwargs: Additional arguments passed to requests.

    Returns:
        JSON response as dict, or None if request failed.

    Example:
        >>> data = safe_request('get', 'https://api.example.com/data')
        >>> if data:
        ...     print(data['results'])
    """
    import requests

    requester = session or requests
    last_error = None

    for attempt in range(retries + 1):
        try:
            response = getattr(requester, method.lower())(
                url, timeout=timeout, **kwargs
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout as e:
            last_error = e
            logger.warning(f"Request timeout (attempt {attempt + 1}): {url}")
        except requests.exceptions.ConnectionError as e:
            last_error = e
            logger.warning(f"Connection error (attempt {attempt + 1}): {url}")
        except requests.exceptions.HTTPError as e:
            last_error = e
            logger.warning(f"HTTP error {e.response.status_code} (attempt {attempt + 1}): {url}")
            if e.response.status_code in (401, 403, 404):
                # Don't retry auth/not-found errors
                break
        except (requests.exceptions.RequestException, ValueError) as e:
            last_error = e
            logger.warning(f"Request failed (attempt {attempt + 1}): {e}")

        if attempt < retries:
            time.sleep(DEFAULT_RETRY_DELAY * (2 ** attempt))

    logger.error(f"Request failed after {retries + 1} attempts: {url} - {last_error}")
    return None


# =============================================================================
# CONFIGURATION
# =============================================================================

def load_config(path: str = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """
    Load and parse YAML configuration file.

    Args:
        path: Path to the YAML configuration file.
              Defaults to 'config.yaml' in current directory.

    Returns:
        Dictionary containing all configuration settings.
        Returns empty dict if file is empty or missing.

    Raises:
        FileNotFoundError: If configuration file doesn't exist.
        yaml.YAMLError: If YAML parsing fails.

    Example:
        >>> config = load_config()
        >>> config['sources']['odds_api']['poll_interval_seconds']
        300
        >>> config['sports']
        ['basketball_nba', 'americanfootball_nfl']
    """
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_source_config(config: dict[str, Any], source_name: str) -> dict[str, Any]:
    """
    Get configuration for a specific data source.

    Args:
        config: Full configuration dictionary from load_config().
        source_name: Name of the source (e.g., 'odds_api', 'polymarket').

    Returns:
        Source-specific configuration dict, or empty dict if not found.

    Example:
        >>> config = load_config()
        >>> odds_cfg = get_source_config(config, 'odds_api')
        >>> odds_cfg['poll_interval_seconds']
        300
    """
    return config.get("sources", {}).get(source_name, {})


# =============================================================================
# DATABASE INITIALIZATION
# =============================================================================

def init_db(
    db_path: str = DEFAULT_DB_PATH,
    schema_path: str = DEFAULT_SCHEMA_PATH
) -> sqlite3.Connection:
    """
    Initialize SQLite database with schema, handling corruption recovery.

    Creates or opens the database file and executes the schema SQL to ensure
    all tables and indices exist. If the database is corrupted, it will be
    deleted and recreated automatically.

    Args:
        db_path: Path to SQLite database file.
        schema_path: Path to SQL schema file.

    Returns:
        Active sqlite3.Connection object with foreign keys enabled.

    Raises:
        FileNotFoundError: If schema file doesn't exist.
        sqlite3.DatabaseError: If database error occurs (other than corruption).

    Example:
        >>> conn = init_db('odds.db', 'schema.sql')
        >>> cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        >>> tables = [row[0] for row in cursor.fetchall()]
        >>> 'games' in tables
        True
    """
    def connect_and_init() -> sqlite3.Connection:
        """Internal: Create connection and apply schema."""
        conn = sqlite3.connect(db_path)
        # Enable foreign key constraint enforcement
        conn.execute("PRAGMA foreign_keys = ON;")
        # Use WAL mode for better concurrency and crash resistance
        conn.execute("PRAGMA journal_mode = WAL;")
        # Apply schema
        with open(schema_path, encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()
        return conn

    try:
        return connect_and_init()
    except sqlite3.DatabaseError as e:
        # Auto-recover from corruption by recreating database
        if "malformed" in str(e).lower() or "corrupt" in str(e).lower():
            print(f"⚠️  Database corrupted, recreating: {db_path}")
            os.remove(db_path)
            return connect_and_init()
        raise


# =============================================================================
# TIME UTILITIES
# =============================================================================

def utc_now_iso() -> str:
    """
    Get current UTC time as ISO 8601 formatted string.

    Returns:
        ISO timestamp string with timezone info.
        Format: 'YYYY-MM-DDTHH:MM:SS.ffffff+00:00'

    Example:
        >>> timestamp = utc_now_iso()
        >>> timestamp  # doctest: +SKIP
        '2026-02-10T15:30:45.123456+00:00'
    """
    return datetime.now(timezone.utc).isoformat()


def parse_iso_timestamp(timestamp: str) -> Optional[datetime]:
    """
    Parse ISO 8601 timestamp string to datetime object.

    Handles various ISO formats including:
        - Full: '2026-02-10T15:30:45+00:00'
        - With Z: '2026-02-10T15:30:45Z'
        - Date only: '2026-02-10'

    Args:
        timestamp: ISO formatted timestamp string.

    Returns:
        Timezone-aware datetime object, or None if parsing fails.

    Example:
        >>> dt = parse_iso_timestamp('2026-02-10T15:30:45Z')
        >>> dt.year
        2026
    """
    if not timestamp:
        return None

    try:
        # Handle 'Z' suffix (common in APIs)
        if timestamp.endswith("Z"):
            timestamp = timestamp.replace("Z", "+00:00")

        # Handle date-only strings
        if len(timestamp.strip()) <= 10:
            dt = datetime.fromisoformat(timestamp.strip())
            return dt.replace(tzinfo=timezone.utc)

        # Full ISO timestamp
        dt = datetime.fromisoformat(timestamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    except ValueError:
        return None


def within_window(commence_time: str, window_days: int) -> bool:
    """
    Check if a game/event is within the bettable time window.

    Used to filter out games that are too far in the future or already past.

    Args:
        commence_time: ISO timestamp of event start time.
        window_days: Number of days from now to include.

    Returns:
        True if event is between now and (now + window_days), False otherwise.

    Example:
        >>> # Game tomorrow is within a 7-day window
        >>> from datetime import datetime, timedelta, timezone
        >>> tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        >>> within_window(tomorrow, 7)
        True
    """
    if not commence_time:
        return False

    dt = parse_iso_timestamp(commence_time)
    if dt is None:
        return False

    now = datetime.now(timezone.utc)

    # For date-only strings, compare dates
    if len(commence_time.strip()) <= 10:
        return now.date() <= dt.date() <= (now.date() + timedelta(days=window_days))

    # For full timestamps, compare datetimes
    return now <= dt <= now + timedelta(days=window_days)


def seconds_since(timestamp: str) -> Optional[float]:
    """
    Calculate seconds elapsed since a given timestamp.

    Args:
        timestamp: ISO formatted timestamp string.

    Returns:
        Number of seconds since the timestamp, or None if parsing fails.
        Negative values indicate future timestamps.

    Example:
        >>> # 5 minutes ago
        >>> from datetime import datetime, timedelta, timezone
        >>> past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        >>> elapsed = seconds_since(past)
        >>> 290 < elapsed < 310  # Approximately 300 seconds
        True
    """
    dt = parse_iso_timestamp(timestamp)
    if dt is None:
        return None

    return (datetime.now(timezone.utc) - dt).total_seconds()


# =============================================================================
# STRING NORMALIZATION
# =============================================================================

def normalize_team(name: str) -> str:
    """
    Normalize team name for consistent matching across sources.

    Removes all non-alphanumeric characters and converts to lowercase.
    This handles variations like:
        - 'Los Angeles Lakers' -> 'losangeleslakers'
        - 'LA Lakers' -> 'lalakers'
        - 'L.A. Lakers' -> 'lalakers'

    Args:
        name: Raw team name string.

    Returns:
        Normalized lowercase alphanumeric string.
        Returns empty string if name is None or empty.

    Example:
        >>> normalize_team('Los Angeles Lakers')
        'losangeleslakers'
        >>> normalize_team('L.A. Clippers')
        'laclippers'
        >>> normalize_team(None)
        ''
    """
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def canonical_game_id(league: str, team_a: str, team_b: str, date_str: str) -> str:
    """
    Generate a consistent game ID that matches across data sources.

    Creates a deterministic ID by sorting team names alphabetically,
    ensuring the same game gets the same ID regardless of which source
    reports it or which team is listed first.

    Format: '{date}_{league}_{team1}_{team2}'
    Where team1 and team2 are sorted alphabetically.

    Args:
        league: League/sport identifier (e.g., 'basketball_nba').
        team_a: First team name (will be normalized).
        team_b: Second team name (will be normalized).
        date_str: Date string in 'YYYY-MM-DD' format.

    Returns:
        Canonical game ID string.

    Example:
        >>> canonical_game_id('basketball_nba', 'Lakers', 'Celtics', '2026-02-10')
        '2026-02-10_basketball_nba_celtics_lakers'
        >>> # Same ID regardless of team order
        >>> canonical_game_id('basketball_nba', 'Celtics', 'Lakers', '2026-02-10')
        '2026-02-10_basketball_nba_celtics_lakers'
    """
    teams = sorted([normalize_team(team_a), normalize_team(team_b)])
    return f"{date_str}_{league}_{teams[0]}_{teams[1]}"


# =============================================================================
# JSON UTILITIES
# =============================================================================

def safe_json(val: Any) -> list | dict | Any:
    """
    Safely parse a value that might be a JSON string.

    Handles API responses where data might be returned as either:
        - Already parsed Python object (list/dict)
        - JSON-encoded string that needs parsing

    Args:
        val: Value to parse (string, list, dict, or None).

    Returns:
        Parsed value if JSON string, original value otherwise.
        Returns empty list for None or unparseable values.

    Example:
        >>> safe_json('["Yes", "No"]')
        ['Yes', 'No']
        >>> safe_json(['Yes', 'No'])  # Already a list
        ['Yes', 'No']
        >>> safe_json(None)
        []
    """
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            return []
    return val if val else []


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def _quote(col: str) -> str:
    """
    Quote a column name for safe SQL usage.

    Args:
        col: Column name to quote.

    Returns:
        Double-quoted column name.
    """
    return f'"{col}"'


def upsert_rows(
    conn: sqlite3.Connection,
    table: str,
    keys: list[str],
    updates: list[str],
    rows: Iterable[dict[str, Any]],
) -> int:
    """
    Insert or update rows in a table (upsert operation).

    Uses SQLite's INSERT ... ON CONFLICT DO UPDATE syntax.
    Rows are matched by the key columns; if a match exists,
    the update columns are overwritten.

    Args:
        conn: Active database connection.
        table: Target table name.
        keys: Column names forming the primary/unique key.
        updates: Column names to update on conflict.
        rows: Iterable of row dictionaries.

    Returns:
        Number of rows processed.

    Example:
        >>> conn = init_db()
        >>> rows = [{'game_id': 'g1', 'league': 'nba', 'home_team': 'Lakers'}]
        >>> upsert_rows(conn, 'games', ['game_id'], ['league', 'home_team'], rows)
        1
    """
    rows = list(rows)
    if not rows:
        return 0

    # Combine keys and updates, preserving order and removing duplicates
    cols = list(dict.fromkeys(keys + updates))
    placeholders = ", ".join(["?"] * len(cols))
    key_clause = ", ".join(_quote(c) for c in keys)
    update_clause = ", ".join(
        f"{_quote(c)}=excluded.{_quote(c)}"
        for c in updates if c not in keys
    )

    sql = (
        f"INSERT INTO {table} ({', '.join(_quote(c) for c in cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT({key_clause}) DO UPDATE SET {update_clause};"
    )

    conn.executemany(sql, [[row.get(c) for c in cols] for row in rows])
    return len(rows)


def insert_history(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> int:
    """
    Append rows to market_history table (no upsert, always insert).

    This table is append-only for time series tracking.
    Each call creates new snapshot records.

    Args:
        conn: Active database connection.
        rows: Iterable of market data dictionaries.

    Returns:
        Number of rows inserted.

    Example:
        >>> conn = init_db()
        >>> rows = [{'game_id': 'g1', 'market': 'h2h', 'snapshot_time': utc_now_iso()}]
        >>> insert_history(conn, rows)
        1
    """
    rows = list(rows)
    if not rows:
        return 0

    cols = [
        "game_id", "market", "side", "line", "source", "provider", "player",
        "price", "implied_prob", "devigged_prob", "provider_updated_at",
        "snapshot_time", "source_event_id", "source_market_id", "outcome",
    ]
    placeholders = ", ".join(["?"] * len(cols))

    sql = f"INSERT INTO market_history ({', '.join(_quote(c) for c in cols)}) VALUES ({placeholders});"
    conn.executemany(sql, [[row.get(c) for c in cols] for row in rows])
    return len(rows)


def update_source_metadata(
    conn: sqlite3.Connection,
    source_name: str,
    success: bool = True,
    error: Optional[str] = None,
    calls_made: int = 0,
) -> None:
    """
    Update polling metadata for a data source.

    Records the result of a polling operation including:
        - Last poll timestamp
        - Success/failure status
        - Error message (if failed)
        - API calls consumed

    Args:
        conn: Active database connection.
        source_name: Name of the source (e.g., 'odds_api').
        success: Whether the poll succeeded.
        error: Error message if poll failed.
        calls_made: Number of API calls made in this poll.

    Example:
        >>> conn = init_db()
        >>> update_source_metadata(conn, 'odds_api', success=True, calls_made=6)
    """
    now = utc_now_iso()

    conn.execute("""
        INSERT INTO source_metadata (
            source_name, last_poll_time, last_poll_success, last_error,
            calls_this_month, total_calls_ever, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            last_poll_time = excluded.last_poll_time,
            last_poll_success = excluded.last_poll_success,
            last_error = excluded.last_error,
            calls_this_month = calls_this_month + ?,
            total_calls_ever = total_calls_ever + ?,
            updated_at = excluded.updated_at
    """, [source_name, now, success, error, calls_made, calls_made, now, now,
          calls_made, calls_made])


def get_source_metadata(conn: sqlite3.Connection, source_name: str) -> Optional[dict[str, Any]]:
    """
    Get current polling metadata for a source.

    Args:
        conn: Active database connection.
        source_name: Name of the source to query.

    Returns:
        Dictionary with source metadata, or None if not found.

    Example:
        >>> conn = init_db()
        >>> meta = get_source_metadata(conn, 'odds_api')
        >>> meta['calls_this_month'] if meta else 0
        0
    """
    cursor = conn.execute(
        "SELECT * FROM source_metadata WHERE source_name = ?",
        [source_name]
    )
    row = cursor.fetchone()
    if row:
        cols = [d[0] for d in cursor.description]
        return dict(zip(cols, row))
    return None


# =============================================================================
# PROBABILITY FUNCTIONS
# =============================================================================

def odds_to_prob(price: Optional[float]) -> Optional[float]:
    """
    Convert decimal odds to implied probability.

    Decimal odds represent the total return per unit staked.
    Implied probability = 1 / decimal_odds.

    Args:
        price: Decimal odds (e.g., 2.0 for even money).

    Returns:
        Implied probability as decimal (0.0 to 1.0), or None if invalid.

    Example:
        >>> odds_to_prob(2.0)   # Even money
        0.5
        >>> odds_to_prob(1.5)   # -200 American
        0.6666666666666666
        >>> odds_to_prob(3.0)   # +200 American
        0.3333333333333333
        >>> odds_to_prob(None)
        None
    """
    if price and price > 0:
        return 1.0 / price
    return None


def prob_to_odds(prob: Optional[float]) -> Optional[float]:
    """
    Convert probability to decimal odds.

    Args:
        prob: Probability as decimal (0.0 to 1.0).

    Returns:
        Decimal odds, or None if invalid probability.

    Example:
        >>> prob_to_odds(0.5)
        2.0
        >>> prob_to_odds(0.25)
        4.0
    """
    if prob and 0 < prob <= 1:
        return 1.0 / prob
    return None


def devig(probs: list[Optional[float]]) -> list[Optional[float]]:
    """
    Remove bookmaker vig (margin) by normalizing probabilities to sum to 1.

    Sportsbooks inflate probabilities (e.g., both sides sum to 1.05).
    This function scales them back to true probabilities.

    Method: Multiplicative de-vigging (proportional reduction).
    Each probability is divided by the sum of all probabilities.

    Args:
        probs: List of implied probabilities (may contain None).

    Returns:
        List of de-vigged probabilities, same length as input.
        Returns original list if cannot de-vig.

    Example:
        >>> devig([0.55, 0.55])  # 10% vig market
        [0.5, 0.5]
        >>> devig([0.526, 0.526])  # ~5% vig
        [0.5, 0.5]
    """
    if not probs or any(p is None or p <= 0 for p in probs):
        return probs

    total = sum(p for p in probs if p)
    if total > 0:
        return [p / total if p else None for p in probs]
    return probs


def devig_market(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Apply de-vigging to a group of market outcome rows.

    Handles different sources appropriately:
        - Polymarket/Kalshi: Prices ARE fair probabilities (no vig)
        - Sportsbooks: Apply multiplicative de-vigging

    Modifies rows in-place by adding 'devigged_prob' field.

    Args:
        rows: List of market row dictionaries from same market/book.
              Each must have 'source' and 'implied_prob' keys.

    Returns:
        Same list with 'devigged_prob' added to each row.

    Example:
        >>> rows = [
        ...     {'source': 'odds_api', 'implied_prob': 0.55},
        ...     {'source': 'odds_api', 'implied_prob': 0.55}
        ... ]
        >>> result = devig_market(rows)
        >>> result[0]['devigged_prob']
        0.5
    """
    if not rows:
        return []

    # Open markets have no vig - price IS the probability
    source = rows[0].get("source", "")
    if source in ("polymarket", "kalshi"):
        for row in rows:
            row["devigged_prob"] = row.get("implied_prob")
        return rows

    # Sportsbooks: apply de-vigging
    probs = [row.get("implied_prob", 0) for row in rows]
    devigged = devig(probs)

    for row, dv in zip(rows, devigged):
        row["devigged_prob"] = dv

    return rows


def calculate_arb_margin(prob_a: float, prob_b: float) -> float:
    """
    Calculate arbitrage margin between two complementary bets.

    Arbitrage exists when the sum of probabilities < 1.
    The margin represents guaranteed profit percentage.

    Formula: margin = 1 - (prob_a + prob_b)
    Positive margin = arbitrage opportunity.

    Args:
        prob_a: Probability of outcome A from source A.
        prob_b: Probability of outcome B (complement) from source B.

    Returns:
        Arbitrage margin as decimal. Positive = profit opportunity.

    Example:
        >>> calculate_arb_margin(0.45, 0.48)  # Sum = 0.93
        0.07
        >>> calculate_arb_margin(0.52, 0.52)  # Sum = 1.04, no arb
        -0.04
    """
    return 1.0 - (prob_a + prob_b)


def optimal_stakes(
    prob_a: float,
    prob_b: float,
    total_stake: float = 100.0
) -> tuple[float, float]:
    """
    Calculate optimal stake distribution for arbitrage betting.

    Distributes total stake between two bets to guarantee equal
    profit regardless of which outcome occurs.

    Args:
        prob_a: Probability (as decimal) for bet A.
        prob_b: Probability (as decimal) for bet B.
        total_stake: Total amount to distribute.

    Returns:
        Tuple of (stake_a, stake_b) that guarantees equal profit.

    Example:
        >>> stake_a, stake_b = optimal_stakes(0.45, 0.48, 100)
        >>> round(stake_a, 2), round(stake_b, 2)
        (48.39, 51.61)
    """
    if prob_a <= 0 or prob_b <= 0:
        return (0.0, 0.0)

    # Stakes should be proportional to the OTHER side's probability
    # This ensures equal payout regardless of outcome
    total_prob = prob_a + prob_b
    stake_a = total_stake * prob_b / total_prob
    stake_b = total_stake * prob_a / total_prob

    return (stake_a, stake_b)


# =============================================================================
# PLAYER NAME NORMALIZATION
# =============================================================================

def normalize_player(name: str) -> str:
    """
    Normalize player name for consistent matching across sources.

    Removes punctuation, extra spaces, and converts to lowercase.
    Handles variations like:
        - 'LeBron James' -> 'lebronjames'
        - 'Giannis Antetokounmpo' -> 'giannisantetokounmpo'
        - 'P.J. Tucker' -> 'pjtucker'

    Args:
        name: Raw player name string.

    Returns:
        Normalized lowercase alphanumeric string.
        Returns empty string if name is None or empty.

    Example:
        >>> normalize_player('LeBron James')
        'lebronjames'
        >>> normalize_player('P.J. Tucker')
        'pjtucker'
    """
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]+", "", name.lower())


# =============================================================================
# MIDDLE BET CALCULATIONS
# =============================================================================

def calculate_middle_gap(line_a: float, line_b: float) -> float:
    """
    Calculate the gap (middle window) between two lines.

    For spreads: Gap is the range where both bets can win.
    For totals: Gap is the range of totals that hit the middle.

    Args:
        line_a: First line (e.g., -3.5 spread or 218.5 total)
        line_b: Second line (e.g., +5.5 spread or 222.5 total)

    Returns:
        Gap size in points. Positive = middle exists.

    Example:
        >>> # Spread middle: -3.5 vs +5.5
        >>> calculate_middle_gap(-3.5, 5.5)
        2.0
        >>> # Total middle: Over 218.5 vs Under 222.5
        >>> calculate_middle_gap(218.5, 222.5)
        4.0
    """
    return abs(line_b - line_a)


def estimate_middle_probability(
    gap: float,
    market_type: str = "spreads",
    std_dev: float = 12.0,
) -> float:
    """
    Estimate probability of hitting a middle based on gap size.

    Uses normal distribution approximation. Larger gaps = higher
    probability of middle hitting.

    Args:
        gap: Size of the middle window in points.
        market_type: "spreads" or "totals" (affects std_dev default).
        std_dev: Standard deviation of outcome distribution.
                 ~12 for NBA spreads, ~15 for NBA totals.

    Returns:
        Estimated probability of middle hitting (0 to 1).

    Example:
        >>> # 2-point gap in spreads
        >>> prob = estimate_middle_probability(2.0, "spreads")
        >>> 0.10 < prob < 0.20  # Roughly 10-20%
        True
    """
    import math

    # Adjust std_dev based on market type
    if market_type == "totals":
        std_dev = 15.0  # NBA totals have higher variance

    # Probability density over the gap
    # Approximation using normal distribution CDF
    # P(middle) ≈ gap / std_dev * 0.4 (simplified)
    # More accurate: use scipy.stats.norm.cdf difference
    try:
        from scipy.stats import norm
        # Probability that outcome falls within gap range
        # Centered around 0 (relative to spread line)
        prob = norm.cdf(gap / 2, 0, std_dev) - norm.cdf(-gap / 2, 0, std_dev)
    except ImportError:
        # Fallback: rough approximation
        prob = min(0.5, gap / std_dev * 0.4)

    return max(0.0, min(1.0, prob))


def calculate_middle_ev(
    stake_total: float,
    prob_over_a: float,
    prob_under_b: float,
    middle_prob: float,
    vig_loss: float = 0.05,
) -> dict[str, float]:
    """
    Calculate expected value of a middle bet.

    A middle bet wins both legs if outcome falls in the gap,
    wins one leg minus the other's stake otherwise.

    Args:
        stake_total: Total amount staked across both bets.
        prob_over_a: Probability for over/favorite side of bet A.
        prob_under_b: Probability for under/underdog side of bet B.
        middle_prob: Estimated probability of hitting the middle.
        vig_loss: Expected loss as decimal if middle doesn't hit.

    Returns:
        Dictionary with 'ev', 'win_both', 'win_one', 'middle_prob'.

    Example:
        >>> result = calculate_middle_ev(100, 0.52, 0.48, 0.15)
        >>> result['ev'] > 0  # Positive EV
        True
    """
    # If middle hits: win both bets
    # Approximate payout: stake_total * (1/prob_over_a - 1) + stake_total * (1/prob_under_b - 1)
    # Simplified: win roughly stake_total on each side
    payout_middle = stake_total * 2  # Win both at ~even odds

    # If middle doesn't hit: lose vig on the losing bet
    # Win one side, lose one side
    payout_no_middle = stake_total * (1 - vig_loss)  # Net small loss

    ev = (middle_prob * payout_middle) + ((1 - middle_prob) * payout_no_middle) - stake_total

    return {
        "ev": ev,
        "ev_percent": ev / stake_total if stake_total > 0 else 0,
        "win_both_payout": payout_middle,
        "win_one_payout": payout_no_middle,
        "middle_prob": middle_prob,
        "stake_total": stake_total,
    }
