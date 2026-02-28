"""
Insights Generator Configuration
================================

Loads and validates configuration for the insights_generator module.
Configuration is stored in the main config.yaml under the 'insights_generator' key.

Usage:
------
    from insights_generator.config import get_config, get_news_sources, get_ollama_config
    
    config = get_config()
    sources = get_news_sources()
    ollama = get_ollama_config()
"""

import sqlite3
from pathlib import Path
from typing import Any

import yaml

from . import PROJECT_ROOT, MODULE_ROOT


# =============================================================================
# DEFAULT CONFIGURATION
# =============================================================================
# These defaults are used if not specified in config.yaml

DEFAULT_CONFIG: dict[str, Any] = {
    "enabled": True,
    "database": "odds.db",
    
    "news": {
        "sources": [
            {
                "name": "espn_nba",
                "url": "https://www.espn.com/espn/rss/nba/news",
                "type": "rss",
            },
            {
                "name": "espn_nfl", 
                "url": "https://www.espn.com/espn/rss/nfl/news",
                "type": "rss",
            },
            {
                "name": "espn_nhl",
                "url": "https://www.espn.com/espn/rss/nhl/news",
                "type": "rss",
            },
            {
                "name": "espn_mlb",
                "url": "https://www.espn.com/espn/rss/mlb/news",
                "type": "rss",
            },
            {
                "name": "reddit_sportsbook",
                "type": "api",
                "api_type": "reddit",
                "subreddit": "sportsbook",
                "limit": 100,
            },
            {
                "name": "weather_nfl",
                "type": "api",
                "api_type": "weather",
                "league": "americanfootball_nfl",
                "hours_ahead": 72,
            },
            {
                "name": "espn_nba_injuries",
                "type": "api",
                "api_type": "espn_injuries",
                "sport": "basketball",
                "league": "nba",
            },
            {
                "name": "espn_nba_lineups",
                "type": "api",
                "api_type": "espn_lineups",
                "sport": "basketball",
                "league": "nba",
            },
        ],
    },
    
    "nlp": {
        "ollama_model": "llama3.2",
        "ollama_host": "http://localhost:11434",
        "batch_size": 10,
    },
    
    "lag_detection": {
        "lookback_minutes": 30,
        "min_probability_delta": 0.02,
        "min_lag_seconds": 5,
        "max_lag_seconds": 300,
    },

    "event_impact": {
        "pre_window_minutes": 30,
        "post_window_minutes": 120,
        "max_event_age_hours": 72,
        "min_snapshot_count": 1,
    },

    "api": {
        "user_agent": "insights-generator/0.1",
        "request_timeout_seconds": 15,
    },

    "espn": {
        "cache_hours": 24,
    },
    
    "scoring": {
        "weights": {
            "injury": 0.25,
            "weather": 0.10,
            "news_momentum": 0.15,
            "market_momentum": 0.20,
            "provider_lag": 0.20,
            "lineup": 0.10,
        },
        "lookback_hours": 72,
        "max_severity": 5,
        "outdoor_leagues": [
            "americanfootball_nfl",
            "baseball_mlb",
        ],
    },

    "ml": {
        "min_training_samples": 1000,
        "model_path": "insights_generator/trained_model.pkl",
        "features": [
            "price_velocity",
            "volatility", 
            "provider_spread",
            "time_to_game",
            "injury_severity",
            "news_count",
        ],
    },
}


# =============================================================================
# CONFIGURATION LOADING
# =============================================================================

def load_main_config() -> dict[str, Any]:
    """
    Load the main config.yaml file from the project root.
    
    Returns:
        dict: Full configuration dictionary
        
    Raises:
        FileNotFoundError: If config.yaml doesn't exist
    """
    config_path = PROJECT_ROOT / "config.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def get_config() -> dict[str, Any]:
    """
    Get the insights_generator configuration with defaults applied.
    
    Returns:
        dict: Merged configuration (defaults + config.yaml overrides)
    """
    try:
        main_config = load_main_config()
        user_config = main_config.get("insights_generator", {})
    except FileNotFoundError:
        user_config = {}
    
    # Deep merge user config over defaults
    return _deep_merge(DEFAULT_CONFIG.copy(), user_config)


def _deep_merge(base: dict, override: dict) -> dict:
    """
    Deep merge two dictionaries. Override values take precedence.
    
    Args:
        base: Base dictionary with defaults
        override: Override dictionary with user values
        
    Returns:
        dict: Merged dictionary
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


# =============================================================================
# CONVENIENCE GETTERS
# =============================================================================

def get_news_sources() -> list[dict[str, str]]:
    """
    Get configured news sources for scraping.
    
    Returns:
        list: List of source configs with 'name', 'url', 'type' keys
    """
    config = get_config()
    return config.get("news", {}).get("sources", [])


def get_ollama_config() -> dict[str, Any]:
    """
    Get Ollama NLP configuration.
    
    Returns:
        dict: Ollama config with 'ollama_model', 'ollama_host', 'batch_size'
    """
    config = get_config()
    return config.get("nlp", DEFAULT_CONFIG["nlp"])


def get_lag_detection_config() -> dict[str, Any]:
    """
    Get lag detection configuration.
    
    Returns:
        dict: Lag detection config with thresholds and limits
    """
    config = get_config()
    return config.get("lag_detection", DEFAULT_CONFIG["lag_detection"])


def get_event_impact_config() -> dict[str, Any]:
    """
    Get event impact configuration.

    Returns:
        dict: Event impact config with windows and thresholds
    """
    config = get_config()
    return config.get("event_impact", DEFAULT_CONFIG["event_impact"])


def get_api_config() -> dict[str, Any]:
    """
    Get API helper configuration.

    Returns:
        dict: API config with user agent and timeout settings
    """
    config = get_config()
    return config.get("api", DEFAULT_CONFIG["api"])


def get_espn_config() -> dict[str, Any]:
    """
    Get ESPN config for roster/injury cache behavior.

    Returns:
        dict: ESPN config with cache settings
    """
    config = get_config()
    return config.get("espn", DEFAULT_CONFIG["espn"])


def get_scoring_config() -> dict[str, Any]:
    """
    Get AI scoring configuration.

    Returns:
        dict: Scoring config with weights, lookback, and league settings
    """
    config = get_config()
    return config.get("scoring", DEFAULT_CONFIG["scoring"])


def get_ml_config() -> dict[str, Any]:
    """
    Get ML pipeline configuration.
    
    Returns:
        dict: ML config with model path, features, and thresholds
    """
    config = get_config()
    return config.get("ml", DEFAULT_CONFIG["ml"])


def get_database_path() -> Path:
    """
    Get the path to the database file.
    
    Returns:
        Path: Absolute path to the database file
    """
    config = get_config()
    db_name = config.get("database", "odds.db")
    return PROJECT_ROOT / db_name


def is_enabled() -> bool:
    """
    Check if insights_generator is enabled in config.
    
    Returns:
        bool: True if enabled, False otherwise
    """
    config = get_config()
    return config.get("enabled", True)


# =============================================================================
# DATABASE INITIALIZATION
# =============================================================================

def init_insights_db(conn: sqlite3.Connection | None = None) -> sqlite3.Connection:
    """
    Initialize the insights_generator database tables.
    
    Creates the additional tables defined in schema.sql if they don't exist.
    Uses the same database as the main system (odds.db).
    
    Args:
        conn: Existing database connection (optional)
        
    Returns:
        sqlite3.Connection: Database connection with tables created
    """
    if conn is None:
        db_path = get_database_path()
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    
    # Read and execute schema
    schema_path = MODULE_ROOT / "schema.sql"
    
    if schema_path.exists():
        with open(schema_path, "r") as f:
            schema_sql = f.read()
        conn.executescript(schema_sql)
        conn.commit()
    
    return conn


# =============================================================================
# VALIDATION
# =============================================================================

def validate_config() -> list[str]:
    """
    Validate the current configuration.
    
    Returns:
        list: List of validation error messages (empty if valid)
    """
    errors = []
    config = get_config()
    
    # Check news sources
    sources = config.get("news", {}).get("sources", [])
    for i, source in enumerate(sources):
        if "name" not in source:
            errors.append(f"News source {i} missing 'name'")
        if "type" not in source:
            errors.append(f"News source {i} missing 'type'")
        source_type = source.get("type")
        if source_type == "rss" and "url" not in source:
            errors.append(f"News source {i} missing 'url'")
        if source_type == "api" and "api_type" not in source:
            errors.append(f"News source {i} missing 'api_type'")
    
    # Check Ollama config
    nlp = config.get("nlp", {})
    if not nlp.get("ollama_host"):
        errors.append("NLP config missing 'ollama_host'")
    if not nlp.get("ollama_model"):
        errors.append("NLP config missing 'ollama_model'")
    
    # Check lag detection thresholds
    lag = config.get("lag_detection", {})
    if lag.get("min_lag_seconds", 0) >= lag.get("max_lag_seconds", 300):
        errors.append("lag_detection: min_lag_seconds must be less than max_lag_seconds")
    
    return errors
