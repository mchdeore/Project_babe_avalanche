-- Core tables for snapshot odds + polymarket data, plus empty history tables
PRAGMA foreign_keys = ON;

-- Retire old normalized tables
DROP TABLE IF EXISTS odds_timeseries;
DROP TABLE IF EXISTS current_odds;
DROP TABLE IF EXISTS bets;
DROP TABLE IF EXISTS game_market_odds;
DROP TABLE IF EXISTS games;

-- Canonical games (current snapshot only)
CREATE TABLE IF NOT EXISTS games_current (
    game_id TEXT PRIMARY KEY,
    league TEXT NOT NULL,
    odds_event_id TEXT,
    polymarket_event_id TEXT,
    commence_time TEXT,
    home_team TEXT,
    away_team TEXT,
    last_updated TEXT
);

-- One row per game + market (current snapshot), with per-book columns added dynamically.
CREATE TABLE IF NOT EXISTS game_market_current (
    game_id TEXT NOT NULL,
    market TEXT NOT NULL,
    odds_updated_at TEXT,
    pm_home_price REAL,
    pm_away_price REAL,
    pm_over_price REAL,
    pm_under_price REAL,
    pm_market_id TEXT,
    pm_event_id TEXT,
    pm_updated_at TEXT,
    PRIMARY KEY (game_id, market),
    FOREIGN KEY (game_id) REFERENCES games_current(game_id)
);

-- Empty history table (not used yet)
CREATE TABLE IF NOT EXISTS game_market_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    market TEXT NOT NULL,
    source TEXT NOT NULL, -- odds_api | polymarket (future)
    sportsbook TEXT,
    side TEXT, -- home | away | over | under
    odds REAL,
    line REAL,
    pm_price REAL,
    observed_at TEXT NOT NULL,
    FOREIGN KEY (game_id) REFERENCES games_current(game_id)
);

CREATE INDEX IF NOT EXISTS idx_game_market_current_game
    ON game_market_current(game_id);
CREATE INDEX IF NOT EXISTS idx_game_market_history_game
    ON game_market_history(game_id);
