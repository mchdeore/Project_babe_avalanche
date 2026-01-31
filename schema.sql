-- Core tables for normalized odds data

-- Bet identity (stable)
CREATE TABLE IF NOT EXISTS bets (
    bet_id TEXT PRIMARY KEY,
    league TEXT,
    event_id TEXT,
    market TEXT,
    outcome TEXT,
    event_date TEXT
);

-- Latest odds snapshot per sportsbook
CREATE TABLE IF NOT EXISTS current_odds (
    bet_id TEXT,
    sportsbook TEXT,
    odds REAL,
    last_updated TEXT,
    PRIMARY KEY (bet_id, sportsbook)
);

-- Coarse history (one row per bet per poll)
CREATE TABLE IF NOT EXISTS odds_timeseries (
    bet_id TEXT,
    best_odds REAL,
    observed_at TEXT
);
