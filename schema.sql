-- Core tables for snapshot odds + polymarket data (normalized, no timeseries yet)
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS odds_market_current;
DROP TABLE IF EXISTS pm_market_current;
DROP TABLE IF EXISTS odds_games_current;
DROP TABLE IF EXISTS pm_games_current;
DROP TABLE IF EXISTS games_current;
DROP TABLE IF EXISTS game_market_current;
DROP TABLE IF EXISTS game_market_odds;
DROP TABLE IF EXISTS bets;
DROP TABLE IF EXISTS current_odds;
DROP TABLE IF EXISTS odds_timeseries;

CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    league TEXT NOT NULL,
    odds_event_id TEXT,
    pm_event_id TEXT,
    commence_time TEXT,
    home_team TEXT,
    away_team TEXT,
    last_updated TEXT
);

-- Odds API prices (snapshot)
CREATE TABLE IF NOT EXISTS odds_prices (
    game_id TEXT NOT NULL,
    market TEXT NOT NULL,
    sportsbook TEXT NOT NULL,
    side TEXT NOT NULL, -- home | away | over | under
    odds REAL,
    line REAL,
    odds_updated_at TEXT,
    PRIMARY KEY (game_id, market, sportsbook, side),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE INDEX IF NOT EXISTS idx_odds_prices_game
    ON odds_prices(game_id);
CREATE INDEX IF NOT EXISTS idx_odds_prices_market
    ON odds_prices(market);

-- Polymarket prices (snapshot)
CREATE TABLE IF NOT EXISTS pm_prices (
    game_id TEXT NOT NULL,
    market TEXT NOT NULL,
    side TEXT NOT NULL, -- home | away | over | under
    price REAL,
    pm_market_id TEXT,
    pm_event_id TEXT,
    pm_updated_at TEXT,
    PRIMARY KEY (game_id, market, side),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE INDEX IF NOT EXISTS idx_pm_prices_game
    ON pm_prices(game_id);
CREATE INDEX IF NOT EXISTS idx_pm_prices_market
    ON pm_prices(market);
