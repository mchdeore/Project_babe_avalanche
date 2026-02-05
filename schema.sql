PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY, league TEXT NOT NULL, commence_time TEXT,
    home_team TEXT, away_team TEXT, last_refreshed TEXT
);

CREATE TABLE IF NOT EXISTS market_latest (
    game_id TEXT NOT NULL, market TEXT NOT NULL, side TEXT NOT NULL, line REAL NOT NULL,
    source TEXT NOT NULL, provider TEXT NOT NULL, price REAL, implied_prob REAL, devigged_prob REAL,
    provider_updated_at TEXT, last_refreshed TEXT, source_event_id TEXT, source_market_id TEXT, outcome TEXT,
    PRIMARY KEY (game_id, market, side, line, source, provider),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE TABLE IF NOT EXISTS market_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT, game_id TEXT NOT NULL, market TEXT NOT NULL, side TEXT NOT NULL,
    line REAL NOT NULL, source TEXT NOT NULL, provider TEXT NOT NULL, price REAL, implied_prob REAL, devigged_prob REAL,
    provider_updated_at TEXT, snapshot_time TEXT NOT NULL, source_event_id TEXT, source_market_id TEXT, outcome TEXT,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE TABLE IF NOT EXISTS outcomes (
    game_id TEXT PRIMARY KEY, home_score INTEGER, away_score INTEGER, winner TEXT,
    final_total REAL, home_spread_covered BOOLEAN, notes TEXT, updated_at TEXT,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE INDEX IF NOT EXISTS idx_market_latest_game ON market_latest(game_id);
CREATE INDEX IF NOT EXISTS idx_market_history_game ON market_history(game_id);
CREATE INDEX IF NOT EXISTS idx_market_history_snapshot ON market_history(snapshot_time);
