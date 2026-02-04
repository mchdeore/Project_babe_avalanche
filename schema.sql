-- Snapshot-only schema: latest rows per provider/outcome (no timeseries)
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    league TEXT NOT NULL,
    commence_time TEXT,
    home_team TEXT,
    away_team TEXT,
    last_refreshed TEXT
);

CREATE TABLE IF NOT EXISTS market_latest (
    game_id TEXT NOT NULL,
    market TEXT NOT NULL, -- h2h | totals | spreads | other
    side TEXT NOT NULL, -- home | away | draw | over | under
    line REAL NOT NULL, -- use 0.0 when a market has no line
    source TEXT NOT NULL, -- odds | polymarket
    provider TEXT NOT NULL, -- sportsbook key or "polymarket"
    price REAL,
    implied_prob REAL,
    provider_updated_at TEXT,
    last_refreshed TEXT,
    source_event_id TEXT,
    source_market_id TEXT,
    outcome TEXT,
    PRIMARY KEY (game_id, market, side, line, source, provider),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE INDEX IF NOT EXISTS idx_market_latest_game
    ON market_latest(game_id);
CREATE INDEX IF NOT EXISTS idx_market_latest_source
    ON market_latest(source);
CREATE INDEX IF NOT EXISTS idx_market_latest_market
    ON market_latest(market);
