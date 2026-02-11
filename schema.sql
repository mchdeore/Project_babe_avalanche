-- =============================================================================
-- ARBITRAGE DETECTION SYSTEM - DATABASE SCHEMA
-- =============================================================================
-- 
-- This schema supports:
--   1. Game/event tracking across all sources
--   2. Current market prices (latest snapshot)
--   3. Historical price time series
--   4. Game outcomes for performance metrics
--   5. Source metadata for intelligent polling
--   6. Player props with player identification
--
-- Tables:
--   - games: Core event information
--   - market_latest: Most recent prices per market/source/provider
--   - market_history: Time series of all price snapshots
--   - outcomes: Actual game results for backtesting
--   - source_metadata: Polling state and quota tracking per data source
--
-- Market Types:
--   - h2h: Moneyline (who wins)
--   - spreads: Point spread betting
--   - totals: Over/under total points
--   - futures: Championship/season outcomes
--   - player_points: Player points over/under
--   - player_rebounds: Player rebounds over/under
--   - player_assists: Player assists over/under
--   - player_threes: Player 3-pointers over/under
--
-- =============================================================================

PRAGMA foreign_keys = ON;

-- -----------------------------------------------------------------------------
-- GAMES TABLE
-- -----------------------------------------------------------------------------
-- Stores unique game/event identifiers with basic metadata.
-- game_id is a canonical ID generated from (date, league, sorted team names).

CREATE TABLE IF NOT EXISTS games (
    game_id         TEXT PRIMARY KEY,   -- Canonical ID: date_league_team1_team2
    league          TEXT NOT NULL,      -- Sport/league key (e.g., basketball_nba)
    commence_time   TEXT,               -- ISO timestamp of game start
    home_team       TEXT,               -- Home team name (normalized)
    away_team       TEXT,               -- Away team name (normalized)
    last_refreshed  TEXT                -- Last time this game was updated
);

-- -----------------------------------------------------------------------------
-- MARKET_LATEST TABLE
-- -----------------------------------------------------------------------------
-- Current/most recent prices for each unique market position.
-- Primary key ensures one row per (game, market, side, line, source, provider, player).
--
-- For game lines: player is NULL or empty
-- For player props: player contains normalized player name

CREATE TABLE IF NOT EXISTS market_latest (
    game_id             TEXT NOT NULL,      -- FK to games.game_id
    market              TEXT NOT NULL,      -- Market type: h2h, spreads, totals, futures, player_*
    side                TEXT NOT NULL,      -- Position: home, away, over, under, team_name
    line                REAL NOT NULL,      -- Point line (0.0 for h2h/futures)
    source              TEXT NOT NULL,      -- Data source: odds_api, polymarket, kalshi
    provider            TEXT NOT NULL,      -- Specific book/exchange within source
    player              TEXT DEFAULT '',    -- Player name for props (empty for game lines)
    price               REAL,               -- Raw decimal odds or probability
    implied_prob        REAL,               -- Probability before de-vigging
    devigged_prob       REAL,               -- Fair probability (vig removed)
    provider_updated_at TEXT,               -- When provider last updated this price
    last_refreshed      TEXT,               -- When we last fetched this data
    source_event_id     TEXT,               -- Original event ID from source API
    source_market_id    TEXT,               -- Original market ID from source API
    outcome             TEXT,               -- Raw outcome name from source
    
    PRIMARY KEY (game_id, market, side, line, source, provider, player),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-- -----------------------------------------------------------------------------
-- MARKET_HISTORY TABLE
-- -----------------------------------------------------------------------------
-- Append-only time series of all price snapshots.
-- Used for historical analysis, backtesting, and price movement tracking.

CREATE TABLE IF NOT EXISTS market_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id             TEXT NOT NULL,      -- FK to games.game_id
    market              TEXT NOT NULL,      -- Market type
    side                TEXT NOT NULL,      -- Position
    line                REAL NOT NULL,      -- Point line
    source              TEXT NOT NULL,      -- Data source
    provider            TEXT NOT NULL,      -- Specific provider
    player              TEXT DEFAULT '',    -- Player name for props (empty for game lines)
    price               REAL,               -- Raw price
    implied_prob        REAL,               -- Implied probability
    devigged_prob       REAL,               -- De-vigged probability
    provider_updated_at TEXT,               -- Provider timestamp
    snapshot_time       TEXT NOT NULL,      -- When we captured this snapshot
    source_event_id     TEXT,               -- Source event ID
    source_market_id    TEXT,               -- Source market ID
    outcome             TEXT,               -- Raw outcome name
    
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-- -----------------------------------------------------------------------------
-- OUTCOMES TABLE
-- -----------------------------------------------------------------------------
-- Actual game results for performance metric calculation.
-- Used to compute Brier scores, log loss, and P&L tracking.

CREATE TABLE IF NOT EXISTS outcomes (
    game_id             TEXT PRIMARY KEY,   -- FK to games.game_id
    home_score          INTEGER,            -- Final home team score
    away_score          INTEGER,            -- Final away team score
    winner              TEXT,               -- Winning side: home, away, draw
    final_total         REAL,               -- Actual total points scored
    home_spread_covered BOOLEAN,            -- Did home team cover spread?
    notes               TEXT,               -- Additional notes (OT, cancelled, etc.)
    updated_at          TEXT,               -- When this outcome was recorded
    
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-- -----------------------------------------------------------------------------
-- SOURCE_METADATA TABLE
-- -----------------------------------------------------------------------------
-- Tracks polling state and quota usage for each data source.
-- Enables intelligent scheduling and rate limit compliance.

CREATE TABLE IF NOT EXISTS source_metadata (
    source_name         TEXT PRIMARY KEY,   -- Source identifier: odds_api, polymarket, kalshi
    last_poll_time      TEXT,               -- ISO timestamp of last successful poll
    last_poll_success   BOOLEAN DEFAULT 1,  -- Did last poll succeed?
    last_error          TEXT,               -- Error message if last poll failed
    calls_this_month    INTEGER DEFAULT 0,  -- API calls used this billing period
    quota_reset_date    TEXT,               -- When monthly quota resets
    total_calls_ever    INTEGER DEFAULT 0,  -- Lifetime call count
    created_at          TEXT,               -- When this source was first tracked
    updated_at          TEXT                -- Last modification time
);

-- =============================================================================
-- INDICES FOR ARBITRAGE & MIDDLE DETECTION QUERIES
-- =============================================================================
-- These indices optimize the most common queries:
--   1. Finding matching markets across sources
--   2. Filtering by source category (sportsbook vs open market)
--   3. Time-based queries on history
--   4. Player prop lookups by player name

-- Primary lookup: find all prices for a specific game
CREATE INDEX IF NOT EXISTS idx_market_latest_game 
    ON market_latest(game_id);

-- Arbitrage detection: find complementary sides quickly
CREATE INDEX IF NOT EXISTS idx_market_latest_arb 
    ON market_latest(market, side, line);

-- Source filtering: separate sportsbooks from open markets
CREATE INDEX IF NOT EXISTS idx_market_latest_source 
    ON market_latest(source);

-- Provider filtering: compare specific bookmakers
CREATE INDEX IF NOT EXISTS idx_market_latest_provider 
    ON market_latest(provider);

-- Composite for full market lookup
CREATE INDEX IF NOT EXISTS idx_market_latest_full 
    ON market_latest(game_id, market, line, source);

-- Player prop lookups: find same player across sources
CREATE INDEX IF NOT EXISTS idx_market_latest_player 
    ON market_latest(player, market, line);

-- Player prop by game: all props for a specific game
CREATE INDEX IF NOT EXISTS idx_market_latest_game_player 
    ON market_latest(game_id, player);

-- Middle detection: find different lines for same market type
CREATE INDEX IF NOT EXISTS idx_market_latest_middle 
    ON market_latest(game_id, market, source, player);

-- History: game lookups
CREATE INDEX IF NOT EXISTS idx_market_history_game 
    ON market_history(game_id);

-- History: time-based queries
CREATE INDEX IF NOT EXISTS idx_market_history_snapshot 
    ON market_history(snapshot_time);

-- History: source filtering for category analysis
CREATE INDEX IF NOT EXISTS idx_market_history_source 
    ON market_history(source);

-- History: player prop tracking over time
CREATE INDEX IF NOT EXISTS idx_market_history_player 
    ON market_history(player, game_id, snapshot_time);
