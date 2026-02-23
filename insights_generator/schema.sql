-- =============================================================================
-- INSIGHTS GENERATOR - DATABASE SCHEMA
-- =============================================================================
--
-- Additional tables for the insights_generator module.
-- These tables are created in the same database as the main system (odds.db).
--
-- Tables:
--   - news_headlines      : Raw scraped headlines from RSS feeds
--   - structured_events   : Ollama-extracted structured features
--   - market_lag_signals  : Detected lead/lag relationships between providers
--   - ml_predictions      : Model prediction outputs
--
-- Run this schema after the main schema.sql to add insights_generator tables.
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- NEWS_HEADLINES TABLE
-- -----------------------------------------------------------------------------
-- Raw headlines scraped from RSS feeds and news sources.
-- Each headline is stored once (deduplicated by URL hash).
-- The 'processed' flag indicates whether NLP analysis has been run.

CREATE TABLE IF NOT EXISTS news_headlines (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Source information
    source          TEXT NOT NULL,          -- Source name (e.g., 'espn_nba', 'rotoworld')
    source_type     TEXT NOT NULL,          -- Source type: 'rss', 'api', 'scrape'
    
    -- Headline content
    headline        TEXT NOT NULL,          -- The headline text
    summary         TEXT,                   -- Article summary/description if available
    url             TEXT NOT NULL,          -- Original URL (used for deduplication)
    url_hash        TEXT NOT NULL UNIQUE,   -- SHA256 hash of URL for fast dedup lookups
    
    -- Timestamps
    published_at    TEXT,                   -- When the article was published (from source)
    scraped_at      TEXT NOT NULL,          -- When we scraped this headline
    
    -- Game matching (nullable - may not match a specific game)
    game_id         TEXT,                   -- FK to games.game_id if matched
    matched_teams   TEXT,                   -- JSON array of team names detected in headline
    
    -- Processing state
    processed       INTEGER DEFAULT 0,      -- 0=unprocessed, 1=processed by NLP
    processed_at    TEXT,                   -- When NLP processing completed
    
    -- Relevance scoring
    relevance_score REAL,                   -- 0.0-1.0 relevance to betting (set by NLP)
    
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-- Index for finding unprocessed headlines
CREATE INDEX IF NOT EXISTS idx_headlines_unprocessed 
    ON news_headlines(processed, scraped_at);

-- Index for game-based lookups
CREATE INDEX IF NOT EXISTS idx_headlines_game 
    ON news_headlines(game_id);

-- Index for source filtering
CREATE INDEX IF NOT EXISTS idx_headlines_source 
    ON news_headlines(source, published_at);


-- -----------------------------------------------------------------------------
-- STRUCTURED_EVENTS TABLE
-- -----------------------------------------------------------------------------
-- Ollama-extracted structured features from news headlines.
-- Each row represents a structured interpretation of a headline.
-- One headline may produce multiple events (e.g., injury + trade rumor).

CREATE TABLE IF NOT EXISTS structured_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Link to source headline
    headline_id         INTEGER NOT NULL,       -- FK to news_headlines.id
    
    -- Event classification
    event_type          TEXT NOT NULL,          -- injury, weather, trade, rumor, lineup, other
    
    -- Entity extraction
    player              TEXT,                   -- Player name (normalized)
    team                TEXT,                   -- Team name (normalized)
    opponent_team       TEXT,                   -- Opponent team if relevant
    
    -- Severity and importance (1-5 scale)
    severity            INTEGER,                -- Impact severity: 1=minor, 5=critical
    position_importance INTEGER,                -- How important is this position: 1=bench, 5=star
    
    -- Player status
    starter_status      TEXT,                   -- starter, bench, unknown
    injury_type         TEXT,                   -- For injuries: ankle, knee, illness, rest, etc.
    expected_absence    TEXT,                   -- For injuries: game, week, season, unknown
    
    -- Weather-specific (for weather events)
    weather_condition   TEXT,                   -- rain, snow, wind, extreme_heat, etc.
    weather_severity    INTEGER,                -- 1-5 scale
    
    -- Trade-specific
    trade_status        TEXT,                   -- confirmed, rumor, speculation
    
    -- Confidence and timestamps
    confidence          REAL,                   -- 0.0-1.0 confidence in extraction
    extracted_at        TEXT NOT NULL,          -- When this was extracted
    ollama_model        TEXT,                   -- Which Ollama model was used
    
    -- Raw Ollama response for debugging
    raw_response        TEXT,                   -- Full JSON response from Ollama
    
    FOREIGN KEY (headline_id) REFERENCES news_headlines(id)
);

-- Index for event type analysis
CREATE INDEX IF NOT EXISTS idx_events_type 
    ON structured_events(event_type, extracted_at);

-- Index for team-based lookups
CREATE INDEX IF NOT EXISTS idx_events_team 
    ON structured_events(team, event_type);

-- Index for player-based lookups
CREATE INDEX IF NOT EXISTS idx_events_player 
    ON structured_events(player, event_type);

-- Index for headline linking
CREATE INDEX IF NOT EXISTS idx_events_headline 
    ON structured_events(headline_id);


-- -----------------------------------------------------------------------------
-- MARKET_LAG_SIGNALS TABLE
-- -----------------------------------------------------------------------------
-- Detected lead/lag relationships between market providers.
-- Each row represents a detected signal where one provider moved before another.
-- Bidirectional: can detect any provider leading any other.

CREATE TABLE IF NOT EXISTS market_lag_signals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Market identification
    game_id             TEXT NOT NULL,          -- FK to games.game_id
    market              TEXT NOT NULL,          -- h2h, spreads, totals
    side                TEXT NOT NULL,          -- home, away, over, under
    line                REAL,                   -- Point line (0.0 for h2h)
    
    -- Provider relationship
    leader_source       TEXT NOT NULL,          -- Source that moved first (odds_api, polymarket, kalshi)
    leader_provider     TEXT NOT NULL,          -- Specific provider (draftkings, polymarket, etc.)
    lagger_source       TEXT NOT NULL,          -- Source that moved later
    lagger_provider     TEXT NOT NULL,          -- Specific provider that lagged
    
    -- Timing metrics
    leader_move_time    TEXT NOT NULL,          -- ISO timestamp when leader moved
    lagger_move_time    TEXT NOT NULL,          -- ISO timestamp when lagger moved
    lag_seconds         REAL NOT NULL,          -- Seconds between leader and lagger moves
    
    -- Price/probability metrics
    leader_prob_before  REAL,                   -- Leader's probability before move
    leader_prob_after   REAL,                   -- Leader's probability after move
    lagger_prob_before  REAL,                   -- Lagger's probability before move
    lagger_prob_after   REAL,                   -- Lagger's probability after move
    probability_delta   REAL NOT NULL,          -- Size of the probability move
    
    -- Signal strength
    signal_strength     REAL,                   -- Combined metric (lag_time * delta magnitude)
    
    -- Detection metadata
    detected_at         TEXT NOT NULL,          -- When this signal was detected
    lookback_minutes    INTEGER,                -- How far back we looked to find this
    
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-- Index for game-based signal lookups
CREATE INDEX IF NOT EXISTS idx_lag_signals_game 
    ON market_lag_signals(game_id, market);

-- Index for provider relationship analysis
CREATE INDEX IF NOT EXISTS idx_lag_signals_providers 
    ON market_lag_signals(leader_provider, lagger_provider);

-- Index for time-based analysis
CREATE INDEX IF NOT EXISTS idx_lag_signals_time 
    ON market_lag_signals(detected_at);

-- Index for finding strong signals
CREATE INDEX IF NOT EXISTS idx_lag_signals_strength 
    ON market_lag_signals(signal_strength DESC);


-- -----------------------------------------------------------------------------
-- ML_PREDICTIONS TABLE
-- -----------------------------------------------------------------------------
-- Model prediction outputs for line movement forecasting.
-- Stores both the prediction and the features used to generate it.

CREATE TABLE IF NOT EXISTS ml_predictions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Target identification
    game_id             TEXT NOT NULL,          -- FK to games.game_id
    market              TEXT NOT NULL,          -- h2h, spreads, totals
    side                TEXT NOT NULL,          -- home, away, over, under
    provider            TEXT NOT NULL,          -- Provider we're predicting will move
    
    -- Prediction output
    predicted_move      REAL,                   -- Predicted probability change
    predicted_direction TEXT,                   -- up, down, stable
    confidence          REAL,                   -- Model confidence 0.0-1.0
    
    -- Prediction horizon
    horizon_minutes     INTEGER,                -- How far ahead we're predicting
    
    -- Features used (stored as JSON for reproducibility)
    features_json       TEXT,                   -- JSON object of all feature values
    
    -- Model information
    model_version       TEXT,                   -- Model version identifier
    model_type          TEXT,                   -- xgboost, lightgbm, linear, etc.
    
    -- Timestamps
    created_at          TEXT NOT NULL,          -- When prediction was made
    
    -- Outcome tracking (filled in later when we know what happened)
    actual_move         REAL,                   -- What actually happened
    actual_direction    TEXT,                   -- What direction it actually moved
    outcome_recorded_at TEXT,                   -- When we recorded the outcome
    prediction_correct  INTEGER,                -- 1=correct direction, 0=wrong
    
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-- Index for game-based prediction lookups
CREATE INDEX IF NOT EXISTS idx_predictions_game 
    ON ml_predictions(game_id, market);

-- Index for model performance analysis
CREATE INDEX IF NOT EXISTS idx_predictions_model 
    ON ml_predictions(model_version, prediction_correct);

-- Index for time-based analysis
CREATE INDEX IF NOT EXISTS idx_predictions_time 
    ON ml_predictions(created_at);

-- Index for finding predictions needing outcome updates
CREATE INDEX IF NOT EXISTS idx_predictions_pending 
    ON ml_predictions(outcome_recorded_at) WHERE outcome_recorded_at IS NULL;
