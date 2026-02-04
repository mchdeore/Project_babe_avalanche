# Project_babe_avalanche

Lightweight odds ingestion using The Odds API + Polymarket Gamma API, stored as latest-only rows in SQLite.

## Quick start
1) Activate the virtual environment:
   - `source .venv/bin/activate`
2) Run both ingests together:
   - `python ingest_data.py`
3) Or run them individually:
   - `python Ingest_odds_api.py`
   - `python ingest_polymarket_api`

## Configuration
Edit `config.yaml` to control:
- Odds API sports, markets, regions, and books
- Polymarket sports and league aliases
- database path
- optional Polymarket market filter (`polymarket.markets`)

Set your Odds API key in `.env`:
```
ODDS_API_KEY=your_key_here
```

## How data is stored
This project uses a latest-only schema (no timeseries). The schema is designed for:
- **One canonical game row**
- **A single tall latest table** (one row per market + side + provider)

Tables:
- **games**: canonical game identity (one row per game)
- **market_latest**: latest Odds API + Polymarket rows

### Column strategy
`market_latest` columns:
- `game_id`, `market`, `side`, `line`
- `source` (`odds` or `polymarket`)
- `provider` (sportsbook key or `polymarket`)
- `price`, `implied_prob`
- `provider_updated_at`, `last_refreshed`
- `source_event_id`, `source_market_id`, `outcome`
  - `line` is `0.0` for markets without a line (e.g., h2h/draw).

### Latest-only behavior
Each ingest upserts into `market_latest` keyed by:
`(game_id, market, side, line, source, provider)`.
The row is overwritten on each run, so the table always reflects the latest snapshot.

If you want a clean rebuild of the schema, delete `odds.db` before running an ingest.

## Bettable window
Only games whose `commence_time` is within `bettable_window_days` from now are ingested.

## Notes
- The Polymarket ingest uses `tag_id` to filter to sports game markets.
- Canonical game IDs are generated from date + league + normalized team names.
- `implied_prob` is computed as `1 / price`.
