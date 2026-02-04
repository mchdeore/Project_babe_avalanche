# Project_babe_avalanche

Lightweight odds ingestion using The Odds API + Polymarket Gamma API, stored as snapshot rows in SQLite.

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
- `reset_snapshot` (optional)

Set your Odds API key in `.env`:
```
ODDS_API_KEY=your_key_here
```

## How data is stored
This project uses a snapshot-only schema (no timeseries yet). The schema is designed for:
- **One canonical game row**
- **Tall prices tables** (one row per market + side + source)

Tables:
- **games**: canonical game identity (one row per game)
- **odds_prices**: Odds API prices (one row per game + market + sportsbook + side)
- **pm_prices**: Polymarket prices (one row per game + market + side)

### Column strategy
`odds_prices` columns:
- `game_id`, `market`, `sportsbook`, `side`, `odds`, `line`, `odds_updated_at`

`pm_prices` columns:
- `game_id`, `market`, `side`, `price`, `pm_market_id`, `pm_event_id`, `pm_updated_at`

### Snapshot behavior
If `storage.reset_snapshot` is `true`, each ingest resets its own price table
before writing fresh values:
- Odds ingest resets `odds_prices`
- Polymarket ingest resets `pm_prices`

This keeps the latest snapshot without deleting rows. You can disable resets by setting
`storage.reset_snapshot` to `false` in `config.yaml` if you want to preserve the last
known values when one source is down.

If you want a clean rebuild of the schema, delete `odds.db` before running an ingest.

## Bettable window
Only games whose `commence_time` is within `bettable_window_days` from now are ingested.

## Notes
- The Polymarket ingest uses `tag_id` to filter to sports game markets.
- Canonical game IDs are generated from date + league + normalized team names.
