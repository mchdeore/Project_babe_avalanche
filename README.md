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
This project uses a snapshot-only schema (no timeseries). The schema is designed for:
- **One row per game + market**
- **One column per book** (for odds and lines)
- **Polymarket prices appended as columns**

Tables:
- **games_current**: canonical game identity (one row per game)
- **game_market_current**: one row per game + market, with per-book columns added dynamically
- **game_market_history**: empty timeseries table (not populated yet)

### Column strategy (game_market_current)
Each book adds these columns dynamically:
- `home_odds_<book>` / `away_odds_<book>`
- `home_line_<book>` / `away_line_<book>`
- `over_odds_<book>` / `under_odds_<book>`
- `total_line_<book>`

Polymarket adds:
- `pm_home_price`, `pm_away_price`
- `pm_over_price`, `pm_under_price`
- `pm_market_id`, `pm_event_id`, `pm_updated_at`

### Snapshot behavior
If `storage.reset_snapshot` is `true`, each ingest resets its own columns to `NULL`
before writing fresh values:
- Odds ingest resets only book-related columns
- Polymarket ingest resets only `pm_*` columns

This keeps the latest snapshot without deleting rows. You can disable resets by setting
`storage.reset_snapshot` to `false` in `config.yaml` if you want to preserve the last
known values when one source is down.

## Notes
- The Polymarket ingest uses `tag_id` to filter to sports game markets.
- Canonical game IDs are generated from date + league + normalized team names.
