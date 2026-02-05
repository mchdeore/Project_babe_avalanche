# Project Babe Avalanche

Lightweight odds ingestion from The Odds API + Polymarket, stored in SQLite.

## Quick Start

1. Activate virtual environment:
   ```bash
   source .venv/bin/activate
   ```

2. Set your Odds API key in `.env`:
   ```
   ODDS_API_KEY=your_key_here
   ```

3. Run the ingest:
   ```bash
   python ingest_data.py
   ```

Or run individually:
```bash
python ingest_odds_api.py
python ingest_polymarket_api.py
```

## Configuration

Edit `config.yaml` to control:
- Which sports/leagues to track
- Which markets (h2h, spreads, totals)
- Which sportsbooks
- Bettable window (days ahead)

## Schema

Two tables in `odds.db`:
- **games**: One row per game (game_id, league, teams, commence_time)
- **market_latest**: Latest odds per market/side/provider (upserted each run)

To rebuild from scratch, delete `odds.db` before running.
