# Odds Comparison Tool

Compare sportsbook odds against prediction markets (Polymarket) with full bookmaker granularity.

## Quick Start

```bash
# 1. Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure API key
echo "ODDS_API_KEY=your_key" > .env

# 3. Run
python main.py
```

## Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point - runs ingestion + shows analysis |
| `ingest.py` | Fetches from Odds API, Polymarket, Kalshi |
| `analysis.py` | Comparison queries and metrics |
| `utils.py` | Shared helpers (db, normalization, devigging) |
| `config.yaml` | Sports, markets, bookmakers to track |
| `schema.sql` | SQLite schema |

## Database Schema

- **games**: Game metadata (teams, date, league)
- **market_latest**: Current odds per game/market/side/bookmaker
- **market_history**: Time series of all snapshots
- **outcomes**: Actual results (for Brier/log loss)

## Key Features

### Multi-Bookmaker Storage
Every bookmaker is stored separately, enabling:
- Book vs book comparison
- Book vs Polymarket edge detection
- Spread analysis (min/max across books)

### De-Vigging
Sportsbook odds are de-vigged (multiplicative method) for fair comparison against Polymarket's vig-free prices.

### Analysis Queries

```python
from analysis import compare_books_to_polymarket, get_book_spread
import sqlite3

conn = sqlite3.connect("odds.db")

# Edge vs Polymarket by bookmaker
for row in compare_books_to_polymarket(conn):
    print(f"{row['bookmaker']}: {row['edge']:+.1%}")

# Spread across all books
for row in get_book_spread(conn):
    print(f"{row['team']}: {row['spread']:.1%} spread")
```

## Cron Setup

```bash
# Every 5 minutes
*/5 * * * * cd /path/to/project && .venv/bin/python ingest.py >> cron.log 2>&1
```

## Data Sources

| Source | Data |
|--------|------|
| Odds API | Game odds + futures (all configured books) |
| Polymarket | Futures (NBA, NHL championships) |
| Kalshi | Game references from parlays |
