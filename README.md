# Arbitrage Detection System

A modular sports betting arbitrage and middle bet detection platform that compares odds across sportsbooks and prediction markets.

## Features

- **Arbitrage Detection (Risk-Free Profit)**
  - Open Market: Polymarket vs Kalshi
  - Sportsbook: Between regulated bookmakers (DraftKings, FanDuel, BetMGM, etc.)
  - Cross-Market: Sportsbooks vs prediction markets

- **Middle Bet Detection (Conditional Profit)**
  - Sportsbook Middles: Different spread/total lines between bookmakers
  - Open Market Middles: Different lines between Polymarket/Kalshi
  - Cross-Market Middles: Sportsbooks vs prediction markets
  - Player Prop Middles: Same player, different O/U lines across sources

- **Player Props Support**
  - Polymarket: Points, rebounds, assists per player
  - Kalshi: Points, rebounds, assists, 3-pointers
  - Odds API: All player prop markets (optional, uses extra API calls)

- **Intelligent Polling**
  - Per-source configurable polling intervals
  - Monthly quota tracking (for rate-limited APIs like Odds API)
  - Automatic rate limiting between calls

- **Comprehensive Data Storage**
  - SQLite database with optimized indices for arbitrage/middle queries
  - Historical time series for backtesting
  - Outcome tracking for performance metrics

## Quick Start

```bash
# 1. Setup virtual environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure API key
echo "ODDS_API_KEY=your_key_here" > .env

# 3. Run full pipeline (ingest + detect)
python main.py
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `python main.py` | Full pipeline: ingest data + detect arbitrage |
| `python main.py ingest` | Ingest data from all sources |
| `python main.py detect` | Detect arbitrage (using existing data) |
| `python main.py status` | Show database stats and polling status |
| `python main.py daemon` | Run continuous polling daemon |

### Targeted Arbitrage Detection

```bash
python main.py open        # Open market arbitrage only
python main.py sportsbook  # Sportsbook arbitrage only
python main.py cross       # Cross-market arbitrage only
```

### Middle Bet Detection

```bash
python main.py middle          # All middle opportunities
python main.py middle-sb       # Sportsbook middles only
python main.py middle-open     # Open market middles only
python main.py middle-cross    # Cross-market middles only
python main.py middle-props    # Player prop middles only
```

### Standalone Scripts

```bash
python arbitrage.py              # Run all detection algorithms
python arbitrage.py sportsbook   # Sportsbook arbs only
python middles.py                # Run all middle detection
python middles.py props          # Player prop middles only
python poll_manager.py status    # Polling status
python poll_manager.py daemon    # Start polling daemon
python analysis.py               # Run analysis report
```

## Project Structure

| File | Purpose |
|------|---------|
| `main.py` | CLI entry point with multiple commands |
| `ingest.py` | Data fetching from Odds API, Polymarket, Kalshi |
| `arbitrage.py` | Three arbitrage detection algorithms |
| `middles.py` | Four middle bet detection algorithms |
| `poll_manager.py` | Per-source polling scheduler with quota tracking |
| `analysis.py` | Market comparison and performance metrics |
| `utils.py` | Shared utilities (DB, normalization, probability, EV calcs) |
| `config.yaml` | Per-source polling config, sports, bookmakers, middles |
| `schema.sql` | SQLite schema with arbitrage/middle-optimized indices |

## Configuration

Edit `config.yaml` to customize:

```yaml
sources:
  odds_api:
    enabled: true
    poll_interval_seconds: 300    # 5 minutes
    monthly_quota: 500            # API call limit
    
  polymarket:
    enabled: true
    poll_interval_seconds: 60     # 1 minute
    
  kalshi:
    enabled: true
    poll_interval_seconds: 120    # 2 minutes

sports:
  - basketball_nba
  - americanfootball_nfl

markets:
  - h2h        # Moneyline
  - spreads    # Point spreads
  - totals     # Over/under

books:
  - draftkings
  - fanduel
  - betmgm
  - betrivers
  - betonlineag

arbitrage:
  min_edge_percent: 0.5       # Minimum edge to report
  max_data_age_seconds: 600   # Ignore stale data
  reference_bankroll: 100     # For stake calculations
```

## Database Schema

| Table | Purpose |
|-------|---------|
| `games` | Game/event metadata (teams, date, league) |
| `market_latest` | Current prices per game/market/side/provider |
| `market_history` | Time series of all price snapshots |
| `outcomes` | Actual results for performance metrics |
| `source_metadata` | Polling state and quota tracking |

## Arbitrage Detection

### How It Works

Arbitrage exists when complementary bets across different sources sum to < 100%:

```
Source A: Team wins @ 45% implied probability
Source B: Team loses @ 48% implied probability
Sum: 93% < 100% → 7% guaranteed profit margin
```

### Output Example

```
============================================================
[SPORTSBOOK] 2.34% MARGIN
============================================================
Game: 2026-02-10_basketball_nba_celtics_lakers
Market: h2h
Teams: Boston Celtics vs Los Angeles Lakers

Leg 1: home @ draftkings
  Probability: 52.3%
  Decimal Odds: 1.912
  Stake: $47.82

Leg 2: away @ fanduel
  Probability: 45.3%
  Decimal Odds: 2.207
  Stake: $52.18

💰 Guaranteed Profit: $2.34 on $100.00
```

## Data Sources

| Source | Type | Data |
|--------|------|------|
| Odds API | Sportsbook | Game odds from DraftKings, FanDuel, BetMGM, etc. |
| Polymarket | Open Market | Prediction market prices (futures) |
| Kalshi | Open Market | US-regulated prediction exchange |

## Cron Setup (Optional)

For automated polling without the daemon:

```bash
# Poll every 5 minutes
*/5 * * * * cd /path/to/project && .venv/bin/python poll_manager.py poll >> cron.log 2>&1
```

Or run the built-in daemon:

```bash
python main.py daemon
```

## API Usage

### Programmatic Access

```python
import sqlite3
from arbitrage import detect_all_arbitrage
from utils import init_db, load_config

config = load_config()
conn = init_db(config["storage"]["database"])

# Detect all arbitrage opportunities
results = detect_all_arbitrage(conn)

for category, opportunities in results.items():
    print(f"{category}: {len(opportunities)} opportunities")
    for arb in opportunities[:3]:
        print(f"  {arb['margin']:.2%} margin - {arb['game_id']}")

conn.close()
```

### Analysis Queries

```python
from analysis import (
    compare_books_to_open_markets,
    get_book_spread,
    get_price_history,
)

# Find edges between sportsbooks and open markets
edges = compare_books_to_open_markets(conn, min_edge=0.02)
for e in edges[:10]:
    print(f"{e['team']}: {e['bookmaker']} vs {e['open_market']} = {e['edge']:+.1%}")

# Spread analysis across bookmakers
spreads = get_book_spread(conn)
for s in spreads[:5]:
    print(f"{s['team']}: {s['spread']:.1%} spread across {s['num_books']} books")
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ODDS_API_KEY` | Yes | API key from [the-odds-api.com](https://the-odds-api.com) |

## Requirements

- Python 3.10+
- See `requirements.txt` for dependencies

## License

MIT
