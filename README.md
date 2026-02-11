# Arbitrage & Middle Bet Detection System

A modular sports betting analysis platform that detects arbitrage and middle bet opportunities across sportsbooks and prediction markets.

## System Overview

This system compares odds/prices across three source categories:
1. **Sportsbooks** (via Odds API): DraftKings, FanDuel, BetMGM, etc.
2. **Open Markets** (direct API): Polymarket, Kalshi

### What It Detects

**Arbitrage** (Risk-Free Profit):
- When complementary bets across sources sum to <100% probability
- Example: Team A wins at 45% (Polymarket) + Team A loses at 48% (Kalshi) = 93% → 7% guaranteed profit

**Middles** (Conditional Profit):
- When spread/total lines differ across sources, creating a "middle" window
- Example: Team A -3.5 (DraftKings) vs Team B +5.5 (FanDuel) → If Team A wins by 4-5, both bets win

## Features

### Detection Algorithms
| Type | Description | Module |
|------|-------------|--------|
| Sportsbook Arbitrage | Between bookmakers | `arbitrage.py` |
| Open Market Arbitrage | Between Polymarket/Kalshi | `arbitrage.py` |
| Cross-Market Arbitrage | Sportsbooks vs open markets | `arbitrage.py` |
| Sportsbook Middles | Different spread lines between books | `middles.py` |
| Open Market Middles | Different lines on Polymarket/Kalshi | `middles.py` |
| Cross-Market Middles | Sportsbooks vs open markets | `middles.py` |
| Player Prop Middles | Same player, different O/U lines | `middles.py` |

### Data Sources

| Source | Category | Markets | Rate Limit |
|--------|----------|---------|------------|
| Odds API | Sportsbook | h2h, spreads, totals, futures, player props | 500 calls/month (free) |
| Polymarket | Open Market | h2h, spreads, totals, player props | Unlimited |
| Kalshi | Open Market | h2h, spreads, totals, player props | Unlimited |

### Market Types Captured
- `h2h` - Moneyline (who wins)
- `spreads` - Point spread betting
- `totals` - Over/under total points
- `futures` - Championship/season outcomes
- `player_points` - Player points over/under
- `player_rebounds` - Player rebounds over/under
- `player_assists` - Player assists over/under
- `player_threes` - Player 3-pointers over/under

## Quick Start

```bash
# 1. Setup virtual environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure API key (optional - needed for sportsbook odds)
echo "ODDS_API_KEY=your_key_here" > .env

# 3. Run full pipeline (ingest + detect)
python main.py
```

## CLI Commands

### Core Operations
| Command | Description |
|---------|-------------|
| `python main.py` | Full pipeline: ingest data + detect arbitrage |
| `python main.py ingest` | Ingest data from all sources |
| `python main.py detect` | Detect arbitrage (using existing data) |
| `python main.py status` | Show database stats and polling status |
| `python main.py daemon` | Run continuous polling daemon |

### Arbitrage Detection
```bash
python main.py open        # Open market arbitrage only (Polymarket vs Kalshi)
python main.py sportsbook  # Sportsbook arbitrage only (between bookmakers)
python main.py cross       # Cross-market arbitrage (sportsbooks vs open markets)
```

### Middle Bet Detection
```bash
python main.py middle          # All middle opportunities
python main.py middle-sb       # Sportsbook middles only
python main.py middle-open     # Open market middles only
python main.py middle-cross    # Cross-market middles
python main.py middle-props    # Player prop middles only
```

### Standalone Module Execution
```bash
python arbitrage.py              # Run all arbitrage detection
python arbitrage.py sportsbook   # Sportsbook arbs only
python middles.py                # Run all middle detection
python middles.py props          # Player prop middles only
python poll_manager.py status    # Polling status
python poll_manager.py daemon    # Start polling daemon
python analysis.py               # Run analysis report
```

## Project Architecture

### File Structure
```
Project_babe_avalanche/
├── main.py           # CLI entry point - orchestrates all operations
├── ingest.py         # Data fetching from Odds API, Polymarket, Kalshi
├── arbitrage.py      # Three arbitrage detection algorithms
├── middles.py        # Four middle bet detection algorithms
├── poll_manager.py   # Per-source polling scheduler with quota tracking
├── analysis.py       # Market comparison and performance metrics
├── utils.py          # Shared utilities (DB, normalization, probability)
├── config.yaml       # Configuration for sources, sports, books, detection
├── schema.sql        # SQLite schema with optimized indices
├── requirements.txt  # Python dependencies
├── odds.db           # SQLite database (generated)
└── .env              # API keys (not committed)
```

### Module Responsibilities

| Module | Purpose | Key Functions |
|--------|---------|---------------|
| `main.py` | CLI entry point | `run_full_pipeline()`, `main()` |
| `ingest.py` | Data fetching | `ingest()`, `fetch_odds_api_games()`, `fetch_polymarket()`, `fetch_kalshi()` |
| `arbitrage.py` | Arbitrage detection | `detect_open_market_arbitrage()`, `detect_sportsbook_arbitrage()`, `detect_cross_market_arbitrage()` |
| `middles.py` | Middle detection | `detect_sportsbook_middles()`, `detect_open_market_middles()`, `detect_cross_market_middles()`, `detect_player_prop_middles()` |
| `poll_manager.py` | Polling scheduler | `should_poll()`, `run_poll_cycle()`, `run_daemon()` |
| `analysis.py` | Analysis utilities | `get_all_sources()`, `get_book_spread()`, `brier_score()` |
| `utils.py` | Shared utilities | `init_db()`, `load_config()`, `normalize_team()`, `odds_to_prob()`, `devig()` |

### Data Flow
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Odds API   │    │ Polymarket  │    │   Kalshi    │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └─────────────┬────┴──────────────────┘
                     │
              ┌──────▼──────┐
              │  ingest.py  │  ← Fetches, normalizes, de-vigs
              └──────┬──────┘
                     │
              ┌──────▼──────┐
              │  odds.db    │  ← SQLite storage
              └──────┬──────┘
                     │
       ┌─────────────┼─────────────┐
       │             │             │
┌──────▼──────┐ ┌────▼────┐ ┌──────▼──────┐
│ arbitrage.py│ │middles.py│ │ analysis.py │
└─────────────┘ └──────────┘ └─────────────┘
```

## Database Schema

### Tables

| Table | Purpose | Primary Key |
|-------|---------|-------------|
| `games` | Game/event metadata | `game_id` |
| `market_latest` | Current prices per market/source | `(game_id, market, side, line, source, provider, player)` |
| `market_history` | Historical price snapshots | `id` (auto-increment) |
| `outcomes` | Actual game results | `game_id` |
| `source_metadata` | Polling state per source | `source_name` |

### Key Fields in market_latest
| Field | Description | Example |
|-------|-------------|---------|
| `game_id` | Canonical game ID | `2026-02-11_basketball_nba_cavaliers_wizards` |
| `market` | Market type | `h2h`, `spreads`, `totals`, `player_points` |
| `side` | Position | `home`, `away`, `over`, `under` |
| `line` | Point line | `-3.5`, `218.5`, `27.5` |
| `source` | Data source | `odds_api`, `polymarket`, `kalshi` |
| `provider` | Specific book | `draftkings`, `fanduel`, `polymarket` |
| `player` | Player name (props) | `lebronjames`, `stephencurry` |
| `implied_prob` | Raw probability | `0.55` |
| `devigged_prob` | Fair probability | `0.50` |

## Configuration (config.yaml)

### Key Settings

```yaml
# Source polling rates
sources:
  odds_api:
    poll_interval_seconds: 300   # 5 minutes
    monthly_quota: 500           # Free tier API calls
  polymarket:
    poll_interval_seconds: 60    # 1 minute (faster for open markets)
  kalshi:
    poll_interval_seconds: 120   # 2 minutes

# Detection thresholds
arbitrage:
  min_edge_percent: 0.5          # Report opportunities > 0.5% edge
  max_data_age_seconds: 600      # Ignore data older than 10 minutes
  fees:
    polymarket: 0.02             # 2% trading fee
    kalshi: 0.01                 # 1% fee

# Middle detection
middles:
  min_gap_points: 1.0            # Minimum spread gap
  min_gap_total: 2.0             # Minimum total gap
```

## Probability & De-Vigging

### Sportsbooks
Sportsbooks add "vig" (margin) to odds. A fair coin flip (50/50) might be priced as:
- DraftKings: 52.5% / 52.5% (sum = 105%, vig = 5%)

We apply **multiplicative de-vigging** to normalize:
```python
devigged_prob = implied_prob / sum(all_implied_probs)
# 52.5 / 105 = 50%
```

### Open Markets (Polymarket/Kalshi)
Prices ARE fair probabilities (no vig). We use them directly.

## How Arbitrage Detection Works

1. **Query matching markets** - Find same game/market across different sources
2. **Filter stale data** - Ignore prices older than `max_data_age_seconds`
3. **Calculate margin** - `margin = 1 - (prob_a + prob_b)`
4. **If margin > 0** - Arbitrage exists
5. **Calculate stakes** - Distribute bankroll to guarantee equal profit
6. **Apply fees** - Subtract platform fees from gross profit

### Example
```
Game: Lakers vs Celtics
Source A (Polymarket): Lakers win = 45%
Source B (Kalshi): Lakers lose = 48%
Sum: 93%
Margin: 7%

Stake $100 total:
  - $48.39 on Lakers (Polymarket)
  - $51.61 on Celtics (Kalshi)

If Lakers win: Win $107.53, Lose $51.61 → Net: $55.92 → Profit: $5.53
If Celtics win: Lose $48.39, Win $107.52 → Net: $59.13 → Profit: $5.53
```

## How Middle Detection Works

1. **Query spread/total markets** - Find same game with different lines
2. **Calculate gap** - `gap = abs(line_a - line_b)`
3. **If gap >= min_gap** - Middle exists
4. **Estimate probability** - Use normal distribution based on historical variance
5. **Calculate EV** - Expected value based on middle probability

### Example
```
Game: Lakers vs Celtics
DraftKings: Lakers -3.5 (52% to cover)
FanDuel: Celtics +5.5 (52% to cover)

Gap: 2 points (scores of -4, -5 by Lakers)
Middle probability: ~8% (based on score variance)

Bet $50 each side:
  - If Lakers win by 1-3: Win Celtics bet, lose Lakers bet (small loss)
  - If Lakers win by 6+: Win Lakers bet, lose Celtics bet (small loss)
  - If Lakers win by 4-5: WIN BOTH BETS! (~$100 profit)
```

## Cron/Daemon Setup

### Option 1: System Cron
```bash
# Add to crontab (crontab -e)
*/5 * * * * cd /path/to/project && .venv/bin/python main.py poll >> cron.log 2>&1
```

### Option 2: Continuous Daemon
```bash
# Run in background with nohup
nohup python main.py daemon > daemon.log 2>&1 &

# Or use screen/tmux
screen -S arb
python main.py daemon
# Ctrl+A, D to detach
```

## API Usage Notes

### Odds API (odds-api.com)
- Free tier: 500 calls/month
- Each sport/market combo = 1 call
- 2 sports × 3 markets = 6 calls per poll
- ~83 polls/month on free tier (~2.7/day)

### Polymarket (gamma-api.polymarket.com)
- No hard rate limit
- Game events: `GET /events?slug={sport}-{away}-{home}-{date}`
- Futures: `GET /markets?closed=false`

### Kalshi (api.elections.kalshi.com)
- No monthly limit
- Markets available via parlay leg tickers
- Direct market fetch: `GET /markets/{ticker}`

## Dependencies

```
requests>=2.25.0      # HTTP client for API calls
pyyaml>=6.0           # YAML config parsing
python-dotenv>=0.19   # Environment variable loading
urllib3<2             # HTTP library (version lock for compatibility)
scipy                 # Statistical functions (middle probability)
```

## Troubleshooting

### Database Corrupted
The system auto-recovers from corruption by recreating the database:
```python
# In utils.py init_db()
if "malformed" in str(e).lower():
    os.remove(db_path)
    return connect_and_init()
```

### No Opportunities Found
1. Check data freshness: `python main.py status`
2. Verify API key: `echo $ODDS_API_KEY`
3. Run fresh ingest: `python main.py ingest`
4. Lower threshold: Edit `config.yaml` → `min_edge_percent: 0.1`

### API Rate Limited
Adjust `config.yaml`:
```yaml
sources:
  odds_api:
    poll_interval_seconds: 600  # Increase to 10 minutes
```

## License

MIT License - See LICENSE file for details.
