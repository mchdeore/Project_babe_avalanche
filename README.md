# Arbitrage Detection System

Sports betting arbitrage and middle detection across sportsbooks and open markets.

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

echo "ODDS_API_KEY=your_key_here" > .env
# Optional STX credentials (required if STX is enabled)
echo "STX_EMAIL=your_email@example.com" >> .env
echo "STX_PASSWORD=your_password" >> .env

python services/ingest_odds_api.py
python services/ingest_polymarket.py
python services/ingest_kalshi.py
python services/ingest_stx.py
python services/detect_opportunities.py
```

## Commands

| Command | Description |
|---------|-------------|
| `python services/ingest_odds_api.py` | Ingest Odds API (sportsbooks) |
| `python services/ingest_polymarket.py` | Ingest Polymarket |
| `python services/ingest_kalshi.py` | Ingest Kalshi |
| `python services/ingest_stx.py` | Ingest STX |
| `python services/detect_opportunities.py` | Detect arbitrage + middles |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ODDS_API_KEY` | Yes* | API key from the Odds API |
| `STX_EMAIL` | No | STX account email (required if STX is enabled) |
| `STX_PASSWORD` | No | STX account password (required if STX is enabled) |
| `STX_DEVICE_ID` | No | Device identifier for STX login (defaults to a generated UUID) |
| `STX_GRAPHQL_URL` | No | Override STX GraphQL URL (default: `https://api.stx.ca/graphql`) |

*At least one data source credential is required. Polymarket and Kalshi do not require API keys.

## Data Sources

| Source | Type | Data |
|--------|------|------|
| Odds API | Sportsbook | Regulated US sportsbooks |
| Polymarket | Open Market | Prediction market prices |
| Kalshi | Open Market | US-regulated prediction exchange |
| STX | Open Market | Canadian regulated sports exchange |

## Config

Edit `config.yaml` to control sources, sports, markets, and fees.
