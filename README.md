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

Manual trading helpers live in `payment_methods/` (callable from your own scripts).

## Insights Generator

On-demand insights tooling lives in `insights_generator/`:

| Command | Description |
|---------|-------------|
| `python -m insights_generator.cli scrape` | Fetch news headlines (RSS + API sources) |
| `python -m insights_generator.cli analyze` | Process headlines with Ollama NLP |
| `python -m insights_generator.cli detect-lag` | Detect lead/lag signals |
| `python -m insights_generator.cli event-impacts` | Compute event → market impact metrics |
| `python -m insights_generator.cli train` | Train ML model |
| `python -m insights_generator.cli predict` | Run ML predictions |
| `python -m insights_generator.cli status` | Show data summary |
| `python -m insights_generator.cli init-db` | Initialize insights tables |

Alias maps for teams/providers/markets live in `data/aliases/`. ESPN roster caches are written to `insights_generator/cache/` (gitignored).

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ODDS_API_KEY` | Yes* | API key from the Odds API |
| `STX_EMAIL` | No | STX account email (required if STX is enabled) |
| `STX_PASSWORD` | No | STX account password (required if STX is enabled) |
| `STX_DEVICE_ID` | No | Device identifier for STX login (defaults to a generated UUID) |
| `STX_GRAPHQL_URL` | No | Override STX GraphQL URL (default: `https://api.stx.ca/graphql`) |
| `STX_GEO_CODE` | No | GeoLocationCode for order confirmation |
| `POLY_HOST` | No | Polymarket CLOB host (default: `https://clob.polymarket.com`) |
| `POLY_CHAIN_ID` | No | Polymarket chain ID (default: `137`) |
| `POLY_PRIVATE_KEY` | No | Polymarket L1 wallet private key |
| `POLY_API_KEY` | No | Polymarket L2 API key |
| `POLY_API_SECRET` | No | Polymarket L2 API secret |
| `POLY_API_PASSPHRASE` | No | Polymarket L2 API passphrase |
| `POLY_FUNDER` | No | Polymarket funder address (optional) |
| `POLY_SIGNATURE_TYPE` | No | Polymarket signature type (default: `0`) |
| `KALSHI_API_KEY_ID` | No | Kalshi API key ID |
| `KALSHI_PRIVATE_KEY_PEM` | No | Kalshi private key PEM (inline) |
| `KALSHI_PRIVATE_KEY_PATH` | No | Kalshi private key PEM file path |
| `KALSHI_BASE_URL` | No | Kalshi base URL (default: `https://api.elections.kalshi.com/trade-api/v2`) |

*At least one data source credential is required for ingestion. Trading requires provider-specific credentials.

## Data Sources

| Source | Type | Data |
|--------|------|------|
| Odds API | Sportsbook | Regulated US sportsbooks |
| Polymarket | Open Market | Prediction market prices |
| Kalshi | Open Market | US-regulated prediction exchange |
| STX | Open Market | Canadian regulated sports exchange |

## Config

Edit `config.yaml` to control sources, sports, markets, and fees. The `insights_generator` section configures RSS/API sources, NLP/ML settings, and event impact windows.
