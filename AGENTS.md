# Agent Instructions

**Purpose**
Provide project-specific guidance so changes stay consistent with the repo’s architecture and workflow.

**Repository Guidelines**
See `CONTRIBUTING.md` for repo-wide standards and workflow expectations.

**Project Summary**
Python-based sports betting arbitrage/middle detection pipeline that ingests odds from multiple sources and stores them in a SQLite database for analysis and detection.

**Environment**
- Python 3.10+
- Virtualenv in `.venv`
- Dependencies in `requirements.txt`

**Setup**
- Create venv: `python -m venv .venv && source .venv/bin/activate`
- Install deps: `pip install -r requirements.txt`
- Configure API keys in `.env`

**Runtime Commands**
- Ingest Odds API: `python services/ingest_odds_api.py`
- Ingest Polymarket: `python services/ingest_polymarket.py`
- Ingest Kalshi: `python services/ingest_kalshi.py`
- Ingest STX: `python services/ingest_stx.py`
- Detect opportunities: `python services/detect_opportunities.py`

**Key Files**
- `schema.sql`: SQLite schema definition
- `utils.py`: DB init, upserts, history inserts, helper utilities
- `sources/`: Source ingestion logic
- `services/`: One-shot workers per source + detection
- `arbitrage.py`: Arbitrage detection algorithms
- `middles.py`: Middle detection algorithms
- `config.yaml`: Source/market configuration

**Database Notes**
- SQLite database file: `odds.db`
- SQLite sidecar files (`odds.db-wal`, `odds.db-shm`) are normal and ignored by git
- Schema changes should be made in `schema.sql`. If you change schema, you may need to delete `odds.db*` to reinitialize via `utils.init_db`
- Core tables: `games`, `market_latest`, `market_history`, `outcomes`

**Workflow Expectations**
- Prefer updating `schema.sql` and `utils.py` together when database behavior changes
- Avoid committing secrets in `.env`
- When adding new sources or markets, update `config.yaml` and add a new module under `sources/`

**Testing**
- No automated test suite is defined. Validate changes by running one of the workers or the detector.
