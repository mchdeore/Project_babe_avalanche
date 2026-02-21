# Agent Instructions

**Purpose**
Provide project-specific guidance for agents working on this repo so changes stay consistent with the system’s architecture and workflow.

**Project Summary**
Python-based sports betting arbitrage/middle detection pipeline that ingests odds from multiple sources and stores them in a SQLite database for analysis and detection.

**Environment**
- Python 3.10+.
- Virtualenv lives in `.venv`.
- Dependencies in `requirements.txt`.

**Setup**
- Create venv: `python -m venv .venv && source .venv/bin/activate`.
- Install deps: `pip install -r requirements.txt`.
- Configure API keys in `.env`.

**Runtime Commands**
- Full pipeline: `python main.py`.
- Ingest only: `python main.py ingest`.
- Detect only: `python main.py detect`.
- Status: `python main.py status`.
- Daemon: `python main.py daemon`.

**Key Files**
- `schema.sql`: SQLite schema definition.
- `utils.py`: DB init, upserts, history inserts, helper utilities.
- `ingest.py`: Data fetch from external sources.
- `arbitrage.py`: Arbitrage detection algorithms.
- `middles.py`: Middle detection algorithms.
- `analysis.py`: Reporting and analytics queries.
- `config.yaml`: Source/market configuration.

**Database Notes**
- SQLite database file: `odds.db`.
- SQLite sidecar files (`odds.db-wal`, `odds.db-shm`) are normal and ignored by git.
- Schema changes should be made in `schema.sql`. If you change schema, you may need to delete `odds.db*` to reinitialize via `utils.init_db`.
- Core tables: `games`, `market_latest`, `market_history`, `outcomes`, `source_metadata`.

**Workflow Expectations**
- Prefer updating `schema.sql` and `utils.py` together when database behavior changes.
- Avoid committing secrets in `.env`.
- When adding new sources or markets, update `config.yaml` and ingestion logic in `ingest.py` or worker scripts under `services/`.

**Testing**
- No automated test suite is defined. If you need validation, run `python main.py status` or a targeted script that exercises the change.
