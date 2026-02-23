# Contributing Guidelines

**Purpose**
Standards for keeping the codebase consistent, reliable, and easy to extend.

**Project Summary**
Python-based sports betting arbitrage/middle detection pipeline that ingests odds from multiple sources and stores them in a SQLite database.

**Repository Layout**
- `services/` one-shot workers per source + detection
- `sources/` ingestion adapter logic
- `services/detect_opportunities.py` arbitrage + middle detection algorithms and detector entrypoint
- `utils.py` shared helpers, DB utilities, normalization
- `schema.sql` database schema
- `config.yaml` source/market configuration

**Environment**
- Python 3.10+
- Virtual environment at `.venv`
- Dependencies in `requirements.txt`

**Setup**
- Create venv: `python -m venv .venv && source .venv/bin/activate`
- Install deps: `pip install -r requirements.txt`
- Configure API keys in `.env`

**Coding Standards**
- Use type annotations for all new public functions and any non-trivial internal functions
- Every new module must have a module docstring
- Public functions must have docstrings with `Args`, `Returns`, and an `Example` for non-trivial logic
- Inline comments only for non-obvious logic
- Do not use bare `except`; catch specific exceptions and include context where errors are handled
- Use `requests.Session` for HTTP calls and always set a timeout
- Reuse shared helpers in `utils.py` (normalization, DB, time parsing)

**Database Rules**
- Use `utils.init_db` for all DB connections
- Use `upsert_rows` for `games` and `market_latest`, `insert_history` for `market_history`
- Update `schema.sql` for any schema change
- Keep `source`, `provider`, `market`, `side`, and `line` consistent across ingestion paths

**Adding a New Data Source**
- Add source config to `config.yaml`
- Implement ingestion in `sources/` and add a worker in `services/`
- Normalize teams/players via `utils.normalize_team` and `utils.normalize_player`

**Operational Safety**
- Do not commit secrets. `.env` must remain local
- `odds.db`, `odds.db-wal`, and `odds.db-shm` are local artifacts and should not be committed

**Validation**
- No formal test suite exists. Validate changes by running workers or the detector.
