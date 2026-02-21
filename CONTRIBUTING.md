# Contributing Guidelines

**Purpose**
This document defines the contribution standards for the Arbitrage Detection System. Follow it so the codebase remains consistent, reliable, and easy to extend.

**Project Summary**
Python-based sports betting arbitrage/middle detection pipeline that ingests odds from multiple sources and stores them in a SQLite database for analysis and detection.

**Repository Layout**
- `main.py` CLI entry point and orchestration.
- `ingest.py` data fetching and normalization.
- `arbitrage.py` arbitrage detection algorithms.
- `middles.py` middle detection algorithms.
- `analysis.py` analytics and reporting queries.
- `poll_manager.py` polling orchestration and quota tracking.
- `utils.py` shared helpers, DB utilities, normalization, probability.
- `services/` worker services for continuous polling.
- `schema.sql` database schema and indices.
- `config.yaml` source/market configuration.

**Environment**
- Python 3.10+.
- Virtual environment at `.venv`.
- Dependencies in `requirements.txt`.

**Setup**
- Create venv: `python -m venv .venv && source .venv/bin/activate`.
- Install deps: `pip install -r requirements.txt`.
- Configure API keys in `.env`.

**Coding Standards**
- Use type annotations for all new public functions and any non-trivial internal functions.
- Every new module must have a module docstring.
- Public functions must have docstrings with `Args`, `Returns`, and at least one `Example` for non-trivial logic.
- Inline comments are required only for non-obvious logic; avoid restating code.
- Prefer `logging` over `print` except in CLI/reporting output.
- Do not use bare `except`; catch specific exceptions and include context in logs.
- Use `requests.Session` for HTTP calls and always set a timeout.
- Reuse shared helpers in `utils.py` (normalization, DB, time parsing) instead of re-implementing.

**Database Rules**
- Use `utils.init_db` for all DB connections so schema and pragmas are applied.
- Use `upsert_rows` for `games` and `market_latest` and `insert_history` for `market_history`.
- Update `schema.sql` for any schema change. If the change is not backward-compatible, document the migration in the PR and expect a DB reinitialize via `odds.db*` removal.
- Keep `source`, `provider`, `market`, `side`, and `line` consistent across ingestion paths.

**Adding a New Data Source**
- Add source config to `config.yaml` with polling settings and enable flags.
- Implement ingestion in `ingest.py` or a dedicated worker under `services/`.
- Normalize teams/players via `utils.normalize_team` and `utils.normalize_player`.
- Update `source_metadata` via `update_source_metadata` with explicit keyword args.
- Add any new fields to `schema.sql` and update `README.md` if needed.

**Operational Safety**
- Do not commit secrets. `.env` must remain local.
- `odds.db`, `odds.db-wal`, and `odds.db-shm` are local artifacts and should not be committed.
- Keep API rate limits in mind; respect per-source delays in `config.yaml`.

**Validation**
- No formal test suite exists. Validate changes by running:
- `python main.py status` for DB health.
- `python main.py ingest` for ingestion changes.
- `python main.py detect` for detection changes.
- `python analysis.py` for analytics changes.

**Code Review Checklist**
- New code includes type annotations and docstrings.
- Logs are meaningful and include enough context for debugging.
- Data normalization is consistent with existing helpers.
- Schema updates are reflected in `schema.sql`.
- No secrets or local DB files are added to git.
