# Agent Instructions

**Purpose**
Provide project-specific guidance so changes stay consistent with the repo’s architecture and workflow.

**Repository Guidelines**
See `CONTRIBUTING.md` for repo-wide standards and workflow expectations.

**Project Summary**
Python-based sports betting arbitrage/middle detection pipeline with an embedded insights generator (news scraping, NLP extraction, lag detection, and ML line-move prediction). Data is stored in SQLite for analysis and detection.

**Environment**
- Python 3.10+
- Virtualenv in `.venv`
- Core dependencies in `requirements.txt`
- Insights dependencies in `insights_generator/requirements.txt`

**Setup**
- Create venv: `python -m venv .venv && source .venv/bin/activate`
- Install core deps: `pip install -r requirements.txt`
- Install insights deps (if using insights_generator): `pip install -r insights_generator/requirements.txt`
- Configure API keys in `.env`

**Runtime Commands**
- Ingest Odds API: `python services/ingest_odds_api.py`
- Ingest Polymarket: `python services/ingest_polymarket.py`
- Ingest Kalshi: `python services/ingest_kalshi.py`
- Ingest STX: `python services/ingest_stx.py`
- Detect opportunities: `python services/detect_opportunities.py`
- Insights: scrape news `python -m insights_generator.cli scrape`
- Insights: analyze headlines `python -m insights_generator.cli analyze`
- Insights: detect lag `python -m insights_generator.cli detect-lag`
- Insights: event impacts `python -m insights_generator.cli event-impacts`
- Insights: train model `python -m insights_generator.cli train`
- Insights: predict `python -m insights_generator.cli predict`
- Insights: status `python -m insights_generator.cli status`
- Insights: init tables `python -m insights_generator.cli init-db`

**Agent Office (read this first every session)**
The `agent_office/` folder is the shared workspace for agent coordination. At the start of every session:
1. Read `agent_office/README.md` for the full protocol
2. Read `agent_office/task_board.md` — check if you have an assigned task from the orchestrator
3. Read `agent_office/dispatch_log.txt` for a quick overview of all work done on this repo
4. Check `agent_office/active_tasks/` for any currently claimed work — do not modify files another agent has claimed
5. Skim the most recent `.txt` plan history files in `agent_office/` for context on recent changes
6. Before starting work, create a claim file in `agent_office/active_tasks/<your-id>.md` listing what you're doing and which files you'll touch
7. When done: delete your claim, write a plan history `.txt`, append one line to `dispatch_log.txt`, and commit

**If you are a Worker (Alpha, Bravo, or Charlie):**
You are part of an orchestrated team. Read `agent_office/task_board.md` to find your assigned task. Execute ONLY that task within the listed file scope. Do NOT edit `task_board.md` — only the orchestrator writes there.

**Key Files**
- `schema.sql`: SQLite schema definition
- `utils.py`: DB init, upserts, history inserts, helper utilities
- `adapters/`: Ingestion adapter logic (Odds API, Polymarket, Kalshi, STX)
- `services/`: One-shot workers per source + detection daemon
- `payment_methods/`: Transaction and funding logic (trading, deposits/withdrawals)
- `services/detect_opportunities.py`: Arbitrage + middle detection algorithms and detector entrypoint
- `insights_generator/`: NLP analysis, ML pipeline, news scraping, lag detection
- `insights_generator/cli.py`: CLI for insights features (`scrape`, `analyze`, `detect-lag`, `event-impacts`, `train`, `predict`, `status`, `init-db`)
- `config.yaml`: Source/market/insights configuration

**Database Notes**
- SQLite database file: `odds.db`
- SQLite sidecar files (`odds.db-wal`, `odds.db-shm`) are normal and ignored by git
- Schema changes should be made in `schema.sql` (core) or `insights_generator/schema.sql` (insights). If you change schema, you may need to delete `odds.db*` to reinitialize via `utils.init_db`
- Core tables: `games`, `market_latest`, `market_history`, `outcomes`
- Insights tables: `news_headlines`, `structured_events`, `market_lag_signals`, `event_market_impacts`, `ml_predictions`

**Workflow Expectations**
- Prefer updating `schema.sql` and `utils.py` together when database behavior changes
- Avoid committing secrets in `.env`
- When adding new sources or markets, update `config.yaml` and add a new module under `adapters/` plus a worker in `services/`

**Testing**
- No automated test suite is defined. Validate changes by running one of the workers or the detector.
