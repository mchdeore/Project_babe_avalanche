# Project_babe_avalanche

Lightweight odds ingestion using The Odds API and SQLite.

## Quick start
1) Activate the virtual environment:
   - `source .venv/bin/activate`
2) Run the ingest script:
   - `python ingest_data.py`

## Configuration
Edit `config.yaml` to control:
- `sports` (Odds API sport keys)
- `markets` and `regions`
- database path

Set your API key in `.env`:
```
ODDS_API_KEY=your_key_here
```

## How data is stored
The ingestion process flattens the API response from:

```
sport → game → bookmaker → market → outcome
```

into atomic bet records (one outcome × one market × one sportsbook × one event).

Data is stored in three tables:
- **bets**: identity table (one row per logical bet). Inserted once and reused.
- **current_odds**: snapshot table (one row per bet × sportsbook for the latest run only).
- **odds_timeseries**: history table (one row per bet per run, storing best odds across books).

### Flattening helper
The function `iter_atomic_records(...)` walks the nested response and yields:
- a **bet row** for the `bets` table
- an **odds row** for the `current_odds` table

These rows are inserted in batches with `executemany` for cleaner and faster writes.

## References
- Odds API: https://the-odds-api.com/#get-access
- Twilio SMS pricing: https://www.twilio.com/en-us/sms/pricing/us
- Project tracker: https://docs.google.com/spreadsheets/d/19c7a3KguasrOAuzVB835iVoJ_xeXy9YuxIv_kUpT1-A/edit?usp=sharing
