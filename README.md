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

## References
- Odds API: https://the-odds-api.com/#get-access
- Twilio SMS pricing: https://www.twilio.com/en-us/sms/pricing/us
- Project tracker: https://docs.google.com/spreadsheets/d/19c7a3KguasrOAuzVB835iVoJ_xeXy9YuxIv_kUpT1-A/edit?usp=sharing
