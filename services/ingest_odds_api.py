"""One-shot ingestion for Odds API."""
from __future__ import annotations

import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters import adapter_odds_api as odds_api
from adapters.adapter_common import apply_canonicalization, apply_devig, save_to_db
from utils import init_db, load_config


def run() -> None:
    load_dotenv()
    config = load_config()
    conn = init_db(config["storage"]["database"])

    with requests.Session() as session:
        games, rows = odds_api.fetch(session, config)

    rows = apply_canonicalization(rows)
    rows = apply_devig(rows)
    save_to_db(conn, games, rows)
    conn.close()

    print(f"odds_api: games={len(games)} rows={len(rows)}")


if __name__ == "__main__":
    run()
