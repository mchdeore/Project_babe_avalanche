"""One-shot ingestion for STX."""
from __future__ import annotations

import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters import adapter_stx as stx
from adapters.adapter_common import apply_devig, save_to_db
from utils import init_db, load_config


def run() -> None:
    load_dotenv()
    config = load_config()
    conn = init_db(config["storage"]["database"])

    with requests.Session() as session:
        games, rows = stx.fetch(session, config)

    rows = apply_devig(rows)
    save_to_db(conn, games, rows)
    conn.close()

    print(f"stx: games={len(games)} rows={len(rows)}")


if __name__ == "__main__":
    run()
