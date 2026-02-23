"""One-shot ingestion for Polymarket."""
from __future__ import annotations

import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sources import adapter_polymarket as polymarket
from sources.adapter_common import apply_devig, save_to_db
from utils import init_db, load_config


def _load_existing_games(conn):
    cursor = conn.execute(
        "SELECT game_id, league, commence_time, home_team, away_team FROM games"
    )
    return {
        row[0]: {
            "game_id": row[0],
            "league": row[1],
            "commence_time": row[2],
            "home_team": row[3],
            "away_team": row[4],
        }
        for row in cursor
    }


def run() -> None:
    load_dotenv()
    config = load_config()
    conn = init_db(config["storage"]["database"])

    existing_games = _load_existing_games(conn)

    with requests.Session() as session:
        games, rows = polymarket.fetch(session, config, existing_games)

    rows = apply_devig(rows)
    save_to_db(conn, games, rows)
    conn.close()

    print(f"polymarket: games={len(games)} rows={len(rows)}")


if __name__ == "__main__":
    run()
