"""Polymarket data ingestion service.

Supports both one-shot and daemon modes:
    python services/ingest_polymarket.py           # Run once
    python services/ingest_polymarket.py --daemon  # Run continuously
    python services/ingest_polymarket.py --daemon --interval 60
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters import adapter_polymarket as polymarket
from adapters.adapter_common import apply_devig, save_to_db
from utils import init_db, load_config

DEFAULT_INTERVAL = 60  # seconds


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


def run_once() -> tuple[int, int]:
    """Run a single ingestion cycle.
    
    Returns:
        Tuple of (games_count, rows_count)
    """
    load_dotenv()
    config = load_config()
    conn = init_db(config["storage"]["database"])

    existing_games = _load_existing_games(conn)

    with requests.Session() as session:
        games, rows = polymarket.fetch(session, config, existing_games)

    rows = apply_devig(rows)
    save_to_db(conn, games, rows)
    conn.close()

    return len(games), len(rows)


def run_daemon(interval: int = DEFAULT_INTERVAL) -> None:
    """Run ingestion continuously at specified interval.
    
    Args:
        interval: Seconds between ingestion cycles
    """
    print(f"[polymarket] Starting daemon mode (interval={interval}s)")
    
    while True:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            games, rows = run_once()
            print(f"[{timestamp}] polymarket: games={games} rows={rows}")
        except KeyboardInterrupt:
            print("\n[polymarket] Shutting down...")
            break
        except Exception as e:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] polymarket ERROR: {e}")
        
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket data ingestion")
    parser.add_argument(
        "--daemon", 
        action="store_true", 
        help="Run continuously instead of once"
    )
    parser.add_argument(
        "--interval", 
        type=int, 
        default=DEFAULT_INTERVAL,
        help=f"Seconds between polls in daemon mode (default: {DEFAULT_INTERVAL})"
    )
    args = parser.parse_args()

    if args.daemon:
        run_daemon(args.interval)
    else:
        games, rows = run_once()
        print(f"polymarket: games={games} rows={rows}")


if __name__ == "__main__":
    main()
