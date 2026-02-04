from __future__ import annotations

"""Run both Odds API and Polymarket ingests in a single pass."""

from Ingest_odds_api import ingest as ingest_odds
from ingest_polymarket_api import ingest as ingest_polymarket


def main() -> None:
    """Run Odds API and Polymarket ingests sequentially."""
    print("=== Odds API ingest ===")
    ingest_odds()
    print("=== Polymarket ingest ===")
    ingest_polymarket()


if __name__ == "__main__":
    main()
