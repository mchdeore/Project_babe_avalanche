"""Run both Odds API and Polymarket ingests."""
from ingest_odds_api import ingest as ingest_odds
from ingest_polymarket_api import ingest as ingest_polymarket


def main() -> None:
    print("=== Odds API ===")
    ingest_odds()
    print("\n=== Polymarket ===")
    ingest_polymarket()


if __name__ == "__main__":
    main()
