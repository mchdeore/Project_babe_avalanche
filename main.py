"""
Main Entry Point
================
Run ingestion and display analysis.
"""
import sqlite3

from ingest import ingest
from analysis import (
    compare_books_to_open_markets,
    get_all_sources,
    get_book_spread,
    SPORTSBOOKS,
)


def main():
    # Run ingestion
    print("=" * 70)
    print("ODDS INGESTION")
    print("=" * 70)
    ingest()

    # Connect to database
    conn = sqlite3.connect("odds.db")

    # Show data sources
    print("\n" + "=" * 70)
    print("DATA SOURCES")
    print("=" * 70)
    for source, count in get_all_sources(conn).items():
        market_type = "SPORTSBOOK" if source in SPORTSBOOKS else "OPEN MARKET"
        print(f"  {source:15} ({market_type}): {count} rows")

    # Show bookmaker spread
    print("\n" + "=" * 70)
    print("BOOKMAKER SPREAD")
    print("=" * 70)
    for row in get_book_spread(conn)[:5]:
        print(
            f"{row['team'][:22]:22} "
            f"Min:{row['min_prob']:5.1%} "
            f"Max:{row['max_prob']:5.1%} "
            f"Spread:{row['spread']:5.1%}"
        )

    # Show top edges
    print("\n" + "=" * 70)
    print("TOP EDGES: SPORTSBOOKS vs OPEN MARKETS")
    print("=" * 70)
    for row in compare_books_to_open_markets(conn)[:10]:
        print(
            f"{row['team'][:16]:16} "
            f"{row['bookmaker']:12} vs {row['open_market']:12} "
            f"Book:{row['book_prob']:5.1%} "
            f"Open:{row['open_market_prob']:5.1%} "
            f"Edge:{row['edge']:+5.1%}"
        )

    conn.close()


if __name__ == "__main__":
    main()
