"""Main entry point."""
import sqlite3
from ingest import ingest
from analysis import compare_books_to_polymarket, get_book_spread


def main():
    print("=" * 65)
    print("ODDS INGESTION")
    print("=" * 65)
    ingest()
    
    conn = sqlite3.connect("odds.db")
    
    print("\n" + "=" * 65)
    print("BOOKMAKER SPREAD (variance across books)")
    print("=" * 65)
    for row in get_book_spread(conn)[:5]:
        print(f"{row['team'][:22]:22} Min:{row['min_prob']:5.1%} Max:{row['max_prob']:5.1%} "
              f"Spread:{row['spread']:5.1%} ({row['num_books']} books)")
    
    print("\n" + "=" * 65)
    print("TOP EDGES vs POLYMARKET")
    print("=" * 65)
    for row in compare_books_to_polymarket(conn)[:10]:
        print(f"{row['team'][:18]:18} {row['bookmaker']:12} "
              f"Book:{row['book_prob']:5.1%} PM:{row['polymarket_prob']:5.1%} Edge:{row['edge']:+5.1%}")
    
    conn.close()


if __name__ == "__main__":
    main()
