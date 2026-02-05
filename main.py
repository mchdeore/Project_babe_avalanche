"""Main entry point."""
from ingest import ingest
from analysis import get_futures_comparison
import sqlite3

def main():
    print("=" * 60)
    print("ODDS INGESTION")
    print("=" * 60)
    ingest()
    
    print("\n" + "=" * 60)
    print("TOP DISCREPANCIES (Sportsbooks vs Polymarket)")
    print("=" * 60)
    conn = sqlite3.connect("odds.db")
    for row in get_futures_comparison(conn)[:10]:
        diff = row["polymarket_prob"] - row["sportsbook_prob"]
        print(f"{row['team'][:22]:22} SB:{row['sportsbook_prob']:5.1%} PM:{row['polymarket_prob']:5.1%} Δ:{diff:+5.1%}")
    conn.close()

if __name__ == "__main__":
    main()
