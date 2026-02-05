"""Analysis utilities for comparing markets across bookmakers."""
from __future__ import annotations
import math
import sqlite3
from utils import utc_now_iso


def get_all_books(conn: sqlite3.Connection) -> list[str]:
    """Get list of all bookmakers in the database."""
    cursor = conn.execute("SELECT DISTINCT provider FROM market_latest WHERE source = 'odds_api' ORDER BY provider")
    return [row[0] for row in cursor.fetchall()]


def compare_books_to_polymarket(conn: sqlite3.Connection, game_id: str = None) -> list[dict]:
    """Compare each bookmaker's de-vigged probability to Polymarket."""
    where = f"AND game_id = '{game_id}'" if game_id else ""
    query = f"""
    SELECT 
        m.game_id, m.side as team, m.provider as bookmaker,
        m.devigged_prob as book_prob,
        pm.devigged_prob as polymarket_prob,
        (pm.devigged_prob - m.devigged_prob) as edge
    FROM market_latest m
    JOIN market_latest pm ON m.game_id = pm.game_id AND m.side = pm.side AND pm.source = 'polymarket'
    WHERE m.source = 'odds_api' AND m.market = 'futures' {where}
    ORDER BY ABS(edge) DESC
    """
    cursor = conn.execute(query)
    return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]


def get_book_spread(conn: sqlite3.Connection, game_id: str = None) -> list[dict]:
    """Show min/max/spread across all books for each team."""
    where = f"WHERE game_id = '{game_id}'" if game_id else "WHERE market = 'futures'"
    query = f"""
    SELECT 
        game_id, side as team,
        MIN(devigged_prob) as min_prob,
        MAX(devigged_prob) as max_prob,
        MAX(devigged_prob) - MIN(devigged_prob) as spread,
        COUNT(DISTINCT provider) as num_books
    FROM market_latest
    {where} AND source = 'odds_api'
    GROUP BY game_id, side
    HAVING num_books > 1
    ORDER BY spread DESC
    """
    cursor = conn.execute(query)
    return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]


def get_price_history(conn: sqlite3.Connection, game_id: str, side: str, source: str = None) -> list[dict]:
    """Get time series for a specific market outcome."""
    where = "WHERE game_id = ? AND side = ?"
    params = [game_id, side]
    if source:
        where += " AND source = ?"
        params.append(source)
    cursor = conn.execute(
        f"SELECT snapshot_time, source, provider, devigged_prob FROM market_history {where} ORDER BY snapshot_time",
        params
    )
    return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]


def brier_score(predictions: list[float], outcome: int) -> float | None:
    """Brier score: lower = better, 0 = perfect."""
    if not predictions:
        return None
    return sum((p - outcome) ** 2 for p in predictions) / len(predictions)


def log_loss(predictions: list[float], outcome: int) -> float | None:
    """Log loss: lower = better."""
    if not predictions:
        return None
    eps = 1e-15
    return sum(-math.log(max(eps, min(1 - eps, p if outcome == 1 else 1 - p))) for p in predictions) / len(predictions)


def record_outcome(conn: sqlite3.Connection, game_id: str, winner: str) -> None:
    """Record actual outcome for performance metrics."""
    conn.execute(
        "INSERT INTO outcomes (game_id, winner, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(game_id) DO UPDATE SET winner=excluded.winner, updated_at=excluded.updated_at",
        [game_id, winner, utc_now_iso()]
    )
    conn.commit()


if __name__ == "__main__":
    conn = sqlite3.connect("odds.db")
    
    print("=" * 65)
    print("BOOKMAKER SPREAD (variance across books)")
    print("=" * 65)
    for row in get_book_spread(conn)[:10]:
        print(f"{row['team'][:22]:22} Min:{row['min_prob']:5.1%} Max:{row['max_prob']:5.1%} "
              f"Spread:{row['spread']:5.1%} ({row['num_books']} books)")
    
    print("\n" + "=" * 65)
    print("TOP EDGES vs POLYMARKET (by bookmaker)")
    print("=" * 65)
    for row in compare_books_to_polymarket(conn)[:15]:
        print(f"{row['team'][:18]:18} {row['bookmaker']:12} "
              f"Book:{row['book_prob']:5.1%} PM:{row['polymarket_prob']:5.1%} Edge:{row['edge']:+5.1%}")
    
    conn.close()
