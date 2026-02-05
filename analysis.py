"""Analysis utilities for comparing markets and calculating performance metrics."""
from __future__ import annotations
import sqlite3
import math
from utils import utc_now_iso

def get_futures_comparison(conn: sqlite3.Connection, game_id: str = None) -> list[dict]:
    """Compare de-vigged probabilities between sportsbooks and Polymarket."""
    where = f"WHERE game_id = '{game_id}'" if game_id else "WHERE market = 'futures'"
    query = f"""
    SELECT game_id, side as team,
        MAX(CASE WHEN source = 'odds_api' THEN devigged_prob END) as sportsbook_prob,
        MAX(CASE WHEN source = 'polymarket' THEN devigged_prob END) as polymarket_prob
    FROM market_latest {where}
    GROUP BY game_id, side
    HAVING sportsbook_prob IS NOT NULL AND polymarket_prob IS NOT NULL
    ORDER BY ABS(sportsbook_prob - polymarket_prob) DESC
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
    cursor = conn.execute(f"SELECT snapshot_time, source, devigged_prob FROM market_history {where} ORDER BY snapshot_time", params)
    return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]

def brier_score(predictions: list[float], outcome: int) -> float:
    """Brier score: lower = better, 0 = perfect."""
    if not predictions:
        return None
    return sum((p - outcome) ** 2 for p in predictions) / len(predictions)

def log_loss(predictions: list[float], outcome: int) -> float:
    """Log loss: lower = better."""
    if not predictions:
        return None
    eps = 1e-15
    return sum(-math.log(max(eps, min(1-eps, p if outcome == 1 else 1-p))) for p in predictions) / len(predictions)

def record_outcome(conn: sqlite3.Connection, game_id: str, winner: str) -> None:
    """Record actual outcome for performance metrics."""
    conn.execute("INSERT INTO outcomes (game_id, winner, updated_at) VALUES (?, ?, ?) ON CONFLICT(game_id) DO UPDATE SET winner=excluded.winner, updated_at=excluded.updated_at",
        [game_id, winner, utc_now_iso()])
    conn.commit()

# Quick CLI
if __name__ == "__main__":
    conn = sqlite3.connect("odds.db")
    print("=" * 50)
    print("TOP DISCREPANCIES (Sportsbooks vs Polymarket)")
    print("=" * 50)
    for row in get_futures_comparison(conn)[:10]:
        diff = row["polymarket_prob"] - row["sportsbook_prob"]
        print(f"{row['team'][:22]:22} SB:{row['sportsbook_prob']:5.1%} PM:{row['polymarket_prob']:5.1%} Δ:{diff:+5.1%}")
    conn.close()
