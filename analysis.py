"""
Analysis Utilities
==================
Compare sportsbooks vs open/prediction markets.
"""
from __future__ import annotations

import math
import sqlite3

from utils import utc_now_iso


# =============================================================================
# CONSTANTS
# =============================================================================

SPORTSBOOKS = {"odds_api"}
OPEN_MARKETS = {"polymarket", "kalshi"}  # Add betfair when available


# =============================================================================
# DATA QUERIES
# =============================================================================

def get_all_sources(conn: sqlite3.Connection) -> dict:
    """Get all sources and their row counts."""
    cursor = conn.execute(
        "SELECT source, COUNT(*) FROM market_latest GROUP BY source"
    )
    return {row[0]: row[1] for row in cursor.fetchall()}


def get_all_books(conn: sqlite3.Connection) -> list[str]:
    """Get list of all bookmakers."""
    cursor = conn.execute(
        "SELECT DISTINCT provider FROM market_latest "
        "WHERE source = 'odds_api' ORDER BY provider"
    )
    return [row[0] for row in cursor.fetchall()]


# =============================================================================
# COMPARISONS
# =============================================================================

def compare_books_to_open_markets(
    conn: sqlite3.Connection,
    game_id: str = None,
) -> list[dict]:
    """
    Compare each bookmaker to ALL open/prediction markets.
    
    Returns list of dicts with:
        - team, bookmaker, book_prob
        - open_market, open_market_prob
        - edge (difference)
    """
    where = f"AND m.game_id = '{game_id}'" if game_id else ""
    open_sources = ", ".join(f"'{s}'" for s in OPEN_MARKETS)

    query = f"""
        SELECT
            m.game_id,
            m.side AS team,
            m.provider AS bookmaker,
            m.devigged_prob AS book_prob,
            om.source AS open_market,
            om.devigged_prob AS open_market_prob,
            (om.devigged_prob - m.devigged_prob) AS edge
        FROM market_latest m
        JOIN market_latest om
            ON m.game_id = om.game_id
            AND m.side = om.side
            AND om.source IN ({open_sources})
        WHERE m.source = 'odds_api' {where}
            AND m.devigged_prob IS NOT NULL
            AND om.devigged_prob IS NOT NULL
        ORDER BY ABS(edge) DESC
    """

    cursor = conn.execute(query)
    return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]


def compare_open_markets(conn: sqlite3.Connection) -> list[dict]:
    """
    Compare open markets against each other.
    Example: Polymarket vs Kalshi
    """
    open_list = list(OPEN_MARKETS)
    if len(open_list) < 2:
        return []

    query = f"""
        SELECT
            a.game_id,
            a.side AS team,
            a.source AS market_a,
            a.devigged_prob AS prob_a,
            b.source AS market_b,
            b.devigged_prob AS prob_b,
            (a.devigged_prob - b.devigged_prob) AS diff
        FROM market_latest a
        JOIN market_latest b
            ON a.game_id = b.game_id
            AND a.side = b.side
        WHERE a.source = '{open_list[0]}'
            AND b.source = '{open_list[1]}'
            AND a.devigged_prob IS NOT NULL
            AND b.devigged_prob IS NOT NULL
        ORDER BY ABS(diff) DESC
    """

    cursor = conn.execute(query)
    return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]


def get_book_spread(conn: sqlite3.Connection, game_id: str = None) -> list[dict]:
    """
    Show min/max/spread across all books for each team.
    Useful for finding arbitrage or book disagreement.
    """
    where = f"WHERE game_id = '{game_id}'" if game_id else "WHERE market = 'futures'"

    query = f"""
        SELECT
            game_id,
            side AS team,
            MIN(devigged_prob) AS min_prob,
            MAX(devigged_prob) AS max_prob,
            MAX(devigged_prob) - MIN(devigged_prob) AS spread,
            COUNT(DISTINCT provider) AS num_books
        FROM market_latest
        {where} AND source = 'odds_api'
        GROUP BY game_id, side
        HAVING num_books > 1
        ORDER BY spread DESC
    """

    cursor = conn.execute(query)
    return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]


# =============================================================================
# TIME SERIES
# =============================================================================

def get_price_history(
    conn: sqlite3.Connection,
    game_id: str,
    side: str,
    source: str = None,
) -> list[dict]:
    """Get time series for a specific market outcome."""
    where = "WHERE game_id = ? AND side = ?"
    params = [game_id, side]

    if source:
        where += " AND source = ?"
        params.append(source)

    cursor = conn.execute(
        f"SELECT snapshot_time, source, provider, devigged_prob "
        f"FROM market_history {where} ORDER BY snapshot_time",
        params,
    )

    return [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]


# =============================================================================
# PERFORMANCE METRICS
# =============================================================================

def brier_score(predictions: list[float], outcome: int) -> float | None:
    """
    Brier score: measures prediction accuracy.
    Lower = better, 0 = perfect.
    
    Args:
        predictions: List of predicted probabilities
        outcome: Actual outcome (0 or 1)
    """
    if not predictions:
        return None
    return sum((p - outcome) ** 2 for p in predictions) / len(predictions)


def log_loss(predictions: list[float], outcome: int) -> float | None:
    """
    Log loss: penalizes confident wrong predictions.
    Lower = better.
    """
    if not predictions:
        return None

    eps = 1e-15
    total = 0.0

    for p in predictions:
        p_clamped = max(eps, min(1 - eps, p if outcome == 1 else 1 - p))
        total += -math.log(p_clamped)

    return total / len(predictions)


def record_outcome(conn: sqlite3.Connection, game_id: str, winner: str) -> None:
    """Record actual outcome for performance metrics."""
    conn.execute(
        "INSERT INTO outcomes (game_id, winner, updated_at) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(game_id) DO UPDATE SET winner=excluded.winner, updated_at=excluded.updated_at",
        [game_id, winner, utc_now_iso()],
    )
    conn.commit()


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    conn = sqlite3.connect("odds.db")

    # Data sources
    print("=" * 70)
    print("DATA SOURCES")
    print("=" * 70)
    for source, count in get_all_sources(conn).items():
        market_type = "SPORTSBOOK" if source in SPORTSBOOKS else "OPEN MARKET"
        print(f"  {source:15} ({market_type}): {count} rows")

    # Bookmaker spread
    print("\n" + "=" * 70)
    print("BOOKMAKER SPREAD (variance across books)")
    print("=" * 70)
    for row in get_book_spread(conn)[:5]:
        print(
            f"{row['team'][:22]:22} "
            f"Min:{row['min_prob']:5.1%} "
            f"Max:{row['max_prob']:5.1%} "
            f"Spread:{row['spread']:5.1%} "
            f"({row['num_books']} books)"
        )

    # Sportsbooks vs open markets
    print("\n" + "=" * 70)
    print("TOP EDGES: SPORTSBOOKS vs OPEN MARKETS")
    print("=" * 70)
    for row in compare_books_to_open_markets(conn)[:15]:
        print(
            f"{row['team'][:16]:16} "
            f"{row['bookmaker']:12} vs {row['open_market']:12} "
            f"Book:{row['book_prob']:5.1%} "
            f"Open:{row['open_market_prob']:5.1%} "
            f"Edge:{row['edge']:+5.1%}"
        )

    # Open market comparison
    print("\n" + "=" * 70)
    print("OPEN MARKET COMPARISON (Polymarket vs Kalshi)")
    print("=" * 70)
    comparisons = compare_open_markets(conn)
    if comparisons:
        for row in comparisons[:10]:
            print(
                f"{row['team'][:20]:20} "
                f"{row['market_a']}:{row['prob_a']:5.1%} "
                f"{row['market_b']}:{row['prob_b']:5.1%} "
                f"Diff:{row['diff']:+5.1%}"
            )
    else:
        print("  No overlapping markets between open markets yet")

    conn.close()
