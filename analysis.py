"""
Analysis Utilities Module
=========================

Supplementary analysis functions for market comparison and performance metrics.

This module provides:
    - Data exploration queries (sources, providers, games)
    - Market comparison functions (spread analysis, edge detection)
    - Time series analysis (price history, movement tracking)
    - Performance metrics (Brier score, log loss)
    - Outcome recording for backtesting

Note: Core arbitrage detection has been moved to arbitrage.py.
      This module focuses on exploratory analysis and performance tracking.

Usage:
    from analysis import (
        get_all_sources,
        get_book_spread,
        compare_books_to_open_markets,
        get_price_history,
        brier_score,
    )

    conn = sqlite3.connect('odds.db')
    sources = get_all_sources(conn)
    spreads = get_book_spread(conn)

Dependencies:
    - sqlite3: Database queries
    - math: Mathematical functions
    - utils: Helper functions

Author: Arbitrage Detection System
"""
from __future__ import annotations

import math
import sqlite3
from typing import Any, Optional

from utils import utc_now_iso


# =============================================================================
# CONSTANTS
# =============================================================================

# Source category definitions (for reference and filtering)
SPORTSBOOK_SOURCES: set[str] = {"odds_api"}
OPEN_MARKET_SOURCES: set[str] = {"polymarket", "kalshi"}


# =============================================================================
# DATA EXPLORATION
# =============================================================================

def get_all_sources(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Get all data sources and their row counts.

    Useful for quick overview of available data.

    Args:
        conn: Active database connection.

    Returns:
        Dictionary mapping source name to row count.

    Example:
        >>> sources = get_all_sources(conn)
        >>> for source, count in sources.items():
        ...     print(f"{source}: {count} rows")
        odds_api: 1500 rows
        polymarket: 30 rows
        kalshi: 45 rows
    """
    cursor = conn.execute(
        "SELECT source, COUNT(*) FROM market_latest GROUP BY source"
    )
    return {row[0]: row[1] for row in cursor.fetchall()}


def get_all_providers(conn: sqlite3.Connection, source: str = "odds_api") -> list[str]:
    """
    Get all providers (bookmakers) for a given source.

    Args:
        conn: Active database connection.
        source: Data source to query (default: odds_api).

    Returns:
        List of provider names sorted alphabetically.

    Example:
        >>> providers = get_all_providers(conn)
        ['betmgm', 'betrivers', 'betonlineag', 'draftkings', 'fanduel']
    """
    cursor = conn.execute(
        "SELECT DISTINCT provider FROM market_latest "
        "WHERE source = ? ORDER BY provider",
        [source]
    )
    return [row[0] for row in cursor.fetchall()]


def get_all_games(
    conn: sqlite3.Connection,
    league: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Get all games with basic metadata.

    Args:
        conn: Active database connection.
        league: Filter by league (optional).
        limit: Maximum number of games to return.

    Returns:
        List of game dictionaries.

    Example:
        >>> games = get_all_games(conn, league='basketball_nba', limit=10)
        >>> for g in games:
        ...     print(f"{g['home_team']} vs {g['away_team']}")
    """
    where = "WHERE league = ?" if league else ""
    params = [league] if league else []

    cursor = conn.execute(
        f"SELECT * FROM games {where} ORDER BY commence_time DESC LIMIT ?",
        params + [limit]
    )

    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def get_market_data(
    conn: sqlite3.Connection,
    game_id: str,
    market: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    Get all market data for a specific game.

    Args:
        conn: Active database connection.
        game_id: Canonical game identifier.
        market: Filter by market type (optional).

    Returns:
        List of market row dictionaries.

    Example:
        >>> data = get_market_data(conn, 'game_123', market='h2h')
        >>> for row in data:
        ...     print(f"{row['provider']}: {row['side']} @ {row['price']}")
    """
    where = "WHERE game_id = ?"
    params = [game_id]

    if market:
        where += " AND market = ?"
        params.append(market)

    cursor = conn.execute(
        f"SELECT * FROM market_latest {where} ORDER BY provider, side",
        params
    )

    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# =============================================================================
# MARKET COMPARISONS
# =============================================================================

def get_book_spread(
    conn: sqlite3.Connection,
    game_id: Optional[str] = None,
    market: str = "futures",
) -> list[dict[str, Any]]:
    """
    Calculate min/max/spread across all bookmakers for each outcome.

    Useful for identifying:
        - Book disagreement (high spread = different opinions)
        - Potential arbitrage (if spread is large)
        - Market inefficiencies

    Args:
        conn: Active database connection.
        game_id: Filter to specific game (optional).
        market: Market type to analyze (default: futures).

    Returns:
        List of spread analysis dictionaries sorted by spread descending.
        Each dict contains: game_id, team, min_prob, max_prob, spread, num_books.

    Example:
        >>> spreads = get_book_spread(conn)
        >>> for s in spreads[:5]:
        ...     print(f"{s['team']}: {s['spread']:.1%} spread across {s['num_books']} books")
    """
    where = f"WHERE market = '{market}'"
    if game_id:
        where = f"WHERE game_id = '{game_id}'"

    query = f"""
        SELECT
            game_id,
            side AS team,
            MIN(devigged_prob) AS min_prob,
            MAX(devigged_prob) AS max_prob,
            MAX(devigged_prob) - MIN(devigged_prob) AS spread,
            COUNT(DISTINCT provider) AS num_books,
            GROUP_CONCAT(DISTINCT provider) AS providers
        FROM market_latest
        {where} AND source = 'odds_api'
            AND devigged_prob IS NOT NULL
        GROUP BY game_id, side
        HAVING num_books > 1
        ORDER BY spread DESC
    """

    cursor = conn.execute(query)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def compare_books_to_open_markets(
    conn: sqlite3.Connection,
    game_id: Optional[str] = None,
    min_edge: float = 0.0,
) -> list[dict[str, Any]]:
    """
    Compare each bookmaker to all open/prediction markets.

    Identifies edges where sportsbook probabilities differ significantly
    from prediction market prices. Useful for:
        - Finding value bets
        - Identifying market inefficiencies
        - Cross-market arbitrage signals

    Args:
        conn: Active database connection.
        game_id: Filter to specific game (optional).
        min_edge: Minimum edge percentage to include.

    Returns:
        List of comparison dictionaries sorted by absolute edge descending.

    Example:
        >>> comps = compare_books_to_open_markets(conn, min_edge=0.02)
        >>> for c in comps[:10]:
        ...     print(f"{c['team']}: {c['bookmaker']} vs {c['open_market']}")
        ...     print(f"  Edge: {c['edge']:+.1%}")
    """
    where = f"AND m.game_id = '{game_id}'" if game_id else ""
    open_sources = ", ".join(f"'{s}'" for s in OPEN_MARKET_SOURCES)

    query = f"""
        SELECT
            m.game_id,
            m.market,
            m.side AS team,
            m.provider AS bookmaker,
            m.devigged_prob AS book_prob,
            m.price AS book_odds,
            om.source AS open_market,
            om.devigged_prob AS open_market_prob,
            (om.devigged_prob - m.devigged_prob) AS edge
        FROM market_latest m
        JOIN market_latest om
            ON m.game_id = om.game_id
            AND m.side = om.side
            AND m.market = om.market
            AND om.source IN ({open_sources})
        WHERE m.source = 'odds_api' {where}
            AND m.devigged_prob IS NOT NULL
            AND om.devigged_prob IS NOT NULL
            AND ABS(om.devigged_prob - m.devigged_prob) >= {min_edge}
        ORDER BY ABS(edge) DESC
    """

    cursor = conn.execute(query)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def compare_open_markets(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """
    Compare prices between different open markets (Polymarket vs Kalshi).

    Useful for:
        - Identifying prediction market disagreement
        - Finding open market arbitrage opportunities
        - Market sentiment analysis

    Args:
        conn: Active database connection.

    Returns:
        List of comparison dictionaries sorted by absolute difference descending.

    Example:
        >>> comps = compare_open_markets(conn)
        >>> for c in comps[:5]:
        ...     print(f"{c['team']}: Poly {c['prob_a']:.1%} vs Kalshi {c['prob_b']:.1%}")
    """
    open_list = list(OPEN_MARKET_SOURCES)
    if len(open_list) < 2:
        return []

    query = f"""
        SELECT
            a.game_id,
            a.market,
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
            AND a.market = b.market
        WHERE a.source = '{open_list[0]}'
            AND b.source = '{open_list[1]}'
            AND a.devigged_prob IS NOT NULL
            AND b.devigged_prob IS NOT NULL
        ORDER BY ABS(diff) DESC
    """

    cursor = conn.execute(query)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# =============================================================================
# TIME SERIES ANALYSIS
# =============================================================================

def get_price_history(
    conn: sqlite3.Connection,
    game_id: str,
    side: str,
    source: Optional[str] = None,
    provider: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    Get historical price data for a specific market outcome.

    Useful for:
        - Analyzing price movements over time
        - Identifying line moves
        - Backtesting strategies

    Args:
        conn: Active database connection.
        game_id: Canonical game identifier.
        side: Market position (home, away, over, under, team_name).
        source: Filter by source (optional).
        provider: Filter by specific provider (optional).

    Returns:
        List of historical price snapshots sorted by time.

    Example:
        >>> history = get_price_history(conn, 'game_123', 'home', source='odds_api')
        >>> for h in history:
        ...     print(f"{h['snapshot_time']}: {h['devigged_prob']:.1%}")
    """
    where = "WHERE game_id = ? AND side = ?"
    params: list[Any] = [game_id, side]

    if source:
        where += " AND source = ?"
        params.append(source)

    if provider:
        where += " AND provider = ?"
        params.append(provider)

    cursor = conn.execute(
        f"""
        SELECT snapshot_time, source, provider, price, implied_prob, devigged_prob
        FROM market_history
        {where}
        ORDER BY snapshot_time
        """,
        params,
    )

    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def get_line_movements(
    conn: sqlite3.Connection,
    game_id: str,
    market: str = "h2h",
) -> list[dict[str, Any]]:
    """
    Track line movements for a game over time.

    Shows how odds/probabilities changed from first to last snapshot.

    Args:
        conn: Active database connection.
        game_id: Canonical game identifier.
        market: Market type to analyze.

    Returns:
        List of movement summaries per side/provider.

    Example:
        >>> moves = get_line_movements(conn, 'game_123')
        >>> for m in moves:
        ...     print(f"{m['provider']} {m['side']}: {m['open_prob']:.1%} -> {m['close_prob']:.1%}")
    """
    query = """
        SELECT
            side,
            provider,
            MIN(snapshot_time) AS first_snapshot,
            MAX(snapshot_time) AS last_snapshot,
            (SELECT devigged_prob FROM market_history h2
             WHERE h2.game_id = h.game_id AND h2.side = h.side AND h2.provider = h.provider
             ORDER BY snapshot_time LIMIT 1) AS open_prob,
            (SELECT devigged_prob FROM market_history h3
             WHERE h3.game_id = h.game_id AND h3.side = h.side AND h3.provider = h.provider
             ORDER BY snapshot_time DESC LIMIT 1) AS close_prob
        FROM market_history h
        WHERE game_id = ? AND market = ?
        GROUP BY side, provider
    """

    cursor = conn.execute(query, [game_id, market])
    cols = [d[0] for d in cursor.description]
    results = [dict(zip(cols, row)) for row in cursor.fetchall()]

    # Calculate movement
    for r in results:
        if r["open_prob"] and r["close_prob"]:
            r["movement"] = r["close_prob"] - r["open_prob"]
        else:
            r["movement"] = None

    return results


# =============================================================================
# PERFORMANCE METRICS
# =============================================================================

def brier_score(predictions: list[float], outcome: int) -> Optional[float]:
    """
    Calculate Brier score for a set of predictions.

    Brier score measures prediction accuracy. Lower = better.
    - 0.0 = perfect prediction
    - 0.25 = coin flip / no skill
    - 1.0 = completely wrong

    Formula: BS = (1/N) * Σ(p - o)²

    Args:
        predictions: List of predicted probabilities (0-1).
        outcome: Actual outcome (0 or 1).

    Returns:
        Brier score, or None if predictions is empty.

    Example:
        >>> # Predicted 80% chance, event happened
        >>> brier_score([0.8], 1)
        0.04
        >>> # Predicted 80% chance, event didn't happen
        >>> brier_score([0.8], 0)
        0.64
    """
    if not predictions:
        return None

    return sum((p - outcome) ** 2 for p in predictions) / len(predictions)


def log_loss(predictions: list[float], outcome: int) -> Optional[float]:
    """
    Calculate log loss (cross-entropy) for predictions.

    Log loss penalizes confident wrong predictions more severely than Brier.
    Lower = better.

    Formula: LL = -(1/N) * Σ[o*log(p) + (1-o)*log(1-p)]

    Args:
        predictions: List of predicted probabilities (0-1).
        outcome: Actual outcome (0 or 1).

    Returns:
        Log loss, or None if predictions is empty.

    Example:
        >>> # Confident correct prediction
        >>> round(log_loss([0.9], 1), 3)
        0.105
        >>> # Confident wrong prediction (heavily penalized)
        >>> round(log_loss([0.9], 0), 3)
        2.303
    """
    if not predictions:
        return None

    eps = 1e-15  # Prevent log(0)
    total = 0.0

    for p in predictions:
        # Clamp probability to avoid log(0)
        p_clamped = max(eps, min(1 - eps, p if outcome == 1 else 1 - p))
        total += -math.log(p_clamped)

    return total / len(predictions)


def calculate_roi(
    predictions: list[tuple[float, float, int]],
) -> dict[str, float]:
    """
    Calculate return on investment for a series of bets.

    Args:
        predictions: List of (probability, odds, outcome) tuples.
            - probability: Your predicted probability
            - odds: Decimal odds offered
            - outcome: 0 or 1 for actual result

    Returns:
        Dictionary with roi, profit, total_staked, win_rate.

    Example:
        >>> bets = [
        ...     (0.6, 2.0, 1),  # Predicted 60%, 2.0 odds, won
        ...     (0.6, 2.0, 0),  # Predicted 60%, 2.0 odds, lost
        ... ]
        >>> roi = calculate_roi(bets)
        >>> print(f"ROI: {roi['roi']:.1%}")
    """
    if not predictions:
        return {"roi": 0, "profit": 0, "total_staked": 0, "win_rate": 0}

    total_staked = len(predictions)  # $1 per bet
    total_return = 0
    wins = 0

    for prob, odds, outcome in predictions:
        if outcome == 1:
            total_return += odds  # Won: receive odds * stake
            wins += 1
        # Lost: receive 0

    profit = total_return - total_staked

    return {
        "roi": profit / total_staked if total_staked > 0 else 0,
        "profit": profit,
        "total_staked": total_staked,
        "total_return": total_return,
        "win_rate": wins / len(predictions),
        "num_bets": len(predictions),
    }


# =============================================================================
# OUTCOME RECORDING
# =============================================================================

def record_outcome(
    conn: sqlite3.Connection,
    game_id: str,
    winner: str,
    home_score: Optional[int] = None,
    away_score: Optional[int] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Record actual game outcome for performance tracking.

    Used to evaluate prediction accuracy via Brier score and log loss.

    Args:
        conn: Active database connection.
        game_id: Canonical game identifier.
        winner: Winning side ('home', 'away', or team name for futures).
        home_score: Final home team score (optional).
        away_score: Final away team score (optional).
        notes: Additional notes (OT, cancelled, etc.).

    Example:
        >>> record_outcome(conn, 'game_123', 'home', home_score=110, away_score=105)
    """
    now = utc_now_iso()
    final_total = (home_score + away_score) if home_score and away_score else None

    conn.execute("""
        INSERT INTO outcomes (
            game_id, winner, home_score, away_score, final_total, notes, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id) DO UPDATE SET
            winner = excluded.winner,
            home_score = excluded.home_score,
            away_score = excluded.away_score,
            final_total = excluded.final_total,
            notes = excluded.notes,
            updated_at = excluded.updated_at
    """, [game_id, winner, home_score, away_score, final_total, notes, now])
    conn.commit()


def get_outcome(conn: sqlite3.Connection, game_id: str) -> Optional[dict[str, Any]]:
    """
    Get recorded outcome for a game.

    Args:
        conn: Active database connection.
        game_id: Canonical game identifier.

    Returns:
        Outcome dictionary or None if not recorded.
    """
    cursor = conn.execute(
        "SELECT * FROM outcomes WHERE game_id = ?",
        [game_id]
    )
    row = cursor.fetchone()
    if row:
        cols = [d[0] for d in cursor.description]
        return dict(zip(cols, row))
    return None


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    from utils import init_db, load_config

    config = load_config()
    conn = init_db(config["storage"]["database"])

    try:
        # Data sources summary
        print("="*70)
        print("DATA SOURCES")
        print("="*70)
        for source, count in get_all_sources(conn).items():
            category = "SPORTSBOOK" if source in SPORTSBOOK_SOURCES else "OPEN MARKET"
            print(f"  {source:20} ({category}): {count} rows")

        # Providers
        print("\n" + "="*70)
        print("SPORTSBOOK PROVIDERS")
        print("="*70)
        for provider in get_all_providers(conn):
            print(f"  {provider}")

        # Bookmaker spread analysis
        print("\n" + "="*70)
        print("BOOKMAKER SPREAD ANALYSIS (Top 10)")
        print("="*70)
        print(f"{'Team':<25} {'Min':>8} {'Max':>8} {'Spread':>8} {'Books':>6}")
        print("-"*70)
        for row in get_book_spread(conn)[:10]:
            print(
                f"{row['team'][:24]:<25} "
                f"{row['min_prob']:>7.1%} "
                f"{row['max_prob']:>7.1%} "
                f"{row['spread']:>7.1%} "
                f"{row['num_books']:>6}"
            )

        # Sportsbooks vs open markets
        print("\n" + "="*70)
        print("TOP EDGES: SPORTSBOOKS vs OPEN MARKETS")
        print("="*70)
        comps = compare_books_to_open_markets(conn, min_edge=0.01)[:15]
        if comps:
            for row in comps:
                print(
                    f"{row['team'][:18]:<18} "
                    f"{row['bookmaker']:<12} vs {row['open_market']:<12} "
                    f"Book:{row['book_prob']:>6.1%} "
                    f"Open:{row['open_market_prob']:>6.1%} "
                    f"Edge:{row['edge']:>+6.1%}"
                )
        else:
            print("  No significant edges found (threshold: 1%)")

        # Open market comparison
        print("\n" + "="*70)
        print("OPEN MARKET COMPARISON")
        print("="*70)
        open_comps = compare_open_markets(conn)[:10]
        if open_comps:
            for row in open_comps:
                print(
                    f"{row['team'][:22]:<22} "
                    f"{row['market_a']}:{row['prob_a']:>6.1%} "
                    f"{row['market_b']}:{row['prob_b']:>6.1%} "
                    f"Diff:{row['diff']:>+6.1%}"
                )
        else:
            print("  No overlapping markets between open markets")

    finally:
        conn.close()
