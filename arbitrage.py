"""
Arbitrage Detection Module
==========================

Three specialized arbitrage detection algorithms for different market types.

This module provides:
    1. Open Market Arbitrage - Between prediction markets (Polymarket vs Kalshi)
    2. Sportsbook Arbitrage - Between regulated bookmakers (DraftKings vs FanDuel, etc.)
    3. Cross-Market Arbitrage - Between sportsbooks and open markets

Arbitrage Concept:
    Arbitrage exists when you can bet on all outcomes of an event across
    different sources such that the sum of probabilities < 1 (or sum of
    1/odds < 1). This guarantees profit regardless of outcome.

    Example:
        - Source A: Team wins at 45% implied probability
        - Source B: Team loses at 48% implied probability
        - Sum: 93% < 100% = 7% guaranteed profit margin

Usage:
    from arbitrage import (
        detect_open_market_arbitrage,
        detect_sportsbook_arbitrage,
        detect_cross_market_arbitrage,
    )

    conn = sqlite3.connect('odds.db')

    # Detect opportunities in each market category
    open_arbs = detect_open_market_arbitrage(conn)
    book_arbs = detect_sportsbook_arbitrage(conn)
    cross_arbs = detect_cross_market_arbitrage(conn)

    # Print all opportunities
    for arb in open_arbs:
        print(f"OPEN MARKET ARB: {arb['game_id']} - {arb['margin']:.2%} edge")

Dependencies:
    - sqlite3: Database queries
    - utils: Helper functions for calculations

Author: Arbitrage Detection System
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from utils import (
    calculate_arb_margin,
    init_db,
    load_config,
    optimal_stakes,
    prob_to_odds,
    seconds_since,
    utc_now_iso,
)


# =============================================================================
# CONSTANTS
# =============================================================================

# Source category definitions
SPORTSBOOK_SOURCES: set[str] = {"odds_api"}
OPEN_MARKET_SOURCES: set[str] = {"polymarket", "kalshi", "stx"}

# Default minimum edge to report (as decimal, e.g., 0.005 = 0.5%)
DEFAULT_MIN_EDGE: float = 0.005

# Default maximum data age in seconds (ignore stale data)
DEFAULT_MAX_AGE: int = 600  # 10 minutes

# Default reference bankroll for stake calculations
DEFAULT_BANKROLL: float = 100.0

# Default fees (can be overridden in config.yaml)
DEFAULT_FEES: dict[str, float] = {
    "polymarket": 0.02,   # 2% trading fee
    "kalshi": 0.01,       # ~1% fee
    "stx": 0.02,          # ~2% exchange fee (Canadian Sports Exchange)
    "default": 0.0,       # Sportsbooks have no explicit fee (vig removed)
}


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

# Arbitrage opportunity record
ArbitrageOpportunity = dict[str, Any]


# =============================================================================
# FEE CALCULATION HELPERS
# =============================================================================

def get_provider_fee(provider: str, config: Optional[dict] = None) -> float:
    """
    Get the trading fee for a specific provider.

    Fees are applied to winnings on prediction markets.
    Sportsbooks have no explicit fee (vig is already removed).

    Args:
        provider: Provider name (e.g., 'polymarket', 'draftkings').
        config: Configuration dict (optional, uses defaults if None).

    Returns:
        Fee as decimal (e.g., 0.02 = 2%).

    Example:
        >>> get_provider_fee('polymarket')
        0.02
        >>> get_provider_fee('draftkings')
        0.0
    """
    if config:
        fees = config.get("arbitrage", {}).get("fees", {})
        return fees.get(provider, fees.get("default", 0.0))
    return DEFAULT_FEES.get(provider, DEFAULT_FEES.get("default", 0.0))


def calculate_net_profit(
    gross_profit: float,
    stake_a: float,
    stake_b: float,
    provider_a: str,
    provider_b: str,
    config: Optional[dict] = None,
) -> tuple[float, float, float]:
    """
    Calculate net profit after platform fees.

    Fees are applied to potential winnings (payout - stake) on each leg.
    Since only one leg wins in an arb, we calculate expected fee.

    Args:
        gross_profit: Profit before fees.
        stake_a: Stake on leg A.
        stake_b: Stake on leg B.
        provider_a: Provider for leg A.
        provider_b: Provider for leg B.
        config: Configuration dict.

    Returns:
        Tuple of (net_profit, fee_a, fee_b).

    Example:
        >>> net, fee_a, fee_b = calculate_net_profit(7.0, 48, 52, 'draftkings', 'polymarket')
        >>> print(f"Net: ${net:.2f}, Fees: ${fee_a + fee_b:.2f}")
    """
    fee_rate_a = get_provider_fee(provider_a, config)
    fee_rate_b = get_provider_fee(provider_b, config)

    # Fee is applied to winnings. In an arb, one side wins.
    # Worst case: higher-fee platform wins.
    # We calculate fee on the gross profit for each potential winning leg.
    
    # Fee on leg A winning = fee_rate_a * (payout_a - stake_a)
    # But for simplicity, we apply fee proportionally to gross profit
    fee_a = gross_profit * fee_rate_a * (stake_a / (stake_a + stake_b))
    fee_b = gross_profit * fee_rate_b * (stake_b / (stake_a + stake_b))

    total_fees = fee_a + fee_b
    net_profit = gross_profit - total_fees

    return net_profit, fee_a, fee_b


# =============================================================================
# OPEN MARKET ARBITRAGE
# =============================================================================

def detect_open_market_arbitrage(
    conn: sqlite3.Connection,
    min_edge: float = DEFAULT_MIN_EDGE,
    max_age_seconds: int = DEFAULT_MAX_AGE,
    bankroll: float = DEFAULT_BANKROLL,
) -> list[ArbitrageOpportunity]:
    """
    Detect arbitrage opportunities between open/prediction markets.

    Compares prices between Polymarket and Kalshi (and other open markets)
    for the same events. Arbitrage exists when complementary outcomes
    sum to < 1 probability.

    Market Categories:
        - Polymarket: Decentralized prediction market (crypto-based)
        - Kalshi: US-regulated prediction exchange

    Args:
        conn: Active SQLite database connection.
        min_edge: Minimum margin to report (default 0.5%).
        max_age_seconds: Ignore data older than this.
        bankroll: Reference amount for stake calculations.

    Returns:
        List of arbitrage opportunity dictionaries, sorted by margin descending.
        Each dict contains:
            - game_id: Event identifier
            - market: Market type (h2h, futures, etc.)
            - side_a / side_b: Bet positions (e.g., home/away)
            - source_a / source_b: Data sources
            - prob_a / prob_b: Probabilities from each source
            - margin: Arbitrage margin (profit %)
            - stake_a / stake_b: Optimal stakes for guaranteed profit

    Example:
        >>> conn = sqlite3.connect('odds.db')
        >>> arbs = detect_open_market_arbitrage(conn)
        >>> for arb in arbs[:5]:
        ...     print(f"{arb['game_id']}: {arb['margin']:.2%} edge")
        ...     print(f"  Bet {arb['stake_a']:.2f} on {arb['source_a']}")
        ...     print(f"  Bet {arb['stake_b']:.2f} on {arb['source_b']}")
    """
    opportunities: list[ArbitrageOpportunity] = []
    now = utc_now_iso()

    # Get all open market sources
    open_sources = list(OPEN_MARKET_SOURCES)
    if len(open_sources) < 2:
        print("⚠️  Need at least 2 open market sources for arbitrage detection")
        return []

    # Query for matching markets across open market sources
    # We're looking for the SAME event with different prices
    source_list = ", ".join(f"'{s}'" for s in open_sources)

    query = f"""
        SELECT
            a.game_id,
            a.market,
            a.side AS side_a,
            a.source AS source_a,
            a.provider AS provider_a,
            a.devigged_prob AS prob_a,
            a.last_refreshed AS time_a,
            b.side AS side_b,
            b.source AS source_b,
            b.provider AS provider_b,
            b.devigged_prob AS prob_b,
            b.last_refreshed AS time_b
        FROM market_latest a
        JOIN market_latest b
            ON a.game_id = b.game_id
            AND a.market = b.market
            AND a.line = b.line
        WHERE a.source IN ({source_list})
            AND b.source IN ({source_list})
            AND a.source != b.source
            AND a.devigged_prob IS NOT NULL
            AND b.devigged_prob IS NOT NULL
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]

    # Process each potential arbitrage pair
    seen: set[tuple] = set()
    for row in rows:
        data = dict(zip(cols, row))

        # Skip if already processed (avoid duplicates)
        key = tuple(sorted([
            (data["game_id"], data["side_a"], data["source_a"]),
            (data["game_id"], data["side_b"], data["source_b"]),
        ]))
        if key in seen:
            continue
        seen.add(key)

        # Check data freshness
        age_a = seconds_since(data["time_a"]) if data["time_a"] else None
        age_b = seconds_since(data["time_b"]) if data["time_b"] else None

        if age_a and age_a > max_age_seconds:
            continue
        if age_b and age_b > max_age_seconds:
            continue

        # For two-way markets (home/away), check if sides are complementary
        # Skip if same side (not an arb opportunity)
        if data["side_a"] == data["side_b"]:
            continue

        prob_a = data["prob_a"]
        prob_b = data["prob_b"]

        # Calculate arbitrage margin
        margin = calculate_arb_margin(prob_a, prob_b)

        # Only report if margin exceeds threshold
        if margin >= min_edge:
            stake_a, stake_b = optimal_stakes(prob_a, prob_b, bankroll)

            opportunities.append({
                "game_id": data["game_id"],
                "market": data["market"],
                "side_a": data["side_a"],
                "source_a": data["source_a"],
                "provider_a": data["provider_a"],
                "prob_a": prob_a,
                "odds_a": prob_to_odds(prob_a),
                "side_b": data["side_b"],
                "source_b": data["source_b"],
                "provider_b": data["provider_b"],
                "prob_b": prob_b,
                "odds_b": prob_to_odds(prob_b),
                "margin": margin,
                "stake_a": stake_a,
                "stake_b": stake_b,
                "total_stake": bankroll,
                "guaranteed_profit": margin * bankroll,
                "detected_at": now,
                "category": "open_market",
            })

    # Sort by margin descending (best opportunities first)
    opportunities.sort(key=lambda x: x["margin"], reverse=True)
    return opportunities


# =============================================================================
# SPORTSBOOK ARBITRAGE
# =============================================================================

def detect_sportsbook_arbitrage(
    conn: sqlite3.Connection,
    min_edge: float = DEFAULT_MIN_EDGE,
    max_age_seconds: int = DEFAULT_MAX_AGE,
    bankroll: float = DEFAULT_BANKROLL,
) -> list[ArbitrageOpportunity]:
    """
    Detect arbitrage opportunities between regulated sportsbooks.

    Compares odds across different bookmakers (DraftKings, FanDuel, BetMGM, etc.)
    for the same game and market. Sportsbook lines can vary due to:
        - Different risk models
        - Liability management
        - Regional factors
        - Timing of line updates

    Market Types Checked:
        - h2h (moneyline): Team A wins vs Team B wins
        - spreads: Team A covers vs Team B covers
        - totals: Over vs Under

    Args:
        conn: Active SQLite database connection.
        min_edge: Minimum margin to report (default 0.5%).
        max_age_seconds: Ignore data older than this.
        bankroll: Reference amount for stake calculations.

    Returns:
        List of arbitrage opportunity dictionaries, sorted by margin descending.

    Example:
        >>> arbs = detect_sportsbook_arbitrage(conn)
        >>> for arb in arbs:
        ...     print(f"SPORTSBOOK ARB: {arb['margin']:.2%}")
        ...     print(f"  {arb['provider_a']}: {arb['side_a']} @ {arb['odds_a']:.2f}")
        ...     print(f"  {arb['provider_b']}: {arb['side_b']} @ {arb['odds_b']:.2f}")
    """
    opportunities: list[ArbitrageOpportunity] = []
    now = utc_now_iso()

    # Query for same game/market across different bookmakers
    # Looking for complementary sides (home vs away, over vs under)
    query = """
        SELECT
            a.game_id,
            a.market,
            a.side AS side_a,
            a.line AS line_a,
            a.provider AS provider_a,
            a.devigged_prob AS prob_a,
            a.price AS price_a,
            a.last_refreshed AS time_a,
            b.side AS side_b,
            b.line AS line_b,
            b.provider AS provider_b,
            b.devigged_prob AS prob_b,
            b.price AS price_b,
            b.last_refreshed AS time_b,
            g.home_team,
            g.away_team,
            g.commence_time
        FROM market_latest a
        JOIN market_latest b
            ON a.game_id = b.game_id
            AND a.market = b.market
        JOIN games g
            ON a.game_id = g.game_id
        WHERE a.source = 'odds_api'
            AND b.source = 'odds_api'
            AND a.provider != b.provider
            AND a.devigged_prob IS NOT NULL
            AND b.devigged_prob IS NOT NULL
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]

    # Process each potential arbitrage pair
    seen: set[tuple] = set()
    for row in rows:
        data = dict(zip(cols, row))

        # For spreads/totals, lines must match
        if data["market"] in ("spreads", "totals"):
            # Spreads: opposite lines (e.g., +3.5 vs -3.5)
            if data["market"] == "spreads":
                if data["line_a"] != -data["line_b"]:
                    continue
            # Totals: same line, different sides (over vs under)
            elif data["market"] == "totals":
                if data["line_a"] != data["line_b"]:
                    continue

        # Skip if same side (not complementary)
        if data["side_a"] == data["side_b"]:
            continue

        # Check for complementary sides
        complementary = False
        if data["market"] == "h2h":
            complementary = {data["side_a"], data["side_b"]} == {"home", "away"}
        elif data["market"] == "spreads":
            complementary = {data["side_a"], data["side_b"]} == {"home", "away"}
        elif data["market"] == "totals":
            complementary = {data["side_a"], data["side_b"]} == {"over", "under"}

        if not complementary:
            continue

        # Create unique key for deduplication
        key = tuple(sorted([
            (data["game_id"], data["market"], data["line_a"], data["provider_a"]),
            (data["game_id"], data["market"], data["line_b"], data["provider_b"]),
        ]))
        if key in seen:
            continue
        seen.add(key)

        # Check data freshness
        age_a = seconds_since(data["time_a"]) if data["time_a"] else None
        age_b = seconds_since(data["time_b"]) if data["time_b"] else None

        if age_a and age_a > max_age_seconds:
            continue
        if age_b and age_b > max_age_seconds:
            continue

        prob_a = data["prob_a"]
        prob_b = data["prob_b"]

        # Calculate arbitrage margin
        margin = calculate_arb_margin(prob_a, prob_b)

        if margin >= min_edge:
            stake_a, stake_b = optimal_stakes(prob_a, prob_b, bankroll)

            opportunities.append({
                "game_id": data["game_id"],
                "market": data["market"],
                "home_team": data["home_team"],
                "away_team": data["away_team"],
                "commence_time": data["commence_time"],
                "side_a": data["side_a"],
                "line_a": data["line_a"],
                "source_a": "odds_api",
                "provider_a": data["provider_a"],
                "prob_a": prob_a,
                "odds_a": data["price_a"],
                "side_b": data["side_b"],
                "line_b": data["line_b"],
                "source_b": "odds_api",
                "provider_b": data["provider_b"],
                "prob_b": prob_b,
                "odds_b": data["price_b"],
                "margin": margin,
                "stake_a": stake_a,
                "stake_b": stake_b,
                "total_stake": bankroll,
                "guaranteed_profit": margin * bankroll,
                "detected_at": now,
                "category": "sportsbook",
            })

    opportunities.sort(key=lambda x: x["margin"], reverse=True)
    return opportunities


# =============================================================================
# CROSS-MARKET ARBITRAGE
# =============================================================================

def detect_cross_market_arbitrage(
    conn: sqlite3.Connection,
    min_edge: float = DEFAULT_MIN_EDGE,
    max_age_seconds: int = DEFAULT_MAX_AGE,
    bankroll: float = DEFAULT_BANKROLL,
) -> list[ArbitrageOpportunity]:
    """
    Detect arbitrage opportunities between sportsbooks and open markets.

    Compares regulated sportsbook odds against prediction market prices.
    This can reveal inefficiencies where:
        - Sportsbooks overestimate underdog chances (creating value on favorites)
        - Prediction markets have different information/sentiment
        - Vig removal creates opportunities vs vig-free markets

    Cross-market arbs are often larger but may have:
        - Liquidity constraints on prediction markets
        - Different settlement rules
        - Jurisdictional considerations

    Args:
        conn: Active SQLite database connection.
        min_edge: Minimum margin to report (default 0.5%).
        max_age_seconds: Ignore data older than this.
        bankroll: Reference amount for stake calculations.

    Returns:
        List of arbitrage opportunity dictionaries, sorted by margin descending.

    Example:
        >>> arbs = detect_cross_market_arbitrage(conn)
        >>> for arb in arbs:
        ...     print(f"CROSS-MARKET ARB: {arb['margin']:.2%}")
        ...     print(f"  Sportsbook ({arb['provider_a']}): {arb['side_a']} @ {arb['prob_a']:.1%}")
        ...     print(f"  Open Market ({arb['provider_b']}): {arb['side_b']} @ {arb['prob_b']:.1%}")
    """
    opportunities: list[ArbitrageOpportunity] = []
    now = utc_now_iso()

    open_sources = ", ".join(f"'{s}'" for s in OPEN_MARKET_SOURCES)

    # Query for same event across sportsbooks and open markets
    query = f"""
        SELECT
            a.game_id,
            a.market,
            a.side AS side_a,
            a.line AS line_a,
            a.source AS source_a,
            a.provider AS provider_a,
            a.devigged_prob AS prob_a,
            a.price AS price_a,
            a.last_refreshed AS time_a,
            b.side AS side_b,
            b.line AS line_b,
            b.source AS source_b,
            b.provider AS provider_b,
            b.devigged_prob AS prob_b,
            b.price AS price_b,
            b.last_refreshed AS time_b,
            g.home_team,
            g.away_team,
            g.commence_time
        FROM market_latest a
        JOIN market_latest b
            ON a.game_id = b.game_id
            AND a.market = b.market
            AND a.line = b.line
        LEFT JOIN games g
            ON a.game_id = g.game_id
        WHERE a.source = 'odds_api'
            AND b.source IN ({open_sources})
            AND a.devigged_prob IS NOT NULL
            AND b.devigged_prob IS NOT NULL
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]

    seen: set[tuple] = set()
    for row in rows:
        data = dict(zip(cols, row))

        # Skip if same side
        if data["side_a"] == data["side_b"]:
            continue

        # Check for complementary sides
        complementary = False
        if data["market"] == "h2h":
            complementary = {data["side_a"], data["side_b"]} == {"home", "away"}
        elif data["market"] == "futures":
            # For futures, we compare same team across sources
            # Skip non-matching teams
            continue
        elif data["market"] == "spreads":
            complementary = {data["side_a"], data["side_b"]} == {"home", "away"}
        elif data["market"] == "totals":
            complementary = {data["side_a"], data["side_b"]} == {"over", "under"}

        if not complementary:
            continue

        # Deduplication
        key = tuple(sorted([
            (data["game_id"], data["market"], data["provider_a"], data["side_a"]),
            (data["game_id"], data["market"], data["provider_b"], data["side_b"]),
        ]))
        if key in seen:
            continue
        seen.add(key)

        # Check data freshness
        age_a = seconds_since(data["time_a"]) if data["time_a"] else None
        age_b = seconds_since(data["time_b"]) if data["time_b"] else None

        if age_a and age_a > max_age_seconds:
            continue
        if age_b and age_b > max_age_seconds:
            continue

        prob_a = data["prob_a"]
        prob_b = data["prob_b"]

        margin = calculate_arb_margin(prob_a, prob_b)

        if margin >= min_edge:
            stake_a, stake_b = optimal_stakes(prob_a, prob_b, bankroll)

            opportunities.append({
                "game_id": data["game_id"],
                "market": data["market"],
                "home_team": data.get("home_team"),
                "away_team": data.get("away_team"),
                "commence_time": data.get("commence_time"),
                "side_a": data["side_a"],
                "line_a": data["line_a"],
                "source_a": data["source_a"],
                "provider_a": data["provider_a"],
                "prob_a": prob_a,
                "odds_a": data["price_a"],
                "side_b": data["side_b"],
                "line_b": data["line_b"],
                "source_b": data["source_b"],
                "provider_b": data["provider_b"],
                "prob_b": prob_b,
                "odds_b": data["price_b"],
                "margin": margin,
                "stake_a": stake_a,
                "stake_b": stake_b,
                "total_stake": bankroll,
                "guaranteed_profit": margin * bankroll,
                "detected_at": now,
                "category": "cross_market",
            })

    opportunities.sort(key=lambda x: x["margin"], reverse=True)
    return opportunities


# =============================================================================
# PLAYER PROP ARBITRAGE
# =============================================================================

def detect_player_prop_arbitrage(
    conn: sqlite3.Connection,
    min_edge: float = DEFAULT_MIN_EDGE,
    max_age_seconds: int = DEFAULT_MAX_AGE,
    bankroll: float = DEFAULT_BANKROLL,
) -> list[ArbitrageOpportunity]:
    """
    Detect arbitrage opportunities on player props across different sources.

    Player props (points, rebounds, assists, threes) are compared across
    sportsbooks and prediction markets for over/under mismatches.

    Args:
        conn: Active SQLite database connection.
        min_edge: Minimum margin to report as profitable.
        max_age_seconds: Maximum age of data to consider fresh.
        bankroll: Reference stake amount.

    Returns:
        List of arbitrage opportunities sorted by margin (best first).

    Example:
        >>> arbs = detect_player_prop_arbitrage(conn)
        >>> for arb in arbs[:5]:
        ...     print(f"{arb['player']}: {arb['margin']:.2%} edge")
    """
    now = utc_now_iso()
    opportunities: list[ArbitrageOpportunity] = []

    # Query player props from all sources
    query = """
    SELECT 
        a.game_id, a.market, a.player, a.side AS side_a, a.line AS line_a,
        a.source AS source_a, a.provider AS provider_a, 
        a.implied_prob AS prob_a, a.price AS price_a, a.last_refreshed AS time_a,
        b.side AS side_b, b.line AS line_b,
        b.source AS source_b, b.provider AS provider_b,
        b.implied_prob AS prob_b, b.price AS price_b, b.last_refreshed AS time_b,
        g.home_team, g.away_team, g.commence_time
    FROM market_latest a
    JOIN market_latest b ON 
        a.game_id = b.game_id 
        AND a.market = b.market 
        AND a.player = b.player
        AND a.line = b.line
        AND a.side != b.side
    JOIN games g ON a.game_id = g.game_id
    WHERE a.market LIKE 'player_%'
      AND a.player != ''
      AND a.implied_prob IS NOT NULL
      AND b.implied_prob IS NOT NULL
      AND (a.source != b.source OR a.provider != b.provider)
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()

    seen: set = set()
    for row in rows:
        data = dict(zip([
            "game_id", "market", "player", "side_a", "line_a", "source_a", "provider_a",
            "prob_a", "price_a", "time_a", "side_b", "line_b", "source_b", "provider_b",
            "prob_b", "price_b", "time_b", "home_team", "away_team", "commence_time"
        ], row))

        # Must be over vs under
        if {data["side_a"], data["side_b"]} != {"over", "under"}:
            continue

        # Deduplication key
        key = tuple(sorted([
            (data["game_id"], data["player"], data["line_a"], data["provider_a"]),
            (data["game_id"], data["player"], data["line_b"], data["provider_b"]),
        ]))
        if key in seen:
            continue
        seen.add(key)

        # Check freshness
        age_a = seconds_since(data["time_a"]) if data["time_a"] else None
        age_b = seconds_since(data["time_b"]) if data["time_b"] else None

        if age_a and age_a > max_age_seconds:
            continue
        if age_b and age_b > max_age_seconds:
            continue

        prob_a = data["prob_a"]
        prob_b = data["prob_b"]
        margin = calculate_arb_margin(prob_a, prob_b)

        if margin >= min_edge:
            stake_a, stake_b = optimal_stakes(prob_a, prob_b, bankroll)
            prop_type = data["market"].replace("player_", "").upper()

            opportunities.append({
                "game_id": data["game_id"],
                "market": data["market"],
                "player": data["player"],
                "prop_type": prop_type,
                "home_team": data.get("home_team"),
                "away_team": data.get("away_team"),
                "commence_time": data.get("commence_time"),
                "side_a": data["side_a"],
                "line_a": data["line_a"],
                "source_a": data["source_a"],
                "provider_a": data["provider_a"],
                "prob_a": prob_a,
                "odds_a": data["price_a"],
                "side_b": data["side_b"],
                "line_b": data["line_b"],
                "source_b": data["source_b"],
                "provider_b": data["provider_b"],
                "prob_b": prob_b,
                "odds_b": data["price_b"],
                "margin": margin,
                "stake_a": stake_a,
                "stake_b": stake_b,
                "total_stake": bankroll,
                "guaranteed_profit": margin * bankroll,
                "detected_at": now,
                "category": "player_prop",
            })

    opportunities.sort(key=lambda x: x["margin"], reverse=True)
    return opportunities


# =============================================================================
# UNIFIED DETECTION
# =============================================================================

def detect_all_arbitrage(
    conn: sqlite3.Connection,
    min_edge: float = DEFAULT_MIN_EDGE,
    max_age_seconds: int = DEFAULT_MAX_AGE,
    bankroll: float = DEFAULT_BANKROLL,
) -> dict[str, list[ArbitrageOpportunity]]:
    """
    Run all four arbitrage detection algorithms.

    Convenience function to detect arbitrage across all market categories
    in a single call, including player props.

    Args:
        conn: Active SQLite database connection.
        min_edge: Minimum margin to report.
        max_age_seconds: Ignore stale data.
        bankroll: Reference amount for stake calculations.

    Returns:
        Dictionary with keys 'open_market', 'sportsbook', 'cross_market', 'player_prop',
        each containing a list of opportunities.

    Example:
        >>> all_arbs = detect_all_arbitrage(conn)
        >>> print(f"Open market: {len(all_arbs['open_market'])} opportunities")
        >>> print(f"Sportsbook: {len(all_arbs['sportsbook'])} opportunities")
        >>> print(f"Cross-market: {len(all_arbs['cross_market'])} opportunities")
        >>> print(f"Player props: {len(all_arbs['player_prop'])} opportunities")
    """
    return {
        "open_market": detect_open_market_arbitrage(
            conn, min_edge, max_age_seconds, bankroll
        ),
        "sportsbook": detect_sportsbook_arbitrage(
            conn, min_edge, max_age_seconds, bankroll
        ),
        "cross_market": detect_cross_market_arbitrage(
            conn, min_edge, max_age_seconds, bankroll
        ),
        "player_prop": detect_player_prop_arbitrage(
            conn, min_edge, max_age_seconds, bankroll
        ),
    }


# =============================================================================
# CLI OUTPUT FUNCTIONS
# =============================================================================

def print_opportunity(arb: ArbitrageOpportunity, config: Optional[dict] = None, compact: bool = False) -> None:
    """
    Print a single arbitrage opportunity in readable format.

    Shows both gross profit (before fees) and net profit (after fees).
    
    Args:
        arb: Arbitrage opportunity dict.
        config: Optional config for fee calculations.
        compact: If True, show compact single-line format.

    Args:
        arb: Arbitrage opportunity dictionary.
        config: Configuration dict for fee lookup (optional).
    """
    print(f"\n{'='*60}")
    print(f"[{arb['category'].upper()}] {arb['margin']:.2%} MARGIN")
    print(f"{'='*60}")
    print(f"Game: {arb['game_id']}")
    print(f"Market: {arb['market']}")

    if arb.get('home_team'):
        print(f"Teams: {arb['home_team']} vs {arb['away_team']}")

    # Get fees for each provider
    fee_a = get_provider_fee(arb['provider_a'], config)
    fee_b = get_provider_fee(arb['provider_b'], config)

    print(f"\nLeg 1: {arb['side_a']} @ {arb['provider_a']}")
    print(f"  Probability: {arb['prob_a']:.1%}")
    print(f"  Decimal Odds: {arb['odds_a']:.3f}" if arb['odds_a'] else "  Decimal Odds: N/A")
    print(f"  Stake: ${arb['stake_a']:.2f}")
    if fee_a > 0:
        print(f"  Platform Fee: {fee_a:.1%}")

    print(f"\nLeg 2: {arb['side_b']} @ {arb['provider_b']}")
    print(f"  Probability: {arb['prob_b']:.1%}")
    print(f"  Decimal Odds: {arb['odds_b']:.3f}" if arb['odds_b'] else "  Decimal Odds: N/A")
    print(f"  Stake: ${arb['stake_b']:.2f}")
    if fee_b > 0:
        print(f"  Platform Fee: {fee_b:.1%}")

    # Calculate net profit after fees
    gross_profit = arb['guaranteed_profit']
    net_profit, fee_cost_a, fee_cost_b = calculate_net_profit(
        gross_profit,
        arb['stake_a'],
        arb['stake_b'],
        arb['provider_a'],
        arb['provider_b'],
        config,
    )
    total_fees = fee_cost_a + fee_cost_b

    print(f"\n{'─'*40}")
    print(f"💵 Gross Profit:  ${gross_profit:.2f} ({arb['margin']:.2%})")
    if total_fees > 0:
        print(f"📉 Est. Fees:    -${total_fees:.2f}")
        print(f"💰 Net Profit:    ${net_profit:.2f} ({net_profit/arb['total_stake']:.2%})")
    else:
        print(f"💰 Net Profit:    ${net_profit:.2f} (no fees)")
    print(f"📊 Total Stake:   ${arb['total_stake']:.2f}")


def print_summary(results: dict[str, list[ArbitrageOpportunity]]) -> None:
    """
    Print summary of all arbitrage opportunities.

    Args:
        results: Dictionary from detect_all_arbitrage().
    """
    print("\n" + "="*70)
    print("ARBITRAGE DETECTION SUMMARY")
    print("="*70)

    total = 0
    for category, opportunities in results.items():
        count = len(opportunities)
        total += count

        if count > 0:
            best_margin = max(o['margin'] for o in opportunities)
            print(f"\n{category.upper().replace('_', ' ')}: {count} opportunities")
            print(f"  Best margin: {best_margin:.2%}")
        else:
            print(f"\n{category.upper().replace('_', ' ')}: No opportunities found")

    print(f"\n{'='*70}")
    print(f"TOTAL: {total} arbitrage opportunities")
    print(f"{'='*70}")


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import sys

    # Load configuration and connect to database
    config = load_config()
    arb_config = config.get("arbitrage", {})

    min_edge = arb_config.get("min_edge_percent", 0.5) / 100
    max_age = arb_config.get("max_data_age_seconds", 600)
    bankroll = arb_config.get("reference_bankroll", 100)

    conn = init_db(config["storage"]["database"])

    try:
        # Parse command line arguments
        if len(sys.argv) > 1:
            category = sys.argv[1].lower()

            if category == "open":
                print("\n" + "="*70)
                print("OPEN MARKET ARBITRAGE (Polymarket vs Kalshi)")
                print("="*70)
                arbs = detect_open_market_arbitrage(conn, min_edge, max_age, bankroll)
                for arb in arbs:
                    print_opportunity(arb, config)
                print(f"\n✅ Found {len(arbs)} open market arbitrage opportunities")

            elif category == "sportsbook":
                print("\n" + "="*70)
                print("SPORTSBOOK ARBITRAGE (Between Bookmakers)")
                print("="*70)
                arbs = detect_sportsbook_arbitrage(conn, min_edge, max_age, bankroll)
                for arb in arbs:
                    print_opportunity(arb, config)
                print(f"\n✅ Found {len(arbs)} sportsbook arbitrage opportunities")

            elif category == "cross":
                print("\n" + "="*70)
                print("CROSS-MARKET ARBITRAGE (Sportsbooks vs Open Markets)")
                print("="*70)
                arbs = detect_cross_market_arbitrage(conn, min_edge, max_age, bankroll)
                for arb in arbs:
                    print_opportunity(arb, config)
                print(f"\n✅ Found {len(arbs)} cross-market arbitrage opportunities")

            else:
                print(f"Unknown category: {category}")
                print("Usage: python arbitrage.py [open|sportsbook|cross]")

        else:
            # Run all detection and show summary
            results = detect_all_arbitrage(conn, min_edge, max_age, bankroll)
            print_summary(results)

            # Show top opportunities from each category
            for category, opportunities in results.items():
                if opportunities:
                    print(f"\n--- TOP {category.upper().replace('_', ' ')} OPPORTUNITIES ---")
                    for arb in opportunities[:3]:
                        print_opportunity(arb, config)

    finally:
        conn.close()
