"""One-shot detection for arbitrage and middles.

Includes arbitrage and middle detection logic (formerly in arbitrage.py and middles.py).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils import (
    calculate_arb_margin,
    calculate_middle_ev,
    estimate_middle_probability,
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

SPORTSBOOK_SOURCES: set[str] = {"odds_api"}
OPEN_MARKET_SOURCES: set[str] = {"polymarket", "kalshi", "stx"}
DEFAULT_MIN_EDGE: float = 0.005
DEFAULT_MAX_AGE: int = 600
DEFAULT_BANKROLL: float = 100.0

ArbitrageOpportunity = dict[str, Any]
MiddleOpportunity = dict[str, Any]


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
    """
    opportunities: list[ArbitrageOpportunity] = []
    now = utc_now_iso()

    open_sources = list(OPEN_MARKET_SOURCES)
    if len(open_sources) < 2:
        print("Warning: need at least 2 open market sources for arbitrage detection")
        return []

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

    seen: set[tuple] = set()
    for row in rows:
        data = dict(zip(cols, row))

        key = tuple(sorted([
            (data["game_id"], data["side_a"], data["source_a"]),
            (data["game_id"], data["side_b"], data["source_b"]),
        ]))
        if key in seen:
            continue
        seen.add(key)

        age_a = seconds_since(data["time_a"]) if data["time_a"] else None
        age_b = seconds_since(data["time_b"]) if data["time_b"] else None

        if age_a and age_a > max_age_seconds:
            continue
        if age_b and age_b > max_age_seconds:
            continue

        if data["side_a"] == data["side_b"]:
            continue

        prob_a = data["prob_a"]
        prob_b = data["prob_b"]

        margin = calculate_arb_margin(prob_a, prob_b)

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
    """Detect arbitrage opportunities between regulated sportsbooks."""
    opportunities: list[ArbitrageOpportunity] = []
    now = utc_now_iso()

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

    seen: set[tuple] = set()
    for row in rows:
        data = dict(zip(cols, row))

        if data["market"] in ("spreads", "totals"):
            if data["market"] == "spreads":
                if data["line_a"] != -data["line_b"]:
                    continue
            elif data["market"] == "totals":
                if data["line_a"] != data["line_b"]:
                    continue

        if data["side_a"] == data["side_b"]:
            continue

        complementary = False
        if data["market"] == "h2h":
            complementary = {data["side_a"], data["side_b"]} == {"home", "away"}
        elif data["market"] == "spreads":
            complementary = {data["side_a"], data["side_b"]} == {"home", "away"}
        elif data["market"] == "totals":
            complementary = {data["side_a"], data["side_b"]} == {"over", "under"}

        if not complementary:
            continue

        key = tuple(sorted([
            (data["game_id"], data["market"], data["line_a"], data["provider_a"]),
            (data["game_id"], data["market"], data["line_b"], data["provider_b"]),
        ]))
        if key in seen:
            continue
        seen.add(key)

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
    """Detect arbitrage opportunities between sportsbooks and open markets."""
    opportunities: list[ArbitrageOpportunity] = []
    now = utc_now_iso()

    open_sources = ", ".join(f"'{s}'" for s in OPEN_MARKET_SOURCES)

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

        if data["side_a"] == data["side_b"]:
            continue

        complementary = False
        if data["market"] == "h2h":
            complementary = {data["side_a"], data["side_b"]} == {"home", "away"}
        elif data["market"] == "futures":
            continue
        elif data["market"] == "spreads":
            complementary = {data["side_a"], data["side_b"]} == {"home", "away"}
        elif data["market"] == "totals":
            complementary = {data["side_a"], data["side_b"]} == {"over", "under"}

        if not complementary:
            continue

        key = tuple(sorted([
            (data["game_id"], data["market"], data["provider_a"], data["side_a"]),
            (data["game_id"], data["market"], data["provider_b"], data["side_b"]),
        ]))
        if key in seen:
            continue
        seen.add(key)

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
    """Detect arbitrage opportunities on player props across sources."""
    now = utc_now_iso()
    opportunities: list[ArbitrageOpportunity] = []

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

        if {data["side_a"], data["side_b"]} != {"over", "under"}:
            continue

        key = tuple(sorted([
            (data["game_id"], data["player"], data["line_a"], data["provider_a"]),
            (data["game_id"], data["player"], data["line_b"], data["provider_b"]),
        ]))
        if key in seen:
            continue
        seen.add(key)

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
    """Run all arbitrage detection algorithms."""
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


def detect_sportsbook_middles(
    conn: sqlite3.Connection,
    config: Optional[dict] = None,
) -> list[MiddleOpportunity]:
    """
    Detect middle opportunities between sportsbooks.

    Finds spread/total lines that differ across bookmakers,
    creating potential middle windows.

    Args:
        conn: Active database connection.
        config: Optional config dict (loads default if None).

    Returns:
        List of MiddleOpportunity dicts.

    Example:
        >>> opps = detect_sportsbook_middles(conn)
        >>> for opp in opps:
        ...     print(f"{opp['game_id']}: {opp['gap']} pt gap")
    """
    config = config or load_config()
    middles_cfg = config.get("middles", {})
    min_gap_spread = middles_cfg.get("min_gap_points", 1.0)
    min_gap_total = middles_cfg.get("min_gap_total", 2.0)
    max_age = config.get("arbitrage", {}).get("max_data_age_seconds", 600)

    opportunities: list[MiddleOpportunity] = []

    # Query spreads from sportsbooks
    query = """
    SELECT 
        game_id, market, side, line, provider, implied_prob, last_refreshed
    FROM market_latest
    WHERE source = 'odds_api'
      AND market IN ('spreads', 'totals')
      AND implied_prob IS NOT NULL
    ORDER BY game_id, market, line
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()

    # Group by game and market
    games: dict[tuple, list[dict]] = {}
    for row in rows:
        game_id, market, side, line, provider, prob, refreshed = row
        key = (game_id, market)
        if key not in games:
            games[key] = []
        games[key].append({
            "side": side,
            "line": line,
            "provider": provider,
            "prob": prob,
            "refreshed": refreshed,
        })

    # Find middles within each game/market
    for (game_id, market), data_list in games.items():
        min_gap = min_gap_spread if market == "spreads" else min_gap_total

        if market == "spreads":
            # For spreads: find best away line vs best home line from DIFFERENT providers
            # Middle exists when: away_line + home_line > 0
            # E.g., away +10.5 and home -9.5 => gap = 10.5 - 9.5 = 1.0
            away_bets = [d for d in data_list if d["side"] == "away"]
            home_bets = [d for d in data_list if d["side"] == "home"]

            for away in away_bets:
                for home in home_bets:
                    # Must be different providers
                    if away["provider"] == home["provider"]:
                        continue

                    # Skip stale data
                    if (seconds_since(away["refreshed"]) > max_age or
                        seconds_since(home["refreshed"]) > max_age):
                        continue

                    # Calculate gap: away line should be positive, home negative
                    # Gap = away_line - abs(home_line) = away_line + home_line
                    gap = away["line"] + home["line"]  # e.g., +10.5 + (-9.5) = 1.0
                    if gap < min_gap:
                        continue

                    mid_prob = estimate_middle_probability(gap, market)
                    stake_total = config.get("arbitrage", {}).get("reference_bankroll", 100)
                    ev_result = calculate_middle_ev(
                        stake_total,
                        away["prob"],
                        home["prob"],
                        mid_prob,
                    )

                    opportunities.append({
                        "type": "sportsbook",
                        "game_id": game_id,
                        "market": market,
                        "side_a": "away",
                        "line_a": away["line"],
                        "provider_a": away["provider"],
                        "prob_a": away["prob"],
                        "side_b": "home",
                        "line_b": home["line"],
                        "provider_b": home["provider"],
                        "prob_b": home["prob"],
                        "gap": gap,
                        "middle_prob": mid_prob,
                        "ev": ev_result["ev"],
                        "ev_percent": ev_result["ev_percent"],
                        "description": (
                            f"SPREAD MIDDLE: away {away['line']:+.1f} ({away['provider']}) "
                            f"vs home {home['line']:+.1f} ({home['provider']}) = {gap:.1f}pt window"
                        ),
                    })

        elif market == "totals":
            # For totals: find over at low line vs under at high line
            # Middle exists when: under_line > over_line
            over_bets = [d for d in data_list if d["side"] == "over"]
            under_bets = [d for d in data_list if d["side"] == "under"]

            for over in over_bets:
                for under in under_bets:
                    # Must be different providers
                    if over["provider"] == under["provider"]:
                        continue

                    # Skip stale data
                    if (seconds_since(over["refreshed"]) > max_age or
                        seconds_since(under["refreshed"]) > max_age):
                        continue

                    # Gap = under_line - over_line
                    gap = under["line"] - over["line"]
                    if gap < min_gap:
                        continue

                    mid_prob = estimate_middle_probability(gap, market)
                    stake_total = config.get("arbitrage", {}).get("reference_bankroll", 100)
                    ev_result = calculate_middle_ev(
                        stake_total,
                        over["prob"],
                        under["prob"],
                        mid_prob,
                    )

                    opportunities.append({
                        "type": "sportsbook",
                        "game_id": game_id,
                        "market": market,
                        "side_a": "over",
                        "line_a": over["line"],
                        "provider_a": over["provider"],
                        "prob_a": over["prob"],
                        "side_b": "under",
                        "line_b": under["line"],
                        "provider_b": under["provider"],
                        "prob_b": under["prob"],
                        "gap": gap,
                        "middle_prob": mid_prob,
                        "ev": ev_result["ev"],
                        "ev_percent": ev_result["ev_percent"],
                        "description": (
                            f"TOTAL MIDDLE: O{over['line']:.1f} ({over['provider']}) "
                            f"vs U{under['line']:.1f} ({under['provider']}) = {gap:.1f}pt window"
                        ),
                    })

    return sorted(opportunities, key=lambda x: -x["ev"])


def detect_open_market_middles(
    conn: sqlite3.Connection,
    config: Optional[dict] = None,
) -> list[MiddleOpportunity]:
    """
    Detect middle opportunities between open/prediction markets.

    Compares Polymarket and Kalshi lines for the same games.

    Args:
        conn: Active database connection.
        config: Optional config dict.

    Returns:
        List of MiddleOpportunity dicts.

    Example:
        >>> opps = detect_open_market_middles(conn)
        >>> print(f"Found {len(opps)} open market middles")
    """
    config = config or load_config()
    middles_cfg = config.get("middles", {})
    min_gap_spread = middles_cfg.get("min_gap_points", 1.0)
    min_gap_total = middles_cfg.get("min_gap_total", 2.0)
    max_age = config.get("arbitrage", {}).get("max_data_age_seconds", 600)

    opportunities: list[MiddleOpportunity] = []

    # Query spreads/totals from open markets
    query = """
    SELECT 
        game_id, market, side, line, source, provider, implied_prob, last_refreshed
    FROM market_latest
    WHERE source IN ('polymarket', 'kalshi')
      AND market IN ('spreads', 'totals')
      AND implied_prob IS NOT NULL
    ORDER BY game_id, market, line
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()

    # Group by game and market
    games: dict[tuple, list[dict]] = {}
    for row in rows:
        game_id, market, side, line, source, provider, prob, refreshed = row
        key = (game_id, market)
        if key not in games:
            games[key] = []
        games[key].append({
            "side": side,
            "line": line,
            "source": source,
            "provider": provider,
            "prob": prob,
            "refreshed": refreshed,
        })

    # Find middles
    for (game_id, market), data_list in games.items():
        min_gap = min_gap_spread if market == "spreads" else min_gap_total

        if market == "spreads":
            away_bets = [d for d in data_list if d["side"] == "away"]
            home_bets = [d for d in data_list if d["side"] == "home"]

            for away in away_bets:
                for home in home_bets:
                    if away["source"] == home["source"]:
                        continue
                    if (seconds_since(away["refreshed"]) > max_age or
                        seconds_since(home["refreshed"]) > max_age):
                        continue

                    gap = away["line"] + home["line"]
                    if gap < min_gap:
                        continue

                    mid_prob = estimate_middle_probability(gap, market)
                    stake_total = config.get("arbitrage", {}).get("reference_bankroll", 100)
                    ev_result = calculate_middle_ev(
                        stake_total, away["prob"], home["prob"], mid_prob,
                    )

                    opportunities.append({
                        "type": "open_market",
                        "game_id": game_id,
                        "market": market,
                        "side_a": "away",
                        "line_a": away["line"],
                        "source_a": away["source"],
                        "provider_a": away["provider"],
                        "prob_a": away["prob"],
                        "side_b": "home",
                        "line_b": home["line"],
                        "source_b": home["source"],
                        "provider_b": home["provider"],
                        "prob_b": home["prob"],
                        "gap": gap,
                        "middle_prob": mid_prob,
                        "ev": ev_result["ev"],
                        "ev_percent": ev_result["ev_percent"],
                        "description": (
                            f"SPREAD: away {away['line']:+.1f} ({away['source']}) "
                            f"vs home {home['line']:+.1f} ({home['source']}) = {gap:.1f}pt"
                        ),
                    })

        elif market == "totals":
            over_bets = [d for d in data_list if d["side"] == "over"]
            under_bets = [d for d in data_list if d["side"] == "under"]

            for over in over_bets:
                for under in under_bets:
                    if over["source"] == under["source"]:
                        continue
                    if (seconds_since(over["refreshed"]) > max_age or
                        seconds_since(under["refreshed"]) > max_age):
                        continue

                    gap = under["line"] - over["line"]
                    if gap < min_gap:
                        continue

                    mid_prob = estimate_middle_probability(gap, market)
                    stake_total = config.get("arbitrage", {}).get("reference_bankroll", 100)
                    ev_result = calculate_middle_ev(
                        stake_total, over["prob"], under["prob"], mid_prob,
                    )

                    opportunities.append({
                        "type": "open_market",
                        "game_id": game_id,
                        "market": market,
                        "side_a": "over",
                        "line_a": over["line"],
                        "source_a": over["source"],
                        "provider_a": over["provider"],
                        "prob_a": over["prob"],
                        "side_b": "under",
                        "line_b": under["line"],
                        "source_b": under["source"],
                        "provider_b": under["provider"],
                        "prob_b": under["prob"],
                        "gap": gap,
                        "middle_prob": mid_prob,
                        "ev": ev_result["ev"],
                        "ev_percent": ev_result["ev_percent"],
                        "description": (
                            f"TOTAL: O{over['line']:.1f} ({over['source']}) "
                            f"vs U{under['line']:.1f} ({under['source']}) = {gap:.1f}pt"
                        ),
                    })

    return sorted(opportunities, key=lambda x: -x["ev"])


def detect_cross_market_middles(
    conn: sqlite3.Connection,
    config: Optional[dict] = None,
) -> list[MiddleOpportunity]:
    """
    Detect middle opportunities between sportsbooks and open markets.

    Cross-market middles can have larger edges due to different
    market efficiencies.

    Args:
        conn: Active database connection.
        config: Optional config dict.

    Returns:
        List of MiddleOpportunity dicts.

    Example:
        >>> opps = detect_cross_market_middles(conn)
        >>> for opp in opps[:5]:
        ...     print(f"{opp['gap']:.1f} pt gap: {opp['description']}")
    """
    config = config or load_config()
    middles_cfg = config.get("middles", {})
    min_gap_spread = middles_cfg.get("min_gap_points", 1.0)
    min_gap_total = middles_cfg.get("min_gap_total", 2.0)
    max_age = config.get("arbitrage", {}).get("max_data_age_seconds", 600)
    arb_fees = config.get("arbitrage", {}).get("fees", {})

    opportunities: list[MiddleOpportunity] = []

    # Query all spread/total lines
    query = """
    SELECT 
        game_id, market, side, line, source, provider, implied_prob, last_refreshed
    FROM market_latest
    WHERE market IN ('spreads', 'totals')
      AND implied_prob IS NOT NULL
    ORDER BY game_id, market, line
    """

    cursor = conn.execute(query)
    rows = cursor.fetchall()

    # Separate by source category
    sportsbook_data: dict[tuple, list[dict]] = {}
    open_market_data: dict[tuple, list[dict]] = {}

    for row in rows:
        game_id, market, side, line, source, provider, prob, refreshed = row
        key = (game_id, market)
        entry = {
            "side": side,
            "line": line,
            "source": source,
            "provider": provider,
            "prob": prob,
            "refreshed": refreshed,
        }

        if source in SPORTSBOOK_SOURCES:
            if key not in sportsbook_data:
                sportsbook_data[key] = []
            sportsbook_data[key].append(entry)
        elif source in OPEN_MARKET_SOURCES:
            if key not in open_market_data:
                open_market_data[key] = []
            open_market_data[key].append(entry)

    # Find cross-market middles
    for key in sportsbook_data.keys() & open_market_data.keys():
        game_id, market = key
        min_gap = min_gap_spread if market == "spreads" else min_gap_total

        sb_list = sportsbook_data[key]
        om_list = open_market_data[key]

        if market == "spreads":
            # Find away/home bets from each source
            sb_away = [d for d in sb_list if d["side"] == "away"]
            sb_home = [d for d in sb_list if d["side"] == "home"]
            om_away = [d for d in om_list if d["side"] == "away"]
            om_home = [d for d in om_list if d["side"] == "home"]

            # Cross combinations: SB away vs OM home, SB home vs OM away
            for away, home in [(a, h) for a in sb_away for h in om_home] + \
                              [(a, h) for a in om_away for h in sb_home]:
                if (seconds_since(away["refreshed"]) > max_age or
                    seconds_since(home["refreshed"]) > max_age):
                    continue

                gap = away["line"] + home["line"]
                if gap < min_gap:
                    continue

                mid_prob = estimate_middle_probability(gap, market)
                stake_total = config.get("arbitrage", {}).get("reference_bankroll", 100)
                ev_result = calculate_middle_ev(
                    stake_total, away["prob"], home["prob"], mid_prob,
                )

                # Apply fee if open market side
                fee = 0
                if away["source"] in OPEN_MARKET_SOURCES:
                    fee = arb_fees.get(away["source"], arb_fees.get("default", 0))
                elif home["source"] in OPEN_MARKET_SOURCES:
                    fee = arb_fees.get(home["source"], arb_fees.get("default", 0))
                adjusted_ev = ev_result["ev"] - (stake_total * fee * mid_prob)

                opportunities.append({
                    "type": "cross_market",
                    "game_id": game_id,
                    "market": market,
                    "side_a": "away",
                    "line_a": away["line"],
                    "source_a": away["source"],
                    "provider_a": away["provider"],
                    "prob_a": away["prob"],
                    "side_b": "home",
                    "line_b": home["line"],
                    "source_b": home["source"],
                    "provider_b": home["provider"],
                    "prob_b": home["prob"],
                    "gap": gap,
                    "middle_prob": mid_prob,
                    "ev": adjusted_ev,
                    "ev_percent": adjusted_ev / stake_total if stake_total > 0 else 0,
                    "fee_applied": fee,
                    "description": (
                        f"SPREAD: away {away['line']:+.1f} ({away['source']}) "
                        f"vs home {home['line']:+.1f} ({home['source']}) = {gap:.1f}pt"
                    ),
                })

        elif market == "totals":
            sb_over = [d for d in sb_list if d["side"] == "over"]
            sb_under = [d for d in sb_list if d["side"] == "under"]
            om_over = [d for d in om_list if d["side"] == "over"]
            om_under = [d for d in om_list if d["side"] == "under"]

            for over, under in [(o, u) for o in sb_over for u in om_under] + \
                               [(o, u) for o in om_over for u in sb_under]:
                if (seconds_since(over["refreshed"]) > max_age or
                    seconds_since(under["refreshed"]) > max_age):
                    continue

                gap = under["line"] - over["line"]
                if gap < min_gap:
                    continue

                mid_prob = estimate_middle_probability(gap, market)
                stake_total = config.get("arbitrage", {}).get("reference_bankroll", 100)
                ev_result = calculate_middle_ev(
                    stake_total, over["prob"], under["prob"], mid_prob,
                )

                fee = 0
                if over["source"] in OPEN_MARKET_SOURCES:
                    fee = arb_fees.get(over["source"], arb_fees.get("default", 0))
                elif under["source"] in OPEN_MARKET_SOURCES:
                    fee = arb_fees.get(under["source"], arb_fees.get("default", 0))
                adjusted_ev = ev_result["ev"] - (stake_total * fee * mid_prob)

                opportunities.append({
                    "type": "cross_market",
                    "game_id": game_id,
                    "market": market,
                    "side_a": "over",
                    "line_a": over["line"],
                    "source_a": over["source"],
                    "provider_a": over["provider"],
                    "prob_a": over["prob"],
                    "side_b": "under",
                    "line_b": under["line"],
                    "source_b": under["source"],
                    "provider_b": under["provider"],
                    "prob_b": under["prob"],
                    "gap": gap,
                    "middle_prob": mid_prob,
                    "ev": adjusted_ev,
                    "ev_percent": adjusted_ev / stake_total if stake_total > 0 else 0,
                    "fee_applied": fee,
                    "description": (
                        f"TOTAL: O{over['line']:.1f} ({over['source']}) "
                        f"vs U{under['line']:.1f} ({under['source']}) = {gap:.1f}pt"
                    ),
                })

    return sorted(opportunities, key=lambda x: -x["ev"])


def detect_player_prop_middles(
    conn: sqlite3.Connection,
    config: Optional[dict] = None,
) -> list[MiddleOpportunity]:
    """
    Detect middle opportunities on player props.

    Finds cases where the same player has different O/U lines
    across sources, creating middle windows.

    Example:
        - Polymarket: LeBron James Points O/U 25.5
        - DraftKings: LeBron James Points O/U 27.5
        - Middle hits if LeBron scores 26 or 27 points

    Args:
        conn: Active database connection.
        config: Optional config dict.

    Returns:
        List of MiddleOpportunity dicts for player props.

    Example:
        >>> opps = detect_player_prop_middles(conn)
        >>> for opp in opps:
        ...     print(f"{opp['player']}: {opp['gap']:.1f} pt gap")
    """
    config = config or load_config()
    middles_cfg = config.get("middles", {}).get("player_props", {})
    
    if not middles_cfg.get("enabled", True):
        return []

    prop_markets = middles_cfg.get("markets", [
        "player_points",
        "player_rebounds",
        "player_assists",
        "player_threes",
    ])
    max_age = config.get("arbitrage", {}).get("max_data_age_seconds", 600)

    opportunities: list[MiddleOpportunity] = []

    # Query player props
    placeholders = ",".join(["?" for _ in prop_markets])
    query = f"""
    SELECT 
        game_id, market, player, side, line, source, provider, implied_prob, last_refreshed
    FROM market_latest
    WHERE market IN ({placeholders})
      AND player != ''
      AND implied_prob IS NOT NULL
    ORDER BY game_id, player, market, line
    """

    cursor = conn.execute(query, prop_markets)
    rows = cursor.fetchall()

    # Group by game, player, market
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        game_id, market, player, side, line, source, provider, prob, refreshed = row
        key = (game_id, player, market)
        if key not in groups:
            groups[key] = []
        groups[key].append({
            "side": side,
            "line": line,
            "source": source,
            "provider": provider,
            "prob": prob,
            "refreshed": refreshed,
        })

    # Find middles for same player/prop
    for (game_id, player, market), data_list in groups.items():
        if len(data_list) < 2:
            continue

        # Get all unique lines
        lines = sorted(set(d["line"] for d in data_list))
        if len(lines) < 2:
            continue

        # Find pairs with different lines from different sources
        over_bets = [d for d in data_list if d["side"] == "over"]
        under_bets = [d for d in data_list if d["side"] == "under"]

        for over in over_bets:
            for under in under_bets:
                # Must be different sources
                if over["source"] == under["source"]:
                    continue

                # Skip stale
                if (seconds_since(over["refreshed"]) > max_age or
                    seconds_since(under["refreshed"]) > max_age):
                    continue

                # Need: over line < under line for middle
                if over["line"] >= under["line"]:
                    continue

                gap = under["line"] - over["line"]
                if gap < 1.0:  # Min 1 point gap for props
                    continue

                # Estimate probability (use smaller std_dev for props)
                mid_prob = estimate_middle_probability(gap, "spreads", std_dev=5.0)

                stake_total = config.get("arbitrage", {}).get("reference_bankroll", 100)
                ev_result = calculate_middle_ev(
                    stake_total,
                    over["prob"],
                    under["prob"],
                    mid_prob,
                )

                opportunities.append({
                    "type": "player_prop",
                    "game_id": game_id,
                    "market": market,
                    "player": player,
                    "over_line": over["line"],
                    "over_source": over["source"],
                    "over_provider": over["provider"],
                    "over_prob": over["prob"],
                    "under_line": under["line"],
                    "under_source": under["source"],
                    "under_provider": under["provider"],
                    "under_prob": under["prob"],
                    "gap": gap,
                    "middle_prob": mid_prob,
                    "ev": ev_result["ev"],
                    "ev_percent": ev_result["ev_percent"],
                    "description": (
                        f"{player.upper()} {market.replace('player_', '').upper()}: "
                        f"O{over['line']:.1f} ({over['source']}) vs U{under['line']:.1f} ({under['source']})"
                    ),
                })

    return sorted(opportunities, key=lambda x: -x["ev"])


# =============================================================================
# UNIFIED DETECTION
# =============================================================================

def detect_all_middles(
    conn: sqlite3.Connection,
    config: Optional[dict] = None,
    types: Optional[list[str]] = None,
) -> list[MiddleOpportunity]:
    """
    Run all middle detection algorithms and combine results.

    Args:
        conn: Active database connection.
        config: Optional config dict.
        types: List of types to detect. Options:
               'sportsbook', 'open_market', 'cross_market', 'player_prop'.
               If None, runs all.

    Returns:
        Combined list of MiddleOpportunity dicts, sorted by EV.

    Example:
        >>> opps = detect_all_middles(conn)
        >>> print(f"Found {len(opps)} total middle opportunities")
        >>> for opp in opps[:10]:
        ...     print(f"  {opp['type']}: {opp['description']} (EV: ${opp['ev']:.2f})")
    """
    config = config or load_config()
    all_types = {"sportsbook", "open_market", "cross_market", "player_prop"}
    types_to_run = set(types) if types else all_types

    opportunities: list[MiddleOpportunity] = []

    if "sportsbook" in types_to_run:
        opportunities.extend(detect_sportsbook_middles(conn, config))

    if "open_market" in types_to_run:
        opportunities.extend(detect_open_market_middles(conn, config))

    if "cross_market" in types_to_run:
        opportunities.extend(detect_cross_market_middles(conn, config))

    if "player_prop" in types_to_run:
        opportunities.extend(detect_player_prop_middles(conn, config))

    # Sort all by EV
    return sorted(opportunities, key=lambda x: -x.get("ev", 0))


def print_middles(opportunities: list[MiddleOpportunity], limit: int = 20) -> None:
    """
    Print middle opportunities in a formatted table.

    Args:
        opportunities: List of middle opportunities.
        limit: Maximum number to print.

    Example:
        >>> opps = detect_all_middles(conn)
        >>> print_middles(opps, limit=10)
    """
    if not opportunities:
        print("No middle opportunities found.")
        return

    print(f"\n{'='*80}")
    print(f"MIDDLE OPPORTUNITIES ({len(opportunities)} found)")
    print(f"{'='*80}")

    for i, opp in enumerate(opportunities[:limit]):
        print(f"\n[{i+1}] {opp['type'].upper()}")
        print(f"    {opp['description']}")
        print(f"    Gap: {opp['gap']:.1f} pts | Middle Prob: {opp['middle_prob']:.1%}")
        print(f"    EV: ${opp['ev']:.2f} ({opp['ev_percent']:.2%})")

    if len(opportunities) > limit:
        print(f"\n... and {len(opportunities) - limit} more opportunities")

MAX_RESULTS = 10


def _format_arb(arb: dict) -> str:
    return (
        f"{arb.get('market', '')} "
        f"{arb.get('side_a', '')}@{arb.get('provider_a', '')} vs "
        f"{arb.get('side_b', '')}@{arb.get('provider_b', '')} "
        f"margin={arb.get('margin', 0):.2%} profit=${arb.get('guaranteed_profit', 0):.2f}"
    )


def _format_middle(mid: dict) -> str:
    return (
        f"{mid.get('market', '')} "
        f"{mid.get('description', '')} "
        f"gap={mid.get('gap', 0):.1f} ev=${mid.get('ev', 0):.2f}"
    )


def run() -> None:
    config = load_config()
    conn = init_db(config["storage"]["database"])

    arb_cfg = config.get("arbitrage", {})
    min_edge = arb_cfg.get("min_edge_percent", 0.5) / 100
    max_age = arb_cfg.get("max_data_age_seconds", 600)
    bankroll = arb_cfg.get("reference_bankroll", 100)

    arbs = detect_all_arbitrage(conn, min_edge, max_age, bankroll)
    all_arbs: list[dict] = []
    for group in arbs.values():
        all_arbs.extend(group)

    all_arbs.sort(key=lambda x: x.get("margin", 0), reverse=True)

    print(
        "arbitrage: total={} open={} sportsbook={} cross={} props={}".format(
            len(all_arbs),
            len(arbs.get("open_market", [])),
            len(arbs.get("sportsbook", [])),
            len(arbs.get("cross_market", [])),
            len(arbs.get("player_prop", [])),
        )
    )
    for arb in all_arbs[:MAX_RESULTS]:
        print(f"- {_format_arb(arb)}")

    middles = detect_all_middles(conn, config)
    print(f"middles: total={len(middles)}")
    for mid in middles[:MAX_RESULTS]:
        print(f"- {_format_middle(mid)}")

    conn.close()


if __name__ == "__main__":
    run()
