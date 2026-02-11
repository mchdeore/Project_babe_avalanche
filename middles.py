"""
Middle Bet Detection Module
============================

Detects middle bet opportunities across different market sources.

A "middle" occurs when you can bet both sides of a spread or total
at different lines, creating a window where both bets can win.

Example:
    - Bet Team A -3.5 on DraftKings
    - Bet Team B +5.5 on FanDuel
    - If Team A wins by 4 or 5 points, BOTH bets win

This module provides detection for:
    1. Sportsbook middles (between bookmakers)
    2. Open market middles (between Polymarket/Kalshi)
    3. Cross-market middles (sportsbooks vs open markets)
    4. Player prop middles (same player, different lines)

Usage:
    from middles import detect_all_middles
    
    conn = init_db("odds.db")
    opportunities = detect_all_middles(conn)
    for opp in opportunities:
        print(f"{opp['type']}: {opp['description']}")

Dependencies:
    - SQLite database with market_latest table
    - utils.py for helper functions
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from utils import (
    calculate_middle_gap,
    estimate_middle_probability,
    calculate_middle_ev,
    load_config,
    normalize_player,
    seconds_since,
)


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

# Middle opportunity record
MiddleOpportunity = dict[str, Any]


# =============================================================================
# CONFIGURATION
# =============================================================================

# Source categories for filtering
SPORTSBOOK_SOURCES = {"odds_api"}
OPEN_MARKET_SOURCES = {"polymarket", "kalshi"}


# =============================================================================
# CORE DETECTION FUNCTIONS
# =============================================================================

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


# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    import sys
    from utils import init_db, load_config

    config = load_config()
    conn = init_db(config["storage"]["database"])

    # Determine which type to run
    if len(sys.argv) > 1:
        type_arg = sys.argv[1].lower()
        if type_arg in ("sportsbook", "sb"):
            opps = detect_sportsbook_middles(conn, config)
        elif type_arg in ("open", "open_market", "om"):
            opps = detect_open_market_middles(conn, config)
        elif type_arg in ("cross", "cross_market", "x"):
            opps = detect_cross_market_middles(conn, config)
        elif type_arg in ("props", "player", "player_prop"):
            opps = detect_player_prop_middles(conn, config)
        else:
            opps = detect_all_middles(conn, config)
    else:
        opps = detect_all_middles(conn, config)

    print_middles(opps)
    conn.close()
