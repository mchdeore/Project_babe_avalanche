"""One-shot detection for arbitrage and middles.

Includes arbitrage detection logic (formerly in arbitrage.py).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from middles import detect_all_middles
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

OPEN_MARKET_SOURCES: set[str] = {"polymarket", "kalshi", "stx"}
DEFAULT_MIN_EDGE: float = 0.005
DEFAULT_MAX_AGE: int = 600
DEFAULT_BANKROLL: float = 100.0

ArbitrageOpportunity = dict[str, Any]


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
