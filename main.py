"""
Main Entry Point
================

Primary entry point for the arbitrage detection system.

This module provides:
    - Full pipeline execution (ingest + detect)
    - Individual command execution (ingest-only, detect-only)
    - Daemon mode for continuous monitoring
    - Summary reporting

Usage:
    # Full pipeline: ingest data then detect arbitrage
    python main.py

    # Ingest data only
    python main.py ingest

    # Detect arbitrage only (using existing data)
    python main.py detect

    # Run continuous polling daemon
    python main.py daemon

    # Show system status
    python main.py status

Dependencies:
    - ingest: Data ingestion from all sources
    - arbitrage: Three arbitrage detection algorithms
    - poll_manager: Per-source polling scheduler
    - analysis: Additional analysis utilities

Author: Arbitrage Detection System
"""
from __future__ import annotations

import sqlite3
import sys
from typing import Any

from arbitrage import (
    detect_all_arbitrage,
    detect_cross_market_arbitrage,
    detect_open_market_arbitrage,
    detect_player_prop_arbitrage,
    detect_sportsbook_arbitrage,
    print_opportunity,
    print_summary,
)
from ingest import ingest
from middles import (
    detect_all_middles,
    detect_cross_market_middles,
    detect_open_market_middles,
    detect_player_prop_middles,
    detect_sportsbook_middles,
    print_middles,
)
from poll_manager import print_status, run_daemon, run_poll_cycle
from utils import init_db, load_config


# =============================================================================
# PIPELINE FUNCTIONS
# =============================================================================

def run_full_pipeline() -> dict[str, Any]:
    """
    Execute full pipeline: ingest data from all sources, then detect arbitrage.

    This is the primary workflow for finding arbitrage opportunities:
        1. Fetch latest odds from all enabled sources
        2. Store normalized data in SQLite
        3. Run all three arbitrage detection algorithms
        4. Display results

    Returns:
        Dictionary containing ingest results and arbitrage opportunities.

    Example:
        >>> results = run_full_pipeline()
        >>> print(f"Found {sum(len(v) for v in results['arbitrage'].values())} opportunities")
    """
    print("="*70)
    print("ARBITRAGE DETECTION SYSTEM")
    print("="*70)

    # Load configuration
    config = load_config()
    arb_config = config.get("arbitrage", {})

    # Step 1: Ingest data
    print("\n" + "="*70)
    print("STEP 1: DATA INGESTION")
    print("="*70)

    ingest_result = ingest()

    # Step 2: Detect arbitrage
    print("\n" + "="*70)
    print("STEP 2: ARBITRAGE DETECTION")
    print("="*70)

    conn = init_db(config["storage"]["database"])

    try:
        min_edge = arb_config.get("min_edge_percent", 0.5) / 100
        max_age = arb_config.get("max_data_age_seconds", 600)
        bankroll = arb_config.get("reference_bankroll", 100)

        arb_results = detect_all_arbitrage(conn, min_edge, max_age, bankroll)

        # Print summary
        print_summary(arb_results)

        # Collect all arbitrage opportunities
        all_arbs = []
        for category, opps in arb_results.items():
            all_arbs.extend(opps)

        # =====================================================================
        # TOP 5 ARBITRAGE OPPORTUNITIES
        # =====================================================================
        if all_arbs:
            all_arbs.sort(key=lambda x: x["margin"], reverse=True)
            print("\n" + "="*70)
            print("TOP 5 ARBITRAGE OPPORTUNITIES")
            print("="*70)
            for i, arb in enumerate(all_arbs[:5], 1): 
                category = arb.get("category", "unknown").upper()
                player = arb.get("player", "")
                if player:
                    title = f"[{i}] {category} - {player.upper()} ({arb['market'].replace('player_', '')})"
                else:
                    title = f"[{i}] {category} - {arb['market'].upper()}"
                print(f"\n{title}")
                print(f"    {arb['side_a']} @ {arb['provider_a']} vs {arb['side_b']} @ {arb['provider_b']}")
                print(f"    💰 Profit: ${arb['guaranteed_profit']:.2f} ({arb['margin']:.2%})")
        else:
            print("\n⚠️  No arbitrage opportunities found")
            print(f"   Minimum edge: {min_edge:.2%}")

        # =====================================================================
        # TOP 5 MIDDLE OPPORTUNITIES
        # =====================================================================
        print("\n" + "="*70)
        print("TOP 5 MIDDLE OPPORTUNITIES")
        print("="*70)

        middles = detect_all_middles(conn, config)
        if middles:
            for i, mid in enumerate(middles[:5], 1):
                mid_type = mid.get("type", "unknown").upper()
                player = mid.get("player", "")
                if player:
                    title = f"[{i}] {mid_type} - {player.upper()} ({mid['market'].replace('player_', '')})"
                else:
                    title = f"[{i}] {mid_type} - {mid['market'].upper()}"
                print(f"\n{title}")
                print(f"    {mid['description']}")
                print(f"    📊 Gap: {mid['gap']:.1f}pts | Prob: {mid['middle_prob']:.1%} | EV: ${mid['ev']:.2f}")
        else:
            print("\n⚠️  No middle opportunities found")

        # =====================================================================
        # FINAL SUMMARY
        # =====================================================================
        print("\n" + "="*70)
        print(f"SUMMARY: {len(all_arbs)} arbitrage | {len(middles)} middles")
        print("="*70)

    finally:
        conn.close()

    return {
        "ingest": ingest_result,
        "arbitrage": arb_results,
    }


def run_ingest_only() -> dict[str, Any]:
    """
    Run data ingestion only (no arbitrage detection).

    Useful for:
        - Building up historical data
        - Scheduled data collection
        - Testing API connections

    Returns:
        Dictionary with ingestion results.
    """
    print("="*70)
    print("DATA INGESTION ONLY")
    print("="*70)

    return ingest()


def run_detect_only() -> dict[str, list]:
    """
    Run arbitrage detection only (using existing data).

    Useful for:
        - Re-analyzing existing data with different thresholds
        - Quick checks without API calls
        - Testing detection algorithms

    Returns:
        Dictionary with arbitrage opportunities by category.
    """
    print("="*70)
    print("ARBITRAGE DETECTION ONLY")
    print("="*70)

    config = load_config()
    arb_config = config.get("arbitrage", {})

    conn = init_db(config["storage"]["database"])

    try:
        min_edge = arb_config.get("min_edge_percent", 0.5) / 100
        max_age = arb_config.get("max_data_age_seconds", 600)
        bankroll = arb_config.get("reference_bankroll", 100)

        results = detect_all_arbitrage(conn, min_edge, max_age, bankroll)
        print_summary(results)

        # Show all opportunities
        for category, opportunities in results.items():
            if opportunities:
                print(f"\n--- {category.upper().replace('_', ' ')} ---")
                for arb in opportunities:
                    print_opportunity(arb, config)

        return results

    finally:
        conn.close()


def show_database_stats() -> None:
    """
    Display database statistics and system status.

    Shows:
        - Row counts per table
        - Source data freshness
        - Polling status
    """
    config = load_config()
    conn = init_db(config["storage"]["database"])

    print("="*70)
    print("DATABASE STATISTICS")
    print("="*70)

    try:
        # Table row counts
        tables = ["games", "market_latest", "market_history", "outcomes", "source_metadata"]
        print("\nTable Row Counts:")
        for table in tables:
            try:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table:20} {count:>8} rows")
            except sqlite3.OperationalError:
                print(f"  {table:20} (table not found)")

        # Source breakdown
        print("\nMarket Data by Source:")
        cursor = conn.execute("""
            SELECT source, COUNT(*), COUNT(DISTINCT game_id)
            FROM market_latest
            GROUP BY source
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]:20} {row[1]:>6} rows, {row[2]:>4} games")

        # Provider breakdown (for sportsbooks)
        print("\nSportsbook Data by Provider:")
        cursor = conn.execute("""
            SELECT provider, COUNT(*)
            FROM market_latest
            WHERE source = 'odds_api'
            GROUP BY provider
            ORDER BY COUNT(*) DESC
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]:20} {row[1]:>6} rows")

    finally:
        conn.close()

    # Also show polling status
    print("\n")
    print_status()


# =============================================================================
# CLI INTERFACE
# =============================================================================

def print_usage() -> None:
    """Print command-line usage information."""
    print("""
Arbitrage Detection System
==========================

Usage: python main.py [command]

Commands:
    (none)      Run full pipeline (ingest + detect)
    ingest      Ingest data from all sources
    detect      Detect arbitrage (using existing data)
    daemon      Run continuous polling daemon
    status      Show database and polling status
    help        Show this help message

Arbitrage Detection:
    open        Detect open market arbitrage only
    sportsbook  Detect sportsbook arbitrage only
    cross       Detect cross-market arbitrage only
    props       Detect player prop arbitrage only

Middle Bet Detection:
    middle          Detect all middle bet opportunities
    middle-sb       Detect sportsbook middles only
    middle-open     Detect open market middles only
    middle-cross    Detect cross-market middles only
    middle-props    Detect player prop middles only

Examples:
    python main.py                  # Full pipeline
    python main.py ingest           # Just fetch new data
    python main.py detect           # Just run detection
    python main.py sportsbook       # Only sportsbook arbs
    python main.py middle           # All middle opportunities
    python main.py middle-props     # Player prop middles
    python main.py daemon           # Continuous monitoring
""")


def main() -> int:
    """
    Main entry point with CLI argument handling.

    Returns:
        Exit code (0 for success).
    """
    if len(sys.argv) < 2:
        # No arguments - run full pipeline
        run_full_pipeline()
        return 0

    command = sys.argv[1].lower()

    if command in ("help", "-h", "--help"):
        print_usage()

    elif command == "ingest":
        run_ingest_only()

    elif command == "detect":
        run_detect_only()

    elif command == "daemon":
        run_daemon()

    elif command == "status":
        show_database_stats()

    elif command == "poll":
        # Run poll cycle (respects timing)
        sources = sys.argv[2:] if len(sys.argv) > 2 else None
        result = run_poll_cycle(sources=sources)
        print(f"\n✅ Poll cycle complete")
        print(f"   Polled: {len(result['polled'])} sources")
        print(f"   Skipped: {len(result['skipped'])} sources")

    elif command == "open":
        # Open market arbitrage only
        config = load_config()
        conn = init_db(config["storage"]["database"])
        arb_config = config.get("arbitrage", {})
        try:
            arbs = detect_open_market_arbitrage(
                conn,
                min_edge=arb_config.get("min_edge_percent", 0.5) / 100,
            )
            print(f"\n{'='*70}")
            print("OPEN MARKET ARBITRAGE")
            print(f"{'='*70}")
            for arb in arbs:
                print_opportunity(arb, config)
            print(f"\n✅ Found {len(arbs)} open market opportunities")
        finally:
            conn.close()

    elif command == "sportsbook":
        # Sportsbook arbitrage only
        config = load_config()
        conn = init_db(config["storage"]["database"])
        arb_config = config.get("arbitrage", {})
        try:
            arbs = detect_sportsbook_arbitrage(
                conn,
                min_edge=arb_config.get("min_edge_percent", 0.5) / 100,
            )
            print(f"\n{'='*70}")
            print("SPORTSBOOK ARBITRAGE")
            print(f"{'='*70}")
            for arb in arbs:
                print_opportunity(arb, config)
            print(f"\n✅ Found {len(arbs)} sportsbook opportunities")
        finally:
            conn.close()

    elif command == "cross":
        # Cross-market arbitrage only
        config = load_config()
        conn = init_db(config["storage"]["database"])
        arb_config = config.get("arbitrage", {})
        try:
            arbs = detect_cross_market_arbitrage(
                conn,
                min_edge=arb_config.get("min_edge_percent", 0.5) / 100,
            )
            print(f"\n{'='*70}")
            print("CROSS-MARKET ARBITRAGE")
            print(f"{'='*70}")
            for arb in arbs:
                print_opportunity(arb, config)
            print(f"\n✅ Found {len(arbs)} cross-market opportunities")
        finally:
            conn.close()

    elif command == "props":
        # Player prop arbitrage only
        config = load_config()
        conn = init_db(config["storage"]["database"])
        arb_config = config.get("arbitrage", {})
        try:
            arbs = detect_player_prop_arbitrage(
                conn,
                min_edge=arb_config.get("min_edge_percent", 0.5) / 100,
            )
            print(f"\n{'='*70}")
            print("PLAYER PROP ARBITRAGE")
            print(f"{'='*70}")
            for arb in arbs:
                player = arb.get("player", "Unknown")
                prop_type = arb.get("prop_type", arb.get("market", ""))
                print(f"\n🏀 {player.upper()} - {prop_type}")
                print(f"   {arb['side_a']} {arb['line_a']} @ {arb['provider_a']}")
                print(f"   {arb['side_b']} {arb['line_b']} @ {arb['provider_b']}")
                print(f"   💰 Profit: ${arb['guaranteed_profit']:.2f} ({arb['margin']:.2%})")
            print(f"\n✅ Found {len(arbs)} player prop opportunities")
        finally:
            conn.close()

    # -------------------------------------------------------------------------
    # MIDDLE BET COMMANDS
    # -------------------------------------------------------------------------

    elif command == "middle":
        # All middle opportunities
        config = load_config()
        conn = init_db(config["storage"]["database"])
        try:
            middles = detect_all_middles(conn, config)
            print_middles(middles)
        finally:
            conn.close()

    elif command == "middle-sb":
        # Sportsbook middles only
        config = load_config()
        conn = init_db(config["storage"]["database"])
        try:
            middles = detect_sportsbook_middles(conn, config)
            print(f"\n{'='*70}")
            print("SPORTSBOOK MIDDLE OPPORTUNITIES")
            print(f"{'='*70}")
            print_middles(middles)
        finally:
            conn.close()

    elif command == "middle-open":
        # Open market middles only
        config = load_config()
        conn = init_db(config["storage"]["database"])
        try:
            middles = detect_open_market_middles(conn, config)
            print(f"\n{'='*70}")
            print("OPEN MARKET MIDDLE OPPORTUNITIES")
            print(f"{'='*70}")
            print_middles(middles)
        finally:
            conn.close()

    elif command == "middle-cross":
        # Cross-market middles only
        config = load_config()
        conn = init_db(config["storage"]["database"])
        try:
            middles = detect_cross_market_middles(conn, config)
            print(f"\n{'='*70}")
            print("CROSS-MARKET MIDDLE OPPORTUNITIES")
            print(f"{'='*70}")
            print_middles(middles)
        finally:
            conn.close()

    elif command == "middle-props":
        # Player prop middles only
        config = load_config()
        conn = init_db(config["storage"]["database"])
        try:
            middles = detect_player_prop_middles(conn, config)
            print(f"\n{'='*70}")
            print("PLAYER PROP MIDDLE OPPORTUNITIES")
            print(f"{'='*70}")
            print_middles(middles)
        finally:
            conn.close()

    else:
        print(f"Unknown command: {command}")
        print_usage()
        return 1

    return 0


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    sys.exit(main())
