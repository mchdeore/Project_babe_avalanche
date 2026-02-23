"""
Lead/Lag Detector
=================

Detects cross-market lead/lag relationships between providers.
Identifies when one provider's price movement precedes another's.

This is bidirectional - can detect any provider leading any other:
- Sportsbook → Prediction market
- Prediction market → Sportsbook
- Sportsbook A → Sportsbook B
- Polymarket → Kalshi (or reverse)

Algorithm:
----------
1. Query market_history for recent snapshots (configurable lookback)
2. Group by (game_id, market, side, line)
3. For each market, compare all provider pairs:
   - Calculate when each provider first moved past threshold
   - Identify leader (moved first) and lagger (moved later)
   - Record lag in seconds and probability delta
4. Store signals in market_lag_signals table

Usage:
------
    from insights_generator.analyzers.lag_detector import detect_lag_signals
    
    signals = detect_lag_signals(
        conn,
        lookback_minutes=30,
        min_probability_delta=0.02,
    )
"""

import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any

from insights_generator.config import get_lag_detection_config


def detect_lag_signals(
    conn: sqlite3.Connection,
    lookback_minutes: int = 30,
    min_probability_delta: float = 0.02,
    min_lag_seconds: float = 5.0,
    max_lag_seconds: float = 300.0,
) -> list[dict[str, Any]]:
    """
    Detect lead/lag signals from market history.
    
    Analyzes recent price movements to find cases where one provider
    moved before another. Returns signals sorted by strength (strongest first).
    
    Args:
        conn: Database connection
        lookback_minutes: How far back to analyze
        min_probability_delta: Minimum probability change to consider (0.02 = 2%)
        min_lag_seconds: Minimum lag to consider (ignore near-simultaneous)
        max_lag_seconds: Maximum lag to consider (too old = not useful)
        
    Returns:
        list: List of signal dictionaries, sorted by signal_strength descending
    """
    # Calculate time window
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=lookback_minutes)
    cutoff_iso = cutoff.isoformat()
    
    # Get all price snapshots in the window
    query = """
        SELECT 
            game_id,
            market,
            side,
            line,
            source,
            provider,
            devigged_prob,
            implied_prob,
            snapshot_time
        FROM market_history
        WHERE snapshot_time >= ?
        ORDER BY game_id, market, side, line, provider, snapshot_time
    """
    
    try:
        cursor = conn.execute(query, (cutoff_iso,))
        rows = cursor.fetchall()
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            print("ERROR: market_history table not found.")
            print("The lag detector requires market data from the main system.")
            print("Run the sportsbook/openmarket workers first to collect data.")
            return []
        raise
    
    if not rows:
        return []
    
    # Group snapshots by market key
    market_snapshots = defaultdict(lambda: defaultdict(list))
    
    for row in rows:
        market_key = (row["game_id"], row["market"], row["side"], row["line"])
        provider_key = (row["source"], row["provider"])
        
        market_snapshots[market_key][provider_key].append({
            "prob": row["devigged_prob"] or row["implied_prob"],
            "time": row["snapshot_time"],
        })
    
    # Detect signals
    signals = []
    detected_at = now.isoformat()
    
    for market_key, provider_data in market_snapshots.items():
        game_id, market, side, line = market_key
        
        # Need at least 2 providers to compare
        if len(provider_data) < 2:
            continue
        
        # Calculate first significant move for each provider
        provider_moves = {}
        
        for provider_key, snapshots in provider_data.items():
            if len(snapshots) < 2:
                continue
            
            # Find first significant move
            first_move = _find_first_significant_move(
                snapshots, 
                min_probability_delta
            )
            
            if first_move:
                provider_moves[provider_key] = first_move
        
        # Compare all provider pairs
        for (source_a, prov_a), (source_b, prov_b) in combinations(provider_moves.keys(), 2):
            move_a = provider_moves[(source_a, prov_a)]
            move_b = provider_moves[(source_b, prov_b)]
            
            # Calculate lag
            time_a = datetime.fromisoformat(move_a["time"])
            time_b = datetime.fromisoformat(move_b["time"])
            
            lag_seconds = abs((time_b - time_a).total_seconds())
            
            # Check if lag is within acceptable range
            if lag_seconds < min_lag_seconds or lag_seconds > max_lag_seconds:
                continue
            
            # Determine leader and lagger
            if time_a < time_b:
                leader_source, leader_prov = source_a, prov_a
                lagger_source, lagger_prov = source_b, prov_b
                leader_move, lagger_move = move_a, move_b
            else:
                leader_source, leader_prov = source_b, prov_b
                lagger_source, lagger_prov = source_a, prov_a
                leader_move, lagger_move = move_b, move_a
            
            # Calculate signal strength
            # Higher delta + lower lag = stronger signal
            avg_delta = (abs(leader_move["delta"]) + abs(lagger_move["delta"])) / 2
            signal_strength = avg_delta / (lag_seconds / 60.0)  # Normalize lag to minutes
            
            signal = {
                "game_id": game_id,
                "market": market,
                "side": side,
                "line": line,
                "leader_source": leader_source,
                "leader_provider": leader_prov,
                "lagger_source": lagger_source,
                "lagger_provider": lagger_prov,
                "leader_move_time": leader_move["time"],
                "lagger_move_time": lagger_move["time"],
                "lag_seconds": lag_seconds,
                "leader_prob_before": leader_move["prob_before"],
                "leader_prob_after": leader_move["prob_after"],
                "lagger_prob_before": lagger_move["prob_before"],
                "lagger_prob_after": lagger_move["prob_after"],
                "probability_delta": avg_delta,
                "signal_strength": signal_strength,
                "detected_at": detected_at,
                "lookback_minutes": lookback_minutes,
            }
            
            signals.append(signal)
    
    # Sort by signal strength (strongest first)
    signals.sort(key=lambda x: x["signal_strength"], reverse=True)
    
    # Store signals in database
    _store_signals(conn, signals)
    
    return signals


def _find_first_significant_move(
    snapshots: list[dict],
    min_delta: float,
) -> dict | None:
    """
    Find the first significant probability move in a series of snapshots.
    
    A significant move is when the probability changes by at least min_delta
    compared to the initial value.
    
    Args:
        snapshots: List of snapshot dicts with 'prob' and 'time'
        min_delta: Minimum probability change to consider significant
        
    Returns:
        dict with move info, or None if no significant move found
    """
    if len(snapshots) < 2:
        return None
    
    # Sort by time
    snapshots = sorted(snapshots, key=lambda x: x["time"])
    
    initial_prob = snapshots[0]["prob"]
    
    if initial_prob is None:
        return None
    
    for i, snap in enumerate(snapshots[1:], 1):
        if snap["prob"] is None:
            continue
        
        delta = snap["prob"] - initial_prob
        
        if abs(delta) >= min_delta:
            return {
                "time": snap["time"],
                "prob_before": initial_prob,
                "prob_after": snap["prob"],
                "delta": delta,
            }
    
    return None


def _store_signals(conn: sqlite3.Connection, signals: list[dict[str, Any]]) -> int:
    """
    Store detected signals in the market_lag_signals table.
    
    Args:
        conn: Database connection
        signals: List of signal dictionaries
        
    Returns:
        int: Number of signals stored
    """
    if not signals:
        return 0
    
    insert_sql = """
        INSERT INTO market_lag_signals (
            game_id, market, side, line,
            leader_source, leader_provider,
            lagger_source, lagger_provider,
            leader_move_time, lagger_move_time,
            lag_seconds,
            leader_prob_before, leader_prob_after,
            lagger_prob_before, lagger_prob_after,
            probability_delta, signal_strength,
            detected_at, lookback_minutes
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?
        )
    """
    
    count = 0
    for signal in signals:
        try:
            conn.execute(insert_sql, (
                signal["game_id"],
                signal["market"],
                signal["side"],
                signal["line"],
                signal["leader_source"],
                signal["leader_provider"],
                signal["lagger_source"],
                signal["lagger_provider"],
                signal["leader_move_time"],
                signal["lagger_move_time"],
                signal["lag_seconds"],
                signal["leader_prob_before"],
                signal["leader_prob_after"],
                signal["lagger_prob_before"],
                signal["lagger_prob_after"],
                signal["probability_delta"],
                signal["signal_strength"],
                signal["detected_at"],
                signal["lookback_minutes"],
            ))
            count += 1
        except sqlite3.Error as e:
            # Log but don't fail on individual insert errors
            print(f"Warning: Failed to store signal: {e}")
    
    conn.commit()
    return count


def analyze_provider_relationships(
    conn: sqlite3.Connection,
    min_signals: int = 10,
) -> dict[str, Any]:
    """
    Analyze historical lag signals to understand provider relationships.
    
    Returns statistics about which providers typically lead or lag others.
    
    Args:
        conn: Database connection
        min_signals: Minimum signals to include a pair in analysis
        
    Returns:
        dict: Analysis results with pair statistics
    """
    query = """
        SELECT 
            leader_provider,
            lagger_provider,
            COUNT(*) as signal_count,
            AVG(lag_seconds) as avg_lag,
            AVG(probability_delta) as avg_delta,
            AVG(signal_strength) as avg_strength,
            MIN(detected_at) as first_signal,
            MAX(detected_at) as last_signal
        FROM market_lag_signals
        GROUP BY leader_provider, lagger_provider
        HAVING COUNT(*) >= ?
        ORDER BY signal_count DESC
    """
    
    cursor = conn.execute(query, (min_signals,))
    rows = cursor.fetchall()
    
    pairs = []
    for row in rows:
        pairs.append({
            "leader": row["leader_provider"],
            "lagger": row["lagger_provider"],
            "count": row["signal_count"],
            "avg_lag_seconds": row["avg_lag"],
            "avg_probability_delta": row["avg_delta"],
            "avg_strength": row["avg_strength"],
            "first_signal": row["first_signal"],
            "last_signal": row["last_signal"],
        })
    
    # Calculate provider summary stats
    provider_stats = defaultdict(lambda: {"leads": 0, "lags": 0})
    
    for pair in pairs:
        provider_stats[pair["leader"]]["leads"] += pair["count"]
        provider_stats[pair["lagger"]]["lags"] += pair["count"]
    
    # Calculate lead ratio for each provider
    provider_summary = []
    for provider, stats in provider_stats.items():
        total = stats["leads"] + stats["lags"]
        lead_ratio = stats["leads"] / total if total > 0 else 0.5
        
        provider_summary.append({
            "provider": provider,
            "times_leading": stats["leads"],
            "times_lagging": stats["lags"],
            "lead_ratio": lead_ratio,
        })
    
    # Sort by lead ratio (best leaders first)
    provider_summary.sort(key=lambda x: x["lead_ratio"], reverse=True)
    
    return {
        "pair_relationships": pairs,
        "provider_summary": provider_summary,
        "total_signals_analyzed": sum(p["count"] for p in pairs),
    }


def get_recent_signals(
    conn: sqlite3.Connection,
    hours: int = 24,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Get recent lag signals for display or analysis.
    
    Args:
        conn: Database connection
        hours: How many hours back to look
        limit: Maximum signals to return
        
    Returns:
        list: Recent signals sorted by detection time (newest first)
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    
    query = """
        SELECT *
        FROM market_lag_signals
        WHERE detected_at >= ?
        ORDER BY detected_at DESC
        LIMIT ?
    """
    
    cursor = conn.execute(query, (cutoff, limit))
    
    return [dict(row) for row in cursor.fetchall()]
