"""
Polling Manager Module
======================

Intelligent per-source polling scheduler with quota tracking.

This module provides:
    - Per-source polling intervals (respects config settings)
    - Monthly quota tracking for rate-limited APIs (Odds API)
    - Automatic rate limiting between calls
    - Poll scheduling based on elapsed time since last poll

Architecture:
    - Uses source_metadata table to track polling state
    - Each source has independent poll timing
    - Quota usage is tracked and checked before polling

Usage:
    # Check if a source is due for polling
    from poll_manager import should_poll, run_poll_cycle
    
    if should_poll(conn, config, 'polymarket'):
        ingest(sources=['polymarket'])
    
    # Run full poll cycle (all due sources)
    run_poll_cycle()

    # Run as daemon (continuous polling)
    run_daemon()

Dependencies:
    - utils: Database and config functions
    - ingest: Data fetching functions

Author: Arbitrage Detection System
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Optional

from utils import (
    get_source_config,
    get_source_metadata,
    init_db,
    load_config,
    parse_iso_timestamp,
    seconds_since,
    update_source_metadata,
    utc_now_iso,
)


# =============================================================================
# CONSTANTS
# =============================================================================

# Default poll interval if not specified in config (seconds)
DEFAULT_POLL_INTERVAL: int = 300  # 5 minutes

# Default monthly quota (for sources without explicit limit)
DEFAULT_MONTHLY_QUOTA: Optional[int] = None

# Quota buffer - stop polling when this close to limit
QUOTA_BUFFER: int = 10


# =============================================================================
# POLLING STATE FUNCTIONS
# =============================================================================

def should_poll(
    conn,
    config: dict[str, Any],
    source_name: str,
) -> tuple[bool, str]:
    """
    Determine if a source is due for polling.

    Checks:
        1. Source is enabled in config
        2. Enough time has passed since last poll
        3. Monthly quota is not exhausted (for rate-limited sources)

    Args:
        conn: Active database connection.
        config: Full configuration dictionary.
        source_name: Name of the source to check.

    Returns:
        Tuple of (should_poll: bool, reason: str).
        If should_poll is False, reason explains why.

    Example:
        >>> conn = init_db('odds.db')
        >>> config = load_config()
        >>> should, reason = should_poll(conn, config, 'polymarket')
        >>> if should:
        ...     print("Ready to poll!")
        ... else:
        ...     print(f"Skip: {reason}")
    """
    # Check if source is enabled
    source_cfg = get_source_config(config, source_name)
    if not source_cfg.get("enabled", True):
        return False, "Source is disabled in config"

    # Get polling interval
    interval = source_cfg.get("poll_interval_seconds", DEFAULT_POLL_INTERVAL)

    # Get current metadata
    meta = get_source_metadata(conn, source_name)

    if meta is None:
        # Never polled - definitely should poll
        return True, "First poll for this source"

    # Check time since last poll
    last_poll = meta.get("last_poll_time")
    if last_poll:
        elapsed = seconds_since(last_poll)
        if elapsed is not None and elapsed < interval:
            remaining = int(interval - elapsed)
            return False, f"Next poll in {remaining}s"

    # Check monthly quota (for rate-limited sources like Odds API)
    monthly_quota = source_cfg.get("monthly_quota")
    if monthly_quota is not None:
        calls_used = meta.get("calls_this_month", 0)
        remaining_quota = monthly_quota - calls_used

        if remaining_quota <= QUOTA_BUFFER:
            return False, f"Monthly quota exhausted ({calls_used}/{monthly_quota})"

    return True, "Ready to poll"


def get_poll_status(conn, config: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Get polling status for all configured sources.

    Useful for monitoring and debugging polling behavior.

    Args:
        conn: Active database connection.
        config: Full configuration dictionary.

    Returns:
        List of status dictionaries, one per source.

    Example:
        >>> status = get_poll_status(conn, config)
        >>> for s in status:
        ...     print(f"{s['source']}: {s['status']} - {s['reason']}")
    """
    status_list = []
    sources = config.get("sources", {})

    for source_name, source_cfg in sources.items():
        should, reason = should_poll(conn, config, source_name)
        meta = get_source_metadata(conn, source_name)

        status = {
            "source": source_name,
            "enabled": source_cfg.get("enabled", True),
            "category": source_cfg.get("category", "unknown"),
            "should_poll": should,
            "reason": reason,
            "poll_interval_seconds": source_cfg.get("poll_interval_seconds", DEFAULT_POLL_INTERVAL),
            "last_poll_time": meta.get("last_poll_time") if meta else None,
            "last_poll_success": meta.get("last_poll_success") if meta else None,
            "calls_this_month": meta.get("calls_this_month", 0) if meta else 0,
            "monthly_quota": source_cfg.get("monthly_quota"),
        }

        # Calculate time until next poll
        if meta and meta.get("last_poll_time"):
            elapsed = seconds_since(meta["last_poll_time"])
            interval = source_cfg.get("poll_interval_seconds", DEFAULT_POLL_INTERVAL)
            if elapsed is not None:
                status["seconds_until_next"] = max(0, int(interval - elapsed))
            else:
                status["seconds_until_next"] = 0
        else:
            status["seconds_until_next"] = 0

        status_list.append(status)

    return status_list


def estimate_api_calls(config: dict[str, Any], source_name: str) -> int:
    """
    Estimate number of API calls for a single poll of a source.

    Used for quota planning and scheduling decisions.

    Args:
        config: Full configuration dictionary.
        source_name: Name of the source.

    Returns:
        Estimated number of API calls.

    Example:
        >>> config = load_config()
        >>> calls = estimate_api_calls(config, 'odds_api')
        >>> print(f"Odds API poll will use ~{calls} calls")
    """
    if source_name == "odds_api":
        # Each sport/market combination = 1 API call
        sports = len(config.get("sports", []))
        markets = len(config.get("markets", []))
        futures = 2  # NBA + NHL championship futures
        return (sports * markets) + futures

    # Other sources don't have strict quotas
    return 0


def get_quota_info(conn, config: dict[str, Any], source_name: str) -> dict[str, Any]:
    """
    Get detailed quota information for a source.

    Args:
        conn: Active database connection.
        config: Full configuration dictionary.
        source_name: Name of the source.

    Returns:
        Dictionary with quota details.

    Example:
        >>> info = get_quota_info(conn, config, 'odds_api')
        >>> print(f"Used {info['used']}/{info['quota']} calls this month")
    """
    source_cfg = get_source_config(config, source_name)
    meta = get_source_metadata(conn, source_name)

    quota = source_cfg.get("monthly_quota")
    used = meta.get("calls_this_month", 0) if meta else 0

    return {
        "source": source_name,
        "quota": quota,
        "used": used,
        "remaining": (quota - used) if quota else None,
        "percent_used": (used / quota * 100) if quota else None,
        "calls_per_poll": estimate_api_calls(config, source_name),
        "polls_remaining": ((quota - used) // estimate_api_calls(config, source_name))
            if quota and estimate_api_calls(config, source_name) > 0 else None,
    }


def reset_monthly_quota(conn, source_name: str) -> None:
    """
    Reset monthly quota counter for a source.

    Should be called at the start of each billing period.

    Args:
        conn: Active database connection.
        source_name: Name of the source to reset.

    Example:
        >>> reset_monthly_quota(conn, 'odds_api')
    """
    now = utc_now_iso()
    conn.execute("""
        UPDATE source_metadata
        SET calls_this_month = 0,
            quota_reset_date = ?,
            updated_at = ?
        WHERE source_name = ?
    """, [now, now, source_name])
    conn.commit()


# =============================================================================
# POLL EXECUTION
# =============================================================================

def run_poll_cycle(
    sources: Optional[list[str]] = None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Run a single poll cycle for all due sources.

    Checks each source's polling status and runs ingestion for
    those that are due. Respects rate limits and quotas.

    Args:
        sources: List of source names to check. If None, check all.
        force: If True, ignore timing and poll all specified sources.

    Returns:
        Dictionary summarizing the poll cycle results.

    Example:
        >>> result = run_poll_cycle()
        >>> print(f"Polled {len(result['polled'])} sources")

        >>> # Force poll specific sources
        >>> result = run_poll_cycle(sources=['polymarket'], force=True)
    """
    # Import here to avoid circular dependency
    from ingest import ingest

    config = load_config()
    conn = init_db(config["storage"]["database"])

    results = {
        "timestamp": utc_now_iso(),
        "polled": [],
        "skipped": [],
        "errors": [],
        "total_games": 0,
        "total_rows": 0,
    }

    try:
        source_configs = config.get("sources", {})
        sources_to_check = sources or list(source_configs.keys())

        for source_name in sources_to_check:
            # Check if should poll (unless forced)
            if not force:
                should, reason = should_poll(conn, config, source_name)
                if not should:
                    results["skipped"].append({
                        "source": source_name,
                        "reason": reason,
                    })
                    continue

            # Run ingestion for this source
            print(f"\n{'='*60}")
            print(f"POLLING: {source_name.upper()}")
            print(f"{'='*60}")

            try:
                ingest_result = ingest(sources=[source_name])
                results["polled"].append({
                    "source": source_name,
                    "games": ingest_result.get("games", 0),
                    "rows": ingest_result.get("rows", 0),
                    "api_calls": ingest_result.get("api_calls", 0),
                })
                results["total_games"] += ingest_result.get("games", 0)
                results["total_rows"] += ingest_result.get("rows", 0)

            except Exception as e:
                results["errors"].append({
                    "source": source_name,
                    "error": str(e),
                })
                update_source_metadata(conn, source_name, success=False, error=str(e))

    finally:
        conn.close()

    return results


def run_daemon(
    check_interval: int = 30,
    max_iterations: Optional[int] = None,
) -> None:
    """
    Run continuous polling daemon.

    Continuously checks all sources and polls those that are due.
    Useful for running as a background service.

    Args:
        check_interval: Seconds between poll cycle checks.
        max_iterations: Maximum number of cycles (None = infinite).

    Example:
        >>> # Run forever (Ctrl+C to stop)
        >>> run_daemon()

        >>> # Run for 100 iterations then stop
        >>> run_daemon(max_iterations=100)
    """
    print("="*60)
    print("ARBITRAGE DETECTION SYSTEM - POLLING DAEMON")
    print("="*60)
    print(f"Check interval: {check_interval}s")
    print(f"Max iterations: {max_iterations or 'infinite'}")
    print("\nPress Ctrl+C to stop\n")

    iteration = 0
    try:
        while max_iterations is None or iteration < max_iterations:
            iteration += 1
            print(f"\n--- Cycle {iteration} at {utc_now_iso()} ---")

            result = run_poll_cycle()

            if result["polled"]:
                print(f"✅ Polled: {', '.join(r['source'] for r in result['polled'])}")
            if result["skipped"]:
                for skip in result["skipped"]:
                    print(f"⏭️  {skip['source']}: {skip['reason']}")
            if result["errors"]:
                for err in result["errors"]:
                    print(f"❌ {err['source']}: {err['error']}")

            time.sleep(check_interval)

    except KeyboardInterrupt:
        print("\n\n🛑 Daemon stopped by user")


# =============================================================================
# CLI INTERFACE
# =============================================================================

def print_status() -> None:
    """Print current polling status for all sources."""
    config = load_config()
    conn = init_db(config["storage"]["database"])

    print("\n" + "="*70)
    print("POLLING STATUS")
    print("="*70)

    status_list = get_poll_status(conn, config)

    for s in status_list:
        enabled = "✓" if s["enabled"] else "✗"
        ready = "🟢" if s["should_poll"] else "🔴"

        print(f"\n{ready} {s['source'].upper()} [{enabled}] ({s['category']})")
        print(f"   Poll interval: {s['poll_interval_seconds']}s")
        print(f"   Last poll: {s['last_poll_time'] or 'Never'}")
        print(f"   Status: {s['reason']}")

        if s.get("monthly_quota"):
            print(f"   Quota: {s['calls_this_month']}/{s['monthly_quota']} calls")

        if s.get("seconds_until_next", 0) > 0:
            print(f"   Next poll in: {s['seconds_until_next']}s")

    conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "status":
            print_status()
        elif command == "daemon":
            run_daemon()
        elif command == "poll":
            sources = sys.argv[2:] if len(sys.argv) > 2 else None
            result = run_poll_cycle(sources=sources, force=True)
            print(f"\n✅ Polled {len(result['polled'])} sources")
        else:
            print(f"Unknown command: {command}")
            print("Usage: python poll_manager.py [status|daemon|poll [sources...]]")
    else:
        print_status()
