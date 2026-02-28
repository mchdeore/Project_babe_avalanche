#!/usr/bin/env python3
"""
Insights Generator CLI
======================

Command-line interface for running insights_generator features on-demand.

Usage:
------
    python -m insights_generator.cli <command> [options]

Commands:
---------
    scrape       Fetch news headlines from configured RSS sources
    analyze      Process unprocessed headlines through Ollama NLP
    detect-lag   Analyze market history for lead/lag signals
    event-impacts  Compute event → market impact metrics
    train        Train ML model on collected data
    predict      Run predictions on current market data
    status       Show summary of collected data and model status
    init-db      Initialize database tables

Examples:
---------
    # Scrape latest news
    python -m insights_generator.cli scrape
    
    # Process headlines with Ollama
    python -m insights_generator.cli analyze --batch-size 20
    
    # Detect lead/lag signals from last 60 minutes
    python -m insights_generator.cli detect-lag --lookback 60

    # Compute event -> market impacts
    python -m insights_generator.cli event-impacts
    
    # Show status summary
    python -m insights_generator.cli status
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from insights_generator.config import (
    get_database_path,
    get_lag_detection_config,
    get_event_impact_config,
    get_ml_config,
    get_news_sources,
    get_ollama_config,
    get_scoring_config,
    init_insights_db,
    is_enabled,
    validate_config,
)


def cmd_scrape(args: argparse.Namespace) -> int:
    """
    Scrape news headlines from configured RSS sources.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        int: Exit code (0=success, 1=error)
    """
    from insights_generator.scrapers.news_scraper import scrape_all_sources
    
    print("=" * 60)
    print("INSIGHTS GENERATOR - NEWS SCRAPER")
    print("=" * 60)
    
    sources = get_news_sources()
    if not sources:
        print("ERROR: No news sources configured")
        return 1
    
    print(f"\nSources to scrape: {len(sources)}")
    for source in sources:
        print(f"  - {source['name']} ({source['type']})")
    
    print("\nScraping headlines...")
    
    conn = init_insights_db()
    try:
        results = scrape_all_sources(conn, sources)
        
        print("\n" + "-" * 40)
        print("RESULTS:")
        print("-" * 40)
        
        total_new = 0
        for source_name, count in results.items():
            print(f"  {source_name}: {count} new headlines")
            total_new += count
        
        print(f"\nTotal new headlines: {total_new}")
        
    finally:
        conn.close()
    
    return 0


def cmd_analyze(args: argparse.Namespace) -> int:
    """
    Process unprocessed headlines through Ollama NLP.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        int: Exit code (0=success, 1=error)
    """
    from insights_generator.scrapers.news_scraper import get_unprocessed_headlines
    
    print("=" * 60)
    print("INSIGHTS GENERATOR - NLP ANALYZER")
    print("=" * 60)
    
    ollama_config = get_ollama_config()
    batch_size = args.batch_size or ollama_config.get("batch_size", 10)
    
    print(f"\nOllama model: {ollama_config['ollama_model']}")
    print(f"Ollama host: {ollama_config['ollama_host']}")
    print(f"Batch size: {batch_size}")
    
    conn = init_insights_db()
    try:
        if args.dry_run:
            headlines = get_unprocessed_headlines(conn, limit=batch_size)
            print(f"\n[DRY RUN] {len(headlines)} headlines queued for processing:")
            for h in headlines[:20]:
                print(f"  - [{h.get('source', '?')}] {h['headline'][:80]}")
            if len(headlines) > 20:
                print(f"  ... and {len(headlines) - 20} more")
            return 0

        # Check Ollama connectivity before processing
        import requests
        try:
            resp = requests.get(
                f"{ollama_config['ollama_host'].rstrip('/')}/api/tags",
                timeout=5,
            )
            if resp.status_code != 200:
                raise ConnectionError()
        except Exception:
            queued = get_unprocessed_headlines(conn, limit=1)
            cursor = conn.execute(
                "SELECT COUNT(*) FROM news_headlines WHERE processed = 0"
            )
            total_queued = cursor.fetchone()[0]
            print(f"\nERROR: Cannot reach Ollama at {ollama_config['ollama_host']}")
            print("Make sure Ollama is running: ollama serve")
            print(f"\n{total_queued} headline(s) queued and waiting.")
            print("Use --dry-run to see what would be processed.")
            return 1

        print("\nProcessing headlines...")

        from insights_generator.analyzers.nlp_processor import process_headlines
        results = process_headlines(
            conn,
            model=ollama_config["ollama_model"],
            host=ollama_config["ollama_host"],
            batch_size=batch_size,
        )
        
        print("\n" + "-" * 40)
        print("RESULTS:")
        print("-" * 40)
        print(f"  Headlines processed: {results['processed']}")
        print(f"  Events extracted: {results['events_created']}")
        print(f"  Errors: {results['errors']}")
        
    finally:
        conn.close()
    
    return 0


def cmd_detect_lag(args: argparse.Namespace) -> int:
    """
    Analyze market history for lead/lag signals between providers.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        int: Exit code (0=success, 1=error)
    """
    from insights_generator.analyzers.lag_detector import detect_lag_signals
    
    print("=" * 60)
    print("INSIGHTS GENERATOR - LAG DETECTOR")
    print("=" * 60)
    
    lag_config = get_lag_detection_config()
    lookback = args.lookback or lag_config.get("lookback_minutes", 30)
    min_delta = args.min_delta or lag_config.get("min_probability_delta", 0.02)
    
    print(f"\nLookback window: {lookback} minutes")
    print(f"Min probability delta: {min_delta:.1%}")
    print(f"Min lag seconds: {lag_config.get('min_lag_seconds', 5)}")
    print(f"Max lag seconds: {lag_config.get('max_lag_seconds', 300)}")
    
    print("\nAnalyzing market history...")
    
    conn = init_insights_db()
    try:
        signals = detect_lag_signals(
            conn,
            lookback_minutes=lookback,
            min_probability_delta=min_delta,
            min_lag_seconds=lag_config.get("min_lag_seconds", 5),
            max_lag_seconds=lag_config.get("max_lag_seconds", 300),
        )
        
        print("\n" + "-" * 40)
        print("RESULTS:")
        print("-" * 40)
        print(f"  Signals detected: {len(signals)}")
        
        if signals:
            print("\n  Top 5 signals by strength:")
            for i, signal in enumerate(signals[:5], 1):
                print(f"\n  {i}. {signal['leader_provider']} → {signal['lagger_provider']}")
                print(f"     Game: {signal['game_id']}")
                print(f"     Market: {signal['market']} {signal['side']}")
                print(f"     Lag: {signal['lag_seconds']:.1f}s | Delta: {signal['probability_delta']:.1%}")
                print(f"     Strength: {signal['signal_strength']:.3f}")
        
    finally:
        conn.close()
    
    return 0


def cmd_event_impacts(args: argparse.Namespace) -> int:
    """
    Compute event -> market impact metrics.
    """
    from insights_generator.analyzers.event_impact import compute_event_impacts

    print("=" * 60)
    print("INSIGHTS GENERATOR - EVENT IMPACTS")
    print("=" * 60)

    cfg = get_event_impact_config()
    pre_window = args.pre_window or cfg.get("pre_window_minutes", 30)
    post_window = args.post_window or cfg.get("post_window_minutes", 120)
    max_age = args.max_age or cfg.get("max_event_age_hours", 72)
    min_snapshots = args.min_snapshots or cfg.get("min_snapshot_count", 1)

    print(f"\nPre-window: {pre_window} minutes")
    print(f"Post-window: {post_window} minutes")
    print(f"Max event age: {max_age} hours")
    print(f"Min snapshots: {min_snapshots}")

    conn = init_insights_db()
    try:
        impacts = compute_event_impacts(
            conn,
            pre_window_minutes=pre_window,
            post_window_minutes=post_window,
            max_event_age_hours=max_age,
            min_snapshot_count=min_snapshots,
        )
        print("\n" + "-" * 40)
        print("RESULTS:")
        print("-" * 40)
        print(f"  Impacts computed: {len(impacts)}")
    finally:
        conn.close()

    return 0


def cmd_score(args: argparse.Namespace) -> int:
    """Score all upcoming games using the AI scoring system."""
    from insights_generator.scoring import score_all_upcoming

    print("=" * 60)
    print("INSIGHTS GENERATOR - AI SCORING")
    print("=" * 60)

    cfg = get_scoring_config()
    print(f"\nWeights: {cfg.get('weights', {})}")
    print(f"Lookback: {cfg.get('lookback_hours', 72)} hours")

    conn = init_insights_db()
    try:
        scores = score_all_upcoming(conn)

        print("\n" + "-" * 40)
        print("RESULTS:")
        print("-" * 40)
        print(f"  Games scored: {len(scores)}")

        if scores:
            print(f"\n  {'GAME':<45} {'COMP':>5}  {'INJ':>4} {'WX':>4} {'NEWS':>4} {'MKT':>4} {'LAG':>4} {'LU':>4}")
            print("  " + "-" * 79)
            for gs in scores[:15]:
                label = f"{gs.away_team} @ {gs.home_team}"
                if len(label) > 44:
                    label = label[:41] + "..."
                print(
                    f"  {label:<45} {gs.composite_score:>5.3f}"
                    f"  {gs.injury_score:>4.2f} {gs.weather_score:>4.2f}"
                    f" {gs.news_momentum_score:>4.2f} {gs.market_momentum_score:>4.2f}"
                    f" {gs.provider_lag_score:>4.2f} {gs.lineup_score:>4.2f}"
                )
            if len(scores) > 15:
                print(f"\n  ... and {len(scores) - 15} more games")
    finally:
        conn.close()

    return 0


def cmd_train(args: argparse.Namespace) -> int:
    """
    Train ML model on collected data.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        int: Exit code (0=success, 1=error)
    """
    from insights_generator.models.features import train_model, build_feature_matrix
    
    print("=" * 60)
    print("INSIGHTS GENERATOR - MODEL TRAINING")
    print("=" * 60)
    
    ml_config = get_ml_config()
    min_samples = ml_config.get("min_training_samples", 1000)
    model_path = PROJECT_ROOT / ml_config.get("model_path", "insights_generator/trained_model.pkl")
    
    print(f"\nMin training samples: {min_samples}")
    print(f"Model output path: {model_path}")
    print(f"Features: {', '.join(ml_config.get('features', []))}")
    
    print("\nBuilding feature matrix...")
    
    conn = init_insights_db()
    try:
        X, y, metadata = build_feature_matrix(conn)
        
        if len(X) < min_samples:
            print(f"\nERROR: Not enough training samples ({len(X)} < {min_samples})")
            print("Continue collecting data before training.")
            return 1
        
        print(f"  Training samples: {len(X)}")
        
        print("\nTraining model...")
        model_type = args.model_type or "xgboost"
        metrics = train_model(X, y, model_path, model_type=model_type)
        
        print("\n" + "-" * 40)
        print("TRAINING RESULTS:")
        print("-" * 40)
        for metric, value in metrics.items():
            print(f"  {metric}: {value:.4f}")
        
        print(f"\nModel saved to: {model_path}")
        
    finally:
        conn.close()
    
    return 0


def cmd_predict(args: argparse.Namespace) -> int:
    """
    Run predictions on current market data.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        int: Exit code (0=success, 1=error)
    """
    from insights_generator.models.features import predict, build_feature_matrix
    
    print("=" * 60)
    print("INSIGHTS GENERATOR - PREDICTIONS")
    print("=" * 60)
    
    ml_config = get_ml_config()
    model_path = PROJECT_ROOT / ml_config.get("model_path", "insights_generator/trained_model.pkl")
    
    if not model_path.exists():
        print(f"\nERROR: Model not found at {model_path}")
        print("Run 'train' command first to train a model.")
        return 1
    
    print(f"\nLoading model from: {model_path}")
    
    conn = init_insights_db()
    try:
        predictions = predict(conn, model_path)
        
        print("\n" + "-" * 40)
        print("PREDICTIONS:")
        print("-" * 40)
        print(f"  Total predictions: {len(predictions)}")
        
        if predictions:
            print("\n  Top 5 predictions by confidence:")
            sorted_preds = sorted(predictions, key=lambda x: x["confidence"], reverse=True)
            for i, pred in enumerate(sorted_preds[:5], 1):
                print(f"\n  {i}. {pred['game_id']}")
                print(f"     Market: {pred['market']} {pred['side']}")
                print(f"     Direction: {pred['predicted_direction']}")
                print(f"     Confidence: {pred['confidence']:.1%}")
        
    finally:
        conn.close()
    
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """
    Show summary of collected data and model status.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        int: Exit code (0=success, 1=error)
    """
    print("=" * 60)
    print("INSIGHTS GENERATOR - STATUS")
    print("=" * 60)
    
    # Validate config
    errors = validate_config()
    if errors:
        print("\nConfiguration errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("\nConfiguration: OK")
    
    # Check database
    db_path = get_database_path()
    print(f"\nDatabase: {db_path}")
    print(f"  Exists: {db_path.exists()}")
    
    if not db_path.exists():
        print("\nNo data collected yet. Run 'init-db' to initialize.")
        return 0
    
    conn = init_insights_db()
    try:
        # Count headlines
        cursor = conn.execute("SELECT COUNT(*) FROM news_headlines")
        total_headlines = cursor.fetchone()[0]
        
        cursor = conn.execute("SELECT COUNT(*) FROM news_headlines WHERE processed = 0")
        unprocessed = cursor.fetchone()[0]
        
        print(f"\nNews Headlines:")
        print(f"  Total: {total_headlines}")
        print(f"  Unprocessed: {unprocessed}")
        
        # Count structured events
        cursor = conn.execute("SELECT COUNT(*) FROM structured_events")
        total_events = cursor.fetchone()[0]
        
        cursor = conn.execute("""
            SELECT event_type, COUNT(*) as count 
            FROM structured_events 
            GROUP BY event_type 
            ORDER BY count DESC
        """)
        event_counts = cursor.fetchall()
        
        print(f"\nStructured Events:")
        print(f"  Total: {total_events}")
        if event_counts:
            print("  By type:")
            for row in event_counts:
                print(f"    {row['event_type']}: {row['count']}")
        
        # Count lag signals
        cursor = conn.execute("SELECT COUNT(*) FROM market_lag_signals")
        total_signals = cursor.fetchone()[0]
        
        cursor = conn.execute("""
            SELECT leader_provider, lagger_provider, COUNT(*) as count
            FROM market_lag_signals
            GROUP BY leader_provider, lagger_provider
            ORDER BY count DESC
            LIMIT 5
        """)
        signal_pairs = cursor.fetchall()
        
        print(f"\nLag Signals:")
        print(f"  Total: {total_signals}")
        if signal_pairs:
            print("  Top provider pairs:")
            for row in signal_pairs:
                print(f"    {row['leader_provider']} → {row['lagger_provider']}: {row['count']}")

        # Count event impacts
        cursor = conn.execute("SELECT COUNT(*) FROM event_market_impacts")
        total_impacts = cursor.fetchone()[0]

        print(f"\nEvent Impacts:")
        print(f"  Total: {total_impacts}")
        
        # Count game scores
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM game_scores")
            total_scores = cursor.fetchone()[0]
            print(f"\nGame Scores:")
            print(f"  Total: {total_scores}")
        except sqlite3.Error:
            pass

        # Count predictions
        cursor = conn.execute("SELECT COUNT(*) FROM ml_predictions")
        total_predictions = cursor.fetchone()[0]
        
        cursor = conn.execute("""
            SELECT COUNT(*) FROM ml_predictions 
            WHERE prediction_correct IS NOT NULL
        """)
        evaluated = cursor.fetchone()[0]
        
        print(f"\nML Predictions:")
        print(f"  Total: {total_predictions}")
        print(f"  Evaluated: {evaluated}")
        
        # Check model
        ml_config = get_ml_config()
        model_path = PROJECT_ROOT / ml_config.get("model_path", "insights_generator/trained_model.pkl")
        print(f"\nModel:")
        print(f"  Path: {model_path}")
        print(f"  Exists: {model_path.exists()}")
        
    finally:
        conn.close()
    
    return 0


def cmd_init_db(args: argparse.Namespace) -> int:
    """
    Initialize database tables for insights_generator.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        int: Exit code (0=success, 1=error)
    """
    print("=" * 60)
    print("INSIGHTS GENERATOR - DATABASE INIT")
    print("=" * 60)
    
    db_path = get_database_path()
    print(f"\nDatabase path: {db_path}")
    
    print("\nInitializing tables...")
    
    conn = init_insights_db()
    conn.close()
    
    print("Done! Tables created:")
    print("  - news_headlines")
    print("  - structured_events")
    print("  - market_lag_signals")
    print("  - event_market_impacts")
    print("  - ml_predictions")
    print("  - game_scores")
    
    return 0


def main() -> int:
    """
    Main entry point for the CLI.
    
    Returns:
        int: Exit code
    """
    parser = argparse.ArgumentParser(
        prog="insights_generator",
        description="Insights Generator - Sports betting data analysis and pattern detection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s scrape              Fetch news headlines
  %(prog)s analyze             Process headlines with Ollama
  %(prog)s detect-lag          Find lead/lag signals
  %(prog)s event-impacts       Compute event -> market impacts
  %(prog)s status              Show data summary
        """,
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # scrape command
    scrape_parser = subparsers.add_parser("scrape", help="Fetch news headlines from RSS sources")
    
    # analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Process headlines through Ollama NLP")
    analyze_parser.add_argument(
        "--batch-size", "-b",
        type=int,
        help="Number of headlines to process (default: from config)",
    )
    analyze_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show queued headlines without calling Ollama",
    )
    
    # detect-lag command
    lag_parser = subparsers.add_parser("detect-lag", help="Detect lead/lag signals in market history")
    lag_parser.add_argument(
        "--lookback", "-l",
        type=int,
        help="Minutes to look back (default: from config)",
    )
    lag_parser.add_argument(
        "--min-delta", "-d",
        type=float,
        help="Minimum probability delta (default: from config)",
    )

    # event-impacts command
    event_parser = subparsers.add_parser("event-impacts", help="Compute event -> market impact metrics")
    event_parser.add_argument(
        "--pre-window",
        type=int,
        help="Minutes to look back for baseline (default: from config)",
    )
    event_parser.add_argument(
        "--post-window",
        type=int,
        help="Minutes to look forward for impact (default: from config)",
    )
    event_parser.add_argument(
        "--max-age",
        type=int,
        help="Max event age in hours (default: from config)",
    )
    event_parser.add_argument(
        "--min-snapshots",
        type=int,
        help="Minimum snapshots in post-window (default: from config)",
    )
    
    # train command
    train_parser = subparsers.add_parser("train", help="Train ML model on collected data")
    train_parser.add_argument(
        "--model-type", "-m",
        choices=["xgboost", "lightgbm", "linear"],
        default="xgboost",
        help="Model type to train (default: xgboost)",
    )
    
    # score command
    score_parser = subparsers.add_parser("score", help="Score all upcoming games with AI scoring system")

    # predict command
    predict_parser = subparsers.add_parser("predict", help="Run predictions on current data")
    
    # status command
    status_parser = subparsers.add_parser("status", help="Show data and model status")
    
    # init-db command
    init_parser = subparsers.add_parser("init-db", help="Initialize database tables")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Check if enabled
    if not is_enabled():
        print("ERROR: insights_generator is disabled in config.yaml")
        print("Set 'insights_generator.enabled: true' to enable.")
        return 1
    
    # Dispatch to command handler
    commands = {
        "scrape": cmd_scrape,
        "analyze": cmd_analyze,
        "detect-lag": cmd_detect_lag,
        "event-impacts": cmd_event_impacts,
        "score": cmd_score,
        "train": cmd_train,
        "predict": cmd_predict,
        "status": cmd_status,
        "init-db": cmd_init_db,
    }
    
    handler = commands.get(args.command)
    if handler:
        return handler(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
