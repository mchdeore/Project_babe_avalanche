"""
Insights Generator Module
=========================

A self-contained module for sports betting data analysis and pattern detection.

Features:
---------
- News scraping from RSS feeds (ESPN, Rotoworld)
- NLP-based text → structured feature extraction via Ollama
- Cross-market lead/lag detection (bidirectional)
- ML pipeline for line movement prediction (XGBoost/sklearn)

Usage:
------
All features run on-demand via CLI:

    python -m insights_generator.cli scrape       # Fetch news headlines
    python -m insights_generator.cli analyze      # Process with Ollama NLP
    python -m insights_generator.cli detect-lag   # Find lead/lag signals
    python -m insights_generator.cli train        # Train ML model
    python -m insights_generator.cli predict      # Run predictions
    python -m insights_generator.cli status       # Show summary

Architecture:
-------------
- scrapers/     : Data collection (news_scraper.py)
- analyzers/    : Analysis logic (nlp_processor.py, lag_detector.py)
- models/       : ML pipeline (features.py)
- cli.py        : Command-line interface
- config.py     : Module configuration loader
- schema.sql    : Database tables for this module

Database Tables (in main odds.db):
----------------------------------
- news_headlines      : Raw scraped headlines
- structured_events   : Ollama-extracted structured features
- market_lag_signals  : Detected lead/lag relationships
- ml_predictions      : Model prediction outputs
"""

__version__ = "0.1.0"
__author__ = "Insights Generator"

from pathlib import Path

MODULE_ROOT = Path(__file__).parent
PROJECT_ROOT = MODULE_ROOT.parent
