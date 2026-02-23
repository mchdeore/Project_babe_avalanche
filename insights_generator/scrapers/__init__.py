"""
Scrapers Package
================

Data collection modules for the insights generator.

Modules:
--------
- news_scraper : RSS feed scraping for sports news headlines
"""

from .news_scraper import scrape_news, scrape_all_sources

__all__ = ["scrape_news", "scrape_all_sources"]
