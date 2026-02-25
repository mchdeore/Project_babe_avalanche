"""
Scrapers Package
================

Data collection modules for the insights generator.

Modules:
--------
- news_scraper : RSS feed scraping for sports news headlines
- api_scraper  : API-based scrapers (reddit, weather, ESPN)
"""

from .news_scraper import scrape_news, scrape_all_sources
from .api_scraper import scrape_api

__all__ = ["scrape_news", "scrape_all_sources", "scrape_api"]
