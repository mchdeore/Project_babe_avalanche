"""
Analyzers Package
=================

Analysis modules for pattern detection and feature extraction.

Modules:
--------
- lag_detector   : Cross-market lead/lag detection
- nlp_processor  : Ollama-based text → structured features
"""

from .lag_detector import detect_lag_signals, analyze_provider_relationships
from .nlp_processor import process_headlines, extract_structured_features

__all__ = [
    "detect_lag_signals",
    "analyze_provider_relationships", 
    "process_headlines",
    "extract_structured_features",
]
