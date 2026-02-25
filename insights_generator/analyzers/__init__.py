"""
Analyzers Package
=================

Analysis modules for pattern detection and feature extraction.

Modules:
--------
- lag_detector   : Cross-market lead/lag detection
- nlp_processor  : Ollama-based text → structured features
- event_impact   : Event → market impact attribution
"""

from .lag_detector import detect_lag_signals, analyze_provider_relationships
from .event_impact import compute_event_impacts
from .nlp_processor import process_headlines, extract_structured_features

__all__ = [
    "detect_lag_signals",
    "analyze_provider_relationships", 
    "compute_event_impacts",
    "process_headlines",
    "extract_structured_features",
]
