# src/naq/serializers.py
"""
Backward compatibility module for serialization utilities.

This module re-exports all serialization utilities from the new location
at naq.utils.serialization to maintain backward compatibility with existing code.

For new code, please import directly from naq.utils.serialization.
"""

# Re-export all serialization utilities from the new location
from .utils.serialization import (
    Serializer,
    PickleSerializer,
    JsonSerializer,
    get_serializer,
    _normalize_retry_strategy,
)

# Re-export for backward compatibility
__all__ = [
    "Serializer",
    "PickleSerializer",
    "JsonSerializer",
    "get_serializer",
    "_normalize_retry_strategy",
]