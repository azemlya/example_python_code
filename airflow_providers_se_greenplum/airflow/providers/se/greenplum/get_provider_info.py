"""
Provider info
"""
from __future__ import annotations

from .commons import provider_info

__all__ = [
    "get_provider_info",
]

def get_provider_info():
    return provider_info

