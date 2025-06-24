
"""
Обёртка над модулем os
"""
from os import getpid

__all__ = [
    "get_pid",
]

def get_pid():
    return getpid()

