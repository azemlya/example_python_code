
"""
Status Enumeration
"""
from enum import Enum

__all__ = [
    'Status',
]


class Status(Enum):
    """Статус"""
    UNKNOWN = 'Unknown'
    SUCCESS = 'Success'
    FAILED  = 'Failed'

