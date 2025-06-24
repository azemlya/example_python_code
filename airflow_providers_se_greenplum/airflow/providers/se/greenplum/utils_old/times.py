"""
Модуль для различных time утилит
"""
import datetime
from typing import Union, Tuple, Dict

__all__ = ["timedelta_to_human_format", ]


def timedelta_to_human_format(duration: datetime.timedelta, as_dict: bool=False) -> Union[Tuple, Dict]:
    days, seconds = duration.days, duration.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = (seconds % 60)
    if as_dict:
        return dict(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return days, hours, minutes, seconds