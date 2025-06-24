
"""
Модуль для различных time утилит
"""
import time
import datetime
from typing import Optional, Union, Tuple, Dict

__all__ = [
    "timedelta_to_human_format",
]

def timedelta_to_human_format(duration: datetime.timedelta, as_dict: bool=False
                              ) -> Union[Tuple[int, int, int, int], Dict[str, int]]:
    days, seconds = duration.days, duration.seconds
    hours, minutes, seconds = seconds // 3600, (seconds % 3600) // 60, seconds % 60
    if as_dict:
        return dict(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return days, hours, minutes, seconds

# Отказался от использования декоратора, т.к. это лишает гибкости
def sleep(timeout: int = 300, retry: Optional[int] = None):
    """
    Декоратор на засыпание.
    Если декорируемая функция что-то возвращает,
    то всё останавливается и возвращается это значение.
    Если возвращается None, то делается пауза
    в ```timeout``` секунд, и снова вызывается
    функция с теми же параметрами.
    ```retry``` - количество запусков.
    Эти параметры могут быть переданы
    декорируемой функции, такая передача параметров
    приоритетнее, чем переданные в декораторе.
    """
    def the_decorator(function):
        def wrapper(*args, **kwargs):
            _retry: Optional[int] = kwargs.get("retry") or retry
            _timeout: int = kwargs.get("timeout") or timeout or 300  # на случай, если в timeout явно передадут None
            retries = 0
            while (_retry is None) or (retries < _retry):
                value = function(*args, **kwargs)
                if value:
                    return value
                time.sleep(_timeout)
                retries += 1
        return wrapper
    return the_decorator

