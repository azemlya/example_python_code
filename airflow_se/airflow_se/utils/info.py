
"""

"""
from itertools import chain
from typing import Dict

__all__ = [
    'info',
    'info_attrs',
]

def info(*args, **kwargs) -> str:
    """
    Возвращает строку с информацией по аргументам/параметрам
    """
    return ', '.join(chain(
        iter(f'[type: {type(x)}, value: {x}]' for x in args),
        iter(f'{k}=[type: {type(v)}, value: {v}]' for k, v in kwargs.items()),
    ))

def info_attrs(obj: object) -> Dict[str, str]:
    """
    Возвращает словарь - имя_аттрибута : информация_об_атрибуте
    """
    ret: Dict[str, str] = dict()
    for x in dir(obj):
        ret[x] = info(getattr(obj, x))
    return ret

