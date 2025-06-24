
"""
Работа с переменными окружения
"""
from os import environ
from typing import Optional, Dict

from .info import info

__all__ = [
    'get_all_envs',
    'update_envs',
    'get_env',
    'add_env',
    'pop_env',
]

def __check_str(func_name: str, param_name: str, value: Optional[str]):
    if not isinstance(value, str):
        raise ValueError(f'invalid parameter "{param_name}" in function "{func_name}": {info(value)}')
    if value.isspace():
        raise ValueError(f'invalid parameter "{param_name}" in function "{func_name}": value is space')

def get_all_envs() -> Dict[str, str]:
    return dict(**environ)

def update_envs(**kwargs):
    environ.update(**kwargs)

def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Получает значение переменной окружения"""
    __check_str('get_env', 'key', key)
    return environ.get(key, default)

def add_env(key: str, value: str):
    """Добавляет переменную окружения со значением value или перезаписывает существующую"""
    __check_str('add_env', 'key', key)
    __check_str('add_env', 'value', value)
    environ[key] = value

def pop_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Удаляет переменную окружения"""
    __check_str('pop_env', 'key', key)
    try:
        return environ.pop(key, default)
    except:
        return default

