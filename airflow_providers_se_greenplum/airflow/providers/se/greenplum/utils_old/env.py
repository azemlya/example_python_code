"""
Работа с переменными окружения
"""
from os import environ
from typing import Optional, Dict

__all__ = ["get_all_envs", "get_env", "add_env", "pop_env", ]

def __check_is_str(func_name: str, param_name: str, value: Optional[str]):
    if type(value) != str:
        raise ValueError(f"invalid parameter `{param_name}` in function `{func_name}`: \
                           value=`{value}`, type=`{type(value)}`")

def __check_is_space(func_name: str, param_name: str, value: Optional[str]):
    if value.isspace():
        raise ValueError(f"invalid parameter `{param_name}` in function `{func_name}`: \
                           value is space")

def get_all_envs() -> Dict:
    return dict(**environ)

def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Получает значение переменной окружения"""
    __check_is_str("get_env", "key", key)
    __check_is_space("get_env", "key", key)
    return environ.get(key, default)

def add_env(key: str, value: str):
    """Добавляет переменную окружения со значением value или перезаписывает существующую"""
    __check_is_str("add_env", "key", key)
    __check_is_space("add_env", "key", key)
    __check_is_str("add_env", "value", value)
    environ[key] = value

def pop_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Удаляет переменную окружения"""
    __check_is_str("pop_env", "key", key)
    __check_is_space("pop_env", "key", key)
    return environ.pop(key, default)

