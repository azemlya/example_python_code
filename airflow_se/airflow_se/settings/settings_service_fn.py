
"""
Сервисные функции
"""
from typing import Optional, Union, Iterable, Dict, Any
from os import environ
from socket import getfqdn
from re import sub as re_sub
from json import loads as parse_json
from datetime import time, timedelta

from airflow_se.commons import PARAMS_PREFIXES
from airflow_se.as_rust import Result
from airflow_se.utils import info

from .settings_constants import STR_AS_TRUE, STR_AS_FALSE, SEPARATOR

__all__ = [
    "get_param_env",
    "get_fqdn",
    "get_af_home_dir",
    "check_str",
    "check_int",
    "check_bool",
    "check_json",
    "check_time",
    "check_timedelta",
    "check_range_int",
]

# PATH = f"{path.abspath(__file__)}::"
PATH = f"Airflow_SE::settings::service_fn::"

def iter_to_str(value: Optional[Any] = None, separator: Optional[str] = SEPARATOR) -> str:
    """Коллекции, преобразует в строку с разделителями"""
    if isinstance(value, str):
        return value
    elif isinstance(value, Iterable):
        return f"{separator if isinstance(separator, str) else SEPARATOR}".join(value)
    return str(value)

def get_param_env(key: str, prefix: Union[Iterable[str], str, None] = PARAMS_PREFIXES) -> Result:
    """Ищет параметр в переменных окружения, учитывая префиксы."""
    _key = check_str(key)
    if _key.failure:
        _key.error = f"{PATH}get_param_env() >> parameter key is invalid, a non-empty "\
                     f"string was expected // {info(key=key)}"
        return _key
    if type(prefix) == str:
        res = check_str(environ.get(f"{prefix.strip()}{_key.value}"))
        if res.success:
            return res
    elif isinstance(prefix, Iterable):
        for p in prefix:
            res = check_str(environ.get(f"{p.strip()}{_key.value}"))
            if res.success:
                return res
    elif prefix is None:
        res = check_str(environ.get(_key.value))
        if res.success:
            return res
    return Result.Err(f"{PATH}get_param_env() >> parameter \"{_key.value}\" is not found in environment variables")

def get_fqdn() -> Result:
    """Доменное имя сервера, берётся из ОС функцией getfqdn()."""
    res = check_str(getfqdn())
    if res.success:
        return res
    res.error = f"{PATH}get_fqdn() >> fqdn not found"
    return res

def get_af_home_dir() -> Result:
    """Домашняя директория Airflow (переменная окружения `AIRFLOW_HOME`)"""
    param = "AIRFLOW_HOME"
    res = get_param_env(param, None)
    if res.success:
        return res
    res.error = f"{PATH}get_af_home_dir() >> parameter \"{param}\" is not found in the environment variables"
    return res

def get_af_conf_filename() -> Result:
    """Путь к файлу настроек `airflow.cfg` (переменная окружения `AIRFLOW_CONFIG`)"""
    param = "AIRFLOW_CONFIG"
    res = get_param_env(param, None)
    if res.success:
        return res
    res.error = f"{PATH}get_af_conf_filename() >> parameter \"{param}\" is not found in the environment variables"
    return res

def check_str(value: Optional[Any] = None, default: Optional[Any] = None) -> Result:
    """
    Возвращает Result::Ok(), содержащий очищенную от хвостовых пробелов строку или ошибку Result::Err().
    Пустые строки, не считаются за строки!!!
    """
    _value = value.strip() if type(value) == str and not value.isspace() else None
    if _value:
        return Result.Ok(_value)
    _default = default.strip() if type(default) == str and not default.isspace() else None
    if _default:
        return Result.Ok(_default)
    return Result.Err(f"{PATH}check_str() >> failed check for a non-empty string "
                      f"// {info(value=value, default=default)}")

def check_bool(value: Optional[Any] = None, default: Optional[Any] = None,
               as_true: Union[Iterable[str], str, None] = STR_AS_TRUE,
               as_false: Union[Iterable[str], str, None] = STR_AS_FALSE,
               ) -> Result:
    """Проверяет значение на boolean, проверяя различные типы"""
    def conv_bool(v: Optional[Any], as_tr: Union[Iterable[str], str, None], as_fl: Union[Iterable[str], str, None],
                  ) -> Result:
        if type(v) == bool:
            return Result.Ok(v)
        elif isinstance(v, (int, float)):
            return Result.Ok(bool(int(v)))
        elif isinstance(v, str):
            _v, res = v.strip().lower(), Result.Ok()
            if isinstance(as_tr, str):
                _v = True if _v == as_tr.strip().lower() else _v
                if type(_v) == bool:
                    return Result.Ok(_v)
            elif isinstance(as_tr, Iterable):
                _v = True if _v in as_tr else _v
                if type(_v) == bool:
                    return Result.Ok(_v)
            elif as_tr is None:
                pass
            else:
                res.error = f"{PATH}check_bool()::conv_bool() >> invalid parameter // {info(as_tr=as_tr)}"
            if isinstance(as_fl, str):
                _v = True if _v == as_fl.strip().lower() else _v
                if type(_v) == bool:
                    return Result.Ok(_v)
            elif isinstance(as_fl, Iterable):
                _v = False if _v in as_fl else _v
                if type(_v) == bool:
                    return Result.Ok(_v)
            elif as_fl is None:
                pass
            else:
                res.error = f"{PATH}check_bool()::conv_bool() >> invalid parameter // {info(as_fl=as_fl)}"
            res.error = f"{PATH}check_bool()::conv_bool() >> failed convert string \"{v.strip()}\" to boolean"
            return res
        return Result.Err(f"{PATH}check_bool()::conv_bool() >> failed convert type {type(v)} to boolean")
    _value = conv_bool(value, as_true, as_false)
    if _value.success:
        return _value
    _default = conv_bool(default, as_true, as_false)
    if _default.success:
        return _default
    _value.error = _default.error
    _value.error = f"{PATH}check_bool() >> failed convert to boolean: // {info(value=value, default=default)}"
    return _value

def check_int(value: Optional[Any] = None, default: Optional[Any] = None) -> Result:
    """Проверяет значение на integer, проверяя различные типы"""
    def conv_int(v: Optional[Any]) -> Result:
        if type(v) == int:
            return Result.Ok(v)
        elif isinstance(v, (float, bool)):
            return Result.Ok(int(v))
        elif isinstance(v, (str, bytes)):
            try:
                return Result.Ok(int(v.strip()))
            except Exception as e:
                return Result.Err(f"{PATH}check_int()::conv_int() >> failed convert string|bytes \"{v.strip()}\" "
                                  f"to integer: {e}")
        return Result.Err(f"{PATH}check_int()::conv_int() >> failed convert type {type(v)} to integer")
    _value = conv_int(value)
    if _value.success:
        return _value
    _default = conv_int(default)
    if _default.success:
        return _default
    _value.error = _default.error
    _value.error = f"{PATH}check_int() >> failed convert to integer: // {info(value=value, default=default)}"
    return _value

def check_json(value: Optional[Any] = None, default: Optional[Any] = None) -> Result:
    """Проверяет на строку или байтовую строку, и пробует парсить json, иначе возвращает переданное значение."""
    def conv_json(v: Optional[Any]) -> Result:
        if isinstance(v, bytes):
            v = v.decode("utf-8")
        if isinstance(v, str):
            v = v.strip()
            try:
                try:
                    return Result.Ok(parse_json(v))
                except:
                    return Result.Ok(parse_json(re_sub("\\s", "", v)))
            except Exception as e:
                return Result.Err(f"{PATH}check_json()::conv_json() >> failed parse \"{v}\" as json: {e}")
        return Result.Ok(v)
    _value = conv_json(value)
    if _value.success:
        return _value
    _default = conv_json(default)
    if _default.success:
        return _default
    _value.error = _default.error
    _value.error = f"{PATH}check_json() >> failed parse json: // {info(value=value, default=default)}"
    return _value

def check_time(value: Optional[Any] = None, default: Optional[Any] = None) -> Result:
    """Проверяет значение на time, проверяя различные типы"""
    def conv_time(v: Optional[Any]) -> Result:
        if type(v) == time:
            return Result.Ok(v)
        elif isinstance(v, Dict):
            try:
                return Result.Ok(time(**v))
            except Exception as e:
                return Result.Err(f"{PATH}check_time()::conv_time() >> failed convert dict \"{v}\" to time: {e}")
        return Result.Err(f"{PATH}check_time()::conv_time() >> failed convert type {type(v)} to time")
    _value = conv_time(value)
    if _value.success:
        return _value
    _default = conv_time(default)
    if _default.success:
        return _default
    _value.error = _default.error
    _value.error = f"{PATH}check_time() >> failed convert to time: // {info(value=value, default=default)}"
    return _value

def check_timedelta(value: Optional[Any] = None, default: Optional[Any] = None) -> Result:
    """Проверяет значение на datetime, проверяя различные типы"""
    def conv_timedelta(v: Optional[Any]) -> Result:
        if type(v) == timedelta:
            return Result.Ok(v)
        elif type(v) == dict:
            try:
                return Result.Ok(timedelta(**v))
            except Exception as e:
                return Result.Err(f"{PATH}check_timedelta()::conv_timedelta() >> failed convert dict \"{v}\" to "
                                  f"timedelta: {e}")
        return Result.Err(f"{PATH}check_timedelta()::conv_timedelta() >> failed convert type {type(v)} to timedelta")
    _value = conv_timedelta(value)
    if _value.success:
        return _value
    _default = conv_timedelta(default)
    if _default.success:
        return _default
    _value.error = _default.error
    _value.error = f"{PATH}check_timedelta() >> failed convert to timedelta: // {info(value=value, default=default)}"
    return _value

def check_range_int(value: Optional[int] = None,
                    min_value: Optional[int] = None,
                    max_value: Optional[int] = None,
                    def_value: Optional[int] = None,
                    ) -> Result:
    """
    Проверяет value на вхождение в диапазон.
    Когнитивная сложность зашкаливает. Много проверок. Надо придумать как упростить.
    """
    if type(min_value) == int and type(max_value) == int and min_value > max_value:
        return Result.Err(f"{PATH}check_range_int() >> invalid parameters: "
                          f"min_value({min_value}) > max_value({max_value})")
    res_err = Result.Err(f"""{PATH}check_range_int() >> parameter must be range """
                         f"""{min_value if min_value else "<minus_infinity>"}..."""
                         f"""{max_value if max_value else "<plus_infinity>"} """
                         f"""// {info(value=value, min_value=min_value, max_value=max_value, def_value=def_value)}""")
    if type(value) == int:
        if type(min_value) == int:
            if type(max_value) == int:
                if min_value <= value <= max_value:
                    return Result.Ok(value)
                elif type(def_value) == int and min_value <= def_value <= max_value:
                    return Result.Ok(def_value)
                elif min_value <= max_value:
                    # возвращаем среднее значение
                    return Result.Ok((min_value + max_value) // 2)
                return res_err
            elif value >= min_value:
                return Result.Ok(value)
            elif type(def_value) == int and def_value >= min_value:
                return Result.Ok(def_value)
            return Result.Ok(min_value)
        elif type(max_value) == int:
            if value <= max_value:
                return Result.Ok(value)
            elif type(def_value) == int and def_value <= max_value:
                return Result.Ok(def_value)
            return Result.Ok(max_value)
        return Result.Ok(value)
    elif type(def_value) == int:
        if type(min_value) == int:
            if type(max_value) == int:
                if min_value <= max_value:
                    if min_value <= def_value <= max_value:
                        return Result.Ok(def_value)
                    return Result.Ok((min_value + max_value) // 2)
                return res_err
            elif def_value >= min_value:
                return Result.Ok(def_value)
            return Result.Ok(min_value)
        return Result.Ok(def_value)
    elif type(min_value) == int:
        if type(max_value) == int:
            if min_value <= max_value:
                return Result.Ok((min_value + max_value) // 2)
            return res_err
        return Result.Ok(min_value)
    elif type(max_value) == int:
        return Result.Ok(max_value)
    return res_err

