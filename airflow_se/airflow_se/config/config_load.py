
"""
Загрузка конфига
"""
from os import environ as env
from sys import modules as sys_modules, meta_path as sys_meta_path
from types import ModuleType
from importlib.util import spec_from_file_location, module_from_spec
from importlib.machinery import ModuleSpec
from importlib.abc import Loader, MetaPathFinder
from pathlib import Path
from typing import Union, Iterable, Any

from airflow_se.commons import PARAMS_PREFIXES, STR_TO_BOOL_COMPARISON

module_name = 'airflow_se_config'
airflow_se_config_file = 'airflow_se_config.py'


class StringLoader(Loader):
    def __init__(self, modules):
        self._modules = modules

    def has_module(self, fullname):
        return fullname in self._modules

    def create_module(self, spec):
        if self.has_module(spec.name):
            module = ModuleType(spec.name)
            exec(self._modules[spec.name], module.__dict__)
            return module

    def exec_module(self, module):
        pass


class StringFinder(MetaPathFinder):
    def __init__(self, loader):
        self._loader = loader

    def find_spec(self, fullname, path, target=None):
        if self._loader.has_module(fullname):
            return ModuleSpec(fullname, self._loader)


# загрузка конфига, если он есть, если нет, то будет пустой namespace
try:
    airflow_home = env.get('AIRFLOW_HOME')
    if not airflow_home:
        raise RuntimeError('Environment variable "AIRFLOW_HOME" don\'t set')
    airflow_se_config_path = Path(airflow_home).joinpath(airflow_se_config_file)
    if not airflow_se_config_path.exists():
        finder = StringFinder(StringLoader(dict(airflow_se_config='')))
        sys_meta_path.append(finder)
    else:
        spec = spec_from_file_location(module_name, airflow_se_config_path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        sys_modules[module_name] = module
    import airflow_se_config
except:
    print('=== !!! Configuration SE don\'t initialise !!! ===')
    raise

__all__ = [
    'get_config_value',
]

def get_config_value(key: str, prefix: Union[Iterable[str], str, None] = PARAMS_PREFIXES, default: Any = None) -> Any:
    def comparison(v) -> Any:
        if isinstance(v, str):
            _v = v.strip().upper()
            if _v == 'NONE':
                return None
            __v = STR_TO_BOOL_COMPARISON.get(_v)
            if isinstance(__v, bool):
                return __v
        return v
    if not isinstance(key, str) or key.isspace():
        return comparison(default)
    key = key.strip()
    if prefix is None or (isinstance(prefix, str) and prefix.isspace()):
        val = env.get(key)
        if val is not None:
            return comparison(val)
        if hasattr(airflow_se_config, key):
            return comparison(getattr(airflow_se_config, key))
    elif isinstance(prefix, str):
        _key = f'{prefix.strip()}{key}'
        val = env.get(_key)
        if val is not None:
            return comparison(val)
        if hasattr(airflow_se_config, _key):
            return comparison(getattr(airflow_se_config, _key))
    elif isinstance(prefix, Iterable):
        for p in prefix:
            if isinstance(p, str):
                _key = f'{p.strip()}{key}'
                val = env.get(_key)
                if val is not None:
                    return comparison(val)
        for p in prefix:
            if isinstance(p, str):
                _key = f'{p.strip()}{key}'
                if hasattr(airflow_se_config, _key):
                    return comparison(getattr(airflow_se_config, _key))
    return comparison(default)

