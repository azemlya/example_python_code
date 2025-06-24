
"""

"""
from uuid import uuid4
from pathlib import Path
from os import remove
from json import dumps
from typing import Optional, Dict, List, Any

__all__ = [
    "DataPaths",
]


class DataPaths:
    """
    Накопитель случайных сгенерированных путей к файлам
    """
    def __init__(
            self,
            base_path: str,
            inner_dir: Optional[str] = None,
            name: str = "NoName",
            show_del: bool = False,
    ):
        if not isinstance(inner_dir, str) or inner_dir.isspace():
            inner_dir = str(uuid4()).replace('-','')
        self.__name: str = name
        self.__show_del: bool = show_del
        self.__base_path: Path = Path(base_path)
        self.__full_path: Path = self.__base_path.joinpath(inner_dir)
        self.__full_path.mkdir(parents=True, exist_ok=True)
        self.__dict: Dict[str, str] = dict()

    def get(self, key: str) -> str:
        val = self.__dict.get(key)
        if val:
            return str(self.__full_path.joinpath(val))
        else:
            val = str(uuid4()).replace('-','')
            self.__dict[key] = val
            return str(self.__full_path.joinpath(val))

    def items(self):
        return {k: str(self.__full_path.joinpath(v)) for k, v in self.__dict.items()}.items()

    def get_dict(self) -> Dict[str, str]:
        return {k: v for k, v in self.items()}

    def clear(self) -> List[str]:
        ret = []
        for k, v in self.items():
            try:
                remove(v)
                ret.append(f"File `{v}`(key `{k}`) has bin removed")
            except Exception as err:
                ret.append(f"ERROR >> File `{v}`(key `{k}`) don't removed: {err}")
        try:
            self.__full_path.rmdir()
            ret.append(f"Directory `{str(self.__full_path)}` removed")
        except Exception as rm_err:
            ret.append(f"ERROR >> Directory `{str(self.__full_path)}` don't removed: {rm_err}")
        return ret

    def __getattr__(self, item) -> str:
        return self.get(item)

    def __str__(self):
        return dumps(self.get_dict())

    def __repr__(self):
        return f"DataPaths: {self.__str__()}"

    def __del__(self):
        if self.__show_del is True:
            sep = "\n***    "
            print(f"""{'*'*80}{sep}=== Finally clear: `{self.__name}` ==={sep}{sep.join(self.clear())}\n{'*'*80}""")
        else:
            self.clear()

