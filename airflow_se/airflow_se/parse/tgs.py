
"""
Поиск всех TGS-ов в строке
"""
from re import findall
from typing import List, AnyStr

__all__ = [
    'parse_tgs',
]

# шаблон для поиска принципала
pattern = r'([A-Za-z0-9_-]+/[A-Za-z0-9_.-]+@[A-Za-z0-9_.-]+)'

def parse_tgs(text: AnyStr) -> List[AnyStr]:
    return findall(pattern=pattern, string=text)

