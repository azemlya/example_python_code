
"""

"""
from json import dumps, loads, detect_encoding, JSONEncoder, JSONDecoder, JSONDecodeError

__all__ = [
    'decode_json',
    'encode_json',
    'detect_encoding',
    'JSONEncoder',
    'JSONDecoder',
    'JSONDecodeError',
]

def decode_json(*args, **kwargs):
    """
    Преобразование объекта (словаря, списка и т.д.) в строку json.
    -----------------------------------------------------------------
    Реинкарнация функции json.dumps()
    Параметры точно такие же.
    """
    return dumps(*args, **kwargs)

def encode_json(*args, **kwargs):
    """
    Преобразование строки в объект (словарь, список и т.д.)
    -----------------------------------------------------------------
    Реинкарнация функции json.loads()
    Параметры точно такие же.
    """
    return loads(*args, **kwargs)

