
"""
    ¯\_(ツ)_/¯
"""
from os import environ
from sys import stderr
from pathlib import Path
from base64 import b64encode, b64decode
from cryptography.fernet import Fernet
from itertools import zip_longest
from typing import Union

__all__ = [
    "encrypt",
    "decrypt",
]

af_home = environ.get("AIRFLOW_HOME")
af_home_path: Path
try:
    if not isinstance(af_home, str) or af_home.isspace():
        raise RuntimeError('Environment variable is not set or is spase')
    af_home_path = Path(af_home)
    if not isinstance(af_home_path, Path):
        raise RuntimeError('Path is invalid')
    if not af_home_path.is_absolute():
        raise RuntimeError('Path is not absolute')
    if not af_home_path.exists():
        raise RuntimeError('Path is not exists')
except Exception as afh:
    raise RuntimeError(f'Failed environment variable "AIRFLOW_HOME": {afh}')

root_path: Path
try:
    root_path = af_home_path / 'venv'
    if not root_path.exists():
        root_path.mkdir(parents=True, exist_ok=True)
except Exception as rp:
    raise RuntimeError(f'Failed create path for keys: {rp}')

path1 = root_path / 'generated' / '.5d7dd6b96bd94857af2216db4d9262f4'
path2 = root_path/ 'share' / '.401c901c6fc14228b38a0619415473ff'

file1 = path1 / '.8b08c4bf0ff54b8d83d60e62e4f1d544'
file2 = path2 / '.7a77b4852cc94cc2bb3980e3ccd8f825'

def create_key_files():
    def set_path(path: Path, mode: int):
        if path.exists():
            path.chmod(mode)
        else:
            path.mkdir(mode=mode, parents=True, exist_ok=True)
    def set_file(file: Path, mode: int):
        if file.exists():
            file.chmod(mode)
    if not file1.exists() or not file2.exists():
        set_path(path1, 0o700)
        set_file(file1, 0o600)
        set_path(path2, 0o700)
        set_file(file2, 0o600)
        # key = Fernet.generate_key()
        key = b'ZFLbWwc6bnAhZKWTCP9dNoqcvqecZqDy6wq96tsh6qQ='
        key1, key2 = key[::2], key[1::2]
        with open(file1, "wb") as f:
            f.write(b64encode(key1))
        with open(file2, "wb") as f:
            f.write(b64encode(key2))
    set_file(file1, 0o400)
    set_path(path1, 0o500)
    set_file(file2, 0o400)
    set_path(path2, 0o500)

def load_key() -> bytes:
    try:
        create_key_files()
    except Exception as fc:
        print(f'Failed create key files: {fc}', file=stderr)
    try:
        if file1.exists() and file2.exists():
            with open(file1, "rb") as f:
                key1 = b64decode(f.read())
            with open(file2, "rb") as f:
                key2 = b64decode(f.read())
            key = ''.join([
                i[0] + i[1] if i[0] and i[1] else i[0] if i[0] else i[1] if i[1] else ''
                for i in zip_longest(key1.decode('utf-8'), key2.decode('utf-8'))
            ])
            return key.encode('utf-8')
        raise RuntimeError(f'Files is not exists or not permissions')
    except Exception as fl:
        raise RuntimeError(f'Failed load key: {fl}')

__key = Fernet(load_key())

def encrypt(data: Union[str, bytes]) -> str:
    if isinstance(data, str):
        data = data.encode('utf-8')
    return b64encode(__key.encrypt(data)).decode('utf-8')

def decrypt(data: Union[str, bytes]) -> bytes:
    return __key.decrypt(b64decode(data))

