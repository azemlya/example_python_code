
"""

"""
from string import ascii_letters, digits
from secrets import choice
from typing import Optional

__all__ = [
    'gen_pass',
]

def gen_pass(length: int = 32, extended_chars: Optional[str] = None) -> str:
    """
    Генерирует случайный пароль из латинских букв в верхнем и нижнем регистре,
    цифр и дополнительных символов пунктуации, указанных в extended_chars, если там не пусто.
    """
    return ''.join([choice(ascii_letters + digits + extended_chars if extended_chars else '') for _ in range(length)])

