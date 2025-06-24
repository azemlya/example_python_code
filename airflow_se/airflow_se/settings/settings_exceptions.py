
"""
Исключения
"""

__all__ = [
    "SettingsError",
    "SettingsParamError",
]


class SettingsError(Exception):
    """Settings Error"""


class SettingsParamError(SettingsError):
    """Settings Parameter Error"""

