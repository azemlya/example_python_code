
"""
Класс-конструктор по типу Result в Rust
"""
from typing import Optional, Union, Iterable, Any

__all__ = [
    'Result',
]

Error = Union[Iterable[Optional[Any]], Optional[Any]]


class Result:
    """
    Почти как Result в Rust
    """
    @classmethod
    def Err(cls, error: Error = None):
        """Create a Result object for a failed operation"""
        return cls(success=False, error=error)

    @classmethod
    def Ok(cls, value: Optional[Any] = None):
        """Create a Result object for a successful operation"""
        return cls(success=True, value=value)

    def __init__(self, success: bool, value: Optional[Any] = None, error: Error = None):
        if type(success) != bool:
            raise ValueError(f'"success" variable a type "bool" was expected, got type "{type(success)}"')
        self.__success: bool = success
        self.__value: Optional[Any] = value
        self.__error: Error = list()
        self.error = error

    @property
    def success(self) -> bool:
        """True if operation successful, False if failed"""
        return self.__success

    @property
    def failure(self) -> bool:
        """True if operation failed, False if successful"""
        return not self.__success

    @property
    def value(self) -> Optional[Any]:
        """Return value"""
        return self.__value

    @value.setter
    def value(self, value: Optional[Any]):
        self.__success = True
        self.__value = value
        self.__error = list()

    @property
    def error(self) -> Iterable[Optional[Any]]:
        """Return list of errors"""
        return self.__error

    @error.setter
    def error(self, value: Error):
        if value is not None:
            if type(value) == str:
                self.__error.append(value)
            elif isinstance(value, Iterable):
                self.__error.extend(value)
            else:
                self.__error.append(value)
            self.__success = False
            self.__value = None
        else:
            self.__error = list()

    @property
    def error_rev(self) -> Iterable[Optional[Any]]:
        e = self.__error[:]
        e.reverse()
        return e

    def __str__(self):
        if self.success is True:
            return f'[Success] {self.__value}'
        else:
            return f'[Error] {self.__error}'

    def __repr__(self):
        if self.success is True:
            return f'<Result::Ok({self.__value})>'
        else:
            return f'<Result::Err({self.__error})>'

