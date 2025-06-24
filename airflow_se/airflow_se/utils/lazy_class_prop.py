
"""
Ленивое свойство класса, декоратор (эксперимент)
"""

__all__ = [
    "lazy_class_property",
]


class lazy_class_property:
    """Lazy class property, decorator (experimental)"""
    def __init__(self, fn):
        self.fn = fn
        self.__doc__ = fn.__doc__
        self.__name__ = fn.__name__

    def __get__(self, obj, cls=None):
        if cls is None:
            cls = type(obj)
        if not hasattr(cls, "_intern") or any(
            cls._intern is getattr(superclass, "_intern", []) for superclass in cls.__mro__[1:]
        ):
            cls._intern = {}
        attr_name = self.fn.__name__
        if attr_name not in cls._intern:
            cls._intern[attr_name] = self.fn(cls)
        return cls._intern[attr_name]

