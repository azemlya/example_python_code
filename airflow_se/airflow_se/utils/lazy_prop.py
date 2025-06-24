
"""
Декоратор для ленивого свойства
"""

__all__ = ["lazy_property", ]

def lazy_property(fn) -> property:
    """Decorator that makes a property lazy-evaluated"""
    attr_name = "_lazy_" + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _lazy_property

# как вариант ленивого свойства, но оформленный классом
# class cached_property:
#     """Класс-декоратор для ленивых свойств"""
#     def __init__(self, fn):
#         self.fn = fn
#
#     def __get__(self, instance, cls=None):
#         result = instance.__dict__[self.fn.__name__] = self.fn(instance)
#         return result
