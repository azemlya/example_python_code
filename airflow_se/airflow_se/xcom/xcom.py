
"""

"""
from typing import Any
from ..obj_imp import BaseXCom

__all__ = ['XComSE', ]


class XComSE(BaseXCom):
    """

    """

    @staticmethod
    def serialize_value(
        value,
        key = None,
        task_id = None,
        dag_id = None,
        run_id = None,
        map_index = None,
        **kwargs
    ) -> Any:
        return '{}'

    @staticmethod
    def deserialize_value(result) -> Any:
        return dict()

    def orm_deserialize_value(self) -> Any:
        return dict()

