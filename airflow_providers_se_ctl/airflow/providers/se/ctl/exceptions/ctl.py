"""

"""

from airflow.exceptions import AirflowException

__all__ = [
    "CtlMethodNotFound",
]


class CtlMethodNotFound(AirflowException):
    """Unexpected HTTP Method"""