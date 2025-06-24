from airflow.exceptions import AirflowException

__all__ = [
    "GreenplumSEEnvError",
    "GreenplumSEDagContextError",
    "GreenplumSEDagArgumentsError",
    "GreenplumSEKerberosError",
    "GreenplumSECursorError",
    "GreenplumSESetRoleError",
]


class GreenplumSEEnvError(AirflowException):
    """Airflow environment error"""


class GreenplumSEDagContextError(AirflowException):
    """DAG context error"""


class GreenplumSEDagArgumentsError(AirflowException):
    """DAG arguments error"""


class GreenplumSEKerberosError(AirflowException):
    """Kerberos receive ticket file error"""


class GreenplumSECursorError(AirflowException):
    """Set cursor type error"""


class GreenplumSESetRoleError(AirflowException):
    """Set role error"""

