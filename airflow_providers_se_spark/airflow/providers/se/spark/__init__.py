"""
Провайдер Spark SE для Airflow
"""
from __future__ import annotations

from .commons import VERSION, AUTHOR

__all__ = ["__author__", "__version__", ]

__author__ = AUTHOR
__version__ = VERSION

# import packaging.version

# try:
#     from airflow import __version__ as airflow_version
# except ImportError:
#     from airflow.version import version as airflow_version

# if packaging.version.parse(airflow_version) < packaging.version.parse("2.4.0"):
#     raise RuntimeError(
#         f"The package `apache-airflow-providers-se-spark:{__version__}` requires Apache Airflow 2.4.0+"  # NOQA: E501
#     )
