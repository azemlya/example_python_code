
"""
Провайдер Spark SE
"""
from __future__ import annotations

from pathlib import Path

__all__ = [
    "AUTHOR",
    "VERSION",
    "provider_info",
    "name_provider",
    "connection_type",
]

AUTHOR = "Землянский Алексей Альбертович <Zemlyanskiy.A.Al@sberbank.ru>;"

VERSION_FILE = Path(__file__).parent / "VERSION"
VERSION = VERSION_FILE.read_text().strip()

name_provider = "Spark SE"
connection_type = "spark_se"
connection_type_sql = "spark_se_sql"
connection_type_jdbc = "spark_se_jdbc"

provider_info = {
        "package-name": "apache-airflow-providers-se-spark",
        "name": name_provider,
        "description": f"`{name_provider} <https://spark.apache.org/>`__\n",
        "suspended": False,
        "versions": [VERSION, ],
        "dependencies": ["apache-airflow>=2.4.0", "pyspark"],
        "integrations": [
            {
                "integration-name": name_provider,
                "external-doc-url": "https://spark.apache.org/",
                "how-to-guide": ["/docs/apache-airflow-providers-apache-spark/operators.rst"],
                "logo": "/integration-logos/apache/spark.png",
                "tags": ["apache", "airflow", "spark", "se", ],
            },
        ],
        "operators": [
            {
                "integration-name": name_provider,
                "python-modules": [
                    "airflow.providers.se.spark.operators.spark_jdbc",
                    "airflow.providers.se.spark.operators.spark_sql",
                    "airflow.providers.se.spark.operators.spark_submit",
                ],
            },
        ],
        "hooks": [
            {
                "integration-name": name_provider,
                "python-modules": [
                    "airflow.providers.se.spark.hooks.spark_jdbc",
                    "airflow.providers.se.spark.hooks.spark_sql",
                    "airflow.providers.se.spark.hooks.spark_submit",
                ],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.se.spark.hooks.spark_jdbc.SparkJDBCHookSE",
                "connection-type": connection_type_jdbc,
            },
            {
                "hook-class-name": "airflow.providers.se.spark.hooks.spark_sql.SparkSqlHookSE",
                "connection-type": connection_type_sql,
            },
            {
                "hook-class-name": "airflow.providers.se.spark.hooks.spark_submit.SparkSubmitHookSE",
                "connection-type": connection_type,
            },
        ],
    }

