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
VERSION = "3.0.0"

name_provider = "Apache Hive SE"

provider_info = {
        "package-name": "apache-airflow-providers-se-hive",
        "name": name_provider,
        "description": f"`{name_provider} <https://hive.apache.org/>`__\n",
        "suspended": False,
        "versions": [VERSION, ],
        "dependencies": [
            "apache-airflow>=2.4.0",
            "apache-airflow-providers-common-sql>=1.3.1",
            "hmsclient>=0.1.0",
            "pandas>=0.17.1",
            "pyhive[hive_pure_sasl]>=0.7.0",
            "thrift>=0.9.2",
        ],
        "integrations": [
            {
                "integration-name": name_provider,
                "external-doc-url": "https://hive.apache.org/",
                "how-to-guide": ["/docs/apache-airflow-providers-apache-hive/operators.rst"],
                "logo": "/integration-logos/apache/hive.png",
                "tags": ["apache", "airflow", "hive", "se", ],
            },
        ],
        "operators": [
            {
                "integration-name": name_provider,
                "python-modules": [
                    "airflow.providers.se.hive.operators.hive",
                    "airflow.providers.se.hive.operators.hive_stats",
                ],
            },
        ],
        "hooks": [
            {
                "integration-name": name_provider,
                "python-modules": [
                    "airflow.providers.se.hive.hooks.hive",
                ],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.se.hive.hooks.hive.HiveCliHookSE",
                "connection-type": "hive_cli_se",
            },
            {
                "hook-class-name": "airflow.providers.se.hive.hooks.hive.HiveMetastoreHookSE",
                "connection-type": "hive_metastore_se",
            },
        ],
    }

