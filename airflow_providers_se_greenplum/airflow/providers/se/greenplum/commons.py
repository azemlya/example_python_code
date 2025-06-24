"""
Провайдер Greenplum SE
"""
from __future__ import annotations

from pathlib import Path

__all__ = [
    "AUTHOR",
    "VERSION",
    "name_provider",
    "connection_type",
    "provider_info",
]

AUTHOR = "Землянский Алексей Альбертович <Zemlyanskiy.A.Al@sberbank.ru>;"

VERSION_FILE = Path(__file__).parent / "VERSION"
VERSION = VERSION_FILE.read_text().strip()

name_provider = "Greenplum SE"
connection_type = "greenplum_se"

provider_info = {
        'package-name': 'apache-airflow-providers-se-greenplum',
        'name': name_provider,
        'description': f'`{name_provider} <https://greenplum.org/>`__\n',
        'versions': [VERSION, ],
        'additional-dependencies': ['apache-airflow>=2.2.0'],
        'integrations': [
            {
                'integration-name': name_provider,
                'external-doc-url': 'https://greenplum.org/',
                'how-to-guide': [
                    '/docs/apache-airflow-providers-postgres/operators/postgres_operator_howto_guide.rst'
                ],
                'logo': '/integration-logos/postgres/Postgres.png',
                'tags': ['software'],
            }
        ],
        'operators': [
            {
                'integration-name': name_provider,
                'python-modules': ['airflow.providers.se.greenplum.operators.greenplum'],
            }
        ],
        'hooks': [
            {
                'integration-name': name_provider,
                'python-modules': ['airflow.providers.se.greenplum.hooks.greenplum'],
            }
        ],
        'hook-class-names': ['airflow.providers.se.greenplum.hooks.greenplum.GreenplumHookSE'],
        'connection-types': [
            {
                'hook-class-name': 'airflow.providers.se.greenplum.hooks.greenplum.GreenplumHookSE',
                'connection-type': connection_type,
            }
        ],
    }

