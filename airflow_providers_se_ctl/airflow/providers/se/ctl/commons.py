from . import __version__ as pkg_version

__all__ = ["provider_info", "name_provider", "connection_type", ]

name_provider = "CTL SE"
connection_type = "ctl_se"

provider_info = {
        'package-name': 'apache-airflow-providers-se-ctl',
        'name': name_provider,
        'description': f'`{name_provider} <https://confluence.sberbank.ru/display/UMPR/AirflowSE+Home>`__\n',
        'versions': [pkg_version],
        'additional-dependencies': ['apache-airflow>=2.2.0'],
        'integrations': [
            {
                'integration-name': name_provider,
                'external-doc-url': 'https://confluence.sberbank.ru/display/UMPR/AirflowSE+Home',
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
                'python-modules': ['airflow.providers.se.ctl.operators.ctl'],
            }
        ],
        'hooks': [
            {
                'integration-name': name_provider,
                'python-modules': ['airflow.providers.se.ctl.hooks.ctl'],
            }
        ],
        'hook-class-names': ['airflow.providers.se.ctl.hooks.ctl.CtlHookSE'],
        'connection-types': [
            {
                'hook-class-name': 'airflow.providers.se.ctl.hooks.ctl.CtlHookSE',
                'connection-type': connection_type,
            }
        ],
    }

