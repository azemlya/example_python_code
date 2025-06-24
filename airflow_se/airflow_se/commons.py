
"""

"""
from typing import Any, Dict
from pathlib import Path
from pkg_resources import get_distribution

__all__ = [
    'VERSION',
    'AIRFLOW_VERSION',
    'PARAMS_PREFIXES',
    'STR_TO_BOOL_COMPARISON',
    'BEGIN_KEY_FOR_AIRFLOW_SE',
    'END_KEY_FOR_AIRFLOW_SE',
    'CLEAR_MASK_FOR_AIRFLOW_SE',
    'SECMAN_KEY_FOR_SECRET',
    'SECMAN_KEY_FOR_TGT',
    'EMPTY_KEY_SECMAN',
    'TICKET_PREFIX',
    'SQLA_ENGINE_CONF',
]

VERSION_FILE = Path(__file__).parent / 'VERSION'
VERSION = VERSION_FILE.read_text().strip()

AIRFLOW_VERSION = get_distribution('apache-airflow').version

PARAMS_PREFIXES = ('SE_', '', '_SE_', '_', 'AUTH_', '_AUTH_')

STR_TO_BOOL_COMPARISON = {
    'TRUE': True, 'YES': True, 'Y': True, 'ON': True, 'ENABLE': True,
    'FALSE': False, 'NO': False, 'N': False, 'OFF': False, 'DISABLE': False,
}

BEGIN_KEY_FOR_AIRFLOW_SE = f'{"-" * 6}BEGIN{"-" * 6}'
END_KEY_FOR_AIRFLOW_SE = f'{"-" * 7}END{"-" * 7}'
CLEAR_MASK_FOR_AIRFLOW_SE = f'{BEGIN_KEY_FOR_AIRFLOW_SE}|{END_KEY_FOR_AIRFLOW_SE}|\\s'

# ключ для хранения основных секретов
SECMAN_KEY_FOR_SECRET = f'secret_{AIRFLOW_VERSION}'
# ключ для хранения тикет файлов в SecMan
SECMAN_KEY_FOR_TGT = 'krb5ccaches'
EMPTY_KEY_SECMAN = 'no_data_presented'

# префикс для тикет-файлов ПУЗ-ов
TICKET_PREFIX = 'krb5ccache_'

# настройки движка для PostgreSQL БД
SQLA_ENGINE_CONF: Dict[str, Any] = dict(
    pool_size=6,
    max_overflow=11,
    pool_recycle=1800,
    pool_pre_ping=True,
)

