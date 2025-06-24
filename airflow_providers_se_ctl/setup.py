"""
          Сборка пакета
          =============

    1. Перейти в директорию где лежит файл `setup.py`

    2. Создать среду окружения:
        пример для windows:
            ```"c:\Program Files\Python38\python.exe" -m venv venv```
        пример для linux:
            ```python3.8 -m venv venv```

    3. Активировать среду окружения:
        пример для windows:
            ```venv\Scripts\activate```
        пример для linux:
            ```source venv/bin/activate```

    4. Обновить установщик пакетов:
        ```python -m pip install --upgrade pip```

    5. Установить библиотеки поддержки сборки `setuptools` и `wheel`:
        ```python -m pip install setuptools wheel```

    6.1. Собрать "яйцо":
        ```python setup.py sdist -d <путь_куда_положить>```

    6.2. Собрать "колесо":
        ```python setup.py bdist_wheel -d <путь_куда_положить>```
"""

# from warnings import simplefilter
# simplefilter("ignore")

from os.path import join, dirname
from setuptools import setup
from airflow.providers.se.ctl import (
    __version__ as provider_version,
    __author__ as provider_author,
)

setup(
    name="apache-airflow-providers-se-ctl",
    version=provider_version,
    author=provider_author,
    packages=["airflow.providers.se.ctl",
              "airflow.providers.se.ctl.exceptions",
              "airflow.providers.se.ctl.utils",
              "airflow.providers.se.ctl.hooks",
              "airflow.providers.se.ctl.operators",
              ],
    long_description=open(join(dirname(__file__), "README.md")).read(),
    include_package_data=True,
    entry_points={
        "apache_airflow_provider": [
            "provider_info = airflow.providers.se.ctl.get_provider_info:get_provider_info",
        ],
    },
    install_requires=[
        # "psycopg2-binary>=2.9.2",
    ],
    setup_requires=[
        # "apache-airflow>=2.5.3",
    ],
)
