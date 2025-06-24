#!/usr/bin/env python
#
#       Сборка пакета
#       =============
#
# 1. Перейти в директорию где лежит файл `setup.py`
#
# 2. Создать среду окружения:
#     пример для windows:
#         "c:\Program Files\Python38\python.exe" -m venv venv
#     пример для linux:
#         python3.8 -m venv venv
#
# 3. Активировать среду окружения:
#     пример для windows:
#         venv\Scripts\activate
#     пример для linux:
#         source venv/bin/activate
#
# 4. Обновить установщик пакетов:
#     python -m pip install --upgrade pip
#
# 5. Установить библиотеки поддержки сборки `setuptools` и `wheel`:
#     python -m pip install setuptools wheel
#
# 6.1. Собрать egg ("яйцо"):
#     python setup.py sdist -d <путь_куда_положить>
#
# 6.2. Собрать wheel ("колесо"):
#     python setup.py bdist_wheel -d <путь_куда_положить>
#

from setuptools import setup

setup()
