<table><tr><td>Пакет расширений Airflow Sber Edition (SE) для Apache AirFlow >=2.4.0</td><td>&copy; 2023</td></tr></table>

# _ОПИСАНИЕ_

---

## Реализованный функционал

1. Аутентификация пользователя в IPA/AD и авторизация по кэшу LDAP-каталога.
    - Пользователь входит в WebUI/RESTAPI по логину/паролю (пароль нигде не сохраняется и
      используется исключительно в двух шагах - получение TGT-билета Kerberos и при PAM аутентификации);
    - Получение TGT-билета Kerberos для сервисного ТУЗа airflow занимается отдельный процесс airflow kerberos c
      использованием keytab ТУЗа;
    - Получение TGT-билета Kerberos входящего пользователя (используется логин/пароль);
    - Многоэтапная аутентификация пользователя по стандарту GSSAPI (используется логин/TGT-билеты из предыдущих шагов);
    - При успешной аутентификации в IPA, опционально происходит аутентификация PAM (используется логин/пароль);
    - Авторизация пользователя по последнему валидному кэшу LDAP из БД и назначение роли
      в соответствии с маппингом групп LDAP с ролевой моделью Airflow
      (кэш LDAP получает и сохраняет в БД отдельный процесс `se_ldap`);
    - При успешном завершении всех предыдущих шагов, в БД заносится новый пользователь (обновляется существующий);
    - Пользователь допускается в WebUI/RESTAPI с соответствующей ролью из LDAP;
    - В БД обновляется информация по действиям пользователя (внутренний аудит);
2. Получение данных по группам и пользователям LDAP происходит по расписанию из файла настроек и кэшируется в
  БД процессом `se_ldap`.
    - При заходе пользователя, сбрасываются все его предыдущие роли. Например, вошёл пользователь с ролью
      "Admin" и назначил роль "Admin" другому пользователю, чтоб другой пользователь смог получить доступ к чему-то.
      Такое не работает, т.к., при следующем заходе, роль будет взята из последнего кэша LDAP и перезаписана.
      Проще говоря, аутентификация и авторизация происходит вне зависимости от информации в БД и всегда
      происходит централизованно и перезаписывается в БД.
3. Интеграция с ТС Журналирование и ППРБ Аудит.
    - Процесс `se_kafka` забирает события аудита из БД, форматирует и отправляет в Кафку в соответствии с
      настройками.
4. Обновление(`renew`) пользовательских ticket-файлов (продлевает lifetime тикетов):
    - Процесс `se_ticketman` сканирует директории с тикет-файлами и продлевает их (`kinit -R -c /path/to/ticket/file`).

> Примечание: Поддерживается аутентификация AD или IPA+LDAP. Можно использовать любую, они одинаковы с
> точки зрения приложения, т.к. поддерживают стандарт протоколов (сетевые протоколы одинаковые).
> Для простоты, далее буду использовать терминологию IPA и LDAP, т.к. AD, по сути, то же самое.

---

## ТУЗы Airflow

1. Сервисный SPN airflow. Заводится в IPA/AD один раз. Это единый SPN и, скорее всего, он уже существует.
Нужно получить к нему keytab привязанный к хосту.
2. ТУЗ для LDAP. Заводится в IPA/AD. Особых групп и прав не требуется. Обычный ТУЗ.
Нужно получить keytab с привязкой к хосту.

> Для соединения с Kafka(ППРБ Аудит) ТУЗ не требуется, там нужен клиентский сертификат.

#### Прописывание ТУЗов

И SPN и ТУЗ предоставляются с keytab-ами (пароли не нужны и не предусмотрено их использование, только keytab-ы).

Кейтабы, обычно, выдаются с привязкой к хосту, чтоб не было возможности их использовать для получения TGT-билетов
с других хостов. Поэтому, оформлять ЗНО нужно с указанием хоста и IP адреса сервера. SPN с кейтабами, с других
хостов, работать не будут и будут заблокированы в IPA. Советую так не делать. **Инцидент кибербезопасности!**

* Сервисный SPN Airflow, имеет принципал вида:<br>
  `airflow/tklis-supd00016.dev.df.sbrf.ru@DEV.DF.SBRF.RU`<br>
  _Данный SPN используется для аутентификации входящих пользователей в IPA._

> 
> Прописывается в файл `airflow.cfg` в секцию `[kerberos]`. Пример:<br>
> ```
> [kerberos]
> ccache = /opt/airflow/secret/tgt/krb5cc_airflow_tklis-supd00016
> 
> # gets augmented with fqdn
> principal = airflow/tklis-supd00016.dev.df.sbrf.ru@DEV.DF.SBRF.RU
> reinit_frequency = 3600
> kinit_path = kinit
> keytab = /opt/airflow/secret/airflow_tklis-supd00016.keytab
> ticket_lifetime = 8h
> renew_until = 7d
> ```
> 
> > 
> > Получением TGT-билетов занимается процесс `venv/bin/airflow kerberos`, он обязательно должен быть запущен,
> > иначе не будет работать аутентификация входящих пользователей в IPA, и не будет работать back-end
> > аутентификация (RESTAPI).

* ТУЗ для LDAP, обычный ТУЗ, пример принципала:<br>
  `u_ts_testonly_airflow@DEV.DF.SBRF.RU`<br>
  _Данный ТУЗ используется для получения кэша LDAP._

> 
> Этот ТУЗ прописывается в файле `${AIRFLOW_HOME}/webserver_config.py` в параметрах начинающихся с
> `SE_LDAP_BIND_`... Пример:<br>
> ```python
> SE_LDAP_BIND_USER = "uid=u_ts_testonly_airflow,cn=users,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru"  # the special bind username for search
> SE_LDAP_BIND_USER_NAME = "u_ts_testonly_airflow"
> SE_LDAP_BIND_USER_REALM = "DEV.DF.SBRF.RU"
> SE_LDAP_BIND_USER_KEYTAB = "/opt/airflow/secret/u_ts_testonly_airflow.keytab"
> SE_LDAP_BIND_USER_PATH_KRB5CC = "/opt/airflow/secret/tgt/krb5cc_"  # path and prefix for ticket-file
> ```

---

## Требования УЭК:

* Airflow должно быть установлено из-под системного ЛОКАЛЬНОГО пользователя(СУЗ), специально созданного на уровне OS.
* Исходный код должен быть закрыт для других пользователей.
* Процессы нужно оформлять как сервисные процессы OS (systemd unit), чтоб сама операционная система следила
за ними и, при падении, перезапускала.

---

## Процессы Airflow:

* `webserver` - Вэб-сервер, описание и параметры запуска можно найти в
  официальной документации.

* `scheduler` - Планировщик задач, описание и параметры запуска можно найти в
  официальной документации.

* `kerberos` - Обновляет TGT-файл для сервисного ТУЗа Airflow, описание и параметры
  запуска можно найти в официальной документации.

* `worker` - Процесс выполняющий задачи при включённом режиме CeleryExecutor, запускается
  на других серверах, описание и параметры запуска можно найти в официальной документации.

* `se_ldap` - Процесс, получающий из LDAP пользователей и группы, и сохраняющий, так называемый, "кэш" LDAP
  в БД. Работает по расписанию из файла параметров. Это наш процесс. Наша разработка.
  В "коробке" (стандартном дистрибутиве) отсутствует.

* `se_kafka` - процесс, отправки событий аудита в Kafka (ТС Журналирование). Работает по расписанию из файла
  параметров. Это наш процесс. Наша разработка. В "коробке" (стандартном дистрибутиве) отсутствует.

* `se_ticketman` - процесс, просматривает директорию(-ии) с тикет-файлами и делает renew найденных тикетов.
  Это наш процесс. Наша разработка. В "коробке" (стандартном дистрибутиве) отсутствует.

> 
> Стандартные процессы запускаются через "точку входа" `airflow_se`, пример, `venv/bin/airflow_se webserver`.
> Наши процессы - `se_ldap`, `se_kafka` и `se_ticketman`, запускаются через свои, персональные, "точки входа":
> `venv/bin/se_ldap`, `venv/bin/se_kafka` и `venv/bin/se_ticketman`. Справку по командам и ключам можно получить так:
> `venv/bin/airflow_se --help`, `venv/bin/airflow_se webserver --help`, `venv/bin/airflow_se scheduler --help`,
> `venv/bin/se_ldap --help`, `venv/bin/se_kafka --help`, etc.

#### Пример оформления на основе одного из процессов `venv/bin/se_ldap`:

```shell
cat << EOF | tee /etc/systemd/system/airflow_se-se_ldap.service > /dev/null
[Unit]
Description=Airflow SE, LDAP cache refresher
After=network.target
Wants=

[Service]
Environment="PATH=/opt/airflow/venv/bin:/opt/airflow/.local/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin"
Environment="AIRFLOW_HOME=/opt/airflow"
Environment="AIRFLOW_CONFIG=/etc/airflow/airflow.cfg"
Environment="HADOOP_CONF_DIR=/etc/hadoop/conf"
Environment="HIVE_CONF_DIR=/etc/hive/conf"
EnvironmentFile=/etc/sysconfig/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=/opt/airflow/venv/bin/se_ldap --pid /run/airflow/se_ldap.pid
# "on-failure" - перезапуск только при сбое, "always" - перезапуск в любом случае
Restart=always
# пауза перед перезапуском
RestartSec=30s
# ещё пауза, типа таймаут (паузы складываются)
# TimeoutSec=300
# приватный темп, обязательно нужен, чтоб никто не смог украсть TGT-файлы, которые сохраняются в /tmp/
PrivateTmp=true
# PID файл, в принципе, не нужен, там просто pid процесса (эти файлы записываются, но никак не используются в коде)
PIDFile=/run/airflow/se_ldap.pid
# рабочая директория
WorkingDirectory=/opt/airflow
# данный параметр необходим для прав root при запуске команд в ExecStartPre
PermissionsStartOnly=true
# выполнить ДО старта приложения
ExecStartPre=mkdir -p /run/airflow
ExecStartPre=chown -R airflow:airflow /run/airflow

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
```

> Даны команды bash. _Можно, через редактор, создать файл, дело вкуса и предпочтений._

---

## Сборка, установка и настройка функционала

#### Сборка пакета

_Сборка пакета осуществляется при помощи Python >=3.8_

Стоит, для этого, создать отдельную среду окружения Python и активировать её (находясь в папке проекта, где находится
файл `setup.py`):
  - для linux:<br>
    `/usr/bin/python3.8 -m venv venv` - в текущей папке появится директория `venv`<br>
    `source venv/bin/activate` - активация среды окружения (при успешной активации, в приглашении командной строки
    появится `(venv) ... $`)
  - то же самое, для windows:<br>
    `"c:\Program Files\Python38\python.exe" -m venv venv` - в текущей папке создастся директория `venv`<br>
    `venv\Scripts\activate` - активация среды окружения (при успешной активации, в приглашении командной строки
    появится `(venv) ... $`)

Обновляем pip (считаем, что виртуальная среда python создана и активна, см. выше):<br>
  `python -m pip install --upgrade pip`

Для сборки потребуется дополнительно установить 2 пакета:
  - обязательно<br>
    `python -m pip install setuptools`
  - опционально, если нужно собирать "колесо" (wheel)<br>
    `python -m pip install wheel`

Запуск сборки пакета:<br>
  `python setup.py sdist -d /mnt/lib/` - собрать "яйцо" (расширение `.tar.gz`)<br>
  `python setup.py bdist_wheel -d /mnt/lib/` - собирать "колесо" (wheel, расширение файла `.whl`)

> 
> Ключ `-d` указывает директорию, где появится собранный пакет, ключ не
> обязательный, можно не указывать, тогда файл появится в поддиректории `dist/`

#### Установка пакета

Устанавливается, как обычно, через `pip` с указанием пути к файлу:<br>
  `python -m pip install "/mnt/lib/airflow_se-0.2.6-py3-none-any.whl"`
> Переустановка пакета осуществляется с ключом `--force-reinstall`:<br>
> ```shell
> python -m pip install --force-reinstall "/mnt/lib/airflow_se-0.2.6-py3-none-any.whl"
> ```
> > При переустановке пакета на работающий инстанс Airflow, нужно перезапустить процессы, чтоб изменения вступили
> > в силу. Пример:<br> 
> > ```shell
> > systemctl restart airflow_se-{scheduler,webserver,kerberos,se_ldap,se_kafka}
> > ```

#### Подключение класса безопасности _AirflowSecurityManagerSE_

В файл настроек `${AIRFLOW_HOME}/webserver_config.py` нужно добавить следующее:

```python
#  Импортируем класс управления безопасностью SE и присваиваем переменной
from airflow_se import AirflowSecurityManagerSE
SECURITY_MANAGER_CLASS = AirflowSecurityManagerSE

# Подключаем стандартный метод аутентификации
from airflow.www.fab_security.manager import AUTH_DB
AUTH_TYPE = AUTH_DB
```

#### Пример содержимого файла `${AIRFLOW_HOME}/webserver_config.py` полностью:
*** При копировании просьба сохранять комментарии.

```python
"""
    _Пример файла конфигурации web-сервера с комментариями_

Примечание:
    Имена и префиксы параметров задавать только в верхнем регистре!!!
    Параметр состоит из `тела` и `префикса` (далее, префикс я буду обозначать как `<prefix>`).
    Список допустимых префиксов описан в модуле airflow_se.settings.constants, там объявлена константа:
    PARAMS_PREFIXES = ("SE_", "_SE_", "_", "", "AUTH_", "_AUTH_")
    Т.е. `SE_USER_REGISTRATION`, `_SE_USER_REGISTRATION`, `_USER_REGISTRATION`, `USER_REGISTRATION` и т.д.
    ОДНО И ТОЖЕ!!!
    Ищутся параметры по порядку. Первый из найденных будет возвращён, остальные проигнорены.
    Настойчиво рекомендую, использовать префикс `SE_`, чтоб в дальнейшем не путаться.
    Данный функционал был введён для обратной совместимости.
    Большая часть параметров может быть перемещена в переменные окружения (кроме обязательного блока!!!).
    Пример:
        Параметр `<prefix>USER_REGISTRATION = True` удаляем, или комментируем, в данном файле.
        Далее, помещаем его в переменные среды окружения `export SE_USER_REGISTRATION=True`.
"""

#################################################################################################
###                    Начало обязательного блока!!!                                          ###
###          * В этом блоке никакие префиксы не работают!                                     ###
###          * Данные параметры перенести в переменные среды окружения не получится.          ###
###          * В этом блоке лучше ничего не трогать. Оставьте как есть.                       ###
#################################################################################################

from __future__ import annotations
from os import path
from datetime import time, timedelta
from typing import Optional, Union, Iterable
from airflow_se import AirflowSecurityManagerSE
from airflow.www.fab_security.manager import AUTH_DB

# Полный путь к директории, где лежит данный файл
basedir = path.abspath(path.dirname(__file__))

# Подключение класса управления безопасностью.
SECURITY_MANAGER_CLASS = AirflowSecurityManagerSE
# * Сокращение `FAB`, здесь и далее - `Flask Application Builder`
# * Примечание разработчика:
#       Так делать нельзя!!! (учу как не надо делать)
#           `FAB_SECURITY_MANAGER_CLASS = "my_package.my_security.MySecurityManager"`
#       На сайте FAB так написано, но это вводит в заблуждение (на оф.сайте AF, дана ссылка на соответствующую
#       страницу FAB). Прикол, наверное :). По-моему, идиотский. Вводит в заблуждение.
#       Разрабы Airflow сами отнаследовали класс и проверяют, что отнаследованно именно от их класса.
#       Подробности в файле:
#           `${AIRFLOW_HOME}/venv/lib/python3.8/site-packages/airflow_se/www/extensions/init_appbuilder.py`
#       SECURITY_MANAGER_CLASS - это скрытый, не документированный, параметр, который мы здесь используем.
#       Только так можно легитимно подменить класс управления безопасностью Airflow.

# Подключаем стандартный метод аутентификации в БД (можно не подключать, AUTH_TYPE по умолчанию и так равна AUTH_DB)
AUTH_TYPE = AUTH_DB
# * Примечание разработчика:
#     Метод `auth_user_db` перекрыт в дочернем классе и там реализован новый код аутентификации.

# Flask-WTF flag for CSRF: без понимания, лучше не трогать (гуглить)
WTF_CSRF_ENABLED = False
WTF_CSRF_TIME_LIMIT = None

# force users to re-auth after 1 hour (3600) or 15 min (900) of inactivity (to keep roles in sync)
# по-русски: время жизни сессии в браузере, при не активности, требует снова залогиниться
PERMANENT_SESSION_LIFETIME = 3600

#################################################################################################
###                             Конец обязательного блока.                                    ###
#################################################################################################

#################################################################################################
###                                 Настройки общие                                           ###
#################################################################################################

# registration configs

# allow users who are not already in the FAB DB
# Тип bool (True/False)
# Параметр не обязательный
# По умолчанию True
SE_USER_REGISTRATION: Optional[bool] = True

# this role will be given in addition to any <prefix>LDAP_ROLES_MAPPING
# Тип str
# Параметр не обязательный
# По умолчанию "Public"
SE_USER_REGISTRATION_ROLE: Optional[str] = "Public"

# if we should replace ALL the user's roles each login, or only on registration
# Тип bool (True/False)
# Параметр не обязательный
# По умолчанию True
SE_ROLES_SYNC_AT_LOGIN: Optional[bool] = True

#################################################################################################
###                                 Настройки Kerberos                                        ###
#################################################################################################

# Утилиты Kerberos (используются для генерации и чтения тикет-файлов).
# Тип str
# Параметры не обязательные
# По умолчанию `kinit` и `klist`, соответственно
SE_KRB_KINIT_PATH: Optional[str] = "kinit"  # системный путь к утилите kinit
SE_KRB_KLIST_PATH: Optional[str] = "klist"  # системный путь к утилите klist
# * Если не находит, то надо прописать полные пути к данным утилитам

# Лайфтайм тикета и renew until (граница обновления) // Нужно знать настройки IPA/AD
# Тип str
# Параметры не обязательные
# По умолчанию `8h` (8 часов) и `7d` (7 дней), соответственно
SE_KRB_TICKET_LIFETIME: Optional[str] = "8h"
SE_KRB_RENEW_UNTIL: Optional[str] = "7d"
# * Обновлять тикет можно только на протяжении времени указанном в `SE_KRB_RENEW_UNTIL`

#################################################################################################
###                                 Настройки PAM                                             ###
#################################################################################################

# PAM политика, для PAM аутентификации пользователей, файл /etc/pam.d/airflow
# Тип str
# Параметр не обязательный
# По умолчанию равен "airflow"
SE_PAM_POLICY: Optional[str] = "airflow"
# * Можно в этом файле прописать политики PAM-аутентификации для разных инстансов AF,
#   или создать другой файл и указать его имя в этом параметре.

#################################################################################################
###                                 Настройки LDAP                                            ###
#################################################################################################

# Адрес сервера LDAP
# Тип str
# Параметр обязательный!
SE_LDAP_SERVER: str = "ldap://ipa1.dev.df.sbrf.ru"
# * Можно попробовать разные протоколы и порты, как заработает...
#   SE_LDAP_SERVER = "ldaps://ipa1.dev.df.sbrf.ru:636"
#   SE_LDAP_SERVER = "ldaps://ipa1.dev.df.sbrf.ru:389"

# the special bind username for search
# Тип str
# Параметр обязательный!
SE_LDAP_BIND_USER: str = "uid=u_ts_testonly_airflow,cn=users,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru"

# Настройки ТУЗа LDAP
# Тип str
# Параметры обязательные!
SE_LDAP_BIND_USER_NAME: str = "u_ts_testonly_airflow"
SE_LDAP_BIND_USER_REALM: str = "DEV.DF.SBRF.RU"
SE_LDAP_BIND_USER_KEYTAB: str = basedir + "/secret/u_ts_testonly_airflow.keytab"

# the LDAP search base
# Тип str
# Параметр обязательный!
SE_LDAP_SEARCH: str = "cn=users,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru"

# Запрос к LDAP
# Тип str
# Параметр обязательный!
SE_LDAP_SEARCH_FILTER: str = "(memberOf=cn=g_bda_a_af_*,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru)"
# * Есть длинный вариант:
#   SE_LDAP_SEARCH_FILTER = "(|(memberOf=cn=g_bda_a_af_admin,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru)(memberOf=cn=g_bda_a_af_user,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru)(memberOf=cn=g_bda_a_af_viewer,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru)(memberOf=cn=g_bda_a_af_op,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru)(memberOf=cn=g_bda_a_af_public,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru))"

# a mapping from LDAP DN to a list of FAB roles
# Маппинг групп LDAP с ролевой моделью Airflow (FAB)
# Тип dict, может быть представлен строкой json, содержащей соответствующий словарь
# Параметр обязательный!
# Синоним: `<prefix>ROLES_MAPPING`
SE_LDAP_ROLES_MAPPING: dict = {
    "cn=g_bda_a_af_admin,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["Admin"],
    "cn=g_bda_a_af_user,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["User"],
    "cn=g_bda_a_af_viewer,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["Viewer"],
    "cn=g_bda_a_af_op,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["Op"],
    "cn=g_bda_a_af_public,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["Public"],
}

# Настройка сертификатов для LDAP подключения (параметры не обязательные, если сертификаты не используются)
#   - Allow LDAP authentication to use self-signed certificates (LDAPS) // default True
# SE_LDAP_ALLOW_SELF_SIGNED: Optional[bool] = True
#   - Demands TLS peer certificate checking // default False
# SE_LDAP_TLS_DEMAND: Optional[bool] = False
#   - CA Certificate directory to check peer certificate. Certificate files must be PEM-encoded
# SE_LDAP_TLS_CACERTDIR: Optional[str] = None
#   - CA Certificate file to check peer certificate. File must be PEM-encoded
# SE_LDAP_TLS_CACERTFILE: Optional[str] = None
#   - Certificate file for client auth use with SE_LDAP_TLS_KEYFILE
# SE_LDAP_TLS_CERTFILE: Optional[str] = None
#   - Certificate key file for client auth
# SE_LDAP_TLS_KEYFILE: Optional[str] = None

# TLS шифрование подключения к LDAP
# Тип bool (True/False)
# Параметр не обязательный
# По умолчанию True (включено)
SE_LDAP_USE_TLS: Optional[bool] = True
# * Сначала включается TLS-шифрование, затем происходит аутентификация

# Тип str
# Параметр не обязательный
# По умолчанию "givenName"
SE_LDAP_FIRSTNAME_FIELD: Optional[str] = "givenName"

# Тип str
# Параметр не обязательный
# По умолчанию "sn".
SE_LDAP_LASTNAME_FIELD: Optional[str] = "sn"

# Тип str
# Параметр не обязательный
# По умолчанию "mail"
SE_LDAP_EMAIL_FIELD: Optional[str] = "mail"  # if null in LDAP, email is set to: "{username}@email.notfound"

# search configs
# Тип str
# Параметр не обязательный
# По умолчанию "uid"
SE_LDAP_UID_FIELD: Optional[str] = "uid"  # the username field

# the LDAP user attribute which has their role DNs
# Тип str
# Параметр не обязательный
# По умолчанию "memberOf"
SE_LDAP_GROUP_FIELD: Optional[str] = "memberOf"

# Префикс имени файла для хранения `тикета`.
# Тип str
# Параметр не обязательный, но лучше указать явно (могут быть проблемы с автоматическим продлением тикетов)
# По умолчанию "/tmp/krb5cc_" (не тестировалось на обновление тикетов в этой директории)
SE_LDAP_BIND_USER_PATH_KRB5CC: Optional[str] = basedir + "/secret/tgt/krb5cc_"
# * !!! Не указывать просто папку!!! Обязательно с каким-то префиксом файла!!!

#################################################################################################
###                     Настройки относящиеся только к процессу `se_ldap`                     ###
#################################################################################################
# * Краткая подсказка:
#       у класса time параметры: day, hour, minute, second
#       у класса timedelta параметры: days, hours, minutes, seconds
#           (отличие - оканчиваются на "s")
# * Для задания параметров типа time и timedelta через переменные окружения, следует передавать их в виде json.
#   Примеры:
#       export <prefix>LDAP_TIME_START='{"hour":3,"minute":20}'
#       export <prefix>LDAP_TIMEDELTA_UP='{"days":1,"hours":2}'

# Обновлять кэш LDAP при запуске процесса (первая итерация главного цикла)?
# Тип bool (True/False)
# Параметр не обязательный
# По умолчанию False (отключено)
SE_LDAP_REFRESH_CACHE_ON_START: Optional[bool] = True

# Задержка(засыпание) процесса в секундах
# Тип int, целое число в диапазоне от 60 до 1200
# Параметр не обязательный
# По умолчанию 600
SE_LDAP_PROCESS_TIMEOUT: Optional[int] = 60

# Количество повторений главного цикла процесса
# Тип int или None, целое положительное число в диапазоне от 1 до плюс бесконечность или None
# Параметр не обязательный
# По умолчанию None
SE_LDAP_PROCESS_RETRY: Optional[int] = None

# Время предпочтительного старта запроса в LDAP
# Тип time, задавать без использования day, только hour, minute, second // в пределах суток (меньше 24 часов)
# Параметр не обязательный
# По умолчанию time(hour=2) // 2 часа ночи
SE_LDAP_TIME_START: Optional[time] = time(hour=2)
# * Если надо указать время точнее, можно задать так:
#   <prefix>LDAP_TIME_START = time(hour=23, minute=59, second=59, microsecond=999999)

# Верхняя граница
# Тип timedelta, диапазон от timedelta(hours=12) до timedelta(days=3)
# Параметр не обязательный
# По умолчанию timedelta(days=1, hours=2) // 1 день 2 часа, рекомендуемое значение
SE_LDAP_TIMEDELTA_UP: Optional[timedelta] = timedelta(days=1, hours=2)
# * По этой дельте определяем, что кэш совсем старый и точно надо идти в LDAP за новым

# Нижняя граница
# Тип timedelta, диапазон от timedelta(hours=4) до значения параметра <prefix>LDAP_TIMEDELTA_UP
# * Проще говоря, нижняя граница не должна быть выше верхней границы
# Параметр не обязательный
# По умолчанию timedelta(hours=6) // 6 часов, рекомендуемое значение
SE_LDAP_TIMEDELTA_DOWN: Optional[timedelta] = timedelta(hours=6)
# * По этой дельте определяем, что кэш достаточно свежий и не стоит идти в LDAP,
#   даже если время <prefix>LDAP_TIME_START пришло, а так же, руководствуемся этой дельтой для запросов
#   в LDAP // не чаще чем раз в столько-то времени, даже если никакого кэша нет, это чтоб не завалить сервер LDAP

# Удаление кэшей LDAP старше этой временной дельты
# Тип timedelta, диапазон от timedelta(days=3) до плюс бесконечности, например timedelta(days=1000000)
# Параметр не обязательный
# По умолчанию timedelta(days=30) // рекомендуемое значение
SE_LDAP_DELETE_CACHES_DAYS_AGO: Optional[timedelta] = timedelta(days=30)
# * Очистка старого ненужного хлама для лучшего быстродействия и чтоб БД не разрасталась,
#   срабатывает при успешном получении свежего кэша LDAP, перед записью в таблицу БД

#################################################################################################
###                                 Настройки Kafka (события аудита)                          ###
#################################################################################################
# * !!! Для правильной работы, необходим пакет librdkafka >=0.9.4
#   sudo yum install librdkafka

# app_id для Кафки, id приложения
# Тип str
# Параметр обязательный!
SE_KAFKA_APP_ID: str = "airflow_se-ser-4c88-8430-4944bfb31b86"

# Топик Кафки
# Тип str
# Параметр обязательный!
SE_KAFKA_TOPIC: str = "log-kafka-01"

# Настройки продюсера для подключения к Кафке.
# Должен быть словарём (dict) и не пустым!
# Параметр обязательный!
SE_KAFKA_PRODUCER_SETTINGS: dict = {
    "bootstrap.servers": "tkliq-log000007.cm.dev.df.sbrf.ru:9092,"
                         "tkliq-log000008.cm.dev.df.sbrf.ru:9092,"
                         "tkliq-log000009.cm.dev.df.sbrf.ru:9092,"
                         "tkliq-log000010.cm.dev.df.sbrf.ru:9092,"
                         "tkliq-log000011.cm.dev.df.sbrf.ru:9092,"
                         "tkliq-log000012.cm.dev.df.sbrf.ru:9092",
}
# SE_KAFKA_PRODUCER_SETTINGS: dict = {
#     "bootstrap.servers": "tkliq-log000007.cm.dev.df.sbrf.ru:9093,"
#                          "tkliq-log000008.cm.dev.df.sbrf.ru:9093,"
#                          "tkliq-log000009.cm.dev.df.sbrf.ru:9093,"
#                          "tkliq-log000010.cm.dev.df.sbrf.ru:9093,"
#                          "tkliq-log000011.cm.dev.df.sbrf.ru:9093,"
#                          "tkliq-log000012.cm.dev.df.sbrf.ru:9093",  # SSL 9093, PLAIN 9092
#     "security.protocol":        "SSL",
#     "ssl.keystore.location":    basedir + "/secret/u_ts_testonly_airflow.p12",
#     "ssl.keystore.password":    "qwe123",
#     # можно по отдельности сертификаты подсунуть
#     # "ssl.ca.location":          basedir + "/secret/root.pem",
#     # "ssl.key.location":         basedir + "/secret/public_key.pem",
#     # "ssl.certificate.location": basedir + "/secret/cert.pem",
# }

# Таймаут процесса (на сколько секунд он будет "засыпать")
# Тип int, целое число в диапазоне от 0 до 1200
# Параметр не обязательный
# По умолчанию 300
SE_KAFKA_PROCESS_TIMEOUT: Optional[int] = 60

# Количество повторений главного цикла, None - бесконечный цикл
# Тип int
# Параметр не обязательный
# По умолчанию None
SE_KAFKA_PROCESS_RETRY: Optional[int] = None

# Максимальное количество сообщений, которые за раз отправлять в Кафку
# Тип int, диапазон от 100 до 100000
# Параметр не обязательный
# По умолчанию 1000
SE_KAFKA_PAGE_SIZE: Optional[int] = 100000

#################################################################################################
###                     Настройки относящиеся только к процессу `se_ticketman`                ###
#################################################################################################

# Список директорий для просмотра и обновления тикетов
# Тип list | tuple | set | str | None - (либо итерируемый объект (список, кортеж, сет), либо строка, либо None)
# Параметр не обязательный
# По умолчанию None, но, если что, то можно задать список из директорий для просмотра (только абсолютные пути)
SE_TICKETMAN_SCAN_DIRS: Union[Iterable[str], str, None] = None
# * Если None, то берём путь из SE_LDAP_BIND_USER_PATH_KRB5CC или из секции [kerberos] параметр `ccache` (airflow.cfg)

# Задержка(засыпание) процесса в секундах
# Тип int, целое число в диапазоне от 3600 (1 час) до 28800 (8 часов) включительно
# Параметр не обязательный
# По умолчанию 25200 (7 часов)
# * Можно задавать выражениями от 1*60*60 до 8*60*60
SE_TICKETMAN_PROCESS_TIMEOUT: Optional[int] = 1*60*60

# Количество повторений главного цикла процесса
# Тип int или None, целое положительное число в диапазоне от 1 до плюс бесконечность или None
# Параметр не обязательный
# По умолчанию None
SE_TICKETMAN_PROCESS_RETRY: Optional[int] = None

#################################################################################################
###                               Параметры дополнительные.                                   ###
#################################################################################################

# Отладка (DEBUG)
# Тип bool (True/False)
# Параметр не обязательный
# По умолчанию False (отладочные сообщения отключены)
_SE_DEBUG: Optional[bool] = True
# * Это чтоб не включать уровень логирования Airflow в debug. Там такие простыни вываливаются.
#   А, это наш debug для отладки. Только для нашего функционала действительный.

# Формат сообщений при включённом DEBUG
# Тип str
# Параметр не обязательный
# По умолчанию "DEBUG >> {}"
_SE_DEBUG_MASK: Optional[str] = "[ DeBuG ] => {}"
# * Строка форматирования обязательно должна содержать фигурные скобки `{}` в единичном экземпляре

# Включить/отключить PAM аутентификацию
# Тип bool (True/False)
# Параметр не обязательный
# По умолчанию True (включена)
_SE_PAM_IS_AUTH: Optional[bool] = False
```

---

## Популярное пошаговое описание работы функционала

1. Пользователь входит в WebUI/RESTAPI (вводит логин/пароль от AD/IPA аккаунта).
2. Бэк-енд вэбсервера запускает утилиту kinit и получает тикет-файл пользователя, который сохраняется
в директорию и с префиксом указанным в параметре `SE_LDAP_BIND_USER_PATH_KRB5CC` (туда же, куда и тикет ТУЗа для LDAP).
3. Пользователь проходит GSSAPI аутентификацию в AD/IPA с использованием тикета из предыдущего этапа. Создаются
два секьюрити контекста Initiator (контекст входящего пользователя) и Acceptor (контекст сервисного ТУЗа Airflow).
Оба контекста обогащаются своими тикетами и отправляются на AD/IPA сервер, где проходят проверку. Сервер AD/IPA
возвращает токены обоих контекстов. Контексты меняются друг с другом полученными токенами и снова отсылаются на сервер.
Всё это продолжается до тех пор, пока сервер не вернёт пустой токен (по стандарту это означает успешную аутентификацию).
В AD/IPA появляется запись в логах, что пользователь аутентифицировался из-под сервисного ТУЗа Airflow.
4. Пользователь проходит PAM аутентификацию (PAM модуль должен быть установлен и настроен). Это опционально. Можно
отключить, если не требуется. Подробнее о PAM аутентификации и PAM-политиках расскажет администратор стенда.
5. Пользователь проходит авторизацию по кэшу LDAP. Кэш получает и сохраняет в БД с метаданными отдельный процесс
`se_ldap`. Если пользователь находится в какой-либо группе LDAP (найден в кэше), то он получает привилегии в
соответствии с маппингом, прописанным в параметре `SE_LDAP_ROLES_MAPPING` (группа в LDAP = роль Airflow).
6. Пользователь получает доступ к WebUI/RESTAPI функционалу в соответствии с ролью, полученной на предыдущем шаге.
Если, на каком-то шаге, происходят ошибки или статусы отрицательны, то ошибка пишется в лог, аутентификация/авторизация
прерывается, создаётся запись о неуспешном входе во внутренний аудит (БД с метаданными) и пользователь не допускается
к функционалу WebUI/RESTAPI. Если все этапы аутентификации/авторизации прошли успешно, создаётся запись об успешном
входе во внутренний аудит и пользователь допускается к функционалу WebUI/RESTAPI с соответствующей ролью (привилегиями).
7. Отправкой событий аудита занимается отдельный процесс `se_kafka`, который формирует записи в соответствии с
требованиями от ППРБ Аудит и отправляет их в Kafka с подтверждением получения. Если запись доставлена в Kafka и
подтверждение пришло, то запись помечается как отправленная в БД с метаданными.
8. По требованиям ППРБ Аудит, заблокировано изменение ролей и пользователей Airflow (даже Admin не может изменить).
При попытке изменения/добавления/удаления, выдаётся сообщение пользователю и записывается соответствующее событие
во внутренний аудит, которое затем отправляется в Kafka (ППРБ Аудит).
9. Также, по требованиям ППРБ Аудит, происходит запись событий редактирования объектов Connection и Variable с
последующей отправкой в Kafka (ППРБ Аудит). Также перехватываются события по операциям с DAG-ми (запуск, остановка,
постановка на расписание, снятие с расписания). Все события снабжаются полной информацией (кто, когда, какой DAG, что
сделал и т.д.). События можно посмотреть в ППРБ Аудит или в БД с метаданными.

