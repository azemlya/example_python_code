<table><tr><td>Провайдер Greenplum SE для Airflow версии 2.4.0 и выше.</td><td>&copy; 2022</td></tr></table>

# _ОПИСАНИЕ_

---

## Реализованный функционал

1. airflow.providers.se.greenplum.hooks.greenplum.GreenplumHookSE - хук ("крюк")
2. airflow.providers.se.greenplum.operators.greenplum.GreenplumOperatorSE - оператор

---

## Сборка пакета

_Сборка пакета осуществляется при помощи Python 3.6+ (лучше вариант 3.8)._

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
  `python setup.py sdist -d /mnt/pkg/` - собрать "яйцо" (расширение `.tar.gz`)<br>
  `python setup.py bdist_wheel -d /mnt/pkg/` - собирать "колесо" (wheel, расширение файла `.whl`)

> Ключ `-d` указывает директорию, где появится собранный пакет, ключ не
> обязательный, можно не указывать, тогда файл появится в поддиректории `dist/`

---

## Установка пакета

Устанавливается, как обычно, через `pip` с указанием пути к файлу:<br>
`python -m pip install "/mnt/pkg/apache_airflow_se_providers_greenplum-0.2.2-py3-none-any.whl"`
> Переустановка пакета осуществляется с ключом `--force-reinstall`:<br>
> `python -m pip install --force-reinstall "/mnt/pkg/apache_airflow_se_providers_greenplum-0.2.2-py3-none-any.whl"`
> > При переустановке пакета на работающий инстанс Airflow, нужно перезапустить процессы,
> > чтоб изменения вступили в силу:<br> 
> > `sudo systemctl restart airflow-{scheduler,webserver,kerberos,se_ldap,se_kafka,se_ticketman}`

---

## Создание коннекшена

Пример:
```commandline
airflow connections add --conn-type greenplum_se --conn-host tplis-ahd000001.dev.df.sbrf.ru --conn-port 5432 \
--conn-schema test \
--conn-extra '{"ticket_prefix": "/opt/airflow/secret/tgt/krb5cc_"}' name_connection_to_greenplum
```
Ключи в extras:
- `host`: как и в предыдущем, можно перенести из коннекшена в Extra, обязательный параметр
- `port`: аналогично предыдущему, можно не указывать если стандартный, по умолчанию 5432
- `schema`: имя БД, не путать со схемой, это ИМЕННО ИМЯ БД!!! аналогично предыдущим, можно указать в Extra ключ `schema`
или `dbname`(синоним), чтоб не путать, лучше задавать `dbname`, так или иначе, параметр должен быть задан
- `ticket_prefix`: путь с префиксом для тикетов (формируется как {`ticket_prefix`}{`owner`}), `owner` берётся из DAG-а,
из его `default_args`
- `cursor`: тип курсора, один из 3-х вариантов `dictcursor`(по умолчанию), `realdictcursor`, `namedtuplecursor`,
подробнее: https://www.psycopg.org/docs/extras.html
- `options`: низкоуровневые опции соединения, передаётся напрямую в драйвер, пример: `-c statement_timeout=300000`
(таймаут на длительность запроса в микросекундах, 300000 = 300 секунд или 5 минут),
подробнее: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS

---

## Использование в DAG-ах

```python
"""
Простейший демонстрационный DAG с использованием GreenplumOperatorSE
"""
from airflow import DAG
from airflow.providers.se.greenplum.operators.greenplum import GreenplumOperatorSE
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    "owner": "17586173",
}

with DAG(
    dag_id="greenplum_se_operator_demo",
    default_args=args,
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["greenplum_se", "example", "demo"],
    catchup=False,
    ) as dag:

    greenplum_operator_task = GreenplumOperatorSE(
        task_id="greenplum_operator_task",
        greenplum_se_conn_id="name_connection_to_greenplum",
        sql="SELECT 1;",
    )

    greenplum_operator_task

if __name__ == "__main__":
    dag.cli()
```

```python
"""
Более продвинутый демонстрационный DAG с использованием оператора
"""
from airflow import DAG
from airflow.providers.se.greenplum.operators.greenplum import GreenplumOperatorSE
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    "owner": "17586173",
}

with DAG(
    dag_id="greenplum_se_operator",
    default_args=args,
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["greenplum_se", "example", "demo"],
    params={"KeY": "VaLuE"},
    catchup=False,
    ) as dag:

    create_pet_table = GreenplumOperatorSE(
        task_id="create_pet_table",
        greenplum_se_conn_id="name_connection_to_greenplum",
        sql="""
            CREATE TABLE IF NOT EXISTS s_grnplm_ld_rozn_electron_daas_core.pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            owner VARCHAR NOT NULL);
            """,
    )
    populate_pet_table = GreenplumOperatorSE(
        task_id="populate_pet_table",
        greenplum_se_conn_id="name_connection_to_greenplum",
        sql="""
            INSERT INTO s_grnplm_ld_rozn_electron_daas_core.pet (name, pet_type, birth_date, owner)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO s_grnplm_ld_rozn_electron_daas_core.pet (name, pet_type, birth_date, owner)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO s_grnplm_ld_rozn_electron_daas_core.pet (name, pet_type, birth_date, owner)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO s_grnplm_ld_rozn_electron_daas_core.pet (name, pet_type, birth_date, owner)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )
    get_all_pets = GreenplumOperatorSE(task_id="get_all_pets",
                                       greenplum_se_conn_id="name_connection_to_greenplum",
                                       sql="SELECT * FROM s_grnplm_ld_rozn_electron_daas_core.pet;",
                                       )
    get_birth_date = GreenplumOperatorSE(
        task_id="get_birth_date",
        greenplum_se_conn_id="name_connection_to_greenplum",
        sql="SELECT * FROM s_grnplm_ld_rozn_electron_daas_core.pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        # runtime_parameters={'statement_timeout': '3000ms'},
    )

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date

if __name__ == "__main__":
    dag.cli()
```

```python
"""
Демонстрационный DAG по использованию хука
"""
from airflow import DAG
from contextlib import closing
from typing import cast, Optional, Dict
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.se.greenplum.hooks.greenplum import GreenplumHookSE
from psycopg2.extensions import connection, cursor, QueryCanceledError
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
from pandas.io import sql as psql
import pandas

def_args = dict(
    greenplum_se_conn_id="name_connection_to_greenplum",
    options='-c statement_timeout=300000',  # Это пойдёт в опции соединения (таймаут в 5 минут)
    # На данный момент, в args разрешено передавать следующие параметры:
    # "owner", "cursor", "keytab", "tkt_file", "realm", "kinit", "lifetime", "greenplum_se_conn_id"
    # *** Всё, что кроме, пойдёт напрямую в psycopg2.connect() как дополнительные параметры подключения.
    # *** Доп.параметры подключения можно посмотреть в офф.документации по адресу:
    # *** https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    # *** Если хочется передать ещё что-то, то используйте DAG(params=...)
)

# Строковые константы передаём через DAG(params=...), но лучше через Variables это делать.
params = dict(
    owner="17586173",
    table_name_pet="s_grnplm_ld_rozn_electron_daas_core.pet2",
    sess_timeout="300s",
    set_sess_timeout="SET SESSION statement_timeout = '{{ params.sess_timeout }}';",
    drop_table_pet="DROP TABLE IF EXISTS {{ params.table_name_pet }};",
    create_table_pet="""
        CREATE TABLE IF NOT EXISTS {{ params.table_name_pet }} (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            owner VARCHAR NOT NULL);
    """,
    trunc_table_pet="TRUNCATE TABLE {{ params.table_name_pet }};",
    insert_pets_with_params="""
        INSERT INTO {{ params.table_name_pet }} (name, pet_type, birth_date, owner)
            VALUES ( %(name)s, %(pet_type)s, %(birth_date)s, %(owner)s);
    """,
    list_pets=[
        dict(name="Max", pet_type="Dog", birth_date="2018-07-05", owner="Jane"),
        dict(name="Susie", pet_type="Cat", birth_date="2019-05-01", owner="Phil"),
        dict(name="Lester", pet_type="Hamster", birth_date="2020-06-23", owner="Lily"),
        dict(name="Quincy", pet_type="Parrot", birth_date="2013-08-11", owner="Anne"),
    ],
    select_all_pets="SELECT * FROM {{ params.table_name_pet }};",
    select_role_privileges="""
        SELECT * FROM information_schema.table_privileges
        WHERE grantee = '{{ dag.default_args.get("set_role") or dag.default_args.get("owner") }}'
        ORDER BY 5, 6
    """,
)

with DAG(
    dag_id="greenplum_se_hook",
    default_args=def_args,
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["greenplum_se", "example", "demo"],
    params=params,
    catchup=False,
    ) as dag:

    def func_gp_create_table_pet(*args, **kwargs):
        # так, смотрим чего там в контексте передано, там много всякого вкусного
        for k, v in kwargs.items():
            print(f"{k} :: {type(v)} :: {dir(v)} :: {v}")
        # это объект класса, который стартанул эту функцию, в нашем случае это PythonOperator
        task: Optional[PythonOperator] = kwargs.get("task")
        # params из DAG-а, чего туда вставили, то и имеем (словарь, как словарь, ничего необычного)
        dag_params: Optional[Dict] = kwargs.get("params")
        # поверки
        if not isinstance(task, PythonOperator) or not isinstance(dag_params, Dict):
            raise RuntimeError("Ошибка: что-то не так с передачей контекста, task и params не те")
        # Подключаемся к Greenplum
        hook = GreenplumHookSE(*args, **kwargs) # получаем объект "крюк" (hook)
        with closing(hook.get_conn()) as conn:   # получаем подключение
            with closing(conn.cursor()) as cur:  # открываем курсор
                # так можно посмотреть поля и методы объектов (для расширения кругозора или погуглить-посмотреть):
                print(f"@@@ hook :: {type(hook)} :: {dir(hook)}")
                print(f"@@@ conn :: {type(conn)} :: {dir(conn)}")
                print(f"@@@ cur :: {type(cur)} :: {dir(cur)}")
                # проверка подключения, приводит к закрытию и открытию подключения (почему-то...):
                # hook.test_connection()
                print(f"Server version: {conn.server_version}")
                print(f"Connection status: {conn.status}")
                # отключаем автоматический коммит после каждого запуска (по умолчанию включён):
                conn.autocommit = False
                # устанавливаем таймаут для запущенных стейтментов
                #   psycopg2.errors.QueryCanceled: canceling statement due to statement timeout
                # или
                #   psycopg2.extensions.QueryCanceledError: canceling statement due to statement timeout
                # которое можно перехватить так:
                # import psycopg2
                # try:
                #     ...
                # except psycopg2.errors.QueryCanceled, psycopg2.extensions.QueryCanceledError as e:
                #     print(f"Запрос прерван по ошибке: {e}")
                sql = task.render_template(dag_params.get("set_sess_timeout"), kwargs)
                print(sql)
                cur.execute(sql)
                print(f"""Execute statement "{sql}" :: server return message: "{cur.statusmessage}".""")
                # рендерим шаблон в переменную sql, которую потом передаём на запуск
                sql = task.render_template(dag_params.get("drop_table_pet"), kwargs)
                print(sql)
                cur.execute(sql)
                print(f"""Operation 0 (drop_table_pet) :: server return message: "{cur.statusmessage}".""")
                sql = task.render_template(dag_params.get("create_table_pet"), kwargs)
                print(sql)
                cur.execute(sql)
                print(f"""Operation 1 (create_table_pet) :: server return message: "{cur.statusmessage}".""")
                # если надо - транкейтим таблицу:
                sql = task.render_template(dag_params.get("trunc_table_pet"), kwargs)
                print(sql)
                cur.execute(sql)
                print(f"""Operation 2 (trunc_table_pet) :: server return message: "{cur.statusmessage}".""")
                # далее перестаю пользоваться cast() в тех случаях, когда мне не нужны подсказки IDE
                sql = task.render_template(dag_params.get("insert_pets_with_params"), kwargs)
                print(sql)
                cur.executemany(sql, dag_params.get("list_pets"))
                print("Operation 3 (insert_pets) :: server return message:"
                      f""" "{cur.statusmessage}"; records inserted: {cur.rowcount}""")
                conn.commit()
                sql = task.render_template(dag_params.get("select_all_pets"), kwargs)
                print(sql)
                cur.execute(sql)
                print(f"""Operation 4 (select_all_pets):: server return message: "{cur.statusmessage}".""")
                desc = cur.description
                rows = cur.fetchall()
                print(f"Row count: {cur.rowcount}")
                print("======================== First 100 records ========================")
                for d in desc:
                    print(f"{d[0]}: <type {d[1]} {tuple(d[2:])}>")
                print("-------------------------------------------------------------------")
                for x in rows[:100]:
                    print(x)
                print("============================== End ================================")
                conn.commit()
                # *** Работа с pandas
                # Сброс ограничений на количество выводимых рядов 'display.max_rows'
                # Сброс ограничений на число столбцов 'display.max_columns'
                # Сброс ограничений на количество символов в записи 'display.max_colwidth'
                # Сброс ограничений на ширину экрана 'display.width'
                # https://www.delftstack.com/ru/howto/python-pandas/how-to-pretty-print-an-entire-pandas-series-dataframe/
                with pandas.option_context('display.max_rows', None,
                                           'display.max_columns', None,
                                           'display.max_colwidth', None,
                                           'display.width', None,
                                           ):
                    # Запрашиваем данные сразу в pandas.DataFrame
                    sql = task.render_template(dag_params.get("select_all_pets"), kwargs)
                    print(sql)
                    df = psql.read_sql(sql, con=conn)
                    print(f"""Operation 5 (select_all_pets_to_pandas_df) :: Pandas DataFrame contents:\n{df}""")
                    sql = task.render_template(dag_params.get("select_role_privileges"), kwargs)
                    print(sql)
                    df = psql.read_sql(sql, con=conn)
                    print(f"""Operation 6 (select_role_privileges_to_pandas_df) :: Pandas DataFrame contents:\n{df}""")
        return {"status": "OK"}

    gp_create_table_pet = PythonOperator(task_id="create_table_pet",
                                         python_callable=func_gp_create_table_pet,
                                         # можно передавать позиционные аргументы
                                         op_args=list(),
                                         # можно передавать именованные аргументы
                                         op_kwargs=dict(
                                             greenplum_se_conn_id=dag.default_args.get("greenplum_se_conn_id"),
                                         ),
                                         )

    # start = DummyOperator(task_id="start")
    # finish = DummyOperator(task_id="finish")

    # start >> gp_create_table_pet >> finish

    gp_create_table_pet

if __name__ == "__main__":
    dag.cli()
```

---

## Описание модулей входящих в пакет

TODO: позже

#### to be continued...
