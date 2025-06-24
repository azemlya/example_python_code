
"""

"""
from psycopg2 import connect as pg_connect
from contextlib import closing
from os import environ

from airflow_se.utils import gen_pass
from airflow_se.config import get_config_value
from airflow_se.crypt import encrypt
from airflow_se.secman import auth_secman, get_secman_data, push_secman_data
from airflow_se.commons import SECMAN_KEY_FOR_SECRET

__all__ = [
    'change_pass_dbmd',
]

def change_pass_dbmd() -> int:
    try:
        new_pass = gen_pass(length=32, extended_chars="!*()[].,-_")
        print(f'Generated new password: {new_pass}')
        dsn = dict(
            connect_timeout=10,
            application_name="airflow_se",
            user=get_config_value('DB_METADATA_PG_USERNAME'),
            password=get_config_value('DB_METADATA_PG_USERPASS'),
            host=get_config_value('DB_METADATA_PG_HOST'),
            port=get_config_value('DB_METADATA_PG_PORT'),
            dbname=get_config_value('DB_METADATA_PG_DBNAME'),
        )
        _sslrootcert = get_config_value("SE_DB_METADATA_PG_SSLROOTCERT")
        _sslcert = get_config_value("SE_DB_METADATA_PG_SSLCERT")
        _sslkey = get_config_value("SE_DB_METADATA_PG_SSLKEY")
        if _sslrootcert and _sslcert and _sslkey:
            dsn.update(dict(sslmode="verify-full", sslrootcert=_sslrootcert, sslcert=_sslcert, sslkey=_sslkey))
        else:
            dsn.update(dict(sslmode="disable"))
        print(f"Args to connection: {dsn}")
        cmd = [
            f"ALTER USER {dsn.get('user')} WITH PASSWORD '{new_pass}';",
            f"ALTER ROLE {dsn.get('user')} SET search_path TO {get_config_value('DB_METADATA_PG_SCHEMA')};",
            f"ALTER ROLE {dsn.get('user')} SET role TO 'as_admin';",
            f"ALTER ROLE as_admin SET search_path TO {dsn.get('user')};",
        ]
        with closing(pg_connect(**dsn)) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    print(f"Execute statement: {cmd[0]}")
                    cur.execute(cmd[0])
                    if hasattr(cur, "statusmessage"):
                        print(f"    Server return: {cur.statusmessage}")
                    else:
                        print("    Server don't return status message.")
                    token = auth_secman()
                    sm_data = get_secman_data(SECMAN_KEY_FOR_SECRET, token) or dict()
                    sm_data['SE_DB_METADATA_PG_USERPASS'] = encrypt(new_pass)
                    if push_secman_data(SECMAN_KEY_FOR_SECRET, sm_data, token):
                        print("    New password pushed to SecMan")
                    else:
                        raise RuntimeError(f"    New password '{new_pass}' DON'T pushed to SecMan!!!")
                except Exception as ue:
                    print(f"    {ue}")
                    return 1
                for x in cmd[1:]:
                    print(f"Execute statement: {x}")
                    try:
                        cur.execute(x)
                        if hasattr(cur, "statusmessage"):
                            print(f"    Server return: {cur.statusmessage}")
                        else:
                            print("    Server not return status message.")
                    except Exception as xe:
                        print(f"    Executing error: {xe}")

        # from sqlalchemy import create_engine, text
        # engine = create_engine(url=url)
        # cmd = f"ALTER USER {get_config_value('DB_METADATA_PG_USERNAME')} WITH PASSWORD '{new_pass}';"
        # print(f'Execute statement: {cmd}')
        # conn = None
        # try:
        #     conn = engine.connect()
        # except Exception as asd:
        #     print(f'{asd}')
        # if conn:
        #     result = conn.execute(text(cmd))
        #     print(f'{result=}')
        #     print('New password changed on Metadata DB')
        #     token = auth_secman()
        #     sm_data = get_secman_data(SECMAN_KEY_FOR_SECRET, token) or dict()
        #     sm_data['SE_DB_METADATA_PG_USERPASS'] = encrypt(new_pass)
        #     if push_secman_data(SECMAN_KEY_FOR_SECRET, sm_data, token):
        #         print('New password pushed to SecMan')
        #         return 0
        #     else:
        #         raise RuntimeError(f"New password '{new_pass}' DON'T pushed to SecMan!!!")
        # else:
        #     print('Connection has been closed')
        # with engine.connect() as conn:
        #     result = conn.execute(text(cmd))
        #     print(result)
        #     print('New password changed on Metadata DB')
    except Exception as e:
        print(f"{e}")
        raise

