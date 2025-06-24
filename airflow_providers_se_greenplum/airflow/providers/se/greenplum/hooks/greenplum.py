
"""

"""
from warnings import simplefilter as warnings_simplefilter
warnings_simplefilter("ignore")

import os
import datetime, time
from contextlib import closing
from typing import Optional, Mapping, Iterable, List, Tuple, Dict

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor, NamedTupleCursor, RealDictCursor

from airflow import DAG
from airflow.hooks.dbapi import DbApiHook
from airflow.models.connection import Connection

from airflow_se.crypt import decrypt
from airflow_se.obj_imp import create_session, DagRun, Log, AirflowException
from airflow_se.utils import (
    DataPaths,
    run_kinit,
    run_klist,
    get_all_envs,
    get_env,
    add_env,
    pop_env,
    timedelta_to_human_format,
)
from airflow_se.config import get_config_value
from airflow_se.parse import parse_tgs
from airflow_se.db import TGSList
from airflow_se.secman import auth_secman, get_secman_data
from airflow_se.commons import TICKET_PREFIX, SECMAN_KEY_FOR_TGT

from ..exception.exception import (
    GreenplumSEEnvError,
    GreenplumSEDagContextError,
    GreenplumSEDagArgumentsError,
    GreenplumSEKerberosError,
    GreenplumSECursorError,
    GreenplumSESetRoleError,
)
from ..commons import name_provider, connection_type

__all__ = [
    'GreenplumHookSE',
]


class GreenplumHookSE(DbApiHook):
    """
    Interact with Greenplum/Postgres.

    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc.}``.
    Also, you can choose cursor as ``{"cursor": "dictcursor"}``. Refer to the
    psycopg2.extras for more details.

    Note: For Redshift, use keepalives_idle in the extra connection parameters
    and set it to less than 300 seconds.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
    For Redshift, also use redshift in the extra connection parameters and
    set it to true. The cluster-identifier is extracted from the beginning of
    the host field, so is optional. It can however be overridden in the extra field.
    extras example: ``{"iam":true, "redshift":true, "cluster-identifier": "my_cluster_id"}``

    :param greenplum_se_conn_id: The :ref:`greenplum conn id <howto/connection:greenplum>`
        reference to a specific greenplum database.
    :type greenplum_se_conn_id: str
    """
    conn_name_attr = "greenplum_se_conn_id"
    default_conn_name = "greenplum_se_default"
    conn_type = connection_type
    hook_name = name_provider
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection: Optional[Connection] = kwargs.get("connection")
        self.conn: Optional[connection] = None
        self.schema: Optional[str] = kwargs.get("schema")
        self.setrole: Optional[str] = kwargs.get("setrole")
        self.context: Optional[Dict] = kwargs.get("context") or kwargs
        # if not isinstance(self.context, Mapping):
        #     dag: Optional[DAG] = kwargs.get("dag")
        #     if isinstance(dag, DAG):
        #         self.context = dict(dag=dag)
        #     else:
        #         self.log.error("GreenplumSE => DagContextError: Context not found")
        #         raise GreenplumSEDagContextError("Context is not found")
        self.dag_def_args: Dict
        _dag: Optional[DAG] = self.context.get("dag") if isinstance(self.context, Mapping) else None
        if isinstance(_dag, DAG) and hasattr(_dag, "default_args"):
            self.dag_def_args = getattr(_dag, "default_args")
            if not isinstance(self.dag_def_args, Dict):
                self.log.error("GreenplumSE => DagArgumentsError: DAG parameter \"default_args\" must be type dict")
                self.dag_def_args = dict()
                # raise GreenplumSEDagArgumentsError("DAG parameter \"default_args\" must be type dict")
        else:
            self.log.error("GreenplumSE => DagArgumentsError: DAG parameter \"default_args\" must be "
                           "set and must be type dict")
            self.dag_def_args = dict()
            # raise GreenplumSEDagArgumentsError("DAG parameter \"default_args\" must be set and must be type dict")

    def _get_cursor_type(self, raw_cursor: Optional[str]):
        _cursor: str = raw_cursor.lower().strip() if isinstance(raw_cursor, str) else ""
        if _cursor == "dictcursor":
            self.log.info("Cursor type \"DictCursor\" has been applied")
            return DictCursor
        elif _cursor == "realdictcursor":
            self.log.info("Cursor type \"RealDictCursor\" has been applied")
            return RealDictCursor
        elif _cursor == "namedtuplecursor":
            self.log.info("Cursor type \"NamedTupleCursor\" has been applied")
            return NamedTupleCursor
        else:
            # raise GreenplumSECursorError(f"""Invalid cursor passed "{_cursor}", must be set of:"""
            #                              """ "dictcursor", "realdictcursor", "namedtuplecursor".""")
            self.log.info("Cursor type is not defined, default cursor type \"DictCursor\" has been applied")
            return DictCursor

    def get_conn(self) -> connection:
        """Establishes a connection to a greenplum database."""
        # if self.conn:
        #     try:
        #         with closing(self.conn.cursor()) as cur:
        #             cur.execute(f"SELECT 1;")
        #             self.conn.commit()
        #     except Exception as e:
        #         self.conn = None
        #         self.log.warning(f"Reconnecting to Greenplum, as the connection is lost. Error: {e}")
        #     else:
        #         return self.conn

        dp: Optional[DataPaths] = None

        conn_id = getattr(self, self.conn_name_attr)
        conn = self.connection or self.get_connection(conn_id)

        ah = os.environ.get("AIRFLOW_HOME")
        if not ah:
            raise GreenplumSEEnvError("Environment variable \"AIRFLOW_HOME\" don't set")
        _extra = {k.strip().lower(): v for k, v in conn.extra_dejson.items()}
        owner = _extra.get("owner") or self.dag_def_args.get("owner")
        username = _extra.get("login") or _extra.get("user") or conn.login or owner
        prm = dict(
            kinit="kinit", klist="klist", lifetime="8h", renewable="7d",
            host=conn.host,
            port=conn.port or "5432",
            dbname=_extra.get("dbname") or _extra.get("schema") or conn.schema or self.schema,
            user=username,
            owner=owner,
            password=conn.password,
            pwd=conn.password,
        )
        prm.update(_extra)

        conn_args_exclude_keys = (
            "login", "owner", "schema", "keytab", "ticket", "realm", "kinit", "klist", "lifetime", "renewable",
            "setrole", "cursor", "pwd", "ticket_prefix", "description",
        )
        conn_args = {k: v for k, v in prm.items() if k not in conn_args_exclude_keys}

        conn_args.update(cursor_factory=self._get_cursor_type(prm.get("cursor")))

        krb_args_include_keys = (
            "user", "keytab", "ticket", "realm", "kinit", "klist", "lifetime", "renewable", "pwd",
        )
        krb_args = {k: v for k, v in prm.items() if k in krb_args_include_keys}

        # определяем пользователя, под которым подключаемся
        dag, dag_run = self.context.get("dag"), self.context.get("dag_run")
        if not isinstance(dag, DAG) or not isinstance(dag_run, DagRun):
            raise AirflowException(f'invalid context')

        self.log.info(f'DAG run type is "{dag_run.run_type}" ({dag_run.conf=})')

        if dag_run.run_type == 'scheduled':
            user_of_run = dag_run.conf.get('user') or dag.owner
        elif dag_run.run_type == 'manual':
            user_of_run = dag_run.conf.get('user')
            if not user_of_run:
                with create_session() as sess:

                    # log_info = sess.query(
                    #     Log.id, Log.event, Log.owner, Log.dttm, Log.task_id, Log.map_index,
                    #     Log.execution_date, Log.owner_display_name, Log.extra
                    # ).where(Log.dag_id == dag.dag_id).order_by(Log.dttm.desc()).all()
                    # sep = '\n    '
                    # self.log.info(f'Result of Log info:{sep}{sep.join([str(x) for x in log_info])}')

                    log = sess.query(Log).where(
                        Log.dag_id == dag.dag_id,
                        Log.event.in_(['trigger', 'dag_run.create', ])
                    ).order_by(Log.dttm.desc()).first()
                    if log:
                        user_of_run = log.owner
                    else:
                        raise AirflowException('Records of DAG run not fount in table Log')
        else:
            raise AirflowException(f'Unknown run type "{dag_run.run_type}"')

        if user_of_run:  # если пользователь определён, то переопределяем на него
            prm['user'] = user_of_run
            prm['owner'] = user_of_run
            conn_args['user'] = user_of_run
            krb_args['user'] = user_of_run

        if krb_args.get("keytab") and not krb_args.get("keytab").isspace():  # если есть кейтаб, то работаем с ним
            is_start_krb: bool = False  # признак, надо получать тикет или ещё старый не протух
            tgt = krb_args.get("ticket")
            if not isinstance(tgt, str) or tgt.isspace():
                raise GreenplumSEKerberosError(f"Variable \"keytab\" is set, but variable \"ticket\" is not set. Why?")
            if os.path.exists(tgt) and os.path.isfile(tgt):
                ts_tf = time.gmtime(os.path.getmtime(tgt))
                dt_tf = datetime.datetime(ts_tf.tm_year, ts_tf.tm_mon, ts_tf.tm_mday,
                                          ts_tf.tm_hour, ts_tf.tm_min, ts_tf.tm_sec)
                td = datetime.datetime.utcnow() - dt_tf
                td_days, td_hours, td_minutes, td_seconds = timedelta_to_human_format(td)
                self.log.info(f"""The ticket file "{tgt}" is outdated on """
                              f"""{td_days} days, {td_hours:02}:{td_minutes:02}:{td_seconds:02}""")
                if td > datetime.timedelta(hours=4):  # пока 4 часа // потом надо придумать как разрулить
                    is_start_krb = True  # прошло больше 4 часов, надо обновлять тикет // потом разрулить
            else:
                is_start_krb = True
            if is_start_krb is True:
                if not self.exp_auth_kerberos(**krb_args):
                    _krb_args = {k: "*******" if k in ("pwd", "password", ) and v else v for k, v in krb_args.items()}
                    self.log.warning(f"Missing receive a Kerberos ticket, arguments: {_krb_args}")
                    # raise GreenplumSEKerberosError(f"Error receive a kerberos ticket file. Arguments: {_krb_args}")
            else:
                self.log.info("The kerberos ticket is fresh, no generation is required")
        elif krb_args.get("ticket") and not krb_args.get("ticket").isspace():
            # кейтаба нет, но есть тикет, считаем, что он свежий и используем его
            self.log.info(f"For connect to Greenplum use ticket \"{krb_args.get('ticket')}\"")
        elif prm.get("ticket_prefix") and prm.get("owner") and not prm.get("ticket_prefix").isspace() and\
                not prm.get("owner").isspace():  # если есть ticket_prefix и owner, то используем тикет владельца
            krb_args["ticket"] = prm.get("ticket_prefix").strip() + prm.get("owner").strip()
            self.log.info(f"For connect to Greenplum use ticket \"{krb_args.get('ticket')}\"")
        else:  # иначе, используем тикет пользователя
            # _prm = {k: "*******" if k in ("pwd", "password", ) and v else v for k, v in prm.items()}
            # self.log.info(f"Connection information: {_prm}")

            # # проба определить запускающего
            # sep = '\n    '
            # with create_session() as sess:
            #     res = sess.query(Log.dttm, Log.dag_id, Log.execution_date, Log.owner, Log.extra)\
            #         .filter(Log.dag_id == self.context.get('dag').dag_id, Log.event == 'dag_run.create')\
            #         .order_by(Log.dttm.desc()).all()
            #     self.log.info(f'Result select (event == dag_run.create):\n    {sep.join([str(x) for x in res])}')
            #     res = sess.query(Log.dttm, Log.dag_id, Log.execution_date, Log.owner, Log.extra)\
            #         .filter(Log.dag_id == self.context.get('dag').dag_id, Log.event == 'trigger')\
            #         .order_by(Log.dttm.desc()).all()
            #     self.log.info(f'Result select (event == trigger):\n    {sep.join([str(x) for x in res])}')
            #     res = sess.query(Log.dttm, Log.dag_id, Log.execution_date, Log.owner, Log.extra)\
            #         .filter(Log.dag_id == self.context.get('dag').dag_id, Log.event.in_(['trigger', 'dag_run.create', ]))\
            #         .order_by(Log.dttm.desc()).all()
            #     self.log.info(f'Result select (event in [trigger, dag_run.create, ]):\n    {sep.join([str(x) for x in res])}')
            #
            #     res_info = sess.query(
            #         Log.id, Log.event, Log.owner, Log.dttm, Log.task_id, Log.map_index,
            #         Log.execution_date, Log.owner_display_name, Log.extra
            #     ).where(Log.dag_id == self.context.get('dag').dag_id).order_by(Log.dttm.desc()).all()
            #     self.log.info(f'Result select:{sep}{sep.join([str(x) for x in res_info])}')
            #     res = sess.query(Log).where(
            #         Log.dag_id == self.context.get('dag').dag_id,
            #         Log.event.in_(['trigger', 'dag_run.create', ])
            #     ).order_by(Log.dttm.desc()).first()
            #     self.log.info(f'User triggered DAG run: {res}')
            #     self.log.info(f'{self.kwargs.keys()=}')
            #     self.log.info(f'{self.context.keys()=}')
            #     self.log.info(f'{self.context.get("dag_run")=}')
            #     self.log.info(f'{self.context.get("dag_run").run_type=}')

            k = f"{TICKET_PREFIX}{prm.get('owner')}"
            sm_tgt = get_secman_data(SECMAN_KEY_FOR_TGT, auth_secman())
            if sm_tgt is None:
                sm_tgt: Dict[str, str] = dict()
            v = sm_tgt.get(k)
            if v:
                dp = DataPaths(
                    base_path=get_config_value("SECRET_PATH") or "/tmp",
                    name="ProviderGreenplumSE",
                )
                _k = dp.get(k)
                with open(_k, "wb") as f:
                    f.write(decrypt(v))
                os.chmod(_k, 0o600)
                krb_args["ticket"] = _k
                self.log.info(f"For connect to Greenplum use ticket from SecMan \"{k}\"")
            else:
                raise AirflowException(f"Ticket \"{k}\" for user \"{prm.get('owner')}\" is not found in SecMan")

        prev_krb5ccname = pop_env("KRB5CCNAME")
        prev_krb5_ktname = pop_env("KRB5_KTNAME")
        try:
            if krb_args.get("ticket") and not krb_args.get("ticket").isspace():
                add_env("KRB5CCNAME", krb_args.get("ticket"))
            if krb_args.get("keytab") and not krb_args.get("keytab").isspace():
                add_env("KRB5_KTNAME", krb_args.get("keytab"))
            conn_args.update(dict(connect_timeout=10, application_name="airflow_se"))
            # disable gssEncMode (выяснилось, что не требуется)
            # conn_args.update(dict(gssencmode="disable"))
            _sslrootcert = get_env("SE_PROVIDER_GP_SSLROOTCERT")
            _sslcert = get_env("SE_PROVIDER_GP_SSLCERT")
            _sslkey = get_env("SE_PROVIDER_GP_SSLKEY")
            if _sslrootcert and _sslcert and _sslkey:
                self.log.info("SSL enabled, mode \"verify-full\" (mTLS)")
                conn_args.update(dict(sslmode="verify-full", sslrootcert=_sslrootcert, sslcert=_sslcert, sslkey=_sslkey))
            else:
                self.log.warning("SSL disabled (not present certificates in SecMan)")
            try:
                # пробуем подключиться под УЗП (как есть)
                self.log.info(f"(step 1) For GSSAPI authentication use login \"{conn_args.get('user')}\" and ticket")
                self.conn = psycopg2.connect(**conn_args)
            except Exception as ex1:
                # пробуем подключиться под AD УЗП (откусываем хвостик IPA от УЗП)
                self.log.warning(f" --- Exception: {ex1}")
                self.log.warning(f" --- Missing authenticate for login \"{conn_args.get('user')}\", trying digital login...")
                new_user = conn_args.get("user")
                if isinstance(new_user, str) and not new_user.isspace():
                    new_user = new_user.strip().split("_")[0]
                    if new_user.isdigit() and len(new_user) > 6:
                        conn_args["user"] = new_user
                self.log.info(f"(step 2) For GSSAPI authentication use login \"{conn_args.get('user')}\" and ticket")
                self.conn = psycopg2.connect(**conn_args)
            self.log.info(f"Successful connected to server {conn_args.get('host')}:{conn_args.get('port')}")

            newln = '\n'
            ret = run_klist(ticket=krb_args.get('ticket'))
            msg = f'KLIST return code {ret.get("returncode")}: [ {" ".join(ret.get("command"))} ]' \
                  f'{newln + ret.get("stdout").strip() if ret.get("stdout").strip() else ""}' \
                  f'{newln + ret.get("stderr").strip() if ret.get("stderr").strip() else ""}'
            self.log.debug(msg)
            for match in parse_tgs(msg):
                self.log.debug(f'Find match: {match}')
                TGSList.push_tgs(match)
            # self.log.debug(f'List TGS\'s in DB: {TGSList.get_tgs_list()}')

        except Exception as e:
            _conn_args = {k: "*******" if k in ("pwd", "password", ) and v else v for k, v in conn_args.items()}
            _krb_args = {k: "*******" if k in ("pwd", "password", ) and v else v for k, v in krb_args.items()}
            self.log.warning(f"Connection arguments: {_conn_args}")
            self.log.warning(f"Kerberos arguments: {_krb_args}")
            raise e
        finally:
            if prev_krb5ccname:
                add_env("KRB5CCNAME", prev_krb5ccname)
            if prev_krb5_ktname:
                add_env("KRB5_KTNAME", prev_krb5_ktname)
            if dp:
                del dp

        # sr = prm.get("setrole") or self.setrole or self.dag_def_args.get("setrole")
        # if isinstance(sr, str) and not sr.isspace():
        #     with closing(self.conn.cursor()) as cur:
        #         try:
        #             cur.execute(f"SET ROLE \"{sr}\";")
        #             if hasattr(cur, "statusmessage") and isinstance(cur.statusmessage, str) and\
        #                     cur.statusmessage.strip().lower() == "set":
        #                 self.log.info(f"The Role has been changed to \"{sr}\", "
        #                               f"server return status operation: {cur.statusmessage}")
        #             else:
        #                 raise ValueError(f"Missing SET ROLE to \"{sr}\", "
        #                                  f"server return status operation: {cur.statusmessage}")
        #         except Exception as e:
        #             raise GreenplumSESetRoleError(f"SET ROLE Error: {e}")
        #         self.conn.commit()

        return self.conn

    def copy_expert(self, sql: str, filename: str) -> None:
        """
        Executes SQL using psycopg2 copy_expert method.
        Necessary to execute COPY command without access to a superuser.

        Note: if this method is called with a "COPY FROM" statement and
        the specified input file does not exist, it creates an empty
        file and no data is loaded, but the operation succeeds.
        So if users want to be aware when the input file does not exist,
        they have to check its existence by themselves.
        """
        self.log.info(f"Running copy expert: {sql}, filename: {filename}")
        if not os.path.isfile(filename):
            with open(filename, "w"):
                self.log.info(f"The file {filename} did not exist and was created empty.")

        with open(filename, "r+") as file:
            with closing(self.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql, file)
                    file.truncate(file.tell())
                    conn.commit()

    def get_uri(self) -> str:
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        uri = super().get_uri()
        if conn.extra_dejson.get("client_encoding", False):
            charset = conn.extra_dejson["client_encoding"]
            return f"{uri}?client_encoding={charset}"
        return uri

    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Loads a tab-delimited file into a database table"""
        self.copy_expert(f"COPY {table} FROM STDIN", tmp_file)

    def bulk_dump(self, table: str, tmp_file: str) -> None:
        """Dumps a database table into a tab-delimited file"""
        self.copy_expert(f"COPY {table} TO STDOUT", tmp_file)

    @staticmethod
    def _serialize_cell(cell: object, conn: Optional[connection] = None) -> object:
        """
        Postgresql will adapt all arguments to execute() method internally,
        hence we return cell without any conversion.

        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The cell
        :rtype: object
        """
        return cell

    def get_table_primary_key(self, table: str, schema: Optional[str] = "public") -> Optional[List[str]]:
        """
        Helper method that returns the table primary key

        :param table: Name of the target table
        :type table: str
        :param table: Name of the target schema, public by default
        :type table: str
        :param schema: Schema name
        :type schema: str
        :return: Primary key columns list
        :rtype: List[str]
        """
        sql = "select kcu.column_name"\
              "from information_schema.table_constraints tco"\
              "  join information_schema.key_column_usage kcu"\
              "    on kcu.constraint_name = tco.constraint_name"\
              "      and kcu.constraint_schema = tco.constraint_schema"\
              "      and kcu.constraint_name = tco.constraint_name"\
              "where tco.constraint_type = \"PRIMARY KEY\""\
              "  and kcu.table_schema = %s"\
              "  and kcu.table_name = %s"
        pk_columns = [row[0] for row in self.get_records(sql, (schema, table))]
        return pk_columns or None

    @staticmethod
    def _generate_insert_sql(
        table: str, values: Tuple[str, ...], target_fields: Iterable[str], replace: bool, **kwargs
    ) -> str:
        """
        Static helper method that generates the INSERT SQL statement.
        The REPLACE variant is specific to PostgreSQL syntax.

        :param table: Name of the target table
        :type table: str
        :param values: The row to insert into the table
        :type values: tuple of cell values
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param replace: Whether to replace instead of insert
        :type replace: bool
        :param replace_index: the column or list of column names to act as
            index for the ON CONFLICT clause
        :type replace_index: str or list
        :return: The generated INSERT or REPLACE SQL statement
        :rtype: str
        """
        placeholders = [
            "%s",
        ] * len(values)
        replace_index = kwargs.get("replace_index")

        if target_fields:
            target_fields_fragment = ", ".join(target_fields)
            target_fields_fragment = f"({target_fields_fragment})"
        else:
            target_fields_fragment = ""

        sql = f"""INSERT INTO {table} {target_fields_fragment} VALUES ({",".join(placeholders)})"""

        if replace:
            if target_fields is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names")
            if replace_index is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires an unique index")
            if isinstance(replace_index, str):
                replace_index = [replace_index]
            replace_index_set = set(replace_index)

            replace_target = [
                "{0} = excluded.{0}".format(col) for col in target_fields if col not in replace_index_set
            ]
            sql += f""" ON CONFLICT ({", ".join(replace_index)}) DO UPDATE SET {", ".join(replace_target)}"""
        return sql

    def exp_auth_kerberos(self,
                          user, realm, keytab, ticket, kinit="kinit", klist="klist", lifetime="8h", renewable="7d",
                          **kwargs
                          ) -> bool:
        """
        """
        _ = kwargs
        ret = run_kinit(
            userid=user,
            realm=realm,
            keytab=keytab,
            ticket=ticket,
            lifetime=lifetime,
            renewable=renewable,
            kinit=kinit,
            verbose=True,
        )
        self.log.debug(f"KINIT returned: {ret}")
        if ret.get("returncode") != 0:
            self.log.error(f"KINIT returned: {ret}")
            return False
        ret = run_klist(ticket=ticket, klist=klist)
        self.log.debug(f"KLIST returned: {ret}")
        if ret.get("returncode") != 0:
            self.log.error(f"KLIST returned: {ret}")
            return False
        self.log.info(f"The ticket file \"{ticket}\" has been updated")
        return True

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
