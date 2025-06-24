
"""

"""
from __future__ import annotations

from os import environ, chmod
from typing import TYPE_CHECKING, Mapping, Dict, Optional

from airflow import DAG

from airflow_se.crypt import decrypt
from airflow_se.utils import DataPaths
from airflow_se.config import get_config_value
from airflow_se.secman import auth_secman, get_secman_data
from airflow_se.commons import TICKET_PREFIX, SECMAN_KEY_FOR_TGT
from airflow_se.obj_imp import (
    create_session,
    Log,
    DagRun,
    AirflowException,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

__all__ = [
    "krb_auth",
]

def krb_auth(context: Context) -> Optional[DataPaths]:
    dag, dag_run = context.get("dag"), context.get("dag_run")
    if not isinstance(dag, DAG) or not isinstance(dag_run, DagRun):
        raise AirflowException(f'invalid context')

    dag.log.info(f'DAG run type is "{dag_run.run_type}" ({dag_run.conf=})')

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
                # dag.log.info(f'Result of Log info:{sep}{sep.join([str(x) for x in log_info])}')

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

    k = f"{TICKET_PREFIX}{user_of_run}"
    sm_tgt = get_secman_data(SECMAN_KEY_FOR_TGT, auth_secman())
    if sm_tgt is None:
        sm_tgt: Dict[str, str] = dict()
    v = sm_tgt.get(k)
    if v:
        dp = DataPaths(
            base_path=get_config_value("SECRET_PATH"),
            name="ProviderSparkSE",
        )
        _k = dp.get(k)
        with open(_k, "wb") as f:
            f.write(decrypt(v))
        chmod(_k, 0o600)
        environ["KRB5CCNAME"] = _k
        dag.log.info(f"For authenticate to Spark use ticket from SecMan \"{k}\"")
        return dp
    else:
        raise AirflowException(f"Ticket \"{k}\" for user \"{user_of_run}\" is not found in SecMan")

