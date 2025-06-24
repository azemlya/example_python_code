
"""

"""
from __future__ import annotations

from socket import getfqdn
from datetime import datetime
from json import loads, dumps

from airflow_se.obj_imp import (
    Context,
    conf as af_conf,
    create_session,
    Log,
    DagRun,
    TriggerDagRunOperator,
    AirflowException,
)
from airflow_se.db import create_new_audit_record
from airflow_se.settings import Settings, get_settings

from se.airflow import DAG

__all__ = [
    'TriggerDagRunOperatorSE',
]


class TriggerDagRunOperatorSE(TriggerDagRunOperator):
    """
    Triggers a DAG run for a specified ``dag_id``.

    :param trigger_dag_id: The dag_id to trigger (templated).
    :param trigger_run_id: The run ID to use for the triggered DAG run (templated).
        If not provided, a run ID will be automatically generated.
    :param conf: Configuration for the DAG run (templated).
    :param execution_date: Execution date for the dag (templated).
    :param reset_dag_run: Whether clear existing dag run if already exists.
        This is useful when backfill or rerun an existing dag run.
        This only resets (not recreates) the dag run.
        Dag run conf is immutable and will not be reset on rerun of an existing dag run.
        When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
        When reset_dag_run=True and dag run exists, existing dag run will be cleared to rerun.
    :param wait_for_completion: Whether or not wait for dag run completion. (default: False)
    :param poke_interval: Poke interval to check dag run status when wait_for_completion=True.
        (default: 60)
    :param allowed_states: List of allowed states, default is ``['success']``.
    :param failed_states: List of failed or dis-allowed states, default is ``None``.
    :param deferrable: If waiting for completion, whether or not to defer the task until done,
        default is ``False``.
    """

    msg_prefix = '@@@ TriggerDagRunOperatorSE >> '

    def __init__(
            self,
            *,
            trigger_dag_id: str,
            trigger_run_id: str | None = None,
            conf: dict | None = None,
            execution_date: str | datetime | None = None,
            reset_dag_run: bool = False,
            wait_for_completion: bool = False,
            poke_interval: int = 60,
            allowed_states: list[str] | None = None,
            failed_states: list[str] | None = None,
            deferrable: bool = af_conf.getboolean("operators", "default_deferrable", fallback=False),
            **kwargs
    ) -> None:
        self.settings: Settings = get_settings()
        self.trigger_dag_id = trigger_dag_id
        super().__init__(
            trigger_dag_id=trigger_dag_id,
            trigger_run_id=trigger_run_id,
            conf=conf,
            execution_date=execution_date,
            reset_dag_run=reset_dag_run,
            wait_for_completion=wait_for_completion,
            poke_interval=poke_interval,
            allowed_states=allowed_states,
            failed_states=failed_states,
            deferrable=deferrable,
            **kwargs
        )

    @staticmethod
    def audit_action_add(host: str,
                         remote_addr: str,
                         remote_login: str,
                         code_op: str,
                         app_id: str,
                         type_id: str,
                         subtype_id: str,
                         status_op: str,
                         extras: dict,
                         ts: datetime = datetime.now(),
                         ):
        return create_new_audit_record(ts = ts,
                                       host = host,
                                       remote_addr = remote_addr,
                                       remote_login = remote_login,
                                       code_op = code_op,
                                       app_id = app_id,
                                       type_id = type_id,
                                       subtype_id = subtype_id,
                                       status_op = status_op,
                                       extras = extras,
                                       )

    def execute(self, context: Context):

        # self.log.info(f'{self.msg_prefix}context: {type(context)} = {context.keys()}')
        # self.log.info(f'{self.msg_prefix}dag: {type(context.get("dag"))} = {context.get("dag")}')
        # self.log.info(f'{self.msg_prefix}dag_run: {type(context.get("dag_run"))} = {context.get("dag_run")}')

        # self.log.info(f'{self.msg_prefix}self.conf before processing: {self.conf}')

        # проверки переданного параметра conf
        if isinstance(self.conf, dict):
            try:
                dumps(self.conf)
            except TypeError:
                raise AirflowException(f'{self.msg_prefix}conf parameter should be JSON Serializable')
        elif isinstance(self.conf, str):
            try:
                new_conf = loads(self.conf)
                if isinstance(new_conf, dict):
                    self.conf = new_conf
                else:
                    raise AirflowException(f'{self.msg_prefix}conf parameter should be dictionary(dict)')
            except Exception as e:
                raise AirflowException(f'{self.msg_prefix}conf parameter should be JSON Serializable/Deserializable: {e}')
        else:
            self.conf = dict()

        # self.log.info(f'{self.msg_prefix}self.conf after processing: {self.conf}')

        dag, dag_run = context.get("dag"), context.get("dag_run")
        if not isinstance(dag, DAG) or not isinstance(dag_run, DagRun):
            raise AirflowException(f'{self.msg_prefix}invalid context')

        self.log.info(f'{self.msg_prefix}DAG run type is {dag_run.run_type}')

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
                    # self.log.info(f'{self.msg_prefix}Result of Log info:{sep}{sep.join([str(x) for x in log_info])}')

                    log = sess.query(Log).where(
                        Log.dag_id == dag.dag_id,
                        Log.event.in_(['trigger', 'dag_run.create', ])
                    ).order_by(Log.dttm.desc()).first()
                    if log:
                        user_of_run = log.owner
                    else:
                        raise AirflowException('Records of DAG run not fount in table Log')
        else:
            raise AirflowException(f'{self.msg_prefix}unknown run type "{dag_run.run_type}"')

        # self.log.info(f'{self.msg_prefix}finally user = {user_of_run}')
        self.conf.update({'user': user_of_run})
        # self.log.info(f'{self.msg_prefix}finally self.conf: {type(self.conf)} = {self.conf}')

        aud_msg = {
            'ts': datetime.now(),
            'host': getfqdn(),
            'remote_addr': 'no_present_this_context',
            'remote_login': user_of_run,
            'code_op': 'other audit operations',
            'app_id': self.settings.kafka_app_id,
            'type_id': 'Audit',
            'subtype_id': 'F0',
            'status_op': 'SUCCESS',
            'extras': {
                'PARAMS': dict(
                    EVENT_TYPE='TRIGGER_DAG_EVENTS',
                    EVENT=f'User `{user_of_run}` triggered DAG from `{dag.dag_id}` to `{self.trigger_dag_id}`',
                    DAG_ID=dag.dag_id,
                    TRIGGERED_DAG_ID=self.trigger_dag_id,
                ),
                'SESSION_ID': 'no_present_this_context',
                'EDITOR_USER_LOGIN': user_of_run,
                'EDITOR_ROLE': 'no_present_this_context',
            },
        }
        msg = self.audit_action_add(**aud_msg)
        self.log.info(f'{self.msg_prefix}audit record added: {msg}')

        super().execute(context=context)

