
"""
Внутренний аудит
"""
from __future__ import annotations

from socket import getfqdn
from json import loads
from flask import render_template
from functools import lru_cache
from datetime import datetime
from typing import Optional, List

from airflow_se.obj_imp import (
    User,
    Role,
    Permission,
    Connection,
    DagModel,
    DagRun,
    SerializedDagModel,
    DagDependency,
    DagRunState,
    get_airflow_app,
    create_session,
)
from airflow_se.config import get_config_value
from airflow_se.logger import LoggingMixinSE
from airflow_se.settings import Settings, get_settings
from airflow_se.db import create_new_audit_record

__all__ = [
    'BaseAuditAirflow',
]


class BaseAuditAirflow(LoggingMixinSE):
    """Base Audit"""
    # модели
    user_model = User
    role_model = Role
    permission_model = Permission
    connection_model = Connection
    dag_model = DagModel
    dag_run_model = DagRun

    # перечисление с возможными статусами DAG-а
    dag_run_state = DagRunState

    def __init__(self):
        self.conf: Settings = get_settings()
        super().__init__(debug=self.conf.debug)
        self.ldap_tech_ctl_groups: set = set(self.get_ldap_tech_ctl_groups())
        self.ldap_tech_ctl_users: set = set(self.get_ldap_tech_ctl_users())
        self.enable_match_user_groups: bool = self.get_enable_match_user_groups()

    @property
    def create_session(self):
        return create_session

    @property
    def airflow_app(self):
        return get_airflow_app()

    @property
    def sm(self):
        return self.airflow_app.appbuilder.sm

    @property
    def get_session(self):
        return self.sm.get_session

    def render_error(self,
                     status: Optional[int] = None,
                     mes: Optional[str] = None,
                     mes_log_debug: Optional[str] = None,
                     mes_log_info: Optional[str] = None,
                     mes_log_warning: Optional[str] = None,
                     mes_log_error: Optional[str] = None,
                     mes_log_critical: Optional[str] = None,
                     ):
        if not isinstance(status, int):
            status = 500
        if not isinstance(mes, str):
            mes = 'No message'
        if mes_log_debug:
            self.log_debug(mes_log_debug)
        if mes_log_info:
            self.log_info(mes_log_info)
        if mes_log_warning:
            self.log_warning(mes_log_warning)
        if mes_log_error:
            self.log_error(mes_log_error)
        if mes_log_critical:
            self.log_critical(mes_log_critical)
        try:
            resp = render_template(
                'airflow/error.html',
                hostname=getfqdn(),
                status_code=status,
                error_message=mes,
            )
            return resp, status
        except Exception as e:
            self.log_exception(f'Error on {self.__class__.__name__}.{__name__}:')
            return f'Internal server error!\nException on render template "airflow/error.html":\n{e}', 500

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

    def get_ldap_tech_ctl_groups(self) -> List[str]:
        """Считываем группы CTL из параметра"""
        ldap_tech_ctl_groups = get_config_value('LDAP_TECH_CTL_GROUPS')
        try:
            ldap_tech_ctl_groups = loads(ldap_tech_ctl_groups)
            if not isinstance(ldap_tech_ctl_groups, List):
                raise ValueError('parameter is not json list')
        except Exception as e:
            self.log_error(f'Missing loads parameter "SE_LDAP_TECH_CTL_GROUPS": {e}')
            ldap_tech_ctl_groups = list()
        return ldap_tech_ctl_groups

    def get_ldap_tech_ctl_users(self) -> List[str]:
        """Считываем пользователей CTL из параметра"""
        ldap_tech_ctl_users = get_config_value('LDAP_TECH_CTL_USERS')
        try:
            ldap_tech_ctl_users = loads(ldap_tech_ctl_users)
            if not isinstance(ldap_tech_ctl_users, List):
                raise ValueError('parameter is not json list')
        except Exception as e:
            self.log_error(f'Missing loads parameter "SE_LDAP_TECH_CTL_USERS": {e}')
            ldap_tech_ctl_users = list()
        return ldap_tech_ctl_users

    @staticmethod
    def get_enable_match_user_groups() -> bool:
        """Параметр определяющий будут ли сравниваться группы пользователей"""
        enable_match_user_groups = get_config_value('ENABLE_MATCH_USER_GROUPS')
        return True if isinstance(enable_match_user_groups, bool) and enable_match_user_groups is True else False

    @lru_cache(maxsize=128, typed=False)
    def matching_groups(self, user1: str, user2: str) -> bool:
        """Проверка, что пользователи состоят в одной группе в LDAP, и эта группа фигурирует в ROLES_MAPPING"""
        if self.enable_match_user_groups is not True:
            # если матчинг групп отключён, то не проверяем и возвращаем False
            return False
        u1, u2 = user1 if isinstance(user1, str) else '', user2 if isinstance(user2, str) else ''
        ldap_groups = set(self.sm.ext_conf.ldap_roles_mapping.keys())
        user1_groups = self.sm.ext_ldap_user_extract_silent(username=u1).get('ldap_groups')
        user2_groups = self.sm.ext_ldap_user_extract_silent(username=u2).get('ldap_groups')
        # делаем intersect (пересечение) групп
        matching_groups = list(set(ldap_groups) & set(user1_groups) & set(user2_groups))
        if len(matching_groups) > 0:
            self.log_debug(f'Matching groups for users `{u1}` and `{u2}`: {matching_groups}')
            return True
        else:
            self.log_debug(f'Not matching groups for users `{u1}` and `{u2}`')
            self.log_debug(f'Info of users groups:\n>>> {ldap_groups=}\n>>> {user1_groups=}\n>>> {user2_groups=}')
            return False

    def get_active_dags_runs_manual(self, dag_ids: list[str]) -> List[DagRun]:
        sess = self.get_session
        rows = sess.query(self.dag_run_model).where(
            self.dag_run_model.dag_id.in_(dag_ids),
            self.dag_run_model.state.in_([self.dag_run_state.RUNNING, self.dag_run_state.QUEUED]),
            self.dag_run_model.run_type == 'manual',
        ).all()
        sess.commit()
        return rows

    @staticmethod
    def get_dags_dependencies() -> dict[str, list[DagDependency]]:
        """Зависимости DAG-ов"""
        return {k: v for k, v in SerializedDagModel.get_dag_dependencies().items() if v}

    @classmethod
    def get_chain_dependencies(cls, dag_id: str) -> list[str]:
        def _recursive_traversal(tree: dict[str, list[DagDependency]], lst: list[str]):
            for k, v in tree.items():
                for i in v:
                    if i.source == lst[-1]:
                        lst.append(i.target)
                        _recursive_traversal(tree, lst)
                    elif i.target == lst[0]:
                        lst.insert(0, i.source)
                        _recursive_traversal(tree, lst)
        _lst = [dag_id, ]
        _recursive_traversal(cls.get_dags_dependencies(), _lst)
        return _lst

