
"""
Прокладка с импортами объектов из пакета airflow
(уж очень они любят, от версии к версии, устраивать релокации модулей внутри)
Импортируем централизованно
"""
from __future__ import annotations

from typing import TYPE_CHECKING

try:
    # справедливо для версии 2.6.3 и ниже
    from airflow.www.security import AirflowSecurityManager as AFScrtMngr
except:
    # справедливо для версии 2.7.0 и выше
    from airflow.auth.managers.fab.security_manager.override import FabAirflowSecurityManagerOverride as AFScrtMngr

try:
    # справедливо для версии 2.6.3 и ниже
    from airflow.www.fab_security.sqla.models import User, Role, Permission, Action, Resource
except:
    try:
        # справедливо для версии 2.7.0 и выше
        from airflow.auth.managers.fab.models import User, Role, Permission, Action, Resource
    except:
        # справедливо для версии 2.9.0 и выше
        from airflow.providers.fab.auth_manager.models import User, Role, Permission, Action, Resource

try:
    # справедливо для версии 2.7.0 и выше
    from airflow.www.extensions.init_auth_manager import get_auth_manager
    from airflow.auth.managers.base_auth_manager import BaseAuthManager
except:
    get_auth_manager = None
    BaseAuthManager = None

from airflow import settings
from airflow.configuration import AirflowConfigParser, conf
# from airflow.models.dag import DagModel  # таблица dag
# from airflow.models.dagrun import DagRun  # таблица dag_run
# from airflow.models.connection import Connection  # таблица connection
# from airflow.models.log import Log  # таблица log
from airflow.models import Connection, DagModel, DagRun, Log
from airflow.serialization.serialized_objects import DagDependency
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.state import DagRunState
from airflow.models.serialized_dag import SerializedDagModel
from airflow.serialization.serialized_objects import DagDependency
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunLink
from airflow.utils.db import create_session  # получить сессию SQLAlchemy в контексте Airflow
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.exceptions import AirflowException, AirflowNotFoundException, AirflowBadRequest
from airflow.models.xcom import BaseXCom

if TYPE_CHECKING:
    from airflow.utils.context import Context
else:
    Context = None

__all__ = [
    'AFScrtMngr',
    'User',
    'Role',
    'Permission',
    'Action',
    'Resource',
    'settings',
    'Connection',
    'DagModel',
    'DagRun',
    'Log',
    'DagDependency',
    'SerializedDagModel',
    'DagRunState',
    'SerializedDagModel',
    'DagDependency',
    'TriggerDagRunOperator',
    'create_session',
    'LoggingMixin',
    'get_airflow_app',
    'get_auth_manager',
    'BaseAuthManager',
    'AirflowConfigParser',
    'conf',
    'AirflowException',
    'AirflowNotFoundException',
    'AirflowBadRequest',
    'BaseXCom',
    'Context',
]

