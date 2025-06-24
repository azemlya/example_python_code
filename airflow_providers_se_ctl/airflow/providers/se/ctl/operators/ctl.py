from __future__ import annotations
#2023-12-17_17-02
from contextlib import closing
from socket import getfqdn
from typing import TYPE_CHECKING, Any, Callable, Sequence

#from flask_appbuilder.security.sqla.models import User
#from airflow.www.fab_security.sqla.models import User
from airflow_se.obj_imp import User
from requests.auth import AuthBase

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.se.ctl.hooks.ctl import CtlHookSE
#from sqlalchemy.sql.functions import user

if TYPE_CHECKING:
    from airflow.utils.context import Context

from datetime import datetime as dt, datetime
from airflow_se.db import create_new_audit_record, Session
from airflow_se.config import get_config_value # SE_KAFKA_APP_ID

from os import environ   # AIRFLOW_CTX_DAG_OWNER, AIRFLOW_CTX_DAG_ID, AIRFLOW_CTX_TASK_ID
#from airflow.models.taskinstance import TaskInstance as TI

class SimpleCtlOperatorSE(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SimpleHttpOperator`

    :param ctl_conn_id: The :ref:`http connection<howto/connection:http>` to run
        the operator against
    :param endpoint: The relative part of the full url. (templated)
    :param method: The HTTP method to use, default = "POST"
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
    :param response_filter: A function allowing you to manipulate the response
        text. e.g response_filter=lambda response: json.loads(response.text).
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param log_response: Log the response (default: False)
    :param auth_type: The auth type for the service
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    """

    template_fields: Sequence[str] = (
        "endpoint",
        "data",
        "headers",
    )
    template_fields_renderers = {"headers": "json", "data": "py"}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        # data: dict[str, Any] | str | None = None,
        # ===========================================
        loading_id: int | str | None = None,
        entity_id: int | str | None = None,
        stat_id: int | str | None = None,
        avalue: list[Any] | tuple[Any] | set[Any]  = None,
        profile_name: str | None = None,
        #dag_id: str | None = None,
        # ===========================================
        response_check: Callable[..., bool] | None = None,
        response_filter: Callable[..., Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        ctl_conn_id: str = "ctl_default",
        log_response: bool = False,
        auth_type: type[AuthBase] | None = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        dag_executor_or_owner: str = 'dag_executor_or_owner',
        manual_parameter_setting: int = 0,
        krb_url: str = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.ctl_conn_id = ctl_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {'Content-Type': 'application/json'}
        self.data = dict()
        self.loading_id = loading_id
        self.entity_id = entity_id
        self.stat_id = stat_id
        # additional_mandatory_info = [f'filled additional mandatory information into statistics at {dt.now().strftime("%Y-%m-%dT%H:%M:%S")}',]
        # avalue_list = list(avalue) if avalue else []
        # combo_list = avalue_list + additional_mandatory_info
        self.avalue = [f'filled additional mandatory information into statistics at {dt.now().strftime("%Y-%m-%dT%H:%M:%S")}',]   # combo_list
        self.profile_name = profile_name
        self.response_check = response_check
        self.response_filter = response_filter
        self.extra_options = extra_options or {}
        self.log_response = log_response
        self.auth_type = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.tcp_keep_alive_idle = tcp_keep_alive_idle
        self.tcp_keep_alive_count = tcp_keep_alive_count
        self.tcp_keep_alive_interval = tcp_keep_alive_interval
        self.dag_executor_or_owner = dag_executor_or_owner
        self.manual_parameter_setting = manual_parameter_setting
        self.krb_url = krb_url
        #self.dag_id = dag_id if 'dag_id' in kwargs else ''

    def execute(self, context: Context) -> Any:
        # проверка loading_id
        if context.get('dag_run'):
            if hasattr(context.get('dag_run'), 'conf'):
                _loading_id = context.get('dag_run').conf.get('loading_id')
                if isinstance(_loading_id, int):
                    self.loading_id = _loading_id
                elif isinstance(self.loading_id, int):
                    pass
                else:
                    raise AirflowException("Invalid parameter type 'loading_id', int expected")
            else:
                self.log.warning("CTL don't push key 'conf' in body on DAG run, or not DAG executing from CTL")
        else:
            raise AirflowException("Invalid 'context': key 'dag_run' not found in context")
        # проверка остальных наших параметров
        if not isinstance(self.entity_id, int) and self.entity_id is not None:
            raise AirflowException("Invalid parameter type 'entity_id', int expected")
        if not isinstance(self.stat_id, int) and self.stat_id is not None:
            raise AirflowException("Invalid parameter type 'stat_id', int expected")
        # if not isinstance(self.avalue, (list, tuple, set)) and self.avalue is not None:
        #     raise AirflowException("Invalid parameter type 'avalue', list or tuple or set expected")
        # else:
        #     self.avalue = list(self.avalue) if self.avalue else list()
        if not isinstance(self.profile_name, str) or self.profile_name.isspace():
            raise AirflowException("Invalid parameter type 'profile_name', not empty string expected")
        else:
            self.profile_name = self.profile_name.strip()
        # подготовка данных
        # if isinstance(self.data, str):
        #     self.data = json.loads(self.data)
        self.data['loading_id'] = self.loading_id
        self.data['entity_id'] = self.entity_id
        self.data['stat_id'] = self.stat_id
        # if isinstance(self.data.get('avalue'), (list, tuple, set)):
        #     self.avalue.extend(self.data['avalue'])
        self.data['avalue'] = self.avalue
        self.data['profile_name'] = self.profile_name
        #self.data = json.dumps(self.data)
        self.log.info(f"после self.data = json.dumps(self.data): {self.data=}")

        from airflow.utils.operator_helpers import determine_kwargs

        ctl = CtlHookSE(
            self.method,
            ctl_conn_id=self.ctl_conn_id,
            auth_type=self.auth_type,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
            context=context,
            dag_executor_or_owner=self.dag_executor_or_owner,
            manual_parameter_setting=self.manual_parameter_setting,
            krb_url=self.krb_url
        )

        self.log.info("Calling CTL method")
        # self.log.info(f"{context.get('dag_run').conf=}")

        response = ctl.run(self.endpoint, self.data, self.headers, self.extra_options)
        if self.data['avalue']:
            self.statistic_audit()
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            kwargs = determine_kwargs(self.response_check, [response], context)
            self.log.info(f"{kwargs=},{response.status_code=}")
            if not self.response_check(response, **kwargs):
                raise AirflowException("Response check returned False.")
        if self.response_filter:
            kwargs = determine_kwargs(self.response_filter, [response], context)
            return self.response_filter(response, **kwargs)
        return response.text


    @staticmethod
    def audit_action_add(host: str,
                         remote_addr:str,
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

    def statistic_audit(self):
        acdo, acdi, acti = None, None, None
        try:
            acdo, acdi, acti = environ.get('AIRFLOW_CTX_DAG_OWNER'), environ.get('AIRFLOW_CTX_DAG_ID'), environ.get('AIRFLOW_CTX_TASK_ID')
            self.log.info(f" {acdo=}, {acdi=}, {acti=}")
        except Exception as e:
            self.log.info(f" except: {e}")
        user_role = "not applicable"
        try:
            with closing(Session()) as sess:
                row = sess.query(User).filter(User.username == acdo).one_or_none()
                if row:
                    user_role = row.roles[0]
                user_role = str(user_role)
                sess.commit()
        except Exception as e:
            self.log.info(f" Except {e}")
        t_avalue = None
        try:
            t_avalue = " / ".join(self.avalue).strip(" / ")
        except Exception as e:
            self.log.info(f" Exсeption - str(context)+type(context): {e}")
        event_plus = None
        try:  # {airflow_se_config.SE_KAFKA_APP_ID=}, {taskinstance.AIRFLOW_CTX_DAG_OWNER=}, {taskinstance.AIRFLOW_CTX_DAG_ID=}, {taskinstance.AIRFLOW_CTX_TASK_ID=},
            event_plus = f" Dag_id {self.dag_id} / task_id {self.task_id} --> pushed statistic to loading_id '{self.loading_id}', entity_id '{self.entity_id}', stat_id: '{self.stat_id}', avalue: '{t_avalue}' "
        except Exception as e:
            event_plus = f" exception:  Dag_id {self.dag_id} / task_id {self.task_id} --> pushed statistic to loading_id '{self.loading_id}', entity_id '{self.entity_id}', stat_id: '{self.stat_id}' - exception: {e}"
        request_remote_addr = "not applicable"
        session_id = "not applicable"
        aud_msg = {"ts": datetime.now(),
                   "host": getfqdn(),
                   "remote_addr": request_remote_addr,
                   "remote_login": acdo,
                   "code_op": "other audit operations",
                   "app_id": get_config_value('KAFKA_APP_ID'),  # "self.asm.ext_conf.kafka_app_id",
                   "type_id": "Audit",
                   "subtype_id": "F0",
                   "status_op": "SUCCESS",
                   "extras": {
                       "SESSION_ID": session_id,
                       "EDITOR_USER_LOGIN": acdo,
                       "EDITOR_ROLE": user_role,
                       "PARAMS": {"EVENT": event_plus, }
                   }
                   }
        try:
            self.log.info(f" {aud_msg=}")
        except Exception as e:
            self.log.info(f" Exception: aud_msg= {e}")
        msg = self.audit_action_add(**aud_msg)
        self.log.info(f" {msg=}")

    # @property
    # def dag_id(self) -> str:
    #     return self.dag_id
    #
    # @dag_id.setter
    # def dag_id(self, value):
    #     self._dag_id = value



