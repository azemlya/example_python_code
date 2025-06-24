
"""
Аудит по RESTAPI
"""
from __future__ import annotations

from socket import getfqdn
from datetime import datetime
from json import loads
from flask import request, jsonify, session
from typing import Optional, List

from airflow_se.obj_imp import User
# from airflow_se.utils import info, info_attrs
# from airflow_se.config import get_config_value

from .base_audit import BaseAuditAirflow

__all__= ['AuditRestAPI', ]


class AuditRestAPI(BaseAuditAirflow):
    """
    Audit RESTAPI
    """
    def __init__(self):
        super().__init__()
        # # считываем группы CTL
        # self.ldap_tech_ctl_groups = get_config_value('LDAP_TECH_CTL_GROUPS')
        # try:
        #     self.ldap_tech_ctl_groups = loads(self.ldap_tech_ctl_groups)
        #     if not isinstance(self.ldap_tech_ctl_groups, List):
        #         raise ValueError('parameter is not json list')
        # except Exception as e:
        #     self.ldap_tech_ctl_groups = list()
        #     self.log_warning(f'Missing loads parameter "SE_LDAP_TECH_CTL_GROUPS": {e}')
        # # считываем пользователей CTL
        # self.ldap_tech_ctl_users = get_config_value('LDAP_TECH_CTL_USERS')
        # try:
        #     self.ldap_tech_ctl_users = loads(self.ldap_tech_ctl_users)
        #     if not isinstance(self.ldap_tech_ctl_users, List):
        #         raise ValueError('parameter is not json list')
        # except Exception as e:
        #     self.ldap_tech_ctl_users = list()
        #     self.log_warning(f'Missing loads parameter "SE_LDAP_TECH_CTL_USERS": {e}')

    def __call__(self, path: List[str], user: User, is_ctl_user: bool):
        # пример того, как можно пробежаться по объектам и подробно посмотреть их атрибуты
        # try:
        #     from typing import Mapping
        #     self.log_debug('==============================================')
        #     self.log_debug(f'@@@@@@ {info(session)=}')
        #     for k, v in info_attrs(session).items():
        #         if isinstance(v, Mapping):
        #             for x, y in v.items():
        #                 self.log_debug(f'@@@@@@ [session] :: [{k}] :: {x} = {info(y)}')
        #         else:
        #             self.log_debug(f'@@@@@@ [session] :: {k} = {v}')
        #     self.log_debug('==============================================')
        #     self.log_debug(f'@@@@@@ {info(request)=}')
        #     for k, v in info_attrs(request).items():
        #         self.log_debug(f'@@@@@@ [request] :: {k} = {v}')
        #     self.log_debug('==============================================')
        # except:
        #     pass
        return self.call(path, user, is_ctl_user)

    # @staticmethod
    # def get_user() -> User:
    #     """
    #     Получение пользователя
    #     """
    #     auth = request.authorization
    #     if auth is None or not auth.username or not auth.password:  # нет запроса на аутентификацию
    #         sess_user_login: Optional[str] = \
    #             session.get("ext_user").get("login") if session.get('ext_user') else None
    #         if isinstance(sess_user_login, str) and not sess_user_login.isspace():
    #             user = get_airflow_app().appbuilder.sm.find_user(username=sess_user_login)
    #             if isinstance(user, User):
    #                 return user
    #             raise AirflowNotFoundException(f'user "{sess_user_login}" was not found in Metadata DB')
    #         else:  # куки пустые
    #             raise AirflowBadRequest('unauthorized user')
    #     else:  # есть запрос на аутентификацию, значит это не из Сваггера (curl или какой-то другой внешний вызов)
    #         # запускаем аутентификацию
    #         user = get_airflow_app().appbuilder.sm.auth_user_db(auth.username, auth.password)
    #         if isinstance(user, User):  # пользователь успешно аутентифицирован
    #             login_user(user, remember=True)
    #             return user
    #         else:  # пользователь не прошёл аутентификацию
    #             raise AirflowBadRequest(f'failed authentication for user "{auth.username}"')

    def call(self, path: List[str], user: User, is_ctl_user: bool):
        # self.log_debug(f'RESTAPI call: {user.username=}, {path=}, {is_ctl_user=}')
        # отсекаем лишнее сразу
        if (len(path) > 0 and path[0] == "datasets") or (len(path) >= 5 and path[4] == "upstreamDatasetEvents"):
            # Любые операции с датасетами из RESTAPI запрещены по требованию УЭК
            self.log_warning(f'Forbidden: Any operations with datasets are blocked')
            response = jsonify({"action": "Forbidden", "message": "Any operations with datasets are blocked"})
            response.status_code = 403
            return response
        if not isinstance(path, list) or len(path) == 0 or request.method not in ["GET", "POST", "DELETE", "PATCH"] or \
                path[0] not in ("roles", "users", "trigger", "paused", "connection", "connections", "variables",
                                "delete", "dags", "dagrun", ):
            return
        # определение пользователя (уже не надо, user уже гарантированно передаётся)
        # user: User
        # try:
        #     user = self.get_user()
        #     if not isinstance(user, User):  # на всякий случай ещё раз проверяем
        #         raise AirflowNotFoundException('user not found')
        # except Exception as e:  # ошибка определения пользователя
        #     self.log_error(f'Unauthorized RESTAPI call ({request.path}): {e}')
        #     response = jsonify({'action': f'Unauthorized RESTAPI call ({request.path})','message': e})
        #     response.status_code = 401
        #     return response
        # здесь, уже гарантированно, есть валидный объект user
        try:
            if len(path) > 0:
                aud_msg = {
                    "ts": datetime.now(),
                    "host": getfqdn(),
                    "remote_addr": request.remote_addr,
                    "remote_login": user.username,
                    "code_op": "",
                    "app_id": self.conf.kafka_app_id,
                    "type_id": "Audit",
                    "subtype_id": "",
                    "status_op": "",
                    "extras": {
                        "PARAMS": dict(EVENT_TYPE="RESTAPI_EVENTS", ),
                        "SESSION_ID": session.get('ext_user').get("uuid") \
                            if session.get('ext_user') and session.get('ext_user').get("uuid") \
                            else "not_applicable_to_external_REST_API_calls",
                        "EDITOR_USER_LOGIN": user.username,
                        "EDITOR_ROLE": str(user.roles).strip("[]"),
                    },
                }
                if "xcomEntries" in path:
                    aud_msg["extras"]["PARAMS"].update(dict(
                        CONN_ID="XCom",
                        CONN_TYPE="XCom",
                        EVENT=f"RESTAPI User `{user.username}` try to use XCom",
                    ))
                    if self.conf.block_change_policy:
                        aud_msg["status_op"] = "FAIL"
                        aud_msg["extras"]["REASON"] = "OPERATION IS BLOCKED"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI XCom {msg=}")
                        response = jsonify({"action": "Forbidden",
                                            "message": "According to the requirements of УЭК, the management of XCom is blocked ¯\_(ツ)_/¯"})
                        response.status_code = 403
                        # пока отключил блок xcom для демо, для возвращения блокировки раскоментарить return ниже
                        # return response
                    else:
                        aud_msg["status_op"] = "SUCCESS"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI XCom {msg=}")
                elif path[0] == "variables":
                    aud_msg["extras"]["PARAMS"].update(dict(
                        CONN_ID="variables",
                        CONN_TYPE="variables",
                        EVENT=f"RESTAPI User `{user.username}` try to use variables",
                    ))
                    if self.conf.block_change_policy:
                        aud_msg["status_op"] = "FAIL"
                        aud_msg["extras"]["REASON"] = "OPERATION IS BLOCKED"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI variables {msg=}")
                        response = jsonify({"action": "Forbidden",
                                            "message": "According to the requirements of УЭК, the management of variables is blocked ¯\_(ツ)_/¯"})
                        response.status_code = 403
                        return response
                    else:
                        aud_msg["status_op"] = "SUCCESS"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI variables {msg=}")
                elif path[0] == "connections":
                    conn = None
                    try:
                        conn = self.get_session.query(self.connection_model).filter_by(conn_id=path[1]).one_or_none()
                        self.log_info(f"RESTAPI connections: {conn=}")
                    except Exception as e:
                        self.log_error(f"RESTAPI connections: Exception: {e}")
                    if not conn:
                        self.log_error(f"RESTAPI connections - Not found connection with conn_id={path[1]}")
                        response = jsonify({"action": "Forbidden", "message": f"Not found connection with conn_id={path[1]}"})
                        response.status_code = 404
                        return response
                    request_data_connection_id = None
                    try:
                        request_data_connection_id = loads(request.data.decode('utf-8'))['connection_id']
                    except Exception as e:
                        self.log_debug(f"RESTAPI connections Exception - request_data_connection_id = json.loads(request.data(conn_id): {str(e)}")
                    request_data_conn_type = None
                    try:
                        request_data_conn_type = loads(request.data.decode('utf-8'))['conn_type']
                    except Exception as e:
                        self.log_debug(f"RESTAPI connections Exception - request_data_conn_type = json.loads(request.data(conn_type): {str(e)}")
                    if request.method == 'POST' and request_data_connection_id:  # add connection
                        aud_msg["extras"]["PARAMS"].update(dict(
                            CONN_ID=request_data_connection_id,
                            CONN_TYPE=request_data_conn_type,
                            EVENT=f"RESTAPI User `{user.username}` try to add the connection",
                        ))
                    elif request.method == 'PATCH' and path[1]:  # edit connection
                        info_msg = f"RESTAPI User `{user.username}`({request.remote_addr}) try to edit the connection {conn.conn_id} type {conn.conn_type}"
                        self.log_info(info_msg.format(
                            username=user.username,
                            addr=request.remote_addr,
                            conn_id=f"`{conn.conn_id}`" if request_data_connection_id == conn.conn_id else f"`{conn.conn_id}` (try to change for `{request_data_connection_id}`)",
                            conn_type=f"`{conn.conn_type}`" if request_data_conn_type == conn.conn_type else f"`{conn.conn_type}` (try to change for `{request_data_conn_type}`)",
                        ))
                        aud_msg["extras"]["PARAMS"].update(dict(
                            CONN_ID=conn.conn_id,
                            CONN_TYPE=conn.conn_type,
                            EVENT="RESTAPI try to edit the connection",
                        ))
                        if request_data_connection_id != conn.conn_id:
                            aud_msg["extras"]["PARAMS"].update(dict(NEW_CONN_ID=request_data_connection_id, ))
                        if request_data_conn_type != conn.conn_type:
                            aud_msg["extras"]["PARAMS"].update(dict(NEW_CONN_TYPE=request_data_conn_type, ))
                    elif request.method == 'DELETE' and path[1]:
                        aud_msg["extras"]["PARAMS"].update(dict(
                            CONN_ID=conn.conn_id,
                            CONN_TYPE=conn.conn_type,
                            EVENT="RESTAPI try to delete the connection",
                        ))
                    else:
                        aud_msg["extras"]["PARAMS"].update(dict(
                            CONN_ID=conn.conn_id,
                            CONN_TYPE=conn.conn_type,
                            EVENT="<< RESTAPI try to do unknown connection operation >>",
                        ))
                    aud_msg["subtype_id"] = "F0"
                    aud_msg["code_op"] = "other audit operations"
                    if self.conf.block_change_policy:
                        aud_msg["status_op"] = "FAIL"
                        aud_msg["extras"]["REASON"] = "OPERATION IS BLOCKED"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI connections {msg=}")
                        response = jsonify({"action": "Forbidden",
                                            "message": "According to the requirements of УЭК, the management of Connections is blocked ¯\_(ツ)_/¯"})
                        response.status_code = 403
                        return response
                    else:
                        aud_msg["status_op"] = "SUCCESS"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI connections - {msg=}")
                elif path[0] == "roles":
                    role = self.get_session.query(self.role_model).filter_by(name=path[1]).one_or_none()
                    if not role:
                        aud_msg["extras"]["EDITED_ROLE"] = "<<NOT_FOUND_EDITED_ROLE>>"
                        self.log_debug(f"{aud_msg=}")
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"{msg=}")
                        response = jsonify({"action": "Forbidden", "message": f"Not found role with name `{path[1]}`"})
                        response.status_code = 404
                        return response
                    if_fail = "fail " if self.conf.block_change_policy else ""
                    if request.method == "POST":  # add
                        aud_msg["code_op"] = f"{if_fail}create role"
                        aud_msg["subtype_id"] = "B12" if self.conf.block_change_policy else "B11"
                        aud_msg["extras"]["CREATED_ROLE"] = request.json.get('name')
                    elif request.method == "DELETE" and len(path) > 1:
                        aud_msg["code_op"] = f"{if_fail}drop role"
                        aud_msg["subtype_id"] = "B14" if self.conf.block_change_policy else "B13"
                        aud_msg["extras"]["DELETED_ROLE"] = path[1]
                    elif request.method == "PATCH":  # "edit":
                        aud_msg["code_op"] = f"{if_fail}update grants to role"
                        aud_msg["subtype_id"] = "B16" if self.conf.block_change_policy else "B15"
                        aud_msg["extras"]["EDITED_ROLE"] = path[1]
                        aud_msg["extras"]["RENAME_TO_ROLE"] = request.json.get('name')
                    else:
                        aud_msg["code_op"] = f"{if_fail}other grants operation"
                        aud_msg["subtype_id"] = "B0"
                    if self.conf.block_change_policy:
                        aud_msg["status_op"] = "FAIL"
                        aud_msg["extras"]["REASON"] = "OPERATION IS BLOCKED"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI roles - {msg=}")
                        response = jsonify({"action": "Forbidden",
                                            "message": "According to the requirements of УЭК, the management of Roles is blocked ¯\_(ツ)_/¯"})
                        response.status_code = 403
                        return response
                    else:
                        aud_msg["status_op"] = "SUCCESS"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI roles - {msg=}")
                elif path[0] == "users":
                    user = self.get_session.query(self.user_model).filter_by(username=path[1]).one_or_none()
                    if not user:
                        aud_msg["extras"]["EDITED_USER_LOGIN"] = f"<<NOT_FOUND_EDITED_USER>> '{path[1]}'"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI users {msg=}")
                        err_msg = f"RESTAPI Error find user username = {path[1]}"
                        self.log_debug(f"{err_msg}")
                        response = jsonify({"action": "Forbidden", "message": err_msg})
                        response.status_code = 404
                        return response
                    if_fail = "fail " if self.conf.block_change_policy else ""
                    if request.method == "POST":  # "add":
                        aud_msg["code_op"] = f"{if_fail}create user"
                        aud_msg["subtype_id"] = "B2" if self.conf.block_change_policy else "B1"
                        aud_msg["extras"]["CREATED_USER_LOGIN"] = request.get_json()['username']
                    elif request.method == "DELETE" and len(path) > 1:
                        aud_msg["code_op"] = f"{if_fail}drop user"
                        aud_msg["subtype_id"] = "B8" if self.conf.block_change_policy else "B7"
                        aud_msg["extras"]["DROPPED_USER_LOGIN"] = path[1]
                        aud_msg["extras"]["EDITED_USER_LOGIN"] = user.username
                    elif request.method == "PATCH" and len(path) > 1:  # "edit":
                        aud_msg["code_op"] = f"{if_fail}grant to user"
                        aud_msg["subtype_id"] = "B4" if self.conf.block_change_policy else "B3"
                        aud_msg["extras"]["EDITED_USER_LOGIN"] = user.username
                    else:
                        aud_msg["code_op"] = f"{if_fail}other grants operation"
                        aud_msg["subtype_id"] = "B0"
                    if self.conf.block_change_policy:
                        aud_msg["status_op"] = "FAIL"
                        aud_msg["extras"]["REASON"] = "OPERATION IS BLOCKED"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI Users - {msg=}")
                        response = jsonify({"action": "Forbidden",
                                            "message": "According to the requirements of УЭК, the management of Users is blocked ¯\_(ツ)_/¯"})
                        response.status_code = 403
                        return response
                    else:
                        aud_msg["status_op"] = "SUCCESS"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI roles - {msg=}")
                elif path[0] == "dags":
                    # if len(path) >= 3 and path[0] == "dags" and path[2] == "dagRuns" and request.method == "POST":
                    if len(path) > 2 and path[2] == "dagRuns":
                        dag_id = path[1]
                        aud_msg["subtype_id"] = "F0"
                        aud_msg["code_op"] = "other audit operations"
                        # # получение информации о пользователе и его группах в LDAP кэше
                        # ldap_user_info: Optional[Dict[str, Any]] = \
                        #     get_airflow_app().appbuilder.sm.ext_ldap_user_extract_silent(username=user.username)
                        # if not isinstance(ldap_user_info, Dict):
                        #     # информацию о пользователе не удалось вытащить из кэша LDAP
                        #     raise AirflowNotFoundException(f'Information of user "{user.username}" not found in cache LDAP')
                        # ldap_groups: Optional[List[str]] = ldap_user_info.get("ldap_groups")
                        # # self.log_debug(f'{self.ldap_tech_ctl_groups=}, {self.ldap_tech_ctl_users=}, {ldap_groups=}')
                        # # проверка, на аккаунт CTL
                        # if len(list(set(self.ldap_tech_ctl_groups) & set(ldap_groups))) > 0 or \
                        #         user.username in self.ldap_tech_ctl_users:  # This is CTL account
                        #     is_ctl_user = True
                        # else:  # This is NOT CTL account
                        #     is_ctl_user = False
                        # if is_ctl_user is True:
                        #     message = f'Technology account CTL "{user.username}" TRY some kind of DAG "{dag_id}" operations'
                        #     self.log_info(message)
                        # elif request.json.get('conf') and request.json.get('conf').get('loading_id'):
                        #     message = f'User `{user.username}` TRY to run DAG `{dag_id}` as technology account CTL (user set parameter `loading_id`)'
                        #     self.log_warning(message)
                        #     aud_msg["status_op"] = "FAIL"
                        #     aud_msg["extras"]["PARAMS"].update(dict(DAG_ID=dag_id, EVENT=message, ))
                        #     msg = self.audit_action_add(**aud_msg)
                        #     self.log_debug(msg)
                        #     response = jsonify({'action': 'Forbidden', 'message': message})
                        #     response.status_code = 403
                        #     return response
                        dag = self.get_session.query(self.dag_model).filter_by(dag_id=dag_id).one_or_none()
                        if dag:
                            user_from_conf: Optional[str]
                            try:
                                user_from_conf = request.json.get('conf').get('user')
                            except:
                                user_from_conf = None
                            k = 'CTL account ' if is_ctl_user is True else 'RESTAPI user '
                            if (    # это пользователь CTL и он передал правильного пользака в `conf`.`user`
                                    # request.method in ('POST', 'GET', 'DELETE', ) and
                                    is_ctl_user is True and
                                    user_from_conf and
                                    (user_from_conf == dag.owners or self.matching_groups(user_from_conf, dag.owners))
                            ) or (  # ИЛИ это НЕ пользователь CTL, но это владелец DAG-а или у них с владельцем одна группа
                                    # request.method in ('POST', 'GET', 'DELETE', ) and
                                    is_ctl_user is False and
                                    (user.username == dag.owners or self.matching_groups(user.username, dag.owners))
                            ) or (request.method == "GET"):
                                aud_msg["status_op"] = "SUCCESS"
                                if request.method == "POST":
                                    t = f'{k}DAG run'
                                    if is_ctl_user is False and user_from_conf:
                                        # проверяем, что пользователь, который "не CTL", передал в conf юзера - жестоко наказываем :)
                                        self.log_debug(f'RESTAPI dag run :: The DAG `{dag_id}` cannot be manual '
                                                       f'started because config contains key "user". '
                                                       f'Reject the run request.')
                                        response = jsonify({'action': 'Forbidden',
                                                            'message':
                                                                f'The DAG `{dag_id}` cannot be manual started '
                                                                f'because config contains key "user". '
                                                                f'Reject the run request.'
                                                            })
                                        response.status_code = 403
                                        return response
                                    elif is_ctl_user is True and user_from_conf:
                                        # если это CTL и он передал правильного пользака в conf
                                        t = f'{t} as user `{user_from_conf}` (owner the DAG: {dag.owners})'
                                    # проверяем, что в данный момент DAG не запущен
                                    dag_runs = self.get_active_dags_runs_manual([dag_id, ])
                                    # self.log_debug(f'RESTAPI result of active manual DAGRuns: {dag_runs}')
                                    if len(dag_runs) != 0:
                                        self.log_debug(f'RESTAPI dag run :: The DAG `{dag_id}` cannot be started '
                                                       f'because it is already running a little.')
                                        response = jsonify({"action": "Forbidden",
                                                            "message":
                                                                f"The DAG `{dag_id}` cannot be started "
                                                                f"because it is already running a little."
                                                            })
                                        response.status_code = 403
                                        return response
                                elif request.method == "GET":
                                    t = f'{k}DAG status survey'
                                elif request.method == "DELETE":
                                    t = f'{k}DELETE the history of dag_run_id'
                                else:
                                    t = f"{k}DAG in method '{request.method}'"
                                aud_msg["extras"]["PARAMS"].update(dict(DAG_ID=dag_id, EVENT=t, ))
                                msg = self.audit_action_add(**aud_msg)
                                self.log_debug(f"RESTAPI dags, inserted audit record: {msg}")
                            else:  # юзер не прошёл проверки, в сад
                                aud_msg["status_op"] = "FAIL"
                                aud_msg["extras"]["PARAMS"].update(dict(
                                    DAG_ID=dag_id,
                                    EVENT=f"DAG NOT run, {k}'{(user.username if is_ctl_user is False else user_from_conf)}' are not owner(or not in group) of the DAG",
                                ))
                                msg = self.audit_action_add(**aud_msg)
                                self.log_debug(f"RESTAPI dags, inserted audit record: {msg}")
                                response = jsonify({"action": "Forbidden",
                                                    "message":
                                                        f"You are not the owner of the DAG '{dag_id}'!"
                                                        f" Launching DAGs, whose owners you are not is - prohibited!"
                                                    })
                                response.status_code = 403
                                return response
                        else:
                            aud_msg["status_op"] = "FAIL"
                            aud_msg["extras"]["PARAMS"].update(dict(EVENT=f"DAG '{dag_id}' NOT found", ))
                            if request.method != "GET":
                                msg = self.audit_action_add(**aud_msg)
                                self.log_debug(f"RESTAPI dags, inserted audit record: {msg}")
                            response = jsonify({"action": "Forbidden", "message": f"dag_id `{dag_id}` not found"})
                            response.status_code = 403
                            return response
                    elif request.method == "DELETE" and len(path) > 1:  # delete history of DAG
                        aud_msg["extras"]["PARAMS"].update(dict(
                            DAG_ID=path[1],
                            EVENT=f"User `{user.username}`({request.remote_addr}) try to delete"
                                  f" history of the DAG `{path[1]}`",
                        ))
                        aud_msg["status_op"] = "SUCCESS"
                        aud_msg["subtype_id"] = "F0"
                        aud_msg["code_op"] = "other audit operations"
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI - DAG delete history - {msg=}")
                    elif request.method == "PATCH" and request.args.to_dict().get('dag_id_pattern') is not None:  # "paused for template":
                        is_paused = request.get_json().get("is_paused")  # request.values.get("is_paused")
                        dag_id_pattern = request.args.to_dict().get('dag_id_pattern')  # request.values.get("dag_id")
                        aud_msg["status_op"] = "SUCCESS"
                        aud_msg["subtype_id"] = "F0"
                        aud_msg["code_op"] = "other audit operations"
                        if is_paused:
                            aud_msg["extras"]["PARAMS"].update(dict(
                                DAG_ID_PATTERN=dag_id_pattern,
                                EVENT=f"all DAG for template `{dag_id_pattern}` deactivate",
                            ))
                        else:
                            aud_msg["extras"]["PARAMS"].update(dict(
                                DAG_ID_PATTERN=dag_id_pattern,
                                EVENT=f"all DAG for template `{dag_id_pattern}` activate",
                            ))
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI DAG template PAUSED - {msg=}")
                    elif request.method == "PATCH" and len(path) > 1:  # "paused for a DAG":
                        is_paused = request.get_json().get("is_paused")  # request.values.get("is_paused")
                        dag_id = path[1]  # request.values.get("dag_id")
                        aud_msg["status_op"] = "SUCCESS"
                        aud_msg["subtype_id"] = "F0"
                        aud_msg["code_op"] = "other audit operations"
                        if is_paused:
                            aud_msg["extras"]["PARAMS"].update(dict(
                                DAG_ID=dag_id,
                                EVENT=f"DAG `{dag_id}` deactivate",
                            ))
                        else:
                            aud_msg["extras"]["PARAMS"].update(dict(DAG_ID=dag_id, EVENT=f"DAG `{dag_id}` activate"))
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f"RESTAPI paused for a DAG - {msg=}")
        except Exception as e:
            self.log_error(f"Internal server error on RESTAPI: {e}")
            self.log_exception('')
            response = jsonify({"action": "Internal RESTAPI server error", "message": e})
            response.status_code = 500
            return response

