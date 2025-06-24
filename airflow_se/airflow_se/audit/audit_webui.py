
"""
Аудит по WEB UI
"""
from __future__ import annotations

from socket import getfqdn
from datetime import datetime
from flask import request, session
from typing import List
from pprint import pformat

from .base_audit import BaseAuditAirflow

__all__= ['AuditWebUI', ]


class AuditWebUI(BaseAuditAirflow):
    """
    Audit Web UI
    """
    def __init__(self):
        super().__init__()

    def __call__(self, path: List[str]):
        return self.call(path)

    def call(self, path: List[str]):
        if not isinstance(path, List) or len(path) == 0 or request.method not in ['GET', 'POST', 'DELETE'] or \
                path[0] not in ('roles', 'users', 'trigger', 'paused', 'connection', 'variable', 'logout',
                                'code_editor', 'delete', 'dags', 'dagrun', 'taskinstance',
                                # 'get_logs_with_metadata', 'log'
                                ):
            return
        try:
            ext_user = session.get('ext_user')
            if not ext_user:
                err_msg = 'Invalid session parameters'
                return self.render_error(status=404, mes=err_msg, mes_log_error=err_msg)
            ext_user_login = ext_user.get('login')
            ext_user_uuid = ext_user.get('uuid')
            user = self.sm.find_user(username=ext_user_login)
            if not user:
                err_msg = f'User `{ext_user_login}` was not found in Metadata DB'
                return self.render_error(status=404, mes=err_msg, mes_log_error=err_msg)
            aud_msg = {
                'ts': datetime.now(),
                'host': getfqdn(),
                'remote_addr': request.remote_addr,
                'remote_login': user.username,
                'code_op': '',
                'app_id': self.conf.kafka_app_id,
                'type_id': 'Audit',
                'subtype_id': '',
                'status_op': '',
                'extras': {
                    'PARAMS': dict(EVENT_TYPE="WEBUI_EVENTS", ),
                    'SESSION_ID': ext_user_uuid,
                    'EDITOR_USER_LOGIN': user.username,
                    'EDITOR_ROLE': str(user.roles).strip('[]'),
                },
            }

            if path[0] == 'logout':
                aud_msg['status_op'] = 'SUCCESS'
                aud_msg['subtype_id'] = 'A1'
                aud_msg['code_op'] = 'Sign Out'
                aud_msg['extras']['PARAMS'].update(dict(
                    USERNAME=user.username,
                    REMOTE_ADDR=request.remote_addr,
                    ACTION='logout',
                ))
                msg = self.audit_action_add(**aud_msg)
                self.log_debug(f'WebUI/logout :: {msg}')

            elif path[0] in ('get_logs_with_metadata', 'log') and not (len(path) == 2 and path[1] == 'list'):
                dag_id = request.values.get('dag_id') or request.args.get('dag_id')
                if not dag_id:
                    return self.render_error(status=404, mes='DAG ID is not found', mes_log_error='DAG ID is not found')
                dag = self.get_session.query(self.dag_model).filter_by(dag_id=dag_id).one_or_none()
                if dag:
                    if dag.owners == ext_user_login or self.matching_groups(dag.owners, ext_user_login):
                        aud_msg['status_op'] = 'SUCCESS'
                        aud_msg['subtype_id'] = 'F0'
                        aud_msg['code_op'] = 'other audit operations'
                        aud_msg['extras']['PARAMS'].update(dict(DAG_ID=dag_id, EVENT='Get log of the DAG runs', ))
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f'WebUI/get log of the DAG runs success :: {msg}')
                    else:
                        aud_msg['status_op'] = 'FAIL'
                        aud_msg['subtype_id'] = 'F0'
                        aud_msg['code_op'] = 'other audit operations'
                        aud_msg['extras']['PARAMS'].update(dict(
                            DAG_ID=dag_id,
                            EVENT=(f'User NO get log of the DAG runs, user `{ext_user_login}` '
                                   f'are not owner of the DAG `{dag_id}` (owner `{dag.owners}`)'),
                        ))
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f'WebUI/get log of the DAG runs fail :: {msg}')
                        return self.render_error(
                            status=403,
                            mes=(f'Forbidden!!! You are not owner of the DAG `{dag_id}`! '
                                 'Get log of the DAG runs, whose owners you are not is - prohibited!'),
                            mes_log_warning=(f'User `{user.username}`({request.remote_addr}) NOT '
                                             f'owner DAG `{dag_id}`, reject request get log.'),
                            mes_log_debug=(f'{request.method}; {request.path}; {request.args.to_dict()}; '
                                           f'{request.query_string}; {request.values.to_dict()};'),
                        )

            elif len(path) >= 2 and path[0] == 'variable' and path[1] != 'list':
                return self.render_error(status=403,
                                         mes='Forbidden!!! Variables is not editable',
                                         mes_log_error='Forbidden!!! Variables is not editable')

            elif len(path) >= 2 and path[0] == 'taskinstance' and path[1] != 'list':
                return self.render_error(status=403,
                                         mes='Forbidden!!! Task Instances is not editable',
                                         mes_log_error='Forbidden!!! Task Instances is not editable')

            elif len(path) >= 2 and path[0] == 'dagrun' and path[1] == 'action_post':
                return self.render_error(status=404,
                                         mes='Forbidden!!! Multi operation is blocked.',
                                         mes_log_error='Forbidden!!! Multi operation is blocked.')

            elif path[0] == 'delete' or (len(path) >= 3 and path[0] == 'dagrun' and path[1] == 'delete'):
                dag_id = request.values.get('dag_id')
                if not dag_id and len(path) >= 3:
                    dag_run = self.get_session.query(self.dag_run_model).filter_by(id=path[2]).one_or_none()
                    if dag_run:
                        dag_id = dag_run.dag_id
                if not dag_id:
                    return self.render_error(status=404, mes='DAG ID is not found', mes_log_error='DAG ID is not found')
                dag = self.get_session.query(self.dag_model).filter_by(dag_id=dag_id).one_or_none()
                if dag:
                    if dag.owners == ext_user_login or self.matching_groups(dag.owners, ext_user_login):
                        aud_msg['status_op'] = 'SUCCESS'
                        aud_msg['subtype_id'] = 'F0'
                        aud_msg['code_op'] = 'other audit operations'
                        aud_msg['extras']['PARAMS'].update(dict(DAG_ID=dag_id, EVENT='Delete history runs of the DAG', ))
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f'WebUI/delete DAG runs success :: {msg}')
                    else:
                        aud_msg['status_op'] = 'FAIL'
                        aud_msg['subtype_id'] = 'F0'
                        aud_msg['code_op'] = 'other audit operations'
                        aud_msg['extras']['PARAMS'].update(dict(
                            DAG_ID=dag_id,
                            EVENT=(f'User NO delete history of DAG, user `{ext_user_login}` '
                                   f'are not owner of the DAG `{dag_id}` (owner `{dag.owners}`)'),
                        ))
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f'WebUI/delete DAG runs fail :: {msg}')
                        return self.render_error(
                            status=403,
                            mes=(f'Forbidden!!! You are not the owner of the DAG `{dag_id}`! '
                                 'Deleting history of the DAG, whose owners you are not is - prohibited!'),
                            mes_log_warning=(f'User `{user.username}`({request.remote_addr}) NOT '
                                             f'owner DAG `{dag_id}`, reject delete history of the DAG.'),
                            mes_log_debug=(f'{request.method}; {request.path}; {request.args.to_dict()}; '
                                           f'{request.query_string}; {request.values.to_dict()};'),
                        )

            elif path[0] == 'roles' and len(path) >= 2 and path[1] not in ['list', ]:
                aud_msg['extras']['PARAMS'].update(dict(INFO='Operation of Roles'))
                if len(path) >= 3:
                    role = self.get_session.query(self.role_model).filter_by(id=path[2]).one_or_none()
                    if not role:
                        aud_msg['extras']['EDITED_ROLE'] = '<<NOT_FOUND_EDITED_ROLE>>'
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f'WebUI/roles :: {msg}')
                        err_msg = f'Error find role `{path[2]}`'
                        return self.render_error(status=404, mes=err_msg, mes_log_error=err_msg)
                    aud_msg['extras']['EDITED_ROLE'] = role.name
                if_fail = 'fail ' if self.conf.block_change_policy else ''
                if path[1] == 'add':
                    aud_msg['code_op'] = f'{if_fail}create role'
                    aud_msg['subtype_id'] = 'B12' if self.conf.block_change_policy else 'B11'
                    aud_msg['extras']['CREATED_ROLE'] = request.form.get('name')
                elif path[1] == 'delete':
                    aud_msg['code_op'] = f'{if_fail}drop role'
                    aud_msg['subtype_id'] = 'B14' if self.conf.block_change_policy else 'B13'
                elif path[1] == 'edit':
                    aud_msg['code_op'] = f'{if_fail}update grants to role'
                    aud_msg['subtype_id'] = 'B16' if self.conf.block_change_policy else 'B15'
                else:
                    aud_msg['code_op'] = 'other roles operation'
                    aud_msg['subtype_id'] = 'B0'
                if self.conf.block_change_policy:
                    aud_msg['status_op'] = 'FAIL'
                    aud_msg['extras']['REASON'] = 'OPERATION IS BLOCKED'
                    msg = self.audit_action_add(**aud_msg)
                    self.log_debug(f'WebUI/roles :: {msg}')
                    return self.render_error(
                        status=403,
                        mes=('Forbidden!!! According to the requirements of UEK, the management of roles '
                             f'is blocked: {aud_msg.get("code_op")}'),
                        mes_log_warning=f'Audit :: {msg}',
                        mes_log_debug=(f'{request.method}; {request.path}; {request.args.to_dict()}; '
                                       f'{request.query_string}; {request.values.to_dict()};'),
                    )
                else:
                    aud_msg['status_op'] = 'SUCCESS'
                    msg = self.audit_action_add(**aud_msg)
                    self.log_debug(f'WebUI/roles :: {msg}')

            elif path[0] == 'users' and len(path) >= 2 and path[1] not in ['list', ]:
                aud_msg['extras']['PARAMS'].update(dict(INFO='Operation of Users', ))
                if len(path) >= 3:
                    user = self.get_session.query(self.user_model).filter_by(id=path[2]).one_or_none()
                    if not user:
                        aud_msg['extras']['EDITED_USER_LOGIN'] = '<<NOT_FOUND_EDITED_USER>>'
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f'WebUI/users :: {msg}')
                        err_msg = f'Error find user `{path[2]}`'
                        return self.render_error(status=404, mes=err_msg, mes_log_error=err_msg)
                    aud_msg['extras']['EDITED_USER_LOGIN'] = user.username
                if_fail = 'fail ' if self.conf.block_change_policy else ''
                if path[1] == 'add':
                    aud_msg['code_op'] = f'{if_fail}create user'
                    aud_msg['subtype_id'] = 'B2' if self.conf.block_change_policy else 'B1'
                    aud_msg['extras']['CREATED_USER_LOGIN'] = request.form.get('username')
                elif path[1] == 'delete':
                    aud_msg['code_op'] = f'{if_fail}drop user'
                    aud_msg['subtype_id'] = 'B8' if self.conf.block_change_policy else 'B7'
                elif path[1] == 'edit':
                    aud_msg['code_op'] = f'{if_fail}grant to user'
                    aud_msg['subtype_id'] = 'B4' if self.conf.block_change_policy else 'B3'
                else:
                    aud_msg['code_op'] = 'other users operation'
                    aud_msg['subtype_id'] = 'B0'
                if self.conf.block_change_policy:
                    aud_msg['status_op'] = 'FAIL'
                    aud_msg['extras']['REASON'] = 'OPERATION IS BLOCKED'
                    msg = self.audit_action_add(**aud_msg)
                    self.log_debug(f'WebUI/users :: {msg}')
                    return self.render_error(
                        status=403,
                        mes=(f'Forbidden!!! According to the requirements of UEK, the management '
                             f'of users is blocked: {aud_msg.get("code_op")}'),
                        mes_log_warning=f'Audit :: {msg}',
                        mes_log_debug=(f'{request.method}; {request.path}; {request.args.to_dict()}; '
                                       f'{request.query_string}; {request.values.to_dict()};'),
                    )
                else:
                    aud_msg['status_op'] = 'SUCCESS'
                    msg = self.audit_action_add(**aud_msg)
                    self.log_debug(f'WebUI/users :: {msg}')

            elif path[0] == 'code_editor':
                dag_id = request.values.get('dag_id')
                dag = self.get_session.query(self.dag_model).filter_by(dag_id=dag_id).one_or_none()
                list_of_dag_in_db = self.get_session.query(self.dag_model).all()
                list_of_dag_in_db_only_dag_id = [str(dag_row).strip('< >').split(': ')[1] for dag_row in list_of_dag_in_db]
                git_args: list = []
                try:
                    if request.get_json(silent=True):
                        git_args = request.get_json(silent=True).get('args') or []
                    #self.log_debug(f"""before_request - code_editor - git_args[1].lstrip('/').rpartition('.')[0] = {git_args[1].lstrip('/').rpartition('.')[0]}""")
                except Exception as e:
                    git_args = []
                    self.log_debug(f"""before_request - code_editor - Exception - request.json.get('args', []): {str(e)}""")
                if isinstance(git_args, list) and len(git_args) > 1:
                    if path[1] == 'repo' and len(path) < 3:  # Удаление: path[0] == code_editor, and path[1] = 'repo', и path[2] = 'отсутствует'
                        self.log_info(f""" (if path[1] == 'repo' and len(path) < 3:) User `{user.username}`({request.remote_addr}) deleted the DAG `{git_args[1].lstrip('/').rpartition('.')[0]}`""")
                        aud_msg['extras']['PARAMS'].update(dict(
                            DAG_ID=git_args[1].lstrip('/').rpartition('.')[0] if len(git_args) > 1 else 'NOT_FOUND',
                            EVENT=f"""User `{user.username}`({request.remote_addr}) deleted the DAG `{git_args[1].lstrip('/').rpartition('.')[0] if len(git_args) > 1 else 'NOT_FOUND'}` in before_request""",
                        ))
                    if path[1] == 'files' and len(path) > 2:  # Новый или редактирование: path[0] == code_editor, and path[1] = repo    или     'files', и   path[2] = 'ctl_operator_demo_Roma_test_02.py'
                        edit_or_add = 'edited the' if path[2].rpartition('.')[0] in list_of_dag_in_db_only_dag_id else 'added new'
                        aud_msg['extras']['PARAMS'].update(dict(
                            DAG_ID=path[2].rpartition('.')[0],
                            EVENT=f"""User `{user.username}`({request.remote_addr}) {edit_or_add}  DAG `{path[2].rpartition('.')[0]}` in before_request""",
                        ))
                aud_msg['status_op'] = 'SUCCESS'
                aud_msg['subtype_id'] = 'F0'
                aud_msg['code_op'] = 'other audit operations'
                msg = self.audit_action_add(**aud_msg)
                self.log_debug(f'WebUI/code_editor :: {msg}')

            elif path[0] == 'trigger' or (
                    # для >= 2.7.0 (path поменялся)
                    len(path) >= 3 and path[0] == 'dags' and path[2] == 'trigger'
            ):
                dag_id = request.values.get('dag_id') or path[1]
                dag = self.get_session.query(self.dag_model).filter_by(dag_id=dag_id).one_or_none()
                # dag_run = self.get_session.query(self.dag_run_model).filter_by(dag_id == dag_id).all()
                # self.log_debug(f'{dag_run}')
                t = None
                if dag:
                    # if dag.owners == ext_user_login:  # это владелец ДАГа, даём запустить
                    # концепция поменялась, теперь проверяем по группам (пользователь и владелец в одной группе)
                    if dag.owners == ext_user_login or self.matching_groups(dag.owners, ext_user_login):
                        aud_msg['status_op'] = 'SUCCESS'
                        aud_msg['subtype_id'] = 'F0'
                        aud_msg['code_op'] = 'other audit operations'
                        if request.method == 'POST':
                            t = 'DAG run'
                            # проверяем, что пользователь намеренно не передаёт другого пользователя и жестоко наказываем
                            try:
                                if request.json.get('conf').get('user'):
                                    return self.render_error(
                                        status=403,
                                        mes=(f'Forbidden!!! The DAG `{dag_id}` cannot be manual started '
                                             'because config contains key "user".'),
                                        mes_log_debug=(f'User `{user.username}`({request.remote_addr}) is '
                                                       f'try run of the DAG `{dag_id}`. The DAG `{dag_id}` cannot be '
                                                       f'manual started because config contains key "user". '
                                                       f'Reject the run request.'),
                                    )
                            except:
                                pass
                            # проверяем, что в данный момент DAG не запущен (запуск по расписанию не считается)
                            dag_runs = self.get_active_dags_runs_manual([dag_id, ])
                            # self.log_debug(f'WebUI result of active manual DAGRuns: {dag_runs}')
                            if len(dag_runs) != 0:
                                return self.render_error(
                                    status=403,
                                    mes=(f'Forbidden!!! The DAG `{dag_id}` cannot be started '
                                         'because it is already running a little.'),
                                    mes_log_debug=(f'User `{user.username}`({request.remote_addr}) is '
                                                   f'try run of the DAG `{dag_id}`. The DAG cannot '
                                                   f'be started because it is already running a little. '
                                                   f'Reject the run request.'),
                                )
                            # # проверяем в цепочках ДАГов
                            # deps = self.get_dags_dependencies()
                            # self.log_debug(f'Dependencies of DAG\'s (no modified):\n{pformat(deps)}')
                            # deps = self.get_chain_dependencies(dag_id)
                            # self.log_info(f'DAG `{dag_id}` find in chain dependencies: {deps}')
                            # # проверяем, что DAG-и в цепочке зависимостей, не запущены
                            # dags_runs = self.get_active_dags_runs(deps)
                            # lst_runs_dags = [x.dag_id + ' (' + x.state + ' :: ' + x.run_id + ')' for x in dags_runs]
                            # if len(dags_runs) != 0:
                            #     return self.render_error(
                            #         status=403,
                            #         mes=(f'Forbidden!!! The DAG `{dag_id}` cannot be started '
                            #              f'because it is dependencies {deps} already running '
                            #              f'a little // {lst_runs_dags}.'),
                            #         mes_log_info=(f'User `{user.username}`({request.remote_addr}) is '
                            #                       f'try run of the DAG `{dag_id}`. The DAG cannot '
                            #                       f'be started because it is dependencies {deps} '
                            #                       f'already running a little // {lst_runs_dags}. '
                            #                       f'Reject the run request.'),
                            #     )
                            # from json import loads
                            # id, cnf = request.values.get('dag_id'), loads(request.values.get('conf'))
                            # if id:
                            #     from airflow.api.common.trigger_dag import trigger_dag
                            #     dag_run_conf = {'user_is_run': ext_user_login}
                            #     dag_run_conf.update({'user_is_run': ext_user_login})
                            #     self.log_debug(f'$$$$$$$$$$$ {dag_run_conf=}')
                            #     dag_run = trigger_dag(dag_id=dag.dag_id, conf=dag_run_conf)
                            #     self.log_debug(f'$$$$$$$$$$$ {dag_run=}')
                        elif request.method == 'GET':
                            t = 'DAG status survey'
                        elif request.method == 'DELETE':
                            t = 'DELETE the history of DAG runs'
                        else:
                            t = f'DAG in method `{request.method}`'
                        aud_msg['extras']['PARAMS'].update(dict(DAG_ID=dag_id, EVENT=t, ))
                    else:  # это не владелец ДАГа и не в одной группе с ним, в сад
                        aud_msg['status_op'] = 'FAIL'
                        aud_msg['subtype_id'] = 'F0'
                        aud_msg['code_op'] = 'other audit operations'
                        aud_msg['extras']['PARAMS'].update(dict(
                            DAG_ID=dag_id,
                            EVENT=f'DAG NOT run, user `{ext_user_login}` are not owner of the DAG (`{dag.owners}`)',
                        ))
                        msg = self.audit_action_add(**aud_msg)
                        self.log_debug(f'WebUI/trigger dag :: {msg}')
                        return self.render_error(
                            status=403,
                            mes=(f"""Forbidden!!! You are not the owner of the DAG '{dag_id}'! """
                                 """Launching DAGs, whose owners you are not is - prohibited!"""),
                            mes_log_warning=(f"""User `{user.username}`({request.remote_addr}) is """
                                             f"""NOT owner of the DAG `{dag_id}`, reject the launch request."""),
                            mes_log_debug=(f'{request.method}; {request.path}; {request.args.to_dict()}; '
                                           f'{request.query_string}; {request.values.to_dict()};'),
                        )
                if isinstance(t, str) and request.method != 'GET':
                    msg = self.audit_action_add(**aud_msg)
                    self.log_debug(f'Action of the DAG: {t}, {msg=}')

            elif path[0] == 'paused':
                is_paused = request.values.get('is_paused')
                dag_id = request.values.get('dag_id')
                aud_msg['status_op'] = 'SUCCESS'
                aud_msg['subtype_id'] = 'F0'
                aud_msg['code_op'] = 'other audit operations'
                if is_paused == 'true':
                    aud_msg['extras']['PARAMS'].update(dict(DAG_ID=dag_id, EVENT='DAG activate', ))
                    self.log_info(f"""User `{user.username}`({request.remote_addr}) activate DAG `{dag_id}`""")
                elif is_paused == 'false':
                    aud_msg['extras']['PARAMS'].update(dict(DAG_ID=dag_id, EVENT='DAG deactivate', ))
                    self.log_info(f"""User `{user.username}`({request.remote_addr}) deactivate DAG `{dag_id}`""")
                else:
                    aud_msg['extras']['PARAMS'].update(dict(DAG_ID=dag_id, EVENT='DAG set is unknown state', ))
                    self.log_info(f"""User `{user.username}`({request.remote_addr}) set DAG `{dag_id}` is unknown state""")
                msg = self.audit_action_add(**aud_msg)
                self.log_debug(f'{msg=}')

            elif path[0] == 'connection' and len(path) >= 2 and path[1] not in ['list', ]:
                if self.conf.block_change_policy:
                    aud_msg['status_op'] = 'FAIL'
                    aud_msg['subtype_id'] = 'F0'
                    aud_msg['code_op'] = 'other audit operations'
                    conn = None
                    if path[1] == 'add':
                        aud_msg['extras']['PARAMS'].update(dict(
                            CONN_ID=request.values.get('conn_id'),
                            CONN_TYPE=request.values.get('conn_type'),
                            EVENT='try to add connection',
                        ))
                    elif path[1] == 'edit':
                        conn = self.get_session.query(self.connection_model).filter_by(id=path[2]).one_or_none()
                        if not conn:
                            return self.render_error(status=404, mes=f"""Not found connection with id={path[2]}""")
                        info_msg = 'User `{username}`({addr}) try to edit connection {conn_id} type {conn_type}'
                        self.log_info(info_msg.format(
                            username=user.username,
                            addr=request.remote_addr,
                            conn_id=f'`{conn.conn_id}`' if request.values.get('conn_id') == conn.conn_id else f'`{conn.conn_id}`(changed to `{request.values.get("conn_id")}`)',
                            conn_type=f'`{conn.conn_type}`' if request.values.get('conn_type') == conn.conn_type else f'`{conn.conn_type}`(changed to `{request.values.get("conn_type")}`)',
                        ))
                        aud_msg['extras']['PARAMS'].update(dict(
                            CONN_ID=request.values.get('conn_id'),
                            CONN_TYPE=request.values.get('conn_type'),
                            EVENT='try to edit connection',
                        ))
                        if request.values.get('conn_id') != conn.conn_id:
                            aud_msg['extras']['PARAMS'].update(dict(NEW_CONN_ID=request.values.get('conn_id'), ))
                        if request.values.get('conn_type') != conn.conn_type:
                            aud_msg['extras']['PARAMS'].update(dict(NEW_CONN_TYPE=request.values.get('conn_type'), ))
                    elif path[1] == 'delete':
                        conn = self.get_session.query(self.connection_model).filter_by(id=path[2]).one_or_none()
                        aud_msg['extras']['PARAMS'].update(dict(
                            CONN_ID=request.values.get('conn_id'),
                            CONN_TYPE=request.values.get('conn_type'),
                            EVENT='try to delete connection',
                        ))
                    else:
                        aud_msg['extras']['PARAMS'].update(dict(
                            CONN_ID=request.values.get('conn_id'),
                            CONN_TYPE=request.values.get('conn_type'),
                            EVENT='<< try to unknown connection operation >>',
                        ))
                    msg = self.audit_action_add(**aud_msg)
                    self.log_debug(f'WebUI/connection :: {msg}')
                    return self.render_error(
                        status=403,
                        mes=f'Forbidden!!! По требованиям УЭК, управление Connections заблокировано ¯\_(ツ)_/¯',
                        mes_log_warning=(f"""User `{user.username}`({request.remote_addr}) try to"""
                                         f""" operation on `Connections`"""),
                        mes_log_debug=(f'{request.method} {request.path} {request.args.to_dict()}'
                                       f' {request.query_string} {request.values.to_dict()}'),
                    )
                else:
                    aud_msg['status_op'] = 'SUCCESS'
                    aud_msg['subtype_id'] = 'F0'
                    aud_msg['code_op'] = 'other audit operations'
                    if path[1] == 'add':
                        aud_msg['extras']['PARAMS'].update(dict(
                            CONN_ID=request.values.get('conn_id'),
                            CONN_TYPE=request.values.get('conn_type'),
                            EVENT='added the connection',
                        ))
                    elif path[1] == 'edit':
                        conn = self.get_session.query(self.connection_model).filter_by(id=path[2]).one_or_none()
                        if not conn:
                            return self.render_error(status=404, mes=f"""Not found connection with id={path[2]}""")
                        info_msg = 'User `{username}`({addr}) edited the connection {conn_id} type {conn_type}'
                        self.log_info(info_msg.format(
                            username=user.username,
                            addr=request.remote_addr,
                            conn_id=f'`{conn.conn_id}`' if request.values.get('conn_id') == conn.conn_id else f'`{conn.conn_id}`(changed to `{request.values.get("conn_id")}`)',
                            conn_type=f'`{conn.conn_type}`' if request.values.get('conn_type') == conn.conn_type else f'`{conn.conn_type}`(changed to `{request.values.get("conn_type")}`)',
                        ))
                        aud_msg['extras']['PARAMS'].update(dict(
                            CONN_ID=request.values.get('conn_id'),
                            CONN_TYPE=request.values.get('conn_type'),
                            EVENT='edited the connection',
                        ))
                        if request.values.get('conn_id') != conn.conn_id:
                            aud_msg['extras']['PARAMS'].update(dict(NEW_CONN_ID=request.values.get('conn_id'), ))
                        if request.values.get('conn_type') != conn.conn_type:
                            aud_msg['extras']['PARAMS'].update(dict(NEW_CONN_TYPE=request.values.get('conn_type'), ))
                    elif path[1] == 'delete':
                        conn = self.get_session.query(self.connection_model).filter_by(id=path[2]).one_or_none()
                        aud_msg['extras']['PARAMS'].update(dict(
                            CONN_ID=request.values.get('conn_id'),
                            CONN_TYPE=request.values.get('conn_type'),
                            EVENT='deleted the connection',
                        ))
                    else:
                        aud_msg['extras']['PARAMS'].update(dict(
                            CONN_ID=request.values.get('conn_id'),
                            CONN_TYPE=request.values.get('conn_type'),
                            EVENT='<< try to unknown connection operation >>',
                        ))
                    msg = self.audit_action_add(**aud_msg)
                    self.log_debug(f'WebUI/connection :: {msg}')

        except Exception as e:
            self.log_exception('500, Internal server error')
            return self.render_error(500, 'Internal server error',
                                     mes_log_error=f'{datetime.now()}, WebUI unknown error:\n{e}')

