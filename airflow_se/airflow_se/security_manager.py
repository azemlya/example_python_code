
"""
Модуль кастомного класса безопасности AirFlow.
В файл `${AIRFLOW_HOME}/webserver_config.py` нужно добавить две строки:
```
from airflow_se.security_manager import AirflowSecurityManagerSE
SECURITY_MANAGER_CLASS = AirflowSecurityManagerSE
```
"""
from __future__ import annotations

# отключение лишних сообщений в логах (из подключаемых библиотек) о неподдерживаемых функциях и т.д.
from warnings import simplefilter as warnings_simplefilter
warnings_simplefilter("ignore")

import gssapi

from typing import Optional, Callable, Union, Iterable, List, Dict, Tuple, Set, Any
from ssl import SSLSocket
from os import path as os_path
from uuid import uuid4
from socket import getfqdn
from base64 import b64encode
from datetime import datetime, timedelta
from time import gmtime
from flask import Flask, current_app, request, session
from flask.helpers import flash

from airflow_se.obj_imp import AFScrtMngr, User, Role, Permission
from airflow_se.crypt import encrypt
from airflow_se.settings import Settings, get_settings
from airflow_se.db import get_max_ldap_cache as sqla_get_max_ldap_cache, TGSList
from airflow_se.utils import run_kinit, run_klist, run_kvno, timedelta_to_human_format, DataPaths, info
from airflow_se.audit import AuditAirflow
from airflow_se.secman import auth_secman, get_secman_data, push_secman_data
from airflow_se.commons import SECMAN_KEY_FOR_TGT, TICKET_PREFIX, EMPTY_KEY_SECMAN

__all__ = [
    "AirflowSecurityManagerSE",
]

Array = (List, Tuple, Set)


class AirflowSecurityManagerSE(AFScrtMngr):
    """
    Класс управления безопасностью Airflow SE ¯\_(ツ)_/¯
    """
    newln = '\n'

    def __init__(self, appbuilder):
        super().__init__(appbuilder)
        # self.__path = f"Airflow_SE::security::{self.__class__.__name__}::"
        # self.__path = f"{path.abspath(__file__)}::{self.__class__.__name__} >> "
        self.__ext_flask_app: Optional[Flask] = current_app
        self.__ext_settings: Optional[Settings] = get_settings(flask_app=self.ext_flask_app)
        self.__ext_ldap_cache_ts: Optional[datetime] = None
        self.__ext_ldap_cache: Optional[Union[List[Any], Tuple[Any], Set[Any]]] = None
        self.__ext_tkt_path: DataPaths = DataPaths(
            base_path=self.ext_conf.secret_path if isinstance(self.ext_conf.secret_path, str) else "/tmp",
            name="AirflowSecurityManagerSE"
        )
        self.ext_log_debug: Callable[..., None] = \
            self.log.info if self.ext_conf.debug is True else self.log.debug
        # обогащаем список TGS из параметра (вариант добавления не распознанных TGS)
        for tgs in self.ext_conf.krb_tgs_list:
            TGSList.push_tgs(tgs)
        # запускаем внутренний аудит
        self.__ext_audit = AuditAirflow()

    def get_role_permissions_from_db(self, role_id: int) -> List[Permission]:
        """
        *** для версии 2.6.3
        В Airflow, по какой-то причине, не перекрыт абстрактный метод из класса FAB.
        По всей видимости, у Airflow класса свой метод, но называется по другому.
        Позже, когда время будет, надо бы разобраться. Потенциальная бага.
        """
        return list()

    @property
    def ext_flask_app(self) -> Flask:
        """Объект текущего приложения Flask"""
        if isinstance(self.__ext_flask_app, Flask):
            return self.__ext_flask_app
        self.__ext_flask_app = current_app
        if isinstance(self.__ext_flask_app, Flask):
            return self.__ext_flask_app
        raise RuntimeError("instance Flask application not found")

    @property
    def ext_conf(self) -> Settings:
        """Настройки"""
        if isinstance(self.__ext_settings, Settings):
            return self.__ext_settings
        self.__ext_settings = Settings(flask_app=self.ext_flask_app)
        if isinstance(self.__ext_settings, Settings):
            return self.__ext_settings
        raise RuntimeError("problems when creating an instance class Settings")

    @property
    def ext_gunicorn_socket(self) -> Optional[SSLSocket]:
        return request.environ.get('gunicorn.socket')

    @property
    def ext_socket_protocol_version(self) -> Optional[str]:
        return self.ext_gunicorn_socket.version() if isinstance(self.ext_gunicorn_socket, SSLSocket) else None

    def auth_user_db(self, username, password) -> Optional[User]:
        """Перекрытый стандартный метод аутентификации"""
        self.log.info(f"@@@ Begin authentication and authorization user \"{username}\" "
                      f"from remote IP {request.remote_addr}, protocol version {self.ext_socket_protocol_version}")
        tkt_file: str = self.__ext_tkt_path.get(username)
        self.ext_log_debug(f"Ticket file path: \"{tkt_file}\"")
        # get ticket for SPN
        if not self.ext_get_ticket_spn():
            self.log.error("Ticket file for Airflow SPN is not receiving")
            self.ext_audit_add(username, "A3", {"REASON": "USER WAS NOT FOUND",
                                                "PARAMS": {"EVENT_TYPE": "SECURITY_MANAGER_LOGIN",
                                                           "EVENT": "Ticket file for Airflow SPN is not receiving"}})
            flash("Ticket file for Airflow SPN is not receiving", "error")
            return None
        # Kerberos authentication
        if not self.ext_auth_kerberos(username, password, tkt_file=tkt_file):
            self.ext_audit_add(username, "A3", {"REASON": "USER WAS NOT FOUND",
                                                "PARAMS": {"EVENT_TYPE": "SECURITY_MANAGER_LOGIN",
                                                           "EVENT": "Error Kerberos authentication"}})
            flash("Error Kerberos authentication", "error")
            return None
        # GSSAPI authentication
        if not self.ext_auth_gssapi(username, tkt_file=tkt_file):
            self.ext_audit_add(username, "A3", {"REASON": "USER WAS NOT FOUND",
                                                "PARAMS": {"EVENT_TYPE": "SECURITY_MANAGER_LOGIN",
                                                           "EVENT": "Error GSSAPI authentication"}})
            flash("Error GSSAPI authentication", "error")
            return None
        # PAM authentication
        if not self.ext_auth_pam(username, password):
            self.ext_audit_add(username, "A3", {"REASON": "USER WAS NOT FOUND",
                                                "PARAMS": {"EVENT_TYPE": "SECURITY_MANAGER_LOGIN",
                                                           "EVENT": "Error PAM authentication"}})
            flash("Error PAM authentication", "error")
            return None
        # LDAP authorization
        ret = self.ext_ldap_authorize(username)
        if ret:
            sess_ext_user = {"login": username, "uuid": str(uuid4())}
            session["ext_user"] = sess_ext_user.copy()
            session.modified = True
            self.ext_audit_add(username, "A2", {"SESSION_ID": sess_ext_user["uuid"],
                                                "PARAMS": {"EVENT_TYPE": "SECURITY_MANAGER_LOGIN",
                                                           "EVENT": "User admitted"}})
            self.log.info(f"LDAP authorization for user \"{sess_ext_user['login']}\" complete.")
            self.log.info(f"All authentication and authorization for user \"{sess_ext_user['login']}\" complete. "
                          f"Session UUID: \"{sess_ext_user['uuid']}\". User admitted.")
        else:
            self.ext_audit_add(username, "A3", {"REASON": "USER WAS NOT FOUND",
                                                "PARAMS": {"EVENT_TYPE": "SECURITY_MANAGER_LOGIN",
                                                           "EVENT": "LDAP authorization NOT completed for user"}})
            self.log.warning(f"LDAP authorization for user \"{username}\" NOT completed")
            self.log.warning(f"All authentication and authorization for user \"{username}\" NOT complete. "
                             f"User NOT admitted.")
            flash("Error LDAP authorization", "error")
            return None
        return ret

    def ext_get_ticket_spn(self) -> bool:
        """Получение тикета для Airflow SPN"""
        # del_envs = lambda x: {k: " ".join(v) if k == "command" and not isinstance(v, str) else v for k, v in x.items() if k != "environments"}
        is_start: bool = False  # признак, надо получать тикет для SPN или ещё старый не протух
        if os_path.exists(self.ext_conf.krb_ccache) and os_path.isfile(self.ext_conf.krb_ccache):
            ts_tf = gmtime(os_path.getmtime(self.ext_conf.krb_ccache))
            dt_tf = datetime(ts_tf.tm_year, ts_tf.tm_mon, ts_tf.tm_mday, ts_tf.tm_hour, ts_tf.tm_min, ts_tf.tm_sec)
            td = datetime.utcnow() - dt_tf
            td_days, td_hours, td_minutes, td_seconds = timedelta_to_human_format(td)
            self.log.info(f"""The ticket file for SPN "{self.ext_conf.krb_ccache}" is outdated on """
                          f"""{td_days} days, {td_hours:02}:{td_minutes:02}:{td_seconds:02}""")
            if td > timedelta(hours=7, minutes=50):  # пока 7 часов 50 минут // потом надо придумать как разрулить
                is_start = True  # прошло больше 7 часов 50 минут, надо обновлять тикет // потом разрулить
        else:
            is_start = True
        if is_start is True:
            ret = run_kinit(
                userid=f"{self.ext_conf.krb_service}/{self.ext_conf.krb_hostname}",
                realm=self.ext_conf.krb_realm,
                keytab=self.ext_conf.krb_keytab,
                ticket=self.ext_conf.krb_ccache,
                renewable=self.ext_conf.krb_renew_until,
                lifetime=self.ext_conf.krb_ticket_lifetime,
                kinit=self.ext_conf.krb_kinit_path,
                verbose=True,
            )
            ret_msg = f"(SPN) KINIT return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ] " \
                      f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''} " \
                      f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
            if ret.get("returncode") != 0:
                self.log.error(ret_msg)
                # flash(ret_msg, "error")
                return False
            else:
                self.ext_log_debug(ret_msg)

            ret = run_klist(ticket=self.ext_conf.krb_ccache, klist=self.ext_conf.krb_klist_path)
            ret_msg = f"(SPN) KLIST return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ] " \
                      f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''} " \
                      f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
            if ret.get("returncode") != 0:
                self.log.error(ret_msg)
                # flash(ret_msg, "error")
                return False
            else:
                self.ext_log_debug(ret_msg)

            self.log.info(f"Kerberos receiving ticket file for SPN successfully completed")
            return True
        else:
            self.log.info("The kerberos ticket for SPN is fresh, no generation is required")
            return True

    def ext_auth_kerberos(self, username: str, password: str, tkt_file: str) -> bool:
        """Аутентификация Kerberos"""
        # del_envs = lambda x: {k: " ".join(v) if k == "command" and not isinstance(v, str) else v for k, v in x.items() if k != "environments"}
        ret = run_kinit(
            userid=username,
            realm=self.ext_conf.krb_realm,
            pwd=password,
            ticket=tkt_file,
            renewable=self.ext_conf.krb_renew_until,
            lifetime=self.ext_conf.krb_ticket_lifetime,
            kinit=self.ext_conf.krb_kinit_path,
            verbose=True,
        )
        ret_msg = f"KINIT return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ] " \
                  f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''} " \
                  f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
        if ret.get("returncode") != 0:
            self.log.error(ret_msg)
            # flash(ret_msg, "error")
            return False
        else:
            self.ext_log_debug(ret_msg)

        # получение TGS по списку (если не получается, то ошибку пишем только в лог, процесс не должен прерываться)
        for tgs in TGSList.get_tgs_list():
            ret = run_kvno(ticket=tkt_file, service=tgs)
            ret_msg = f"KVNO return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ]" \
                      f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''}" \
                      f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
            if ret.get("returncode") != 0:
                self.log.error(ret_msg)
                self.log.error(f'Missing get TGS "{tgs}" for user "{username}" (ticket file "{tkt_file}")')
                TGSList.del_tgs(tgs)
            else:
                self.ext_log_debug(ret_msg)

        ret = run_klist(ticket=tkt_file, klist=self.ext_conf.krb_klist_path)
        ret_msg = f"KLIST return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ] " \
                  f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''} " \
                  f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
        if ret.get("returncode") != 0:
            self.log.error(ret_msg)
            # flash(ret_msg, "error")
            return False
        else:
            self.ext_log_debug(ret_msg)

        self.log.info(f"Kerberos receiving ticket file for user \"{username}\" successfully completed")
        # пушим в SecMan полученный тикет
        try:
            _token = auth_secman()
            sm_tgt: Dict[str, str] = get_secman_data(SECMAN_KEY_FOR_TGT, _token) or dict()
            if EMPTY_KEY_SECMAN in sm_tgt.keys():
                _ = sm_tgt.pop(EMPTY_KEY_SECMAN)
            with open(tkt_file, "rb") as f:
                enc_tkt = encrypt(f.read())
                tkt_file_name = f"{TICKET_PREFIX}{username}"
                sm_tgt[tkt_file_name] = enc_tkt
            if push_secman_data(SECMAN_KEY_FOR_TGT, sm_tgt, _token):
                self.log.info("Tickets being pushed to SecMan")
            else:
                self.log.error("Tickets don't pushed to SecMan")
        except Exception as e:
            self.log.error(f"Failed push ticket to SecMan: {e}")
        return True

    def ext_auth_gssapi(self, username: str, tkt_file: str) -> bool:
        """Аутентификация GSSAPI"""
        def conv(s: Union[str, bytes, None]) -> Optional[str]:
            if isinstance(s, bytes):
                return b64encode(s).decode('utf-8')
            return s
        self.log.info(f"Start GSSAPI(IPA/Kerberos) authentication for user \"{username}\"")
        try:
            krb_service, krb_hostname, krb_realm = self.ext_conf.krb_principal_split
            serv_fqdn, krb_ccache = self.ext_conf.serv_fqdn, self.ext_conf.krb_ccache
            self.ext_log_debug(f"Params: {krb_service=}, {krb_hostname=}, {krb_realm=}, {serv_fqdn=}, {krb_ccache=}")
            # server
            server_hostbased_name = gssapi.Name(f"{krb_service}@{serv_fqdn}",
                                                name_type=gssapi.NameType.hostbased_service)
            self.ext_log_debug(f"GSSAPI: Step 1 - [{server_hostbased_name=}] // {username=}")
            server_name = gssapi.Name(f"{krb_service}/{serv_fqdn}@{krb_realm}")
            self.ext_log_debug(f"GSSAPI: Step 2 - [{server_name=}] // {username=}")
            server_canon_name = server_name.canonicalize(gssapi.MechType.kerberos)
            self.ext_log_debug(f"GSSAPI: Step 3 - [{server_canon_name=}] // {username=}")
            server_hostbased_canon_name = server_hostbased_name.canonicalize(gssapi.MechType.kerberos)
            self.ext_log_debug(f"GSSAPI: Step 4 - [{server_hostbased_canon_name=}] // {username=}")
            server_creds = gssapi.Credentials.acquire(
                name=server_name, usage="accept", store={"ccache": krb_ccache}
            ).creds
            self.ext_log_debug(f"GSSAPI: Step 5 - [{server_creds=}] // {username=}")
            # client
            client_name = gssapi.Name(f"{username}@{krb_realm}")
            self.ext_log_debug(f"GSSAPI: Step 6 - [{client_name=}] // {username=}")
            client_creds = gssapi.Credentials.acquire(
                name=client_name, usage="initiate", store={"ccache": tkt_file}
            ).creds
            self.ext_log_debug(f"GSSAPI: Step 7 - [{client_creds=}] // {username=}")
            # security context
            client_ctx = gssapi.SecurityContext(name=server_name, creds=client_creds, usage="initiate")
            self.ext_log_debug(f"GSSAPI: Step 8 - [{client_ctx=}] // {username=}")
            initial_client_token = client_ctx.step()
            self.ext_log_debug(f"GSSAPI: Step 9 - [initial_client_token = \"{conv(initial_client_token)}\"] // {username=}")
            server_ctx = gssapi.SecurityContext(creds=server_creds, usage="accept")
            self.ext_log_debug(f"GSSAPI: Step 10 - [{server_ctx=}] // {username=}")
            initial_server_token = server_ctx.step(initial_client_token)
            self.ext_log_debug(f"GSSAPI: Step 11 - [initial_server_token = \"{conv(initial_server_token)}\"] // {username=}")
            # change tokens
            server_token = initial_server_token[:]
            client_token = None
            iteration = 0
            while not (client_ctx.complete and server_ctx.complete):
                iteration += 1
                self.ext_log_debug(
                    f"GSSAPI: Step 12 processing (iteration {iteration}) input data:"
                    f"\n    [server_token = \"{conv(server_token)}\"]"
                    f"\n    [client_token = \"{conv(client_token)}\"]"
                    f"\n    Server context complete: {server_ctx.complete}"
                    f"\n    Client context complete: {client_ctx.complete}"
                    f"\n    // {username=}"
                )
                self.ext_log_debug(f"Client security context step [{iteration}] // {username=}")
                client_token = client_ctx.step(server_token)
                self.ext_log_debug(f"[{iteration}] client <<== server_token = \"{conv(server_token)}\" // {username=}")
                self.ext_log_debug(f"[{iteration}] client ==>> client_token = \"{conv(client_token)}\" // {username=}")
                if not client_token:
                    self.ext_log_debug(f"[{iteration}] !!! DON't client token, cycle break !!! // {username=}")
                    break
                self.ext_log_debug(f"Server security context step {iteration} // {username=}")
                server_token = server_ctx.step(client_token)
                self.ext_log_debug(f"[{iteration}] server <<== client_token = \"{conv(client_token)}\" // {username=}")
                self.ext_log_debug(f"[{iteration}] server ==>> server_token = \"{conv(server_token)}\" // {username=}")
            self.ext_log_debug(
                f"GSSAPI: Step 12 processing (iteration finally) output data:"
                f"\n    [server_token = \"{conv(server_token)}\"]"
                f"\n    [client_token = \"{conv(client_token)}\"]"
                f"\n    Server context complete: {server_ctx.complete}"
                f"\n    Client context complete: {client_ctx.complete}"
                f"\n    // {username=}"
            )
            if client_ctx.complete and server_ctx.complete:
                self.log.info(f"GSSAPI/Kerberos authentication for user \"{username}\" successfully completed")
                return True
            else:
                self.log.error(f"GSSAPI/Kerberos authentication FAILED for user \"{username}\"")
                # flash(f"GSSAPI/Kerberos authentication FAILED for user \"{username}\"", "error")
                return False
        except Exception as e:
            self.log.error(
                f"GSSAPI/Kerberos authentication FAILED for user \"{username}\". An Exception has occurred: {e}"
            )
            # flash(f"GSSAPI/Kerberos authentication FAILED for user \"{username}\"", "error")
            return False

    def ext_auth_pam(self, username: str, password: str) -> bool:
        """Аутентификация PAM"""
        if self.ext_conf.pam_is_auth:
            self.ext_log_debug(f"Start PAM authentication for user \"{username}\".")
            import pamela
            try:
                pam_service = self.ext_conf.pam_policy  # This is a PAM policy, see `/etc/pam.d/`
                pamela.authenticate(username, password, service=pam_service, resetcred=pamela.PAM_ESTABLISH_CRED)
                pamela.check_account(username, service=pam_service)
            except pamela.PAMError as e:
                self.log.error(f"PAM authentication FAILED for user \"{username}\": {e}")
                # flash(f"PAM authentication FAILED for user \"{username}\"", "error")
                return False
            except Exception as e:
                self.log.error(
                    f"Unknown Exception to PAM authentication for user \"{username}\": {e}")
                # flash(f"PAM authentication FAILED for user \"{username}\"", "error")
                return False
            else:
                self.log.info(f"PAM authentication for user \"{username}\" successfully completed")
                return True
        else:
            return True

    def ext_decode_str(self, val: Union[Iterable[Union[str, bytes, None]], str, bytes, None], sep: str = ","
                       ) -> Optional[str]:
        """Декодирует данные в строку UTF-8 (с разделителями, если это массив строк)"""
        if isinstance(val, str):
            v = val.strip()
            return v if v else None
        elif isinstance(val, bytes):
            v = val.decode("utf-8").strip()
            return v if v else None
        elif isinstance(val, Iterable):
            a = list()
            for x in val:
                if x is None:
                    continue
                elif isinstance(x, str):
                    v = x.strip()
                    if v:
                        a.append(v)
                elif isinstance(x, bytes):
                    v = x.decode("utf-8").strip()
                    if v:
                        a.append(v)
                else:
                    self.log.error(f"method decode_str({val=}, {sep=}): invalid element was encountered "
                                   f"unexpectedly >> element = \"{x}\", type {type(x)}")
            return sep.join(a) if len(a) > 0 else None
        self.log.error(f"method decode_str({val=}, {sep=}): parameter \"val\" invalid, type {type(val)}")

    def ext_ldap_user_extract(self, username: str) -> Optional[dict]:
        """Получение данных пользователя из кэша LDAP"""
        if self.ext_conf.ldap_is_auth is False:
            return {"username": username,
                    "first_name": username,
                    "last_name": username,
                    "email": username + "@email.notfound",
                    "roles": list(self.ext_conf.ldap_is_auth_def_roles),
                    "ldap_groups": list(),
                    }
        try:
            # пробуем получить свежий кэш LDAP
            self.log.info("Checking cache LDAP...")
            cache_ts = sqla_get_max_ldap_cache(only_ts=True)
            if isinstance(cache_ts, datetime) and (
                    (isinstance(self.__ext_ldap_cache_ts, datetime) and cache_ts > self.__ext_ldap_cache_ts) or
                    not isinstance(self.__ext_ldap_cache_ts, datetime)
            ):
                # свежий кэш имеется, получаем из БД
                ldap_cache_ts, ldap_cache = sqla_get_max_ldap_cache()
                if isinstance(ldap_cache_ts, datetime) and isinstance(ldap_cache, Array) and len(ldap_cache) > 0:
                    # свежий кэш валиден, обновляем
                    self.__ext_ldap_cache_ts, self.__ext_ldap_cache = ldap_cache_ts, ldap_cache
                    self.log.info(f"The current LDAP cache has been updated to latest from DB, "
                                  f"timestamp {self.__ext_ldap_cache_ts} contains {len(self.__ext_ldap_cache)} records")
                elif isinstance(self.__ext_ldap_cache_ts, datetime) and isinstance(self.__ext_ldap_cache, Array) and \
                            len(self.__ext_ldap_cache) > 0:
                    # свежий кэш не валиден, но старый ещё пригоден, работаем на старом
                    self.log.error(f"The latest LDAP cache from DB invalid!")
                    self.log.warning(f"Continue to work with outdated LDAP cache "
                                     f"from {self.__ext_ldap_cache_ts} contains {len(self.__ext_ldap_cache)} records")
                    self.ext_log_debug(f"{ldap_cache_ts=} -> {ldap_cache=}")
                else:
                    # всё плохо, новый кэш пуст, старого нет
                    self.log.critical(f"Everything bad, new cache LDAP invalid, old one not!")
                    # flash("Everything bad, new cache LDAP invalid, old one not!", "error")
            else:
                # кэш не устарел, обновляться не за чем
                self.log.info(f"The current LDAP cache of actual state. Use current LDAP cache "
                              f"from {self.__ext_ldap_cache_ts} contains {len(self.__ext_ldap_cache)} records")
            # проверяем текущий кэш
            if isinstance(self.__ext_ldap_cache_ts, datetime) and isinstance(self.__ext_ldap_cache, Array) and \
                    len(self.__ext_ldap_cache) > 0:
                # текущий кэш не пустой
                self.ext_log_debug(f"The current LDAP cache "
                                   f"from {self.__ext_ldap_cache_ts} contains {len(self.__ext_ldap_cache)} records")
            else:
                # текущий кэш пустой, продолжать не имеет смысла
                self.log.critical(f"Check cache LDAP failed! Can't continue authorization!")
                # flash("Check cache LDAP failed! Can't continue authorization!", "error")
                return None
            ret = dict()
            for u in self.__ext_ldap_cache:
                uid_str = self.ext_decode_str(u[0], ",") or ""
                uid_dct = {y[0]: y[1] for y in (x.split("=") for x in uid_str.split(","))}
                uid = uid_dct.get(self.ext_conf.ldap_uid_field) or ""
                if uid != username:
                    continue
                first_name = self.ext_decode_str(u[1].get(self.ext_conf.ldap_firstname_field), " ") or ""
                last_name = self.ext_decode_str(u[1].get(self.ext_conf.ldap_lastname_field), " ") or ""
                email = self.ext_decode_str(u[1].get(self.ext_conf.ldap_email_field), "; ") or ""
                self.ext_log_debug(f"User data obtained from LDAP cache: {uid=}")
                roles = set()
                ldap_groups = set()
                for x in u[1].get(self.ext_conf.ldap_group_field):
                    x_str = self.ext_decode_str(x)
                    ldap_groups.add(x_str)
                    role_from_map = self.ext_conf.ldap_roles_mapping.get(x_str)
                    if role_from_map:
                        if isinstance(role_from_map, str):
                            roles.add(role_from_map.strip())
                        elif isinstance(role_from_map, Array):
                            roles.update(map(str.strip, role_from_map))
                if len(roles) == 0 and self.ext_conf.user_registration is True and self.ext_conf.user_registration_role:
                    roles.add(self.ext_conf.user_registration_role)
                ret.update({"username": uid,
                            "first_name": first_name,
                            "last_name": last_name,
                            "email": email,
                            "roles": list(roles),
                            "ldap_groups": list(ldap_groups),
                            })
                break
            if len(ret) == 0:
                self.log.error(f"User \"{username}\" NOT found in LDAP cache")
                # flash(f"User \"{username}\" NOT found in LDAP cache", "error")
                return None
            else:
                self.log.info(f"User \"{username}\" extracted from LDAP cache, user roles: {ret.get('roles')}")
                return ret
        except Exception as e:
            self.log.error(f"Failed extract user \"{username}\" from LDAP cache. Error: {e}")
            # flash(f"Failed extract user \"{username}\" from LDAP cache", "error")
            return None

    def ext_ldap_user_extract_silent(self, username: str) -> Optional[dict]:
        if self.ext_conf.ldap_is_auth is False:
            return {"username": username,
                    "first_name": username,
                    "last_name": username,
                    "email": username + "@email.notfound",
                    "roles": list(self.ext_conf.ldap_is_auth_def_roles),
                    "ldap_groups": list(),
                    }
        try:
            # пробуем получить свежий кэш LDAP
            cache_ts = sqla_get_max_ldap_cache(only_ts=True)
            if isinstance(cache_ts, datetime) and (
                    (isinstance(self.__ext_ldap_cache_ts, datetime) and cache_ts > self.__ext_ldap_cache_ts) or
                    not isinstance(self.__ext_ldap_cache_ts, datetime)
            ):
                # свежий кэш имеется, получаем из БД
                ldap_cache_ts, ldap_cache = sqla_get_max_ldap_cache()
                if isinstance(ldap_cache_ts, datetime) and isinstance(ldap_cache, Array) and len(ldap_cache) > 0:
                    # свежий кэш валиден, обновляем
                    self.__ext_ldap_cache_ts, self.__ext_ldap_cache = ldap_cache_ts, ldap_cache
                elif isinstance(self.__ext_ldap_cache_ts, datetime) and isinstance(self.__ext_ldap_cache, Array) and \
                            len(self.__ext_ldap_cache) > 0:
                    # свежий кэш не валиден, но старый ещё пригоден, работаем на старом
                    self.log.error(f"The latest LDAP cache from DB invalid!")
                    self.log.warning(f"Continue to work with outdated LDAP cache from "
                                     f"{self.__ext_ldap_cache_ts} contains {len(self.__ext_ldap_cache)} records")
                    self.ext_log_debug(f"{ldap_cache_ts=} -> {ldap_cache=}")
                else:
                    # всё плохо, новый кэш пуст, старого нет
                    self.log.critical(f"Everything bad, new cache LDAP invalid, old one not!")
            else:
                # кэш не устарел, обновляться не за чем
                pass
            # проверяем текущий кэш
            if isinstance(self.__ext_ldap_cache_ts, datetime) and isinstance(self.__ext_ldap_cache, Array) and \
                    len(self.__ext_ldap_cache) > 0:
                # текущий кэш не пустой
                self.ext_log_debug(f"The current LDAP cache "
                                   f"from {self.__ext_ldap_cache_ts} contains {len(self.__ext_ldap_cache)} records")
            else:
                # текущий кэш пустой, продолжать не имеет смысла
                self.log.critical(f"Check cache LDAP failed!!")
                return None
            ret = dict()
            for u in self.__ext_ldap_cache:
                uid_str = self.ext_decode_str(u[0], ",") or ""
                uid_dct = {y[0]: y[1] for y in (x.split("=") for x in uid_str.split(","))}
                uid = uid_dct.get(self.ext_conf.ldap_uid_field) or ""
                if uid != username:
                    continue
                first_name = self.ext_decode_str(u[1].get(self.ext_conf.ldap_firstname_field), " ") or ""
                last_name = self.ext_decode_str(u[1].get(self.ext_conf.ldap_lastname_field), " ") or ""
                email = self.ext_decode_str(u[1].get(self.ext_conf.ldap_email_field), "; ") or ""
                roles = set()
                ldap_groups = set()
                for x in u[1].get(self.ext_conf.ldap_group_field):
                    x_str = self.ext_decode_str(x)
                    ldap_groups.add(x_str)
                    role_from_map = self.ext_conf.ldap_roles_mapping.get(x_str)
                    if role_from_map:
                        if isinstance(role_from_map, str):
                            roles.add(role_from_map.strip())
                        elif isinstance(role_from_map, Array):
                            roles.update(map(str.strip, role_from_map))
                if len(roles) == 0 and self.ext_conf.user_registration is True and self.ext_conf.user_registration_role:
                    roles.add(self.ext_conf.user_registration_role)
                ret.update({"username": uid,
                            "first_name": first_name,
                            "last_name": last_name,
                            "email": email,
                            "roles": list(roles),
                            "ldap_groups": list(ldap_groups),
                            })
                break
            if len(ret) == 0:
                self.log.error(f"User \"{username}\" NOT found in LDAP cache")
                return None
            else:
                return ret
        except Exception as e:
            self.log.error(f"Failed extract user \"{username}\" from LDAP cache. Error: {e}")
            return None

    def ext_find_user(self, username) -> Optional[User]:
        """Обёртка над стандартной функцией поиска пользователя в БД"""
        try:
            return self.find_user(username=username)
        except Exception as e:
            self.log.error(f"Find user \"{username}\" error: {e}")

    def ext_find_roles(self, roles: Union[Iterable[str], str], default_roles: Union[Iterable[str], str, None] = None
                       ) -> List[Role]:
        """Возвращает список ролей как объектов класса Role(модель)"""
        def clear_list(s: Union[Iterable[str], str, None]) -> Set[str]:
            if type(s) == str and not s.isspace():
                return {s.strip(), }
            elif isinstance(s, Array):
                return set(map(str.strip, (x for x in s if type(x) == str and not x.isspace())))
            return set()
        rls = set()
        rls.update(clear_list(roles), clear_list(default_roles))
        ret = list()
        for r in rls:
            _r = self.find_role(r)
            if isinstance(_r, Role):
                ret.append(_r)
            else:
                self.log.error(f"Role \"{r}\" NOT found in DB!")
                # flash(f"Role \"{r}\" NOT found in DB!", "error")
        return ret

    def add_user(self,  username, first_name, last_name, email, role, password="", hashed_password="",
    ) -> Optional[User]:
        """
        Generic function to create user
        ---
        Метод перекрыт
        """
        sess = self.get_session
        try:
            user = self.user_model()
            user.first_name = first_name
            user.last_name = last_name
            user.username = username
            user.email = email
            user.active = True
            user.roles = role if isinstance(role, list) else [role]
            user.password = "-"
            # if hashed_password:
            #     user.password = hashed_password
            # else:
            #     user.password = generate_password_hash(password)
            sess.add(user)
            sess.commit()
            return user
        except Exception as e:
            self.log.error(f"User \"{username}\" NOT added: {e}")
            sess.rollback()

    def ext_add_user(self, **kwargs) -> Optional[User]:
        """Обёртка на стандартную функцию добавления нового пользователя в БД"""
        username = kwargs.get("username")
        try:
            if not username:
                raise RuntimeError("method ext_add_user - Argument \"username\" not found in parameters")
            user: Optional[User] = self.add_user(
                username = username,
                first_name = kwargs.get("first_name", "first_name_not_found"),
                last_name = kwargs.get("last_name", "last_name_not_found"),
                email = username + "<" + kwargs.get("email", username + "@email.notfound") + ">",
                role = self.ext_find_roles(kwargs.get("roles"), kwargs.get("default_roles")),
            )
            # self.ext_log_debug(f"{user=} >> {kwargs=}")
            if isinstance(user, User):
                self.ext_log_debug(f"User \"{user.username}\" added in DB. User roles: \"{user.roles}\"")
                return user
            else:
                self.log.error(f"User \"{username}\" NOT added in DB!")
                # flash("User NOT added in DB!", "error")
        except Exception as e:
            self.log.error(f"""User "{username}" NOT added in DB! Error: {e}""")
            # flash("User NOT added in DB!", "error")

    def update_user(self, user) -> bool:
        """Метод перекрыт"""
        sess = self.get_session
        try:
            sess.merge(user)
            sess.commit()
            return True
        except Exception as e:
            self.log.error(f"User \"{user.username}\" NOT updated: {e}")
            sess.rollback()
            return False

    def ext_update_user(self, user: Optional[User]) -> bool:
        """Обёртка над стандартной функцией обновления пользователя"""
        try:
            if isinstance(user, User):
                user.password = "-"
                self.update_user(user)
                self.ext_log_debug(f"User \"{user.username}\" flushed. User roles: \"{user.roles}\"")
                self.log.info(f"User \"{user.username}\" updated in DB")
                return True
            else:
                self.log.error(f"method ext_update_user() -> parameter \"user\" invalid")
        except Exception as e:
            self.log.error(f"User \"{user.username}\" NOT updated in DB! Error: {e}")
            # flash("User NOT updated in DB!", "error")
        return False

    def update_user_auth_stat(self, user, success=True) -> bool:
        """
        Update user authentication stats upon successful/unsuccessful
        authentication attempts.
        :param user:
            The identified (but possibly not successfully authenticated) user
            model
        :param success:
            Defaults to true, if true increments login_count, updates
            last_login, and resets fail_login_count to 0, if false increments
            fail_login_count on user model.
        ---
        Метод перекрыт
        """
        if not user.login_count:
            user.login_count = 0
        if not user.fail_login_count:
            user.fail_login_count = 0
        if success:
            user.login_count += 1
            user.last_login = datetime.now()
            user.fail_login_count = 0
        else:
            user.fail_login_count += 1
        return self.update_user(user)

    def ext_update_user_auth_stat(self, user: Optional[User], success: bool = True) -> bool:
        """Обёртка над стандартной функцией, обновляет статистику по пользователю"""
        try:
            if isinstance(user, User):
                self.update_user_auth_stat(user, success)
                self.log.info(f"User \"{user.username}\" authentication information updated in DB")
                return True
            else:
                self.log.error(f"method ext_update_user_auth_stat({user=}) -> parameter \"user\" invalid")
        except Exception as e:
            self.log.error(f"User \"{user.username}\" authentication information NOT updated in DB! Error: {e}")
            # flash(f"User \"{user.username}\" authentication information NOT updated in DB!", "error")
        return False

    def ext_ldap_authorize(self, username: str) -> Optional[User]:
        """Авторизация по кэшу LDAP"""
        # If no username is provided, go away
        if username is None or username.isspace():
            return None
        # Search the DB for this user
        user: Optional[User] = self.ext_find_user(username=username)
        # If user is not active, go away
        if user and user.is_active is not True:
            self.log.warning(f"User \"{user.username}\" not active in DB (user.is_active is not True)")
            return None
        # If user is not registered, and not self-registration, warning and go away
        if (not user) and self.ext_conf.user_registration is not True:
            self.log.warning(f"User \"{username}\" not registered in DB (parameter USER_REGISTRATION not equal True)")
            return None
        user_attrs = self.ext_ldap_user_extract(username)
        # self.ext_log_debug(f"{user_attrs=}")
        # LOGIN SUCCESS (only if user is now registered)
        if user:
            if isinstance(user_attrs, dict):
                if self.ext_conf.roles_sync_at_login:
                    _username = user_attrs.get("username")
                    if not isinstance(_username, str) or _username.isspace():
                        raise RuntimeError("method ext_ldap_authorize - \"username\" is invalid "
                                           f"// {info(username=_username)}")
                    user.username = _username
                    user.first_name = user_attrs.get("first_name", "first_name_not_found")
                    user.last_name = user_attrs.get("last_name", "last_name_not_found")
                    user.email = _username + "<" + user_attrs.get("email", _username + "@email.notfound") + ">"
                    sess = self.get_session
                    try:
                        sess.merge(user)
                        user.roles = sess.query(Role).filter(Role.name.in_(user_attrs.get("roles"))).all()
                        sess.commit()
                        # roles_from_ldap = self.ext_find_roles(user_attrs.get("roles"))
                        # for r in user.roles:
                        #     if r.name not in [_r.name for _r in roles_from_ldap]:
                        #         user.roles.remove(r)
                        # for r in roles_from_ldap:
                        #     if r.name not in [_r.name for _r in user.roles]:
                        #         user.roles.append(r)

                        # user.roles = self.ext_find_roles(user_attrs.get("roles"))
                    except Exception as e:
                        # Возникает ошибка, не понимаю с чем связанная, надо на досуге разобраться
                        sess.rollback()
                        self.log.error(f"Roles NOT update for user \"{user.username}\": {e}")
                        # flash(f"Roles NOT update for user \"{user.username}\"", "error")
                        return None
                    self.ext_log_debug(f"method ext_ldap_authorize() >> {user.username=}, {user.roles=} // {username=}")
                    # self.ext_update_user(user)
                self.ext_update_user_auth_stat(user, True)
                return user
            else:
                self.log.error(f"User \"{user.username}\" authentication information NOT updated in DB, "
                               f"user attributes invalid")  # // {info(user_attrs=user_attrs)}
                self.ext_update_user_auth_stat(user, False)
                # flash(f"User \"{user}\" authentication information NOT updated in DB", "error")
                return None
        # If the user is new, register them
        if (not user) and self.ext_conf.user_registration:
            if isinstance(user_attrs, dict):
                user = self.ext_add_user(**user_attrs)
                # If user registration failed, go away
                if not user:
                    self.log.error(f"Registration in DB FAILED for new user \"{username}\"!")
                    # flash(f"Registration in DB FAILED for new user \"{username}\"!", "error")
                    return None
                self.ext_update_user_auth_stat(user, True)
                self.log.info(f"New user \"{user.username}\" registered in DB")
                return user
            else:
                self.log.error(f"New user \"{username}\" NOT registered in DB, user attributes invalid")
                               # f"// {info(user_attrs=user_attrs)}"
                # flash(f"New user \"{username}\" NOT registered in DB, user attributes invalid", "error")

    def ext_audit_add(self, username: str, subtype_id: str, extras: dict):
        """
        Добавляет событие аудита
        A2 - успешная аутентификация пользователя
        A3 - НЕ успешная
        """
        if isinstance(subtype_id, str) and subtype_id in ("A2", "A3") and isinstance(extras, dict):
            msg = self.__ext_audit.audit_action_add(
                ts=datetime.now(),
                host=getfqdn() or self.ext_conf.serv_fqdn,
                remote_addr=request.remote_addr,
                remote_login=username,
                code_op="Fail sign in" if subtype_id == "A3" else "Sign In" if subtype_id == "A2" else None,
                app_id=self.ext_conf.kafka_app_id,
                type_id="Audit",
                subtype_id=subtype_id,
                status_op="FAIL" if subtype_id == "A3" else "SUCCESS" if subtype_id == "A2" else None,
                extras=extras,
            )
            self.ext_log_debug(f'Audit action added: {msg}')
        else:
            self.log.error(f"method ext_audit_add() >> parameters invalid "
                           f"// {info(username=username, subtype_id=subtype_id, extras=extras)}")

