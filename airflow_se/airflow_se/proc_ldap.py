
"""
    LDAP SE processing
    No comments ¯\_(ツ)_/¯
"""
# отключение лишних сообщений в логах (из подключаемых библиотек) о неподдерживаемых функциях и т.д.
from warnings import simplefilter as warnings_simplefilter
warnings_simplefilter("ignore")

from typing import Optional, Union
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta
from time import sleep
from ldap import (
    set_option,
    OPT_X_TLS_REQUIRE_CERT,
    OPT_X_TLS_NEWCTX,
    OPT_X_TLS_ALLOW,
    OPT_X_TLS_DEMAND,
    OPT_X_TLS_CACERTDIR,
    OPT_X_TLS_CACERTFILE,
    OPT_X_TLS_CERTFILE,
    OPT_X_TLS_KEYFILE,
    initialize,
    OPT_REFERRALS,
    LDAPError,
    sasl,
    SCOPE_SUBTREE,
)

from airflow_se.logger import LoggingMixinSE
from airflow_se.settings import Settings, get_settings
from airflow_se.utils import run_kinit, run_klist, timedelta_to_human_format, get_env, add_env, pop_env, get_pid
from airflow_se.db import get_max_ldap_cache, delete_old_ldap_caches, clear_ldap_caches, set_new_ldap_cache
from airflow_se.as_rust.status import Status

__all__ = [
    "run",
]

def run():
    parser = ArgumentParser(description="Airflow LDAP SE cache refresher")
    parser.add_argument("se_ldap", type=str, help="se_ldap")
    parser.add_argument("--pid", dest="pid", type=str, required=False, help="ProcessID filename")
    return LDAP(parser.parse_args())()


class LDAP(LoggingMixinSE):
    """
    Класс для работы с LDAP.
    """
    newln = '\n'

    def __call__(self, *args, **kwargs):
        """This is a callable object"""
        self.run(*args, **kwargs)

    def __init__(self, cla: Namespace):
        """cla - Command Line Arguments"""
        self.__settings: Optional[Settings] = get_settings()
        super().__init__(self.conf.debug)
        self.__cla = cla
        self.__cla_pid = self.__cla.pid if hasattr(self.__cla, "pid") and self.__cla.pid else None
        hr = "*" * 80
        self.log_info(hr)
        self.log_info(f"""Application is started. ProcessID file name: """
                      f"""{self.__cla_pid if self.__cla_pid else "PID file is not use"}""")
        try:
            if self.__cla_pid:
                with open(self.__cla_pid, "w") as f:
                    f.write(str(get_pid()))
        except Exception as e:
            self.log_error(f"PID file write error: {e}")
        self.log_info(hr)
        self.__app_start: datetime = datetime.now()
        self.__counter: int = 0
        self.__last_cache_ts: Optional[datetime] = None
        self.__time_prev_start: Optional[datetime] = None
        self.__status_prev_start: Status = Status.UNKNOWN

    @property
    def conf(self) -> Settings:
        return self.__settings

    @property
    def counter(self) -> int:
        return self.__counter

    @counter.setter
    def counter(self, value: int):
        self.__counter = value

    @property
    def app_start(self) -> datetime:
        return self.__app_start

    @property
    def last_cache_ts(self) -> Optional[datetime]:
        ch_ts = get_max_ldap_cache(only_ts=True)
        self.__last_cache_ts = ch_ts if ch_ts else None
        return self.__last_cache_ts

    @property
    def time_prev_start(self) -> Optional[datetime]:
        return self.__time_prev_start

    @time_prev_start.setter
    def time_prev_start(self, value: Optional[datetime]):
        self.__time_prev_start = value

    @property
    def status_prev_start(self) -> Status:
        return self.__status_prev_start

    @status_prev_start.setter
    def status_prev_start(self, value: Status):
        self.__status_prev_start = value

    @classmethod
    def recurs_convert(cls, struct: Union[dict, list, tuple, str, bytes, None] = None) -> Union[dict, list, str, None]:
        """
        Рекурсивная функция.
        Конвертирует любую структуру из словарей, списков, кортежей, строк и последовательностей байт
        в такую же структуру с конвертацией всех последовательностей байт в строки utf-8.
        :param struct: Union[dict, list, tuple, str, bytes, None]
        :return: Union[dict, list, str, None]
        """
        if isinstance(struct, (list, tuple)):
            ret = list()
            for x in struct:
                ret.append(cls.recurs_convert(x))
            return ret
        if isinstance(struct, dict):
            ret = dict()
            for k, v in struct.items():
                ret.update({cls.recurs_convert(k): cls.recurs_convert(v)})
            return ret
        if isinstance(struct, str):
            return struct.strip()
        if isinstance(struct, bytes):
            return struct.decode("utf-8").strip()
        return struct

    def worker(self, *args, conf: Optional[Settings] = None, **kwargs) -> bool:
        """
        Собственно, рабочий процесс, который тянет из LDAP информацию.
        :param conf: конфиг (объект класса Settings)
        :param kwargs: набор параметров ключ = значение.
        :return: статус.
        """
        _, _ = args, kwargs
        self.log_info("======== Start LDAP worker ========")
        if not isinstance(conf, Settings):
            conf = self.conf
        # ticket = f"{conf.ldap_bind_user_path_krb5cc}{conf.ldap_bind_user_name}"
        ticket = conf.ldap_bind_user_path_krb5cc
        try:
            # LDAP certificate settings
            if conf.ldap_allow_self_signed is True:
                set_option(OPT_X_TLS_REQUIRE_CERT, OPT_X_TLS_ALLOW)
                set_option(OPT_X_TLS_NEWCTX, 0)
            elif conf.ldap_tls_demand is True:
                set_option(OPT_X_TLS_REQUIRE_CERT, OPT_X_TLS_DEMAND)
                set_option(OPT_X_TLS_NEWCTX, 0)
            if conf.ldap_tls_cacertdir:
                set_option(OPT_X_TLS_CACERTDIR, conf.ldap_tls_cacertdir)
            if conf.ldap_tls_cacertfile:
                set_option(OPT_X_TLS_CACERTFILE, conf.ldap_tls_cacertfile)
            if conf.ldap_tls_certfile:
                set_option(OPT_X_TLS_CERTFILE, conf.ldap_tls_certfile)
            if conf.ldap_tls_keyfile:
                set_option(OPT_X_TLS_KEYFILE, conf.ldap_tls_keyfile)
            # Initialise LDAP connection
            con = initialize(conf.ldap_server)
            con.set_option(OPT_REFERRALS, 0)
            if conf.ldap_use_tls:
                try:
                    con.start_tls_s()
                except LDAPError as e:
                    self.log_error(f"Missing start TLS: {e}")
                    return False
                except Exception as e:
                    self.log_error(f"Could not activate TLS on established connection with {conf.ldap_server}: {e}")
                    return False
                else:
                    self.log_info("TLS is started.")
            # Get Kerberos ticket
            del_envs = lambda x: {k: " ".join(v) if k == "command" and not isinstance(v, str) else v for k, v in x.items() if k != "environments"}
            envs = {"KRB5CCNAME": ticket, "KRB5_KTNAME": conf.ldap_bind_user_keytab}
            ret = run_kinit(
                userid=conf.ldap_bind_user_name,
                realm=conf.ldap_bind_user_realm,
                keytab=conf.ldap_bind_user_keytab,
                ticket=ticket,
                renewable=conf.krb_renew_until,
                lifetime=conf.krb_ticket_lifetime,
                kinit=conf.krb_kinit_path,
                envs=envs,
                verbose=True,
            )
            ret_msg = f"KINIT return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ] " \
                      f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''}" \
                      f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
            if ret.get("returncode") != 0:
                self.log_error(ret_msg)
            else:
                self.log_debug(ret_msg)
            ret = run_klist(
                ticket=ticket,
                klist=conf.krb_klist_path,
                envs=envs,
            )
            ret_msg = f"KLIST return code {ret.get('returncode')}: [ {' '.join(ret.get('command'))} ] " \
                      f"{self.newln + ret.get('stdout').strip() if ret.get('stdout').strip() else ''}" \
                      f"{self.newln + ret.get('stderr').strip() if ret.get('stderr').strip() else ''}"
            if ret.get("returncode") != 0:
                self.log_error(ret_msg)
            else:
                self.log_debug(ret_msg)
            # LDAP GSSAPI authentication
            old_krb5ccname, old_krb5_ktname = get_env("KRB5CCNAME"), get_env("KRB5_KTNAME")
            try:
                add_env("KRB5CCNAME", ticket)
                add_env("KRB5_KTNAME", conf.ldap_bind_user_keytab)
                gssapi_auth = sasl.gssapi("")
                con.sasl_interactive_bind_s("GSSAPI", gssapi_auth)
                self.log_info(f"Connected to server {conf.ldap_server}")
            except Exception as e:
                self.log_error(f"GSSAPI Error: {e}")
                return False
            finally:
                if old_krb5ccname:
                    add_env("KRB5CCNAME", old_krb5ccname)
                else:
                    pop_env("KRB5CCNAME")
                if old_krb5_ktname:
                    add_env("KRB5_KTNAME", old_krb5_ktname)
                else:
                    pop_env("KRB5_KTNAME")
            # LDAP Search
            try:
                search_res = self.recurs_convert(
                    con.search_s(
                        conf.ldap_search,
                        SCOPE_SUBTREE,
                        conf.ldap_search_filter,
                        [
                            conf.ldap_firstname_field,
                            conf.ldap_lastname_field,
                            conf.ldap_email_field,
                            conf.ldap_group_field,
                        ],
                    )
                )
            except Exception as e:
                self.log_error(f"Missing search: {e}")
                return False
            if not isinstance(search_res, list):
                self.log_error(f"Server returned unknown type structure (not list). Type structure: {type(search_res)}")
                return False
            elif len(search_res) == 0:
                self.log_error(f"Server returned empty list. Check parameters LDAP_SEARCH and LDAP_SEARCH_FILTER")
                return False
            self.log_info(f"Server returned {len(search_res)} records of users")
            # self.log_debug(f"Result: {search_res=}")
            try:
                delete_old_ldap_caches(td=conf.ldap_delete_caches_days_ago)
                self.log_info(f"Old records older than {conf.ldap_delete_caches_days_ago} days have been removed "
                              f"from the table")
            except Exception as e:
                self.log_error(f"Missing delete records older than {conf.ldap_delete_caches_days_ago} days from "
                               f"the table: {e}")
            try:
                d = set_new_ldap_cache(search_res)
                self.log_debug(f"method set_new_ldap_cache() returned: {d}")
                self.log_info(f"Cache saved in DB, timestamp: {self.last_cache_ts}")
            except Exception as e:
                self.log_error(f"Missing save cache in DB: {e}")
                return False
            self.log_info(f"======== Normal finish LDAP worker ========")
            return True
        except Exception as e:
            self.log_error(f"Unknown Error: {e}")
            self.log_info(f"======== Abnormal finish LDAP worker ========")
            return False

    def run(self, *args, **kwargs):
        """Главный цикл"""
        while True:
            retry, timeout = self.conf.ldap_process_retry, self.conf.ldap_process_timeout
            td_up, td_down = self.conf.ldap_timedelta_up, self.conf.ldap_timedelta_down
            self.counter += 1
            try:
                is_start_worker: bool = False
                self.log_info(f"""========  Iteration {self.counter} of {retry if retry else "infinity"}  ========""")
                d, h, m, s = timedelta_to_human_format(datetime.now() - self.app_start)
                self.log_info(f"Continuous operation time of the application: {d} days, {h:02}:{m:02}:{s:02}")
                # проверка на количество итераций
                if isinstance(retry, int) and self.counter > retry:
                    self.log_info(f"Number of iterations {retry} completed, application closed")
                    break
                if self.conf.ldap_clear_caches is True and self.counter == 1:
                    # очистка таблицы кэшей LDAP, если запуск первый и установлен параметр
                    clear_ldap_caches()
                    self.log_warning("All LDAP caches has been deleted")
                ch_ts = get_max_ldap_cache(only_ts=True)
                if ch_ts:
                    # кэш существует
                    if self.conf.ldap_refresh_cache_on_start is True and self.counter == 1:
                        # это первая итерация и параметр "старт при первой итерации" установлен
                        if ch_ts < (datetime.now() - td_down):
                            # последний кэш устарел больше, чем на LDAP_TIMEDELTA_DOWN, надо обновлять
                            is_start_worker = True
                    else:
                        dt_start = datetime.combine(datetime.now().date(), self.conf.ldap_time_start)
                        self.log_debug(f"{dt_start=}")
                        if datetime.now() >= dt_start > (datetime.now() - timedelta(seconds=timeout)):
                            # плановое время старта случилось во время прошлого засыпания
                            if ch_ts < (datetime.now() - td_down):
                                # кэш протух больше, чем на LDAP_TIMEDELTA_DOWN, надо обновлять
                                is_start_worker = True
                    if ch_ts < (datetime.now() - td_up):
                        # кэш совсем старый, более чем на LDAP_TIMEDELTA_UP, надо обновлять
                        is_start_worker = True
                else:
                    # кэша не существует, надо стартовать
                    is_start_worker = True
                if is_start_worker is True:
                    # последняя проверка, чтоб не за DDoS-ить LDAP
                    _time_prev_start = self.time_prev_start if self.time_prev_start else (
                        self.last_cache_ts if self.last_cache_ts else datetime(year=1972, month=5, day=5)
                    )
                    if _time_prev_start < (datetime.now() - td_down):
                        self.time_prev_start = datetime.now()
                        if self.worker(*args, conf=self.conf, **kwargs):
                            self.status_prev_start = Status.SUCCESS
                            self.log_info("Cache successfully refreshed")
                        else:
                            self.status_prev_start = Status.FAILED
                            self.log_error("Cache NOT refreshed!")
                self.log_info("Summary:")
                self.log_info(f"    Last request on {self.time_prev_start} -> status {self.status_prev_start.value}")
                self.log_info(f"    Last cache timestamp {self.last_cache_ts}")
            except Exception as e:
                self.log_error(f"Unknown Exception: {e}")
            _, h, m, s = timedelta_to_human_format(timedelta(seconds=timeout))
            self.log_info(f"Sleep at time {h:02}:{m:02}:{s:02}")
            self.log_info("<< ... ... ... ... ...  sweet dream  ... ... ... ... ... >>")
            sleep(timeout)

