
"""
    Общий класс доступа к настройкам

    Примечание:
        Класс конфигурации Airflow берём из модуля `airflow.configuration`:
            * `AirflowConfigParser` - класс конфига
            * `conf` - переменная класса `AirflowConfigParser`,
                уже инициализированный конфиг Airflow (из файла `airflow.cfg`)
        Класс логирования берём из "примеси" LoggingMixin из модуля `airflow.utils.log.logging_mixin`
"""
from typing import Optional, Union, Iterable, Tuple, Set, Any
from datetime import time, timedelta
from flask import Flask, Config

from airflow_se.obj_imp import AirflowConfigParser, conf, LoggingMixin
from airflow_se.commons import PARAMS_PREFIXES
from airflow_se.as_rust import Result
from airflow_se.utils import lazy_property, info
from airflow_se.config import get_config_value

from .settings_exceptions import SettingsError, SettingsParamError
from .settings_constants import WEBSERVER_CONFIG_FILENAME
from .settings_service_fn import (
    get_param_env,
    get_fqdn,
    get_af_home_dir,
    check_str,
    check_int,
    check_bool,
    check_json,
    check_time,
    check_timedelta,
    check_range_int,
)
from .settings_defaults import (
    DEF_USER_REGISTRATION,
    DEF_USER_REGISTRATION_ROLE,
    DEF_ROLES_SYNC_AT_LOGIN,
    DEF_KRB_KINIT_PATH,
    DEF_KRB_KLIST_PATH,
    DEF_KRB_TICKET_LIFETIME,
    DEF_KRB_RENEW_UNTIL,
    DEF_PAM_POLICY,
    DEF_LDAP_USE_TLS,
    DEF_LDAP_ALLOW_SELF_SIGNED,
    DEF_LDAP_TLS_DEMAND,
    DEF_LDAP_FIRSTNAME_FIELD,
    DEF_LDAP_LASTNAME_FIELD,
    DEF_LDAP_EMAIL_FIELD,
    DEF_LDAP_UID_FIELD,
    DEF_LDAP_BIND_USER_PATH_KRB5CC,
    DEF_LDAP_GROUP_FIELD,
    DEF_LDAP_REFRESH_CACHE_ON_START,
    DEF_LDAP_PROCESS_TIMEOUT,
    DEF_LDAP_PROCESS_RETRY,
    DEF_LDAP_TIME_START,
    DEF_LDAP_TIMEDELTA_UP,
    DEF_LDAP_TIMEDELTA_DOWN,
    DEF_LDAP_DELETE_CACHES_DAYS_AGO,
    DEF_KAFKA_PROCESS_TIMEOUT,
    DEF_KAFKA_PROCESS_RETRY,
    DEF_KAFKA_PAGE_SIZE,
    DEF_TICKETMAN_SCAN_DIRS,
    DEF_TICKETMAN_PROCESS_TIMEOUT,
    DEF_TICKETMAN_PROCESS_RETRY,
    DEF_DEBUG,
    DEF_DEBUG_MASK,
    DEF_BLOCK_CHANGE_POLICY,
    DEF_PAM_IS_AUTH,
    DEF_LDAP_CLEAR_CACHES,
    DEF_LDAP_IS_AUTH,
    DEF_LDAP_IS_AUTH_DEF_ROLES,
)

__all__ = [
    "Settings",
]


class Settings(LoggingMixin):
    """Настройки"""
    def __init__(self,
                 flask_app: Optional[Flask] = None,
                 flask_app_config: Optional[Config] = None,
                 airflow_config: Optional[AirflowConfigParser] = None,
                 silent: bool = False,
                 ):
        """
        :silent: генерировать исключения при ошибках параметров?
            True - ошибки будут записываться в лог
            False - будут генерироваться исключения
        """
        super().__init__()
        self.silent: bool = silent if type(silent) == bool else False
        self.__path = f"Airflow_SE::settings::{self.__class__.__name__}::"
        # self.__path = f"{path.abspath(__file__)}::{self.__class__.__name__}::"
        self.__flask_app: Optional[Flask] = None
        self.__flask_app_config: Optional[Config] = None
        self.__airflow_config: Optional[AirflowConfigParser] = None
        if isinstance(flask_app, Flask):
            self.__flask_app = flask_app
        if isinstance(flask_app_config, Config):
            self.__flask_app_config = flask_app_config
        else:
            if self.__flask_app:
                self.__flask_app_config = self.__flask_app.config
            else:
                af_home_dir = get_af_home_dir()
                if af_home_dir.failure:
                    raise SettingsError("\n".join(af_home_dir.error))
                self.__flask_app_config = Config(af_home_dir.value)
                if not self.__flask_app_config.from_pyfile(WEBSERVER_CONFIG_FILENAME):
                    raise SettingsError(f"{self.__path}__init__() >> Missed initialize webserver config:\n"
                                        f"    AIRFLOW_HOME = \"{af_home_dir.value}\"\n"
                                        f"    WEBSERVER_CONFIG_FILENAME = \"{WEBSERVER_CONFIG_FILENAME}\"")
        if isinstance(airflow_config, AirflowConfigParser):
            self.__airflow_config = airflow_config
        else:
            self.__airflow_config = conf

    @lazy_property
    def debug(self) -> bool:
        """
        # Отладка
        # Тип bool (True/False)
        # Параметр не обязательный
        # По умолчанию False (отладочные сообщения отключены)
        """
        res = self.get_bool("DEBUG", DEF_DEBUG)
        if res.success:
            return res.value
        self.log_error(*res.error)
        return False

    @lazy_property
    def debug_mask(self) -> str:
        """
        # Формат сообщений при включённом DEBUG
        # Тип str
        # Параметр не обязательный
        # По умолчанию "DEBUG >> {}"
        """
        res = self.get_str("DEBUG_MASK", DEF_DEBUG_MASK)
        if res.success:
            return res.value
        self.log_error(*res.error)
        return "DEBUG >> {}"

    def log_debug(self, *args):
        """Debug log message"""
        msg = "\n    ".join(args)
        if self.debug is True:
            try:
                msg = self.debug_mask.format(msg)
            except Exception as e:
                self.log_error(f"Invalid format string in parameter \"DEBUG_MASK\": {e}")
                self.log_info(f"DEBUG >> {msg}")
            else:
                self.log_info(msg)
        else:
            self.log.debug(msg)

    def log_info(self, *args):
        """Info log messages"""
        self.log.info("\n".join(args))

    def log_warning(self, *args):
        """Warning log messages"""
        for x in args:
            self.log.warning(x)

    def log_error(self, *args, critical: bool = False):
        """Error log messages"""
        logger = self.log.critical if critical is True else self.log.error
        for x in args:
            logger(x)

    def raise_error(self, *args, critical: bool = False):
        """Raise SettingsParamError or write error messages to log"""
        if self.silent is True:
            self.log_error(*args, critical)
        else:
            raise SettingsParamError(*args)

    @property
    def curr_app(self) -> Optional[Flask]:
        """Ссылка на текущее приложение Flask"""
        return self.__flask_app

    @property
    def curr_app_conf(self) -> Config:
        """Ссылка на конфигурацию текущего приложения Flask, файл `webserver_config.py`"""
        return self.__flask_app_config

    @property
    def af_conf(self) -> AirflowConfigParser:
        """Конфигурация Airflow, файл `airflow.cfg`"""
        return self.__airflow_config

    def get_param_af(self, section: str, key: str, default: Optional[str] = None) -> Result:
        """Получает из файла `airflow.cfg` значение по секции и ключу."""
        _section, _key, _default, _res = check_str(section), check_str(key), check_str(default), Result.Ok()
        if _section.failure:
            _res.error = _section.error
            _res.error = f"{self.__path}get_param_af() >> parameter \"section\" invalid, "\
                         f"non-empty string was expected // {info(section=section)}"
        if _key.failure:
            _res.error = _key.error
            _res.error = f"{self.__path}get_param_af() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
        if _res.success:
            try:
                param_from_af = self.af_conf.get(_section.value, _key.value)
            except Exception as e:
                _res.error = f"{self.__path}get_param_af() >> "\
                             f"self.af_conf.get(\"{_section.value}\", \"{_key.value}\") >> {e} "\
                             f"// {info(section=section, key=key, default=default)}"
                param_from_af = None
            res = check_str(param_from_af, _default.value if _default.success else None)
            if res.success:
                return res
            _res.error = res.error
        _res.error = f"{self.__path}get_param_af() >> parameter not found in Airflow config "\
                     f"// {info(section=section, key=key, default=default)}"
        return _res

    def get_param_ws(self, key: str, prefix: Union[Iterable[str], str, None] = PARAMS_PREFIXES) -> Result:
        """Ищет параметры в конфиге Flask, файл `webserver_config.py`, учитывая префиксы."""
        _key = check_str(key)
        if _key.failure:
            _key.error = f"{self.__path}get_param_ws() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
            return _key
        if type(prefix) == str:
            res = self.curr_app_conf.get(f"{prefix.strip()}{_key.value}")
            if res is not None:
                return Result.Ok(res)
        elif isinstance(prefix, Iterable):
            for p in prefix:
                res = self.curr_app_conf.get(f"{p.strip()}{_key.value}")
                if res is not None:
                    return Result.Ok(res)
        elif prefix is None:
            res = self.curr_app_conf.get(_key.value)
            if res is not None:
                return Result.Ok(res)
        return Result.Err(f"{self.__path}get_param_ws() >> parameter \"{_key.value}\" not found in Webserver config "
                          f"// {info(key=_key.value)}")

    def get_param_se(self, key: str, prefix: Union[Iterable[str], str, None] = PARAMS_PREFIXES) -> Result:
        """Ищет в конфиге `airflow_se_config.py`"""
        _key = check_str(key)
        if _key.failure:
            _key.error = f'{self.__path}get_param_se() >> parameter "key" is invalid, '\
                         f'non-empty string was expected // {info(key=key)}'
            return _key
        ret = get_config_value(
            _key.value,
            prefix,
            Result.Err(
                f'{self.__path}get_param_se() >> parameter "{_key.value}" not found '
                f'in environment variables or "airflow_se_config.py" // {info(key=_key.value)}'
            )
        )
        return ret if isinstance(ret, Result) else Result.Ok(ret)
        # if type(prefix) == str:
        #     _full_key = f"{prefix.strip()}{_key.value}"
        #     res = getattr(se_conf, _full_key) if hasattr(se_conf, _full_key) else None
        #     if res is not None:
        #         return Result.Ok(res)
        # elif isinstance(prefix, Iterable):
        #     for p in prefix:
        #         _full_key = f"{p.strip()}{_key.value}"
        #         res = getattr(se_conf, _full_key) if hasattr(se_conf, _full_key) else None
        #         if res is not None:
        #             return Result.Ok(res)
        # elif prefix is None:
        #     res = getattr(se_conf, _key.value) if hasattr(se_conf, _key.value) else None
        #     if res is not None:
        #         return Result.Ok(res)
        # return Result.Err(f"{self.__path}get_param_se() >> parameter \"{_key.value}\" not found in `airflow_se_config` "
        #                   f"// {info(key=_key.value)}")

    def get_param(self, key: str, default: Optional[Any] = None) -> Result:
        """
        На прямую не использовать!!! Возвращаемое значение (Result.value) не проверяется (тип не определённый).
        Использовать: self.get_str(), self.get_bool(), self.get_int() и т.д.
        Ищет параметр в переменных среды окружения или в файле настроек Flask.
        """
        _key = check_str(key)
        if _key.failure:
            _key.error = f"{self.__path}get_param() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
            return _key
        env = get_param_env(_key.value)
        if env.success:
            return env
        se = self.get_param_se(_key.value)
        if se.success:
            return se
        ws = self.get_param_ws(_key.value)
        if ws.success:
            return ws
        if default is not None:
            return Result.Ok(default)
        res = env
        res.error = se.error
        res.error = ws.error
        res.error = f"{self.__path}get_param() >> parameter \"{_key.value}\" not found nowhere and default not set "\
                    f"// {info(key=_key.value, default=default)}"
        return res

    def get_str(self, key: str, default: Optional[Any] = None) -> Result:
        """Ищет `str` параметр"""
        _key = check_str(key)
        if _key.failure:
            _key.error = f"{self.__path}get_str() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
            return _key
        _res = self.get_param(_key.value, default)
        if _res.success:
            res = check_str(_res.value, default)
            if res.success:
                return res
            res.error = f"{self.__path}get_str() >> value type {type(_res.value)} not converted to string"
            return res
        _res.error = f"{self.__path}get_str() >> string parameter \"{_key.value}\" not found"
        return _res

    def get_bool(self, key: str, default: Optional[Any] = None) -> Result:
        """Ищет `bool` параметр"""
        _key = check_str(key)
        if _key.failure:
            _key.error = f"{self.__path}get_bool() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
            return _key
        _res = self.get_param(_key.value, default)
        if _res.success:
            res = check_bool(_res.value, default)
            if res.success:
                return res
            res.error = f"{self.__path}get_bool() >> value type {type(_res.value)} not converted to boolean"
            return res
        _res.error = f"{self.__path}get_bool() >> boolean parameter \"{_key.value}\" not found"
        return _res

    def get_int(self, key: str, default: Optional[Any] = None) -> Result:
        """Ищет `int` параметр"""
        _key = check_str(key)
        if _key.failure:
            _key.error = f"{self.__path}get_int() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
            return _key
        _res = self.get_param(_key.value, default)
        if _res.success:
            res = check_int(_res.value, default)
            if res.success:
                return res
            res.error = f"{self.__path}get_int() >> value type {type(_res.value)} not converted to integer"
            return res
        _res.error = f"{self.__path}get_int() >> integer parameter \"{_key.value}\" not found"
        return _res

    def get_json(self, key: str, default: Optional[Any] = None) -> Result:
        """
        Если параметр является строкой, то пробует парсить как json.
        Если парсится - возвращает результат парсинга.
        Если не парсится - сообщает об ошибке и возвращает значение default (проводя с ним те же действия).
        Если параметр является не строковым значением, то возвращает это значение.
        """
        _key = check_str(key)
        if _key.failure:
            _key.error = f"{self.__path}get_json() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
            return _key
        _res = self.get_param(_key.value, default)
        if _res.success:
            res = check_json(_res.value, default)
            if res.success:
                return res
            res.error = f"{self.__path}get_json() >> value type {type(_res.value)} don't parse as json"
            return res
        _res.error = f"{self.__path}get_json() >> json parameter \"{_key.value}\" not found"
        return _res

    def get_time(self, key: str, default: Optional[Any] = None) -> Result:
        """Ищет `time` параметр"""
        _key = check_str(key)
        if _key.failure:
            _key.error = f"{self.__path}get_time() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
            return _key
        _res = self.get_json(_key.value, default)
        if _res.success:
            res = check_time(_res.value, default)
            if res.success:
                return res
            res.error = f"{self.__path}get_time() >> value type {type(_res.value)} don't convert to time"
            return res
        _res.error = f"{self.__path}get_time() >> time parameter \"{_key.value}\" not found"
        return _res

    def get_timedelta(self, key: str, default: Optional[Any] = None) -> Result:
        """Ищет `timedelta` параметр"""
        _key = check_str(key)
        if _key.failure:
            _key.error = f"{self.__path}get_timedelta() >> parameter \"key\" invalid, "\
                         f"non-empty string was expected // {info(key=key)}"
            return _key
        _res = self.get_json(_key.value, default)
        if _res.success:
            res = check_timedelta(_res.value, default)
            if res.success:
                return res
            res.error = f"{self.__path}get_timedelta() >> value type {type(_res.value)} don't convert to timedelta"
            return res
        _res.error = f"{self.__path}get_timedelta() >> timedelta parameter \"{_key.value}\" not found"
        return _res

    #################################################################################################
    ###                             Подключение к метаданным                                      ###
    #################################################################################################

    @lazy_property
    def sql_alchemy_conn(self) -> str:
        """Строка подключения к БД с метаданными"""
        section, key = "database", "sql_alchemy_conn"
        res = self.get_param_af(section, key)
        if res.success:
            return res.value
        res.error = f"{self.__path}sql_alchemy_conn() >> missing get required parameter "\
                    f"// {info(section=section, key=key)}"
        self.raise_error(*res.error, True)

    @lazy_property
    def sql_alchemy_schema(self) -> Optional[str]:
        """Схема в БД с метаданными"""
        section, key = "database", "sql_alchemy_schema"
        res = self.get_param_af(section, key)
        if res.success:
            return res.value
        res.error = f"{self.__path}sql_alchemy_schema() >> missing get parameter "\
                    f"// {info(section=section, key=key)}"
        self.log_debug(*res.error)

    #################################################################################################
    ###                                 Настройки общие                                           ###
    #################################################################################################

    @lazy_property
    def serv_fqdn(self) -> str:
        """Доменное имя сервера, должно существовать обязательно!!!"""
        ret = self.krb_hostname
        if ret:
            return ret
        fqdn = get_fqdn()
        if fqdn.success:
            return fqdn.value
        fqdn.error = f"{self.__path}serv_fqdn() >> missing \"get_fqdn()\" "\
                     f"// {info(self_krb_hostname=self.krb_hostname, func_get_fqdn_result=fqdn.value)}"
        self.raise_error(fqdn.error, True)

    @lazy_property
    def user_registration(self) -> bool:
        """
        # allow users who are not already in the FAB DB
        # Тип bool (True/False)
        # Параметр не обязательный
        # По умолчанию True
        """
        res = self.get_bool("USER_REGISTRATION", DEF_USER_REGISTRATION)
        if res.success:
            return res.value
        res.error = f"{self.__path}user_registration() >> parameter \"USER_REGISTRATION\" not found, "\
                    f"set default value True"
        self.log_debug(*res.error)
        return True

    @lazy_property
    def user_registration_role(self) -> str:
        """
        # this role will be given in addition to any <prefix>LDAP_ROLES_MAPPING
        # Тип str
        # Параметр не обязательный
        # По умолчанию "Public"
        """
        res = self.get_str("USER_REGISTRATION_ROLE", DEF_USER_REGISTRATION_ROLE)
        if res.success:
            return res.value
        res.error = f"{self.__path}user_registration_role() >> parameter \"USER_REGISTRATION_ROLE\" not found, "\
                    f"set default value \"Public\""
        self.log_debug(*res.error)
        return "Public"

    @lazy_property
    def roles_sync_at_login(self) -> bool:
        """
        # if we should replace ALL the user's roles each login, or only on registration
        # Тип bool (True/False)
        # Параметр не обязательный
        # По умолчанию True
        """
        res = self.get_bool("ROLES_SYNC_AT_LOGIN", DEF_ROLES_SYNC_AT_LOGIN)
        if res.success:
            return res.value
        res.error = f"{self.__path}roles_sync_at_login() >> parameter \"ROLES_SYNC_AT_LOGIN\" not found, "\
                    f"set default value True"
        self.log_debug(*res.error)
        return True

    @lazy_property
    def secret_path(self) -> str:
        """
        # путь к папке с секретами
        # Тип str
        # Параметр обязательный!
        """
        res = self.get_str("SECRET_PATH")
        if res.success:
            return res.value
        res.error = f"{self.__path}secret_path() >> missing get required parameter \"SECRET_PATH\", "\
                    f"something like string expected: \"\\path\\to\\secret\""
        self.raise_error(*res.error, True)

    #################################################################################################
    ###                                 Настройки Kerberos                                        ###
    #################################################################################################

    @lazy_property
    def krb_principal(self) -> Optional[str]:
        """
        Из файла `airflow.cfg`, секция `[kerberos]`, ключ `principal`.
        Параметр обязательный! Значения по умолчанию не имеет.
        """
        section, key = "kerberos", "principal"
        res = self.get_param_af(section, key)
        if res.success:
            return res.value
        res.error = f"{self.__path}krb_principal() >> missing get required parameter "\
                    f"// {info(section=section, key=key)}"
        self.raise_error(*res.error, True)

    @lazy_property
    def krb_principal_split(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Берёт из файла `airflow.cfg`, секция `[kerberos]`, ключ `principal` и парсит на 3 составляющие.
        Принципал вида `airflow/tklis-supd00016.dev.df.sbrf.ru@DEV.DF.SBRF.RU`
        парсится в кортеж (`airflow`, `tklis-supd00016.dev.df.sbrf.ru`, `DEV.DF.SBRF.RU`).
        """
        kp = self.krb_principal
        p = tuple(map(str.strip, kp.replace("/", "@").split("@"))) if type(kp) == str else tuple()
        if len(p) == 3 and len(p[0]) > 0 and len(p[1]) > 0 and len(p[2]) > 0:
            # self.log_debug(f"{self.__path}krb_principal_split() >> parsing Ok // {info(p)}")
            return p[0], p[1], p[2]
        msg = f"{self.__path}krb_principal_split() >> missing parsing, result: \"{p}\" (source string \"{kp}\"), "\
              f"something like string expected: \"airflow/servername.ipa.sbrf.ru@IPA.SBRF.RU\""
        self.raise_error(msg, True)
        return None, None, None

    @lazy_property
    def krb_service(self) -> Optional[str]:
        """Имя сервисного ТУЗа из принципала, файл `airflow.cfg`"""
        return self.krb_principal_split[0]

    @lazy_property
    def krb_hostname(self) -> Optional[str]:
        """Hostname из принципала, файл `airflow.cfg`"""
        return self.krb_principal_split[1]

    @lazy_property
    def krb_realm(self) -> Optional[str]:
        """Realm из принципала, файл `airflow.cfg`"""
        return self.krb_principal_split[2]

    @lazy_property
    def krb_ccache(self) -> str:
        """
        Файл `airflow.cfg`, секция `[kerberos]`, ключ `ccache`.
        Если там ничего нет, то формируем из того, что есть.
        """
        section, key = "kerberos", "ccache"
        res = self.get_param_af(section, key)
        if res.success:
            return res.value
        res.error = f"{self.__path}krb_ccache() >> missing get parameter section=\"[{section}]\" key=\"{key}\", "\
                    f"something like string expected: \"/tmp/airflow_spn_ccache\""
        res.error = f"{self.__path}krb_ccache() >> parameter recreated and set to \"/tmp/airflow_spn_ccache\""
        self.log_warning(*res.error)
        return "/tmp/airflow_spn_ccache"

    @lazy_property
    def krb_keytab(self) -> str:
        """
        Файл `airflow.cfg`, секция `[kerberos]`, ключ `keytab`.
        Если там ничего нет, то формируем из того, что есть.
        """
        section, key = "kerberos", "keytab"
        res = self.get_param_af(section, key)
        if res.success:
            return res.value
        res.error = f"{self.__path}krb_keytab() >> missing get parameter section=\"[{section}]\" key=\"{key}\", "\
                    f"something like string expected: \"/tmp/airflow_spn.keytab\""
        res.error = f"{self.__path}krb_keytab() >> parameter recreated and set to \"/tmp/airflow_spn.keytab\""
        self.log_warning(*res.error)
        return "/tmp/airflow_spn.keytab"

    @lazy_property
    def krb_kinit_path(self) -> str:
        """
        # Утилиты Kerberos (используются для генерации и чтения тикет-файлов).
        # Тип str
        # Параметры не обязательные
        # По умолчанию `kinit`
        """
        res = self.get_str("KRB_KINIT_PATH")
        if res.success:
            return res.value
        res = self.get_param_af("kerberos", "kinit_path", DEF_KRB_KINIT_PATH)
        if res.success:
            return res.value
        return "kinit"

    @lazy_property
    def krb_klist_path(self) -> str:
        """
        # Утилиты Kerberos (используются для генерации и чтения тикет-файлов).
        # Тип str
        # Параметры не обязательные
        # По умолчанию `klist`
        """
        res = self.get_str("KRB_KLIST_PATH")
        if res.success:
            return res.value
        res = self.get_param_af("kerberos", "klist_path", DEF_KRB_KLIST_PATH)
        if res.success:
            return res.value
        return "klist"

    @lazy_property
    def krb_ticket_lifetime(self) -> str:
        """
        # Лайфтайм тикета и renew (граница обновления) // Нужно знать настройки IPA/AD
        # Тип str
        # Параметры не обязательные
        # По умолчанию `8h` (8 часов) и `7d` (7 дней), соответственно
        SE_KRB_TICKET_LIFETIME: Optional[str] = "8h"
        SE_KRB_RENEW_UNTIL: Optional[str] = "7d"
        # * Обновлять тикет можно только на протяжении времени указанном в `SE_KRB_RENEW_UNTIL`
        """
        res = self.get_str("KRB_TICKET_LIFETIME")
        if res.success:
            return res.value
        res = self.get_param_af("kerberos", "ticket_lifetime", DEF_KRB_TICKET_LIFETIME)
        if res.success:
            return res.value
        return "8h"

    @lazy_property
    def krb_renew_until(self) -> str:
        """
        # Лайфтайм тикета и renew (граница обновления) // Нужно знать настройки IPA/AD
        # Тип str
        # Параметры не обязательные
        # По умолчанию `8h` (8 часов) и `7d` (7 дней), соответственно
        SE_KRB_TICKET_LIFETIME: Optional[str] = "8h"
        SE_KRB_RENEW_UNTIL: Optional[str] = "7d"
        # * Обновлять тикет можно только на протяжении времени указанном в `SE_KRB_RENEW_UNTIL`
        """
        res = self.get_str("KRB_RENEW_UNTIL")
        if res.success:
            return res.value
        res = self.get_param_af("kerberos", "renew_until", DEF_KRB_RENEW_UNTIL)
        if res.success:
            return res.value
        return "7d"

    @lazy_property
    def krb_tgs_list(self) -> list:
        """
        Список сервисов для автоматического получения TGS билетов
        """
        res = self.get_json("KRB_TGS_LIST")
        if res.success and type(res.value) == list:
            return res.value
        res.error = f"{self.__path}krb_tgs_list() >> missing get parameter \"KRB_TGS_LIST\", "\
                    f"something like list expected, example: " \
                    f"'[\"airflow/tklds-airfl0004.dev.df.sbrf.ru@DEV.DF.SBRF.RU\", " \
                    f"\"postgres/tvldd-airfl0001.delta.sbrf.ru@DEV.DF.SBRF.RU\"]'"
        self.log_warning(*res.error)
        return list()

    #################################################################################################
    ###                                 Настройки PAM                                             ###
    #################################################################################################

    @lazy_property
    def pam_policy(self) -> str:
        """
        # PAM политика, для PAM аутентификации пользователей, файл /etc/pam.d/airflow
        # Тип str
        # Параметр не обязательный
        # По умолчанию равен "airflow"
        """
        res = self.get_str("PAM_POLICY", DEF_PAM_POLICY)
        if res.success:
            return res.value
        res.error = f"{self.__path}pam_policy() >> parameter \"PAM_POLICY\" not found, set default value \"airflow\""
        self.log_debug(*res.error)
        return "airflow"

    #################################################################################################
    ###                                 Настройки LDAP                                            ###
    #################################################################################################

    @lazy_property
    def ldap_server(self) -> str:
        """
        # Адрес сервера LDAP
        # Тип str
        # Параметр обязательный!
        """
        res = self.get_str("LDAP_SERVER")
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_server() >> missing get required parameter \"LDAP_SERVER\", "\
                    f"something like string expected: \"ldap://ipa.sbrf.ru\""
        self.raise_error(*res.error, True)

    @lazy_property
    def ldap_search(self) -> str:
        """
        # the LDAP search base
        # Тип str
        # Параметр обязательный!
        """
        res = self.get_str("LDAP_SEARCH")
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_search() >> missing get required parameter \"LDAP_SEARCH\", "\
                    f"something like string expected: \"cn=users,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru\""
        self.raise_error(*res.error, True)

    @lazy_property
    def ldap_search_filter(self) -> str:
        """
        # Запрос к LDAP
        # Тип str
        # Параметр обязательный!
        """
        res = self.get_str("LDAP_SEARCH_FILTER")
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_search_filter() >> missing get required parameter \"LDAP_SEARCH_FILTER\", "\
                    f"something like string expected: "\
                    f"\"(memberOf=cn=g_bda_a_af_*,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru)\""
        self.raise_error(*res.error, True)

    @lazy_property
    def ldap_bind_user(self) -> str:
        """
        # the special bind username for search
        # Тип str
        # Параметр обязательный!
        """
        res = self.get_str("LDAP_BIND_USER")
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_bind_user() >> missing get required parameter \"LDAP_BIND_USER\", "\
                    f"something like string expected: "\
                    f"\"uid=u_ts_example_airflow,cn=users,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru\""
        self.raise_error(*res.error, True)

    @lazy_property
    def ldap_bind_user_name(self) -> str:
        """
        Параметр обязательный! Значения по умолчанию не имеет.
        """
        res = self.get_str("LDAP_BIND_USER_NAME")
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_bind_user_name() >> missing get required parameter \"LDAP_BIND_USER_NAME\", "\
                    f"something like string expected: \"u_ts_example_airflow\""
        self.raise_error(*res.error, True)

    @lazy_property
    def ldap_bind_user_realm(self) -> str:
        """
        Параметр обязательный! Значения по умолчанию не имеет.
        """
        res = self.get_str("LDAP_BIND_USER_REALM")
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_bind_user_realm() >> missing get required parameter "\
                    f"\"LDAP_BIND_USER_REALM\", something like string expected: \"IPA.SBRF.RU\""
        self.raise_error(*res.error, True)

    @lazy_property
    def ldap_bind_user_keytab(self) -> str:
        """
        Параметр обязательный! Значения по умолчанию не имеет.
        """
        res = self.get_str("LDAP_BIND_USER_KEYTAB")
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_bind_user_keytab() >> missing get required parameter "\
                    f"\"LDAP_BIND_USER_KEYTAB\", something like string expected: "\
                    f"\"/opt/airflow/secret/u_ts_example_airflow.keytab\""
        self.raise_error(*res.error, True)

    @lazy_property
    def ldap_roles_mapping(self) -> dict:
        """
        Маппинг групп LDAP к ролям Airflow
        """
        res1 = self.get_json("LDAP_ROLES_MAPPING")
        if res1.success and type(res1.value) == dict:
                return res1.value
        res2 = self.get_json("ROLES_MAPPING")
        if res2.success and type(res2.value) == dict:
            return res2.value
        res1.error = res2.error
        def_dct = {
            "cn=g_bda_a_af_admin,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["Admin"],
            "cn=g_bda_a_af_user,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["User"],
            "cn=g_bda_a_af_viewer,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["Viewer"],
            "cn=g_bda_a_af_op,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["Op"],
            "cn=g_bda_a_af_public,cn=groups,cn=accounts,dc=dev,dc=df,dc=sbrf,dc=ru": ["Public"],
        }
        res1.error = f"{self.__path}ldap_roles_mapping() >> missing get required parameter \"LDAP_ROLES_MAPPING\""\
                     f"(or \"ROLES_MAPPING\"), something like dict expected, example: \"{def_dct}\""
        self.raise_error(res1.error, True)

    @lazy_property
    def ldap_bind_user_path_krb5cc(self) -> str:
        """
        # Префикс имени файла для хранения `тикета`.
        # Тип str
        # Параметр не обязательный
        # По умолчанию "/tmp/airflow_spn_ccache"
        """
        res = self.get_str("LDAP_BIND_USER_PATH_KRB5CC", DEF_LDAP_BIND_USER_PATH_KRB5CC)
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_uid_field() >> parameter \"LDAP_BIND_USER_PATH_KRB5CC\" not found, "\
                    f"set default value \"/tmp/airflow_spn_ccache\""
        self.log_debug(*res.error)
        return "/tmp/airflow_spn_ccache"

    @lazy_property
    def ldap_use_tls(self) -> bool:
        """
        # TLS шифрование подключения к LDAP
        # Тип bool (True/False)
        # Параметр не обязательный
        # По умолчанию True (включено)
        """
        res = self.get_bool("LDAP_USE_TLS", DEF_LDAP_USE_TLS)
        if res.success:
            if res.value is False:
                self.log_warning("Disabled use TLS for LDAP connection!!!")
            return res.value
        return True

    @lazy_property
    def ldap_allow_self_signed(self) -> bool:
        """
        Allow LDAP connection to use self-signed certificates (LDAPS) // default True
        Сертификаты: разрешить самоподписанные
        """
        res = self.get_bool("LDAP_ALLOW_SELF_SIGNED", DEF_LDAP_ALLOW_SELF_SIGNED)
        if res.success:
            if res.value is True:
                self.log_info("Allow LDAP connection to use self-signed certificates")
            else:
                self.log_info("Disallow LDAP connection to use self-signed certificates")
            return res.value
        return True

    @lazy_property
    def ldap_tls_demand(self) -> bool:
        """
        Demands TLS peer certificate checking for LDAP connection // default False
        Сертификаты: Требовать проверки однорангового сертификата TLS
        """
        res = self.get_bool("LDAP_TLS_DEMAND", DEF_LDAP_TLS_DEMAND)
        if res.success:
            if res.value is True:
                self.log_warning("Demands TLS peer certificate checking for LDAP connection!")
            return res.value
        return False

    @lazy_property
    def ldap_firstname_field(self) -> str:
        """
        # Тип str
        # Параметр не обязательный
        # По умолчанию "givenName"
        """
        res = self.get_str("LDAP_FIRSTNAME_FIELD", DEF_LDAP_FIRSTNAME_FIELD)
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_firstname_field() >> parameter \"LDAP_FIRSTNAME_FIELD\" not found, "\
                    f"set default value \"givenName\""
        self.log_debug(*res.error)
        return "givenName"

    @lazy_property
    def ldap_lastname_field(self) -> str:
        """
        # Тип str
        # Параметр не обязательный
        # По умолчанию "sn".
        """
        res = self.get_str("LDAP_LASTNAME_FIELD", DEF_LDAP_LASTNAME_FIELD)
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_lastname_field() >> parameter \"LDAP_LASTNAME_FIELD\" not found, "\
                    f"set default value \"sn\""
        self.log_debug(*res.error)
        return "sn"

    @lazy_property
    def ldap_email_field(self) -> str:
        """
        # Тип str
        # Параметр не обязательный
        # По умолчанию "mail"
        """
        res = self.get_str("LDAP_EMAIL_FIELD", DEF_LDAP_EMAIL_FIELD)
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_email_field() >> parameter \"LDAP_EMAIL_FIELD\" not found, "\
                    f"set default value \"mail\""
        self.log_debug(*res.error)
        return "mail"

    @lazy_property
    def ldap_uid_field(self) -> str:
        """
        # search configs
        # Тип str
        # Параметр не обязательный
        # По умолчанию "uid"
        """
        res = self.get_str("LDAP_UID_FIELD", DEF_LDAP_UID_FIELD)
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_uid_field() >> parameter \"LDAP_UID_FIELD\" not found, "\
                    f"set default value \"uid\""
        self.log_debug(*res.error)
        return "uid"

    @lazy_property
    def ldap_group_field(self) -> str:
        """
        # the LDAP user attribute which has their role DNs
        # Тип str
        # Параметр не обязательный
        # По умолчанию "memberOf"
        """
        res = self.get_str("LDAP_GROUP_FIELD", DEF_LDAP_GROUP_FIELD)
        if res.success:
            return res.value
        res.error = f"{self.__path}ldap_group_field() >> parameter \"LDAP_GROUP_FIELD\" not found, "\
                    f"set default value \"memberOf\""
        self.log_debug(*res.error)
        return "memberOf"

    @lazy_property
    def ldap_tls_cacertdir(self) -> Optional[str]:
        """Сертификаты: """
        res = self.get_str("LDAP_TLS_CACERTDIR")
        if res.success:
            return res.value

    @lazy_property
    def ldap_tls_cacertfile(self) -> Optional[str]:
        """Сертификаты: """
        res = self.get_str("LDAP_TLS_CACERTFILE")
        if res.success:
            return res.value

    @lazy_property
    def ldap_tls_certfile(self) -> Optional[str]:
        """Сертификаты: """
        res = self.get_str("LDAP_TLS_CERTFILE")
        if res.success:
            return res.value

    @lazy_property
    def ldap_tls_keyfile(self) -> Optional[str]:
        """Сертификаты: """
        res = self.get_str("LDAP_TLS_KEYFILE")
        if res.success:
            return res.value

    #################################################################################################
    ###                     Настройки относящиеся только к процессу `se_ldap`                     ###
    #################################################################################################

    @lazy_property
    def ldap_refresh_cache_on_start(self) -> bool:
        """
        Обновлять кэш LDAP при запуске процесса (первая итерация главного цикла)?
        Тип bool (True/False)
        Параметр не обязательный
        По умолчанию False (отключено)
        """
        res = self.get_bool("LDAP_REFRESH_CACHE_ON_START", DEF_LDAP_REFRESH_CACHE_ON_START)
        if res.success:
            if res.value is True:
                self.log_warning("Enabled refresh LDAP cache on start process!!!")
            return res.value
        return False

    @lazy_property
    def ldap_process_timeout(self) -> int:
        """
        Задержка(засыпание) процесса в секундах
        Тип int, целое число в диапазоне от 60 до 1200
        Параметр не обязательный
        По умолчанию 600
        """
        _res = self.get_int("LDAP_PROCESS_TIMEOUT", DEF_LDAP_PROCESS_TIMEOUT)
        if _res.success:
            res = check_range_int(_res.value, 60, 1200, 600)
            if res.success:
                return res.value
            else:
                res.error = f"{self.__path}ldap_process_timeout() >> parameter \"LDAP_PROCESS_TIMEOUT\" "\
                            f"must be range 60...1200 -> value automatically set to equal 600 (seconds)"
                self.log_warning(*res.error)
        else:
            _res.error = f"{self.__path}ldap_process_timeout() >> parameter \"LDAP_PROCESS_TIMEOUT\" not found, "\
                         f"set default value 600 (seconds)"
            self.log_debug(*_res.error)
        return 600

    @lazy_property
    def ldap_process_retry(self) -> Optional[int]:
        """
        Количество повторений главного цикла процесса
        Тип int или None, целое положительное число в диапазоне от 1 до плюс бесконечность или None
        Параметр не обязательный
        По умолчанию None
        """
        _res = self.get_int("LDAP_PROCESS_RETRY", DEF_LDAP_PROCESS_RETRY)
        if _res.success:
            res = check_range_int(value=_res.value, min_value=1)
            if res.success:
                return res.value
            self.log_warning(f"{self.__path}ldap_process_retry() >> parameter \"LDAP_PROCESS_RETRY\" must be range "
                             f"1...<plus_infinity> or None -> value automatically set to equal None")
        else:
            self.log_debug(f"{self.__path}ldap_process_retry() >> parameter \"LDAP_PROCESS_RETRY\" not found, "
                           f"value automatically set to equal None")
        return None

    @lazy_property
    def ldap_time_start(self) -> time:
        """
        Время предпочтительного старта запроса в LDAP
        Тип time, задавать без использования day, только hour, minute, second // в пределах суток
        Параметр не обязательный
        По умолчанию time(hour=2) // 2 часа ночи
        """
        res = self.get_time("LDAP_TIME_START", DEF_LDAP_TIME_START)
        if res.success:
            if time(hour=0) <= res.value <= time(hour=23, minute=59, second=59):
                return res.value
            self.log_warning(f"{self.__path}ldap_time_start() >> parameter \"LDAP_TIME_START\" must be range "
                             f"time(hours=0)...time(hour=23, minute=59, second=59) -> "
                             f"value automatically set to equal time(hour=2)")
        else:
            self.log_debug(f"{self.__path}ldap_time_start() >> parameter \"LDAP_TIME_START\" not found, "
                           f"value automatically set to equal time(hour=2)")
        return time(hour=2)

    @lazy_property
    def ldap_timedelta_up_and_down(self) -> Tuple[timedelta, timedelta]:
        """Промежуточная функция. Возвращает верхнюю и нижнюю границу"""
        up = self.get_timedelta("LDAP_TIMEDELTA_UP", DEF_LDAP_TIMEDELTA_UP)
        down = self.get_timedelta("LDAP_TIMEDELTA_DOWN", DEF_LDAP_TIMEDELTA_DOWN)
        if up.success:
            if not timedelta(hours=12) <= up.value <= timedelta(days=3):
                up.value = timedelta(days=1, hours=2)
                self.log_warning(f"{self.__path}ldap_timedelta_up_and_down() >> parameter \"LDAP_TIMEDELTA_UP\" must "
                                 f"be range timedelta(hours=12)...timedelta(days=3) -> value "
                                 f"automatically set to equal timedelta(days=1, hours=2)")
        else:
            up.error = f"{self.__path}ldap_timedelta_up_and_down() >> parameter \"LDAP_TIMEDELTA_UP\" not found, "\
                       f"value automatically set to equal timedelta(days=1, hours=2)"
            self.log_debug(up.error)
            up.value = timedelta(days=1, hours=2)
        if down.success:
            if not timedelta(hours=4) <= down.value <= up.value:
                down.value = timedelta(hours=6)
                self.log_warning(f"{self.__path}ldap_timedelta_up_and_down() >> parameter \"LDAP_TIMEDELTA_DOWN\" "
                                 f"must be range timedelta(hours=4)...LDAP_TIMEDELTA_UP -> value "
                                 f"automatically set to equal timedelta(hours=6)")
        else:
            down.error = f"{self.__path}ldap_timedelta_up_and_down() >> parameter \"LDAP_TIMEDELTA_DOWN\" "\
                         f"not found, value automatically set to equal timedelta(hours=6)"
            self.log_debug(down.error)
            down.value = timedelta(hours=6)
        return up.value, down.value

    @lazy_property
    def ldap_timedelta_up(self) -> timedelta:
        """
        # Верхняя граница
        # Тип timedelta, диапазон от timedelta(hours=12) до timedelta(days=3)
        # Параметр не обязательный
        # По умолчанию timedelta(days=1, hours=2) // 1 день 2 часа, рекомендуемое значение
        """
        return self.ldap_timedelta_up_and_down[0]

    @lazy_property
    def ldap_timedelta_down(self) -> timedelta:
        """
        # Нижняя граница
        # Тип timedelta, диапазон от timedelta(hours=4) до значения параметра <prefix>LDAP_TIMEDELTA_UP
        # * Проще говоря, нижняя граница не должна быть выше верхней границы
        # Параметр не обязательный
        # По умолчанию timedelta(hours=6) // 6 часов, рекомендуемое значение
        """
        return self.ldap_timedelta_up_and_down[1]

    @lazy_property
    def ldap_delete_caches_days_ago(self) -> timedelta:
        """
        # Удаление кэшей LDAP старше этой временной дельты
        # Тип timedelta, диапазон от timedelta(days=3) до плюс бесконечности, например timedelta(days=1000000)
        # Параметр не обязательный
        # По умолчанию timedelta(days=30) // рекомендуемое значение
        """
        res = self.get_timedelta("LDAP_DELETE_CACHES_DAYS_AGO", DEF_LDAP_DELETE_CACHES_DAYS_AGO)
        if res.success:
            if res.value >= timedelta(days=3):
                return res.value
            self.log_warning(f"{self.__path}ldap_delete_caches_days_ago() >> parameter "
                             f"\"LDAP_DELETE_CACHES_DAYS_AGO\" must be range timedelta(days=3)...<plus_infinity> -> "
                             f"value automatically set to equal timedelta(days=30)")
        else:
            res.error = f"{self.__path}ldap_delete_caches_days_ago() >> parameter \"LDAP_DELETE_CACHES_DAYS_AGO\" "\
                        f"not found, value automatically set to equal timedelta(days=30)"
            self.log_debug(*res.error)
        return timedelta(days=30)

    #################################################################################################
    ###                                 Настройки Kafka                                           ###
    #################################################################################################

    @lazy_property
    def kafka_app_id(self) -> str:
        """
        # app_id для Кафки, id приложения
        # Тип str
        # Параметр обязательный!
        """
        res = self.get_str("KAFKA_APP_ID")
        if res.success:
            return res.value
        res.error = f"{self.__path}kafka_app_id() >> missing get required parameter \"KAFKA_APP_ID\", "\
                    f"something like string expected: \"airflow_se-ser-4c88-8430-4944bfb31b86\""
        self.raise_error(*res.error, True)

    @lazy_property
    def kafka_topic(self) -> str:
        """
        Топик Кафки.
        Параметр обязательный! Должен быть строкой и не пустой!
        """
        res = self.get_str("KAFKA_TOPIC")
        if res.success:
            return res.value
        res.error = f"{self.__path}kafka_topic() >> missing get required parameter \"KAFKA_TOPIC\", "\
                    f"something like string expected: \"log-kafka-01\""
        self.raise_error(*res.error, True)

    @lazy_property
    def kafka_producer_settings(self) -> dict:
        """
        # Настройки продюсера для подключения к Кафке.
        # Должен быть словарём (dict) и не пустым!
        # Параметр обязательный!
        """
        res = self.get_json("KAFKA_PRODUCER_SETTINGS")
        if res.success and type(res.value) == dict:
                return res.value
        def_dct = {
            "bootstrap.servers": "tkliq-log000007.cm.dev.df.sbrf.ru:9093,"
                                 "tkliq-log000008.cm.dev.df.sbrf.ru:9093,"
                                 "tkliq-log000009.cm.dev.df.sbrf.ru:9093,"
                                 "tkliq-log000010.cm.dev.df.sbrf.ru:9093,"
                                 "tkliq-log000011.cm.dev.df.sbrf.ru:9093,"
                                 "tkliq-log000012.cm.dev.df.sbrf.ru:9093",
        }
        res.error = f"{self.__path}kafka_producer_settings() >> missing get required parameter "\
                    f"\"KAFKA_PRODUCER_SETTINGS\", something like dict expected, example: \"{def_dct}\""
        self.raise_error(*res.error, True)

    @lazy_property
    def kafka_process_timeout(self) -> int:
        """
        # Таймаут процесса (на сколько секунд он будет "засыпать")
        # Тип int
        # Параметр не обязательный
        # По умолчанию 300
        """
        _res = self.get_int("KAFKA_PROCESS_TIMEOUT", DEF_KAFKA_PROCESS_TIMEOUT)
        if _res.success:
            res = check_range_int(_res.value, min_value=0, max_value=1200)
            if res.success:
                return res.value
            else:
                self.log_warning(f"{self.__path}kafka_process_timeout() >> parameter \"KAFKA_PROCESS_TIMEOUT\" "
                                 f"must be range 0...1200 -> value automatically set to equal 300 (seconds)")
        else:
            _res.error = f"{self.__path}kafka_process_timeout() >> parameter \"KAFKA_PROCESS_TIMEOUT\" not found, "\
                         f"set default value 300 (seconds)"
            self.log_debug(*_res.error)
        return 300

    @lazy_property
    def kafka_process_retry(self) -> Optional[int]:
        """
        # Количество повторений главного цикла, None - бесконечный цикл
        # Тип int
        # Параметр не обязательный
        # По умолчанию None
        """
        _res = self.get_int("KAFKA_PROCESS_RETRY", DEF_KAFKA_PROCESS_RETRY)
        if _res.success:
            res = check_range_int(_res.value, min_value=1)
            if res.success:
                return res.value
            else:
                self.log_warning(f"{self.__path}kafka_process_retry() >> parameter \"KAFKA_PROCESS_RETRY\" "
                                 f"must be range 1...<plus_infinity> or None -> value automatically set to None")
        else:
            _res.error = f"{self.__path}kafka_process_retry() >> parameter \"KAFKA_PROCESS_RETRY\" not found, "\
                         f"set default value as None"
            self.log_debug(*_res.error)
        return None

    @lazy_property
    def kafka_page_size(self) -> int:
        """
        # Максимальное количество сообщений, которые за раз отправлять в Кафку
        # Тип int, диапазон от 100 до 100000
        # Параметр не обязательный
        # По умолчанию 1000
        """
        _res = self.get_int("KAFKA_PAGE_SIZE", DEF_KAFKA_PAGE_SIZE)
        if _res.success:
            res = check_range_int(_res.value, min_value=100, max_value=100000, def_value=DEF_KAFKA_PAGE_SIZE)
            if res.success:
                return res.value
            else:
                self.log_warning(f"{self.__path}kafka_page_size() >> parameter \"KAFKA_PAGE_SIZE\" "
                                 f"must be range 100...100000 -> value automatically set to equal 1000")
        else:
            _res.error = f"{self.__path}kafka_page_size() >> parameter \"KAFKA_PAGE_SIZE\" not found, "\
                         f"set default value as 1000"
            self.log_debug(*_res.error)
        return 1000

    #################################################################################################
    ###                                 Настройки TicketMan                                       ###
    #################################################################################################

    @lazy_property
    def ticketman_process_timeout(self) -> int:
        """
        # Задержка(засыпание) процесса в секундах
        # Тип int, целое число в диапазоне от 3600 (1 час) до 28800 (8 часов) включительно
        # Параметр не обязательный
        # По умолчанию 25200 (7 часов)
        """
        _res = self.get_int("TICKETMAN_PROCESS_TIMEOUT", DEF_TICKETMAN_PROCESS_TIMEOUT)
        if _res.success:
            res = check_range_int(_res.value, 3600, 28800, 25200)
            if res.success:
                return res.value
            else:
                res.error = f"{self.__path}ticketman_process_timeout() >> parameter \"TICKETMAN_PROCESS_TIMEOUT\" "\
                            f"must be range 3600...28800 -> value automatically set to equal 25200 seconds (7 hours)"
                self.log_warning(*res.error)
        else:
            _res.error = f"{self.__path}ticketman_process_timeout() >> parameter \"LDAP_PROCESS_TIMEOUT\" not found, "\
                         f"set default value 25200 seconds (7 hours)"
            self.log_debug(*_res.error)
        return 25200

    @lazy_property
    def ticketman_process_retry(self) -> Optional[int]:
        """
        # Количество повторений главного цикла процесса
        # Тип int или None, целое положительное число в диапазоне от 1 до плюс бесконечность или None
        # Параметр не обязательный
        # По умолчанию None
        """
        _res = self.get_int("TICKETMAN_PROCESS_RETRY", DEF_TICKETMAN_PROCESS_RETRY)
        if _res.success:
            res = check_range_int(value=_res.value, min_value=1)
            if res.success:
                return res.value
            self.log_warning(f"{self.__path}ticketman_process_retry() >> parameter \"TICKETMAN_PROCESS_RETRY\" "
                             f"must be range 1...<plus_infinity> or None -> value automatically set to equal None")
        else:
            self.log_debug(f"{self.__path}ticketman_process_retry() >> parameter \"TICKETMAN_PROCESS_RETRY\" "
                           f"not found -> value automatically set to equal None")
        return None

    @lazy_property
    def ticketman_scan_dirs(self) -> Optional[Set[str]]:
        """
        # Список директорий для просмотра и обновления тикетов
        # Тип list | tuple | set | str | None - (либо итерируемый объект (список, кортеж, сет), либо строка, либо None)
        # Параметр не обязательный
        # По умолчанию None
        """
        _res, __res, res = self.get_json("TICKETMAN_SCAN_DIRS"), Result.Ok(), Result.Ok()
        if _res.success:
            if type(_res.value) == str:
                res.value = {_res.value, }
            elif isinstance(_res.value, Iterable):
                res.value = set(map(str.strip, _res.value))
            else:
                res.error = f"{self.__path}ticketman_scan_dirs() >> Invalid type value (json) "\
                            f"// {info(_res.value)}"
        else:
            res.error = _res.error
            __res = self.get_str("TICKETMAN_SCAN_DIRS")
            if __res.success:
                if type(__res.value) == str:
                    res.value = {__res.value, }
                elif isinstance(__res.value, Iterable):
                    res.value = set(map(str.strip, __res.value))
                else:
                    res.error = f"{self.__path}ticketman_scan_dirs() >> Invalid type value (str) "\
                                f"// {info(__res.value)}"
            else:
                res.error = __res.error
        if res.success:
            return res.value
        else:
            if type(DEF_TICKETMAN_SCAN_DIRS) == str:
                res.value = {DEF_TICKETMAN_SCAN_DIRS.strip(), }
            elif isinstance(DEF_TICKETMAN_SCAN_DIRS, Iterable):
                res.value = set(map(str.strip, DEF_TICKETMAN_SCAN_DIRS))
            elif DEF_TICKETMAN_SCAN_DIRS is None:
                res.value = None
            else:
                res.error = f"{self.__path}ticketman_scan_dirs() >> Invalid type value "\
                            f"\"DEF_TICKETMAN_SCAN_DIRS\" // {info(__res.value)}"
        if res.success:
            self.log_debug(f"{self.__path}ticketman_scan_dirs() >> parameter \"TICKETMAN_SCAN_DIRS\" "
                           f"set equal to {res.value}")
            return res.value
        else:
            self.log_debug(*res.error)
        res.value = None
        self.log_debug(f"{self.__path}ticketman_scan_dirs() >> parameter \"TICKETMAN_SCAN_DIRS\" "
                       f"set equal to {res.value}")
        return res.value

    #################################################################################################
    ###                                   Hidden parameters                                       ###
    #################################################################################################

    @lazy_property
    def block_change_policy(self) -> bool:
        """
        # Блокировка изменений ролей и пользователей в WebUI
        # Тип bool (True/False)
        # Параметр не обязательный
        # По умолчанию True (блокировать)
        """
        res = self.get_bool("BLOCK_CHANGE_POLICY", DEF_BLOCK_CHANGE_POLICY)
        if res.success:
            if res.value is False:
                self.log_warning("Disabled interface of blocking change Roles and Users!!!")
            return res.value
        return True

    @lazy_property
    def pam_is_auth(self) -> bool:
        """
        # Включить/отключить PAM аутентификацию
        # Тип bool (True/False)
        # Параметр не обязательный
        # По умолчанию False (отключена)
        """
        res = self.get_bool("PAM_IS_AUTH", DEF_PAM_IS_AUTH)
        if res.success and isinstance(res.value, bool):
            return res.value
        return False

    @lazy_property
    def ldap_clear_caches(self) -> bool:
        """
        # Полная очистка кэшей LDAP в таблице при рестарте процесса
        # Параметр не обязательный
        # По умолчанию False (очистка отключена)
        """
        res = self.get_bool("LDAP_CLEAR_CACHES", DEF_LDAP_CLEAR_CACHES)
        if res.success and isinstance(res.value, bool):
            if res.value is True:
                self.log_warning("Enable clear LDAP caches on start process!!!")
            return res.value
        return False

    @lazy_property
    def ldap_is_auth(self) -> bool:
        """
        # Отключение LDAP авторизации
        # Тип bool (True/False)
        # Параметр не обязательный
        # По умолчанию True (включена)
        """
        res = self.get_bool("LDAP_IS_AUTH", DEF_LDAP_IS_AUTH)
        if res.success:
            if res.value is False:
                self.log_warning("Disabled LDAP authorisation!!!")
            return res.value
        return True

    @lazy_property
    def ldap_is_auth_def_roles(self) -> Set[str]:
        """
        Имя роли как строка или список(кортеж, set) ролей, которые будут назначены пользователю,
        если LDAP авторизация отключена (параметр <*>LDAP_IS_AUTH = False).
        # Список ролей
        # * работает только при отключённой LDAP авторизации
        # Тип - любой список из строк (list, tuple, set), примеры: ["Admin", ] или ("Admin", ) или {"Admin", }
        # Параметр не обязательный
        # По умолчанию {"Admin", }
        """
        _res, __res, res = self.get_json("LDAP_IS_AUTH_DEF_ROLES"), Result.Ok(), Result.Ok()
        if _res.success:
            if type(_res.value) == str:
                res.value = {_res.value, }
            elif isinstance(_res.value, Iterable):
                res.value = set(map(str.strip, _res.value))
            else:
                res.error = f"{self.__path}ldap_is_auth_def_roles() >> Invalid type value (json) "\
                            f"// {info(_res.value)}"
        else:
            res.error = _res.error
            __res = self.get_str("LDAP_IS_AUTH_DEF_ROLES")
            if __res.success:
                if type(__res.value) == str:
                    res.value = {__res.value, }
                elif isinstance(__res.value, Iterable):
                    res.value = set(map(str.strip, __res.value))
                else:
                    res.error = f"{self.__path}ldap_is_auth_def_roles() >> Invalid type value (str) "\
                                f"// {info(__res.value)}"
            else:
                res.error = __res.error
        if res.success:
            return res.value
        else:
            if type(DEF_LDAP_IS_AUTH_DEF_ROLES) == str:
                res.value = {DEF_LDAP_IS_AUTH_DEF_ROLES.strip(), }
            elif isinstance(DEF_LDAP_IS_AUTH_DEF_ROLES, Iterable):
                res.value = set(map(str.strip, DEF_LDAP_IS_AUTH_DEF_ROLES))
            else:
                res.error = f"{self.__path}ldap_is_auth_def_roles() >> Invalid type value "\
                            f"\"DEF_LDAP_IS_AUTH_DEF_ROLES\" // {info(__res.value)}"
        if res.success:
            self.log_debug(f"{self.__path}ldap_is_auth_def_roles() >> parameter \"LDAP_IS_AUTH_DEF_ROLES\" "
                           f"set equal to {res.value}")
            return res.value
        else:
            self.log_debug(*res.error)
        res.value = {"Admin", }
        self.log_debug(f"{self.__path}ldap_is_auth_def_roles() >> parameter \"LDAP_IS_AUTH_DEF_ROLES\" "
                       f"set equal to {res.value}")
        return res.value

