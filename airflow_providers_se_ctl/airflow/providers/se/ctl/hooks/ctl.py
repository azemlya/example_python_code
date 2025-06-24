from __future__ import annotations

import asyncio
import json
import os
from os import environ as env
from typing import TYPE_CHECKING, Any, Callable, List, Dict, Optional, Union

import aiohttp
import requests
import tenacity
from aiohttp import ClientResponseError
from airflow_se.settings import Settings, get_settings
from asgiref.sync import sync_to_async
from requests.auth import HTTPBasicAuth
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse
    from airflow.utils.context import Context

from ..exceptions import CtlMethodNotFound

from airflow_se.config import get_config_value
from airflow.security.kerberos import renew_from_kt
from airflow_se.secman import auth_secman, get_secman_data
from airflow_se.commons import TICKET_PREFIX, SECMAN_KEY_FOR_TGT
import kerberos
from requests_kerberos import HTTPKerberosAuth, REQUIRED, OPTIONAL
from airflow_se.crypt import decrypt
from airflow_se.utils import DataPaths, run_kinit, run_klist, get_all_envs, get_env, add_env, pop_env
from flask import Response, _request_ctx_stack as stack, g, make_response, request  # type: ignore
import krb5
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

class CtlHookSE(BaseHook):
    """Interact with HTTP servers.

    :param method: the API method to be called
    :param ctl_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    :param auth_args: extra arguments used to initialize the auth_type if different than default HTTPBasicAuth
    """

    conn_name_attr = "ctl_conn_id"
    default_conn_name = "ctl_default"
    conn_type = "ctl_se"
    hook_name = "CTL SE"

    def __init__(
        self,
        method: str = "POST",
        ctl_conn_id: str = default_conn_name,
        auth_type: Any = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        context: Optional[Context] = None,
        dag_executor_or_owner: str = 'dag_executor_or_owner',
        manual_parameter_setting: int = 0,
        krb_url: str = None
    ) -> None:
        super().__init__()
        self.ctl_conn_id = ctl_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self._auth_type: Any = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.keep_alive_idle = tcp_keep_alive_idle
        self.keep_alive_count = tcp_keep_alive_count
        self.keep_alive_interval = tcp_keep_alive_interval
        self.context = context
        self.log.info(f"{self.context.get('dag_run').conf=}")
        self.dag_executor_or_owner = dag_executor_or_owner,
        self.manual_parameter_setting = manual_parameter_setting
        self.krb_url = krb_url

    @property
    def auth_type(self):
        return self._auth_type or HTTPBasicAuth

    @auth_type.setter
    def auth_type(self, v):
        self._auth_type = v

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(self, headers: dict[Any, Any] | None = None) -> requests.Session:
        """Create a Requests HTTP session.

        :param headers: additional headers to be passed through as a dictionary
        """
        session = requests.Session()

        if self.ctl_conn_id:
            conn = self.get_connection(self.ctl_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            if conn.login:
                session.auth = self.auth_type(conn.login, conn.password)
            elif self._auth_type:
                session.auth = self.auth_type()
            if conn.extra:
                try:
                    session.headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            session.headers.update(headers)

        return session

    def run(
        self,
        endpoint: str | None = None,
        data: Optional[Dict[str, Any]] = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        **request_kwargs: Any,
    ) -> Any:
        r"""Perform the request.

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :param request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        extra_options = extra_options or dict()

        sslrootcert = env.get('SE_PROVIDER_CTL_SSLROOTCERT')
        self.log.info(f"240325_1824-00, type(sslrootcert): {type(sslrootcert)}, SE_PROVIDER_CTL_SSLROOTCERT: {sslrootcert=}")
        self.log.info(f"240325_1824-01 , содержимое sslrootcert ")
        with open(sslrootcert, "rt") as f:
            self.log.info(f.read())
        sslcert = env.get('SE_PROVIDER_CTL_SSLCERT')
        self.log.info(f"240325_1824-02, type(sslcert): {type(sslcert)}, SE_PROVIDER_CTL_SSLCERT: {sslcert=}")
        self.log.info(f"240325_1824-03 , содержимое sslcert")
        with open(sslcert, "rt") as f:
            self.log.info(f.read())
        sslkey = env.get('SE_PROVIDER_CTL_SSLKEY')
        self.log.info(f"240325_1824-04, type(sslkey): {type(sslkey)}, SE_PROVIDER_CTL_SSLKEY: {sslkey=}")
        self.log.info(f"240325_1824-05 , содержимое sslkey")
        with open(sslkey, "rt") as f:
            self.log.info(f.read())
        self.log.info("240325_1824_end, содержимое ----------------------------------------------------------------------------------------  сертов")
        conn = self.get_connection(self.ctl_conn_id)
        _data = data or dict()
        endpoint_v4 = f"/v4/api/loading/{data.get('loading_id')}/entity/{data.get('entity_id')}/stat/{data.get('stat_id')}/statval"
        endpoint_v1 = '/v1/api/statval/m'
        is_v4_api: bool = False
        verify: str | bool = False
        if sslkey and sslrootcert and sslcert and (conn.host.strip().lower().startswith("https://") or conn.schema.strip().lower() == 'https'):
            is_v4_api = True
            verify = True
        if is_v4_api is True:     #   1==1: # временно для теста идём всегда по пути V4
            if 'cert' not in extra_options.keys():
                extra_options['cert'] = (sslcert, sslkey, sslrootcert)
                #extra_options['verify'] = False
            endpoint = endpoint_v4
            self.log.info(f"240314_1241, {endpoint=}")
            # =========================== аутентификация соединения =================================
            self.log.info(f"20240306_0931, аутентификация соединения по сертификатам, если is_v4_api либо аутентификация по токену (V1, {self._auth_type=} or HTTPBasicAuth")
            # ================================== идём по токену керберос, взяв сначала тикет керберос из секмана =================================
            self.log.info(f"240314_1242, {self.manual_parameter_setting=}")
            # ========================================== при этом всё же используя цепочку сертификатов ==========================================
            self.log.info(f"240307_1211-00, в оригинале кортеж: {extra_options['cert']=}")
            if len(str(self.manual_parameter_setting)) > 2 and int(str(self.manual_parameter_setting)[2]) == 0:
                extra_options['cert'] = (sslrootcert,)
                self.log.info(f"240307_1211-02, используем только цепочку сертификатов в кортеже (sslrootcert,)  {extra_options['cert']=}")
            elif len(str(self.manual_parameter_setting)) > 2 and int(str(self.manual_parameter_setting)[2]) == 1:
                extra_options['cert'] = (sslrootcert, sslkey)
                self.log.info(f"240307_1211-04, используем цепочку сертификатов + ключ (sslrootcert, sslkey) {extra_options['cert']=}")
            elif len(str(self.manual_parameter_setting)) > 2 and int(str(self.manual_parameter_setting)[2]) == 2:
                extra_options['cert'] = (sslcert, sslkey)
                self.log.info(f"240307_1211-06, используем клиентский/публичный сертификат + ключ (sslcert, sslkey)   {extra_options['cert']=}")
            else:
                extra_options['cert'] = (sslrootcert, sslcert, sslkey )
                self.log.info(f"240307_1211-08, используем все - цепочку+паблик+ключ (sslrootcert, sslcert, sslkey ) {extra_options['cert']=}")
            # ========================================== при этом всё же используя цепочку сертификатов ==========================================
            _data = {k: v for k, v in data.items() if k in ['avalue', 'profile_name']}  #data.get('avalue')
            self.log.info(f"240314_1911 \n {self.dag_executor_or_owner=} \n {type(self.dag_executor_or_owner)=}")
            self.dag_executor_or_owner = self.dag_executor_or_owner.replace('(', '').replace(')', '').replace(',','').replace("'", '') if isinstance(self.dag_executor_or_owner, str) else self.dag_executor_or_owner[0]
            self.log.info(f"240314_1912 \n {self.dag_executor_or_owner=} \n {type(self.dag_executor_or_owner)=}")
            k = f"{TICKET_PREFIX}{self.dag_executor_or_owner}"
            self.log.info(f"240306_0932, {is_v4_api=}, аутентификация по токену (V1, {self._auth_type=} or HTTPBasicAuth), взяв сначала тикет из секмана {k}")
            sm_tgt = get_secman_data(SECMAN_KEY_FOR_TGT, auth_secman())
            if sm_tgt is None:
                sm_tgt: Dict[str, str] = dict()
            v = sm_tgt.get(k)
            self.log.info(f"240307_1208, {v=}")
            if v:
                dp: Optional[DataPaths] = DataPaths(
                    base_path=get_config_value("SECRET_PATH") or "/tmp",
                    name="ProviderCTLSE",
                )
                krb_ticket_file = dp.get(k)
                self.log.info(f"240307_1209, {krb_ticket_file=}")
                with open(krb_ticket_file, "wb") as f:
                    f.write(decrypt(v))
                os.chmod(krb_ticket_file, 0o600)
                env["KRB5CCNAME"] = krb_ticket_file  # кэшируем в кэше учетных данных Kerberos
                self.log.info(f"240307_1210, For connect to CTL use ticket from SecMan '{k}' and cash it into {env['KRB5CCNAME']=}")
                # из тикета получаем токен
                # __, krb_context = kerberos.authGSSClientInit("HTTP@krbhost.example.com")
                # t = kerberos.authGSSClientStep(krb_context, "") # t = 0
                # negotiate_details = kerberos.authGSSClientResponse(krb_context)
                # headers = {"Authorization": "Negotiate " + negotiate_details}
                # r = requests.get("https://krbhost.example.com/krb/", headers=headers)
                # t = r.status_code # t = 200 , r.json = ["example_data"]
                            # krb_session = self.get_conn(headers)  #Session()     <--      from requests import get, Session
                            # self.log.info(f"240307_1212, {krb_session=}")
                            # # Получаем тикет
                            # krb_ticket = krb_session.get_ticket(url)
                            # self.log.info(f"240307_1213, {krb_ticket=}")  # response.token
                            # # Получаем токен
                            # krb_token = krb_ticket.token #ticket.token
                headers["WWW-Authenticate"] = "Negotiate"
                headers["Authorization"] = "Negotiate"
                self.log.info(f"240307_1212-01, {headers=}")
                conf: Settings = get_settings()
                p1, p2, p3 = conf.krb_principal_split
                self.log.info(f"240307_1212-02, settings --> {conf=} \n {conf.krb_principal=} \n {p1=} \n {p2=} \n {p3=}")
                krb_session = self.get_conn(headers)
                self.log.info(f"240307_1213-01, {krb_session=}\n {dir(krb_session)=}\n {krb_session.__dict__=}\n {krb_session.__repr__=}")
                self.log.info(f"240307_1213-02, {endpoint=}")
                krb_url = self.url_from_endpoint(endpoint) if self.krb_url is None else self.krb_url
                self.log.info(f"240307_1213-03, {krb_url=}")
                self.log.info(f"240307_1213-04, {os.environ['SE_PROVIDER_CTL_SSLROOTCERT']=}\n {dir(os.environ['SE_PROVIDER_CTL_SSLROOTCERT'])=}\n {os.environ['SE_PROVIDER_CTL_SSLROOTCERT'].__dir__=}\n {os.environ['SE_PROVIDER_CTL_SSLROOTCERT'].__repr__=}")
                self.log.info(f"240307_1213-05, {dp.provider_ctl_sslrootcert=}\n {dir(dp.provider_ctl_sslrootcert)=}\n {dp.provider_ctl_sslrootcert.__dir__=}\n {dp.provider_ctl_sslrootcert.__repr__=}")
                self.log.info(f"240326_1344-01, {requests=} \n {dir(requests)=}")
                sess = requests.Session()
                sess.cert = extra_options.get("cert")
                self.log.info(f"240326_1344-01, {requests=}")
                self.log.info(f"240326_1344-02, {sess=} \n {sess.__dict__=} \n {dir(sess)=}")
                self.log.info(f"240327_1529-01,  {verify=}")
                if len(str(self.manual_parameter_setting)) > 3 and int(str(self.manual_parameter_setting)[3]) == 0:
                    self.log.info(f"240307_1200-00, response = requests.get(krb_url, headers=headers, verify=False)")
                    verify = False
                    sess.verify = verify
                elif len(str(self.manual_parameter_setting)) > 3 and int(str(self.manual_parameter_setting)[3]) == 1:
                    self.log.info(f"240307_1200-01, response = requests.get(krb_url, headers=headers, verify=None)")
                    verify = None
                    sess.verify = verify
                elif len(str(self.manual_parameter_setting)) > 3 and int(str(self.manual_parameter_setting)[3]) == 2:
                    self.log.info(f"240307_1200-02, response = requests.get(krb_url, headers=headers, verify=sslrootcert или sslcert)")
                    if len(str(self.manual_parameter_setting)) > 2 and int(str(self.manual_parameter_setting)[2]) == 2:
                        verify = sslrootcert    # sslcert - по совету Вячеслава Поленок в любом случае передать цепочку от сервера СТЛ - /mnt/secrets-B/cert/nginx_chain_from_CTL_240327/cert_chain.pem
                        sess.verify = verify
                    else:
                        verify = sslrootcert
                        sess.verify = verify
                else:
                    verify = sslrootcert        # sslcert - по совету Вячеслава Поленок в любом случае передать цепочку от сервера СТЛ - /mnt/secrets-B/cert/nginx_chain_from_CTL_240327/cert_chain.pem
                    sess.verify = verify
                    self.log.info(f"240307_1200-03, response = requests.get(krb_url, headers=headers)  БЕЗ   {verify=}     )")
                self.log.info(f"240327_1529-02,  {verify=}")
                self.log.info(f"240326_1344-07, {sess=} \n {sess.__dict__=} \n {dir(sess)=}")
                response = sess.get(krb_url, headers=headers, verify=verify) #requests.get(krb_url, headers=headers)
                self.log.info(f"240307_1214-01\n {response=}\n {dir(response)=}\n {response.__dict__=}\n {response.__repr__=}")
                krb_host = krb_url.split(endpoint)[0].split('://')[1]  # получаем host с портом 8087, если сделать без порта то по умолчанию будет 80 и ругань, что там никого нет)))))))))) krb_url.split('://')[1].split(':')[0]  # conn.host.split(":")[:2]
                self.log.info(f"240307_1214-02, {krb_host=}")
                krb_host = krb_host.split(':')[0]   #'tklis-supd00004.dev.df.sbrf.ru'
                self.log.info(f"240307_1214-03, {krb_host=}")
                #############################################  принудительно временно для ручной настройки  ##################################################
                realm = 'DEV.DF.SBRF.RU'
                principal_prefix = 'HTTP/'
                if len(str(self.manual_parameter_setting)) > 0 and int(str(self.manual_parameter_setting)[0]) == 1:
                    principal = ''.join([self.dag_executor_or_owner, '@', realm])  # principal_prefix,
                elif len(str(self.manual_parameter_setting)) > 0 and int(str(self.manual_parameter_setting)[0]) == 2:
                    principal = ''.join(["tklis-supd00004.dev.df.sbrf.ru", '@', realm])
                else:
                    principal = None
                #############################################  принудительно временно для ручной настройки  ##################################################
                self.log.info(f"240307_1211-00, {principal=}")
                krb_auth = HTTPKerberosAuth(mutual_authentication=REQUIRED,
                                            service="HTTP",
                                            force_preemptive=True,
                                            # https://github.com/requests/requests-kerberos/blob/master/README.rst
                                            sanitize_mutual_error_response=True,
                                            principal=principal
                                            # HTTP/21295126_ipa@DEV.DF.SBRF.RU     HTTP/tklis-supd00004.dev.df.sbrf.ru@DEV.DF.SBRF.RU
                                            )
                p1, p2, p3 = conf.krb_principal_split
                self.log.info(f"240307_1211-01, {conf.krb_principal=} \n {p1=} \n {p2=} \n {p3=}")
                self.log.info(f"240307_1212\n {krb_auth=}\n {dir(krb_auth)=}\n {krb_auth.__dict__=}\n {krb_auth.__repr__=}")
                #response = krb_auth.get_ticket(url)
                krb_token = krb_auth.generate_request_header(response, krb_host, False)  # krb_auth.generate_request_header(response, url) return "Negotiate {0}".format(base64.b64encode(gss_response).decode())
                self.log.info(f"240307_1214-04, {krb_token=}")  # response.token
                # используем токен в ответе
                headers["WWW-Authenticate"] = krb_token  # не требуется собирать " ".join(["negotiate", krb_token])  так как generate_request_header уже выдаёт нужную склейку  #response.token    ctx.kerberos_token
                headers["Authorization"] = krb_token
                self.log.info(f"240307_1214-05, {headers=}")
            else:
                self.log.error(f"Ticket \"{k}\" for user \"{self.dag_executor_or_owner}\" is not found in SecMan")
        else:
            endpoint = endpoint_v1
            self.log.info(f"240314_1242, {endpoint=}")
        self.log.debug(
            f"240111_1056, is_v4_api is {is_v4_api} \n type(data)={type(data)}, \n {data=}, \n type(_data)={type(_data)}, \n {_data=},"
            f"\n type(_data.get('avalue'))= {type(_data.get('avalue'))}, _data.get('avalue')= {_data.get('avalue')}"
            f"\n type(json.dumps(_data.get('avalue')))= {type(json.dumps(_data.get('avalue')))},"
            f"\n json.dumps(_data.get('avalue'))= {json.dumps(_data.get('avalue'))}"
            f"\n type([[*_data.get('avalue'), '']])= {type([[*_data.get('avalue'), '']])}"
            f"\n [[*_data.get('avalue'), ''],]= {[[*_data.get('avalue'), ''], ]}")
        # self.log.info(f'{extra_options=} , {_data=}')
        # ========================================================================
        session = self.get_conn(headers)
        url = self.url_from_endpoint(endpoint)
        self.log.info(f"240312_1317, {url=}")
        # ========================================================================
        if self.tcp_keep_alive:
            keep_alive_adapter = TCPKeepAliveAdapter(idle=self.keep_alive_idle, count=self.keep_alive_count, interval=self.keep_alive_interval)
            session.mount(url, keep_alive_adapter)
        _data = _data if is_v4_api is False else _data.get('avalue') #json.dumps(_data.get('avalue')) #[[*_data.get('avalue'), ''],]
        self.log.info(f"20240111_1556, is_v4_api is {is_v4_api} \n {type(_data)=} \n {_data=}")
        if self.method == "GET":
            # GET uses params
            req = requests.Request(self.method, url, params=_data, headers=headers, **request_kwargs)
        elif self.method == "HEAD":
            # HEAD doesn't use params
            req = requests.Request(self.method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = requests.Request(self.method, url, json=_data, headers=headers, **request_kwargs)
        self.log.info(f"20240111_1328 \n {type(req)=} \n {req=} \n {dir(req)=} \n {req.__dict__=} \n {req.__repr__=}")
        prepped_request = session.prepare_request(req)
        self.log.info(f"20240111_1329 \n {type(prepped_request)=} \n {prepped_request=} \n {dir(prepped_request)=} \n {prepped_request.__dict__=} \n {prepped_request.__repr__=}")
        self.log.debug("Sending '%s' to url: %s", self.method, url)
        extra_options["verify"] = verify
        return self.run_and_check(session, prepped_request, extra_options)

    def check_response(self, response: requests.Response) -> None:
        """Check the status code and raise on failure.

        :param response: A requests response object.
        :raise AirflowException: If the response contains a status code not
            in the 2xx and 3xx range.
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.log.error(f"HTTPError: {e}")
            raise AirflowException(f"CTL call error: {response.status_code},  {response.reason}, {response.text}")

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: dict[Any, Any],
    ) -> Any:
        """Grab extra options, actually run the request, and check the result.

        :param session: the session to be used to execute the request
        :param prepped_request: the prepared request generated in run()
        :param extra_options: additional options to be used when executing the request
            i.e. ``{'check_response': False}`` to avoid checking raising exceptions on non 2XX
            or 3XX status codes
        """
        extra_options = extra_options or {}

        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=extra_options.get("proxies", {}),
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify"),
            cert=extra_options.get("cert"),
        )

        # Send the request.
        send_kwargs: dict[str, Any] = {
            "timeout": extra_options.get("timeout"),
            "allow_redirects": extra_options.get("allow_redirects", True),
        }
        send_kwargs.update(settings)

        try:
            response = session.send(prepped_request, **send_kwargs)

            if extra_options.get("check_response", True):
                self.check_response(response)
            return response

        except requests.exceptions.ConnectionError as ex:
            self.log.warning("%s Tenacity will retry to execute the operation", ex)
            raise ex

    def run_with_advanced_retry(self, _retry_args: dict[Any, Any], *args: Any, **kwargs: Any) -> Any:
        """Run the hook with retry.

        This is useful for connectors which might be disturbed by intermittent
        issues and should not instantly fail.

        :param _retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity


        .. code-block:: python

            hook = HttpHook(ctl_conn_id="my_conn", method="GET")
            retry_args = dict(
                wait=tenacity.wait_exponential(),
                stop=tenacity.stop_after_attempt(10),
                retry=tenacity.retry_if_exception_type(Exception),
            )
            hook.run_with_advanced_retry(endpoint="v1/test", _retry_args=retry_args)

        """
        self._retry_obj = tenacity.Retrying(**_retry_args)

        return self._retry_obj(self.run, *args, **kwargs)

    def url_from_endpoint(self, endpoint: str | None) -> str:
        """Combine base url with endpoint."""
        if self.base_url and not self.base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            return self.base_url + "/" + endpoint
        return (self.base_url or "") + (endpoint or "")

    def test_connection(self):
        """Test HTTP Connection."""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)


class CtlAsyncHookSE(BaseHook):
    """Interact with HTTP servers asynchronously.

    :param method: the API method to be called
    :param ctl_conn_id: http connection id that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    """

    conn_name_attr = "ctl_conn_id"
    default_conn_name = "ctl_default"
    conn_type = "ctl_se"
    hook_name = "CtlAsyncHookSE"

    def __init__(
        self,
        method: str = "POST",
        ctl_conn_id: str = default_conn_name,
        auth_type: Any = aiohttp.BasicAuth,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        super().__init__()
        self.ctl_conn_id = ctl_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self.auth_type: Any = auth_type
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> ClientResponse:
        """Perform an asynchronous HTTP request call.

        :param endpoint: Endpoint to be called, i.e. ``resource/v1/query?``.
        :param data: Payload to be uploaded or request parameters.
       :param headers: Additional headers to be passed through as a dict.
        :param extra_options: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as
            ``aiohttp.ClientSession().get(json=obj)``.
        """
        extra_options = extra_options or {}

        # headers may be passed through directly or in the "extra" field in the connection
        # definition
        _headers = {}
        auth = None

        if self.ctl_conn_id:
            conn = await sync_to_async(self.get_connection)(self.ctl_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            if conn.login:
                auth = self.auth_type(conn.login, conn.password)
            if conn.extra:
                try:
                    _headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            _headers.update(headers)

        if self.base_url and not self.base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            url = self.base_url + "/" + endpoint
        else:
            url = (self.base_url or "") + (endpoint or "")

        async with aiohttp.ClientSession() as session:
            if self.method == "GET":
                request_func = session.get
            elif self.method == "POST":
                request_func = session.post
            elif self.method == "PATCH":
                request_func = session.patch
            elif self.method == "HEAD":
                request_func = session.head
            elif self.method == "PUT":
                request_func = session.put
            elif self.method == "DELETE":
                request_func = session.delete
            elif self.method == "OPTIONS":
                request_func = session.options
            else:
                raise CtlMethodNotFound(f"Unexpected HTTP Method: {self.method}")

            attempt_num = 1
            while True:
                response = await request_func(
                    url,
                    json=data if self.method in ("POST", "PATCH") else None,
                    params=data if self.method == "GET" else None,
                    headers=headers,
                    auth=auth,
                    **extra_options,
                )
                try:
                    response.raise_for_status()
                    return response
                except ClientResponseError as e:
                    self.log.warning(
                        "[Try %d of %d] Request to %s failed.",
                        attempt_num,
                        self.retry_limit,
                        url,
                    )
                    if not self._retryable_error_async(e) or attempt_num == self.retry_limit:
                        self.log.exception("HTTP error with status: %s", e.status)
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        raise AirflowException(f"{e.status}:{e.message}")

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    def _retryable_error_async(self, exception: ClientResponseError) -> bool:
        """Determine whether an exceptions may successful on a subsequent attempt.

        It considers the following to be retryable:
            - requests_exceptions.ConnectionError
            - requests_exceptions.Timeout
            - anything with a status code >= 500

        Most retryable errors are covered by status code >= 500.
        """
        if exception.status == 429:
            # don't retry for too Many Requests
            return False
        if exception.status == 413:
            # don't retry for payload Too Large
            return False

        return exception.status >= 500
