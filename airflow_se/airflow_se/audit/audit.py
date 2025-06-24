
"""
Внутренний аудит
"""
from __future__ import annotations

from ssl import SSLSocket
from pprint import pformat
from typing import Optional, Dict, List, Tuple, Any
from flask import request, jsonify, session
from flask_login import login_user

from airflow_se.obj_imp import (
    get_auth_manager,
    BaseAuthManager,
    User,
    AirflowNotFoundException,
    AirflowBadRequest,
)

from .base_audit import BaseAuditAirflow
from .audit_restapi import AuditRestAPI
from .audit_webui import AuditWebUI

__all__ = [
    'AuditAirflow',
]


class AuditAirflow(BaseAuditAirflow):
    """
    Audit
    """
    def __init__(self):
        super().__init__()
        self.restapi: AuditRestAPI = AuditRestAPI()
        self.webui: AuditWebUI = AuditWebUI()
        self.airflow_app.before_request(self.before_request)

    def before_request(self):
        if request.path in ['/', '/metrics', '/api/v1/health', '/api/v1/version', ] or \
                request.path.startswith('/login') or request.path.startswith('/static'):
            return
        path = request.path.strip('/').split('/')
        if path[0] == 'api':
            user: Optional[User] = None
            is_ctl_user: bool = False
            msg: List[str] = list()
            msg.append(f'@@@ Incoming RESTAPI call from IP {request.remote_addr}: {request.method}, {request.path}", '
                       f'values = {request.values.to_dict()}, args = {request.args.to_dict()}, data = {request.data}')
            if not isinstance(user, User):
                user, auth_sess_msg = self.auth_sess()
                msg.extend(auth_sess_msg)
            if not isinstance(user, User):
                user, auth_basic_msg = self.auth_basic()
                msg.extend(auth_basic_msg)
            if not isinstance(user, User):
                user, auth_cert_msg, auth_cert_info, is_ctl_user = self.auth_cert()
                msg.extend([*auth_cert_info, *auth_cert_msg, ])
            self.log_debug('\n'.join(msg).replace('\r', '').replace('\n', '\n|| '))
            if not isinstance(user, User):
                response = jsonify({'action': f'Unauthorized REST API call', 'message': '\n'.join(msg) + '\n'})
                response.status_code = 401
                return response
            return self.restapi(path=path[2:], user=user, is_ctl_user=is_ctl_user)
        else:
            self.log_debug(f'@@@ Incoming WebUI call from IP {request.remote_addr}: {request.method}, {request.path}, '
                           f'values = {request.values.to_dict()}, args = {request.args.to_dict()}, data = {request.data}')
            return self.webui(path=path)

    def auth_basic(self) -> Tuple[Optional[User], List[str]]:
        """Базовая аутентификация по логину/паролю"""
        msg: List[str] = list()
        try:
            auth = request.authorization
            if auth is None or not auth.username or not auth.password:  # нет запроса на аутентификацию
                msg.append("DON't request on basic authentication/authorization")
                return None, msg
            user = self.sm.auth_user_db(auth.username, auth.password)
            if isinstance(user, User):
                login_user(user, remember=True)
                msg.append(f'User "{user.username}" successful basic authentication/authorization')
                return user, msg
            raise AirflowBadRequest('Error Kerberos/GSSAPI/LDAP authentication/authorization, see webserver logs')
        except Exception as e:
            msg.append(f'Missing basic authentication/authorization: {e}')
        return None, msg

    def auth_sess(self) -> Tuple[Optional[User], List[str]]:
        """Аутентификация по сессии"""
        msg: List[str] = list()
        try:
            if get_auth_manager and BaseAuthManager:  # Airflow >= 2.7.0
                auth_mngr: Optional[BaseAuthManager] = get_auth_manager()
                if not isinstance(auth_mngr, BaseAuthManager):
                    raise AirflowNotFoundException('func `get_auth_manager` return object is not `BaseAuthManager`')
                if auth_mngr.is_logged_in():
                    user = auth_mngr.get_user()
                    if isinstance(user, User):
                        msg.append(f'User "{user.username}" successful session authentication')
                        return user, msg
            else:  # Airflow < 2.7.0
                sess_user_login: Optional[str] = session.get('ext_user').get('login') if session.get('ext_user') else None
                if isinstance(sess_user_login, str) and not sess_user_login.isspace():
                    user = self.sm.find_user(username=sess_user_login)
                    if isinstance(user, User):
                        msg.append(f'User "{user.username}" successful session authentication')
                        return user, msg
                    else:
                        raise AirflowNotFoundException('user is not find in DB')
            raise AirflowNotFoundException('user\'s session is not found')
        except Exception as e:
            msg.append(f'Missing session authenticate: {e}')
        return None, msg

    @staticmethod
    def get_client_cert_info() -> Tuple[Optional[Dict[str, Any]], List[str]]:
        """Получение информации о клиентском сертификате"""
        msg: List[str] = list()
        try:
            gunicorn_socket: Optional[SSLSocket] = request.environ.get('gunicorn.socket')
            if gunicorn_socket:
                socket_protocol_version = gunicorn_socket.version()
                msg.append(f'Gunicorn Socket protocol version: {socket_protocol_version}')
                # from os import environ
                # msg.append(f'GUNICORN_CMD_ARGS = {environ.get("GUNICORN_CMD_ARGS")}')
                # msg.append(f'{gunicorn_socket.context.verify_mode=}, {gunicorn_socket.context.verify_flags=}')
                client_cert = gunicorn_socket.getpeercert(binary_form=False)
                if client_cert:
                    # msg.append(f'Client cert raw info (Before modify):\n{pformat(client_cert)}')
                    client_cert_info: Dict[str, Any] = {
                        'socket_protocol_version': socket_protocol_version,
                        'subject':
                            {a[0][0]: a[0][1] for a in client_cert.get('subject') if len(a) >= 1 and len(a[0]) >= 2},
                        'issuer':
                            {a[0][0]: a[0][1] for a in client_cert.get('issuer') if len(a) >= 1 and len(a[0]) >= 2},
                        'OCSP': client_cert.get('OCSP'),
                        'caIssuers': client_cert.get('caIssuers'),
                        'crlDistributionPoints': client_cert.get('crlDistributionPoints'),
                        'notAfter': client_cert.get('notAfter'),
                        'notBefore': client_cert.get('notBefore'),
                        'serialNumber': client_cert.get('serialNumber'),
                        'version': client_cert.get('version'),
                    }
                    msg.append(f"Client's certificate info:\n{pformat(client_cert_info)}")
                    # msg.append('-' * 70)
                    # import requests
                    # import OpenSSL
                    # if isinstance(client_cert_info, Dict):
                    #     crl: Tuple[str, ...] = client_cert_info.get('crlDistributionPoints')
                    #     if isinstance(crl, Tuple):
                    #         msg.append('Check crl list:')
                    #         for r in crl:
                    #             resp = requests.get(r)
                    #             # msg.append(f'  >> for url `{r}` response status {resp.status_code},\n{resp.text}')
                    #
                    #             crl: OpenSSL.crypto.CRL = OpenSSL.crypto.load_crl(OpenSSL.crypto.FILETYPE_PEM, resp.content)
                    #             # Export CRL as a cryptography CRL.
                    #             crl_crypto = crl.to_cryptography()
                    #             # Load CA CERTIFICATE
                    #             # ca = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, '-----BEGIN...'.encode())
                    #             ca = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, client_cert_bin)
                    #             # Get CA Public Key as _RSAPublicKey
                    #             ca_pub_key = ca.get_pubkey().to_cryptography_key()
                    #             # Validate CRL against CA
                    #             valid_signature = crl_crypto.is_signature_valid(ca_pub_key)
                    #             msg.append(f'  >> {valid_signature=}')
                    #
                    # msg.append('-' * 70)
                    return client_cert_info, msg
                else:
                    raise AirflowBadRequest('Client is not provided the certificate')
            else:
                raise AirflowBadRequest('Gunicorn Socket is not found')
        except Exception as e:
            msg.append(f"The certificate is not readable: {e}")
        return None, msg

    def auth_cert(self) -> Tuple[Optional[User], List[str], List[str], bool]:
        """Аутентификация по сертификату"""
        msg: List[str] = list()
        is_ctl_user: bool = False
        client_cert_info, client_cert_info_msg = self.get_client_cert_info()
        try:
            if isinstance(client_cert_info, dict) and client_cert_info.get('subject'):
                # msg.append(f"Extract client's certificate subject info: {client_cert_info.get('subject')}")
                cn = client_cert_info.get('subject').get('commonName')
                if not cn:
                    raise AirflowBadRequest('Missing field "commonName" on certificate subject')
                ldap_user_info: Optional[Dict[str, Any]] =  self.sm.ext_ldap_user_extract_silent(username=cn)
                if not isinstance(ldap_user_info, Dict):
                    # информацию о пользователе не удалось вытащить из кэша LDAP
                    self.sm.ext_audit_add(cn, "A3", {"REASON": "USER WAS NOT FOUND"})
                    raise AirflowNotFoundException(f'Information of user "{cn}" not found in cache LDAP')
                ldap_groups: Optional[List[str]] = ldap_user_info.get("ldap_groups")
                # проверка, на аккаунт CTL
                if len(list(set(self.ldap_tech_ctl_groups) & set(ldap_groups))) > 0 and \
                        cn in self.ldap_tech_ctl_users:  # This is CTL account
                    is_ctl_user = True
                    msg.append(f'User "{cn}" is a CTL account')
                else:  # кто-то пытается пробиться по левому сертификату (юзер не CTL)
                    self.sm.ext_audit_add(cn, "A3", {"REASON": "CN OF CERTIFICATE IS NOT A CTL"})
                    raise AirflowBadRequest(f'User "{cn}" is not CTL account')
                user = self.sm.find_user(username=cn)
                if isinstance(user, User):
                    msg.append(f'User "{user.username}" found in users DB')
                else:
                    msg.append(f'User "{cn}" is new user (not found in users metadata DB)')
                # LDAP authorization
                ret = self.sm.ext_ldap_authorize(cn)
                if ret:
                    self.sm.ext_audit_add(ret.username, "A2", {"SESSION_ID": "NOT APPLICABLE (REST API call)"})
                    login_user(ret, remember=True)
                    return ret, msg, client_cert_info_msg, is_ctl_user
                else:
                    self.sm.ext_audit_add(cn, "A3", {"REASON": "USER WAS NOT FOUND"})
                    raise AirflowBadRequest(f'Error LDAP authorisation for user "{cn}"')
            else:
                raise AirflowBadRequest("DON't information of client's certificate")
        except Exception as e:
            msg.append(f'Missing certificate authentication: {e}')
        return None, msg, client_cert_info_msg, is_ctl_user

