
"""
Интеграция с SecMan
"""
from os import environ as env
from os.path import exists
import logging
import json
import yaml
import requests

requests.packages.urllib3.disable_warnings()

from typing import Optional, Union, Dict, Any

from airflow_se.commons import VERSION

__all__ = [
    "auth_secman",
    "get_secman_data",
    "push_secman_data",
]

log = logging.getLogger(__name__)

_base_url = env.get("SE_SECMAN_BASE_URL")
_path_auth = env.get("SE_SECMAN_PATH_AUTH")
_path = env.get("SE_SECMAN_PATH_SECRET")
_namespace = env.get("SE_SECMAN_NAMESPACE")
_secman_key = env.get("SE_SECMAN_KEY")
_role_id = env.get("SE_SECMAN_ROLE_ID")
_secret_id = env.get("SE_SECMAN_SECRET_ID")
_cert = env.get("SE_SECMAN_SSL_CERT_PATH")
_key = env.get("SE_SECMAN_SSL_KEY_PATH")

def auth_secman() -> Optional[str]:
    """Auth to SecMan, return token"""
    secrets = dict(
        role_id=_role_id.strip() if _role_id else None,
        secret_id=_secret_id.strip() if _secret_id else None,
    )
    auth_params = dict(
        url=f"{_base_url}{_path_auth}",
        headers={
            "Content-Type": "application/json",
            "x-vault-namespace": _namespace,
            "user-agent": f"airflow_se/{VERSION}",
            "charset": "utf-8",
        },
        verify=False if _cert is None or _key is None else None,
        cert=(_cert, _key) if _cert and _key else None,
        data=json.dumps(secrets),
        timeout=60,
    )
    auth_response = requests.post(**auth_params)
    if auth_response.status_code != 200:
        log.error(f"Missed SecMan authentication: {auth_response.status_code}, {auth_response.text.strip()}"
                  f"\nInfo: url={_base_url}{_path_auth}, namespace={_namespace}")
        return None
    # else:
    #     log.info("SecMan authentication successfully complete")
    _token = auth_response.json().get("auth").get("client_token") if \
        isinstance(auth_response.json().get("auth"), dict) else None
    if not _token:
        log.error(f"Missed SecMan authentication: SecMan don't return authentication token ->"
                  f"\n{auth_response.status_code}, {auth_response.text.strip()}"
                  f"\nInfo: url={_base_url}{_path_auth}, namespace={_namespace}")
        return None
    return _token

def get_secman_data(key: str, auth_token: Optional[str] = None) -> Optional[Dict[str, str]]:
    """Get SecMan data"""
    # log.info("=" * 80)
    # log.info("<< Get SecMan data >>")
    if not isinstance(key, str) or key.isspace():
        log.error("Parameter `key` is invalid")
        # log.info("=" * 80)
        return None
    _token = auth_token if auth_token else auth_secman()
    if not _token:
        # log.info("=" * 80)
        return None

    params = dict(
        url=f"{_base_url}{_path}/{_secman_key}_{key.strip()}",
        headers={
            "Content-Type": "application/json",
            "X-Vault-Token": _token,
            "x-vault-namespace": _namespace,
            "user-agent": f"airflow_se/{VERSION}",
            "charset": "utf-8",
        },
        verify=False if _cert is None or _key is None else None,
        cert=(_cert, _key) if _cert and _key else None,
        timeout=60,
    )
    # log.info(f"SecMan get secrets from: {params.get('url')}")
    response = requests.get(**params)
    if response.status_code != 200:
        log.error(f"Missed get SecMan data: {response.status_code}, {response.text.strip()}")
        # log.info("=" * 80)
        return None
    data = response.json().get("data")
    if isinstance(data, dict) and len(data) > 0:
        log.info("Get SecMan data: Successfully complete")
        # log.info("=" * 80)
        return data
    elif isinstance(data, dict) and len(data) == 0:
        log.warning(f"SecMan data is empty: {response.status_code}, {response.text.strip()}")
    else:
        log.error(f"Missed get SecMan data: {response.status_code}, {response.text.strip()}")
    # log.info("=" * 80)
    return None

def push_secman_data(key: str, data: Union[Dict[str, Any], str], auth_token: Optional[str] = None) -> bool:
    """Push SecMan data"""
    # log.info("=" * 80)
    # log.info("<< Push SecMan data >>")

    if not isinstance(key, str) or key.isspace():
        log.error("Parameter `key` is invalid")
        # log.info("=" * 80)
        return False

    if not isinstance(data, (dict, str)):
        log.error("Parameter `data` is invalid")
        # log.info("=" * 80)
        return False

    _token = auth_token if auth_token else auth_secman()
    if not _token:
        # log.info("=" * 80)
        return False

    if isinstance(data, str) and exists(data):
        with open(data, "rb") as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            log.info(f"SecMan data loaded from file {f.name}")

    if isinstance(data, dict) and len(data) > 0:
        data = {k: json.dumps(v) if isinstance(v, (dict, list, set)) else str(v) for k, v in data.items()}

    params = dict(
        url=f"{_base_url}{_path}/{_secman_key}_{key.strip()}",
        headers={
            "Content-Type": "application/json",
            "X-Vault-Token": _token,
            "x-vault-namespace": _namespace,
            "user-agent": f"airflow_se/{VERSION}",
            "charset": "utf-8",
        },
        verify=False if _cert is None or _key is None else None,
        cert=(_cert, _key) if _cert and _key else None,
        timeout=60,
        data=json.dumps(data) if isinstance(data, dict) else data,
    )
    # log.info(f"Push SecMan data to: {params.get('url')}")
    response = requests.post(**params)
    if response.status_code == 204:
        log.info(f"Push SecMan data: Successfully complete")
    else:
        log.error(f"Missed push SecMan data: {response.status_code}, {response.text.strip()}")
        # log.info("=" * 80)
        return False
    # log.info("=" * 80)
    return True

