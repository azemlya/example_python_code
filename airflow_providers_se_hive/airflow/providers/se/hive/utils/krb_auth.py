from __future__ import annotations

import os
from base64 import b64decode
from typing import TYPE_CHECKING, Mapping, Any, Sequence

from airflow.exceptions import AirflowException

from airflow_se.secman import auth_secman, get_secman_data
from airflow_se.commons import SECMAN_KEY_FOR_TGT, TICKET_PREFIX

if TYPE_CHECKING:
    from airflow.utils.context import Context

__all__ = [
    "krb_auth",
]

def krb_auth(context: Context):
    if isinstance(context, Mapping) and context.get("dag"):
        _dag = context.get("dag")
    else:
        raise AirflowException("Invalid context")
    try:
        _dag_owner = _dag.owner
        _tgt = f"{TICKET_PREFIX}{_dag_owner}"
        _path_tgt = f"/tmp/{_tgt}"
        _token = auth_secman()
        sm_tgt: Dict[str, str] = get_secman_data(SECMAN_KEY_FOR_TGT, _token) or dict()
        b64 = sm_tgt.get(_tgt)
        with open(_path_tgt, "wb") as f:
            f.write(b64decode(b64))
        os.chmod(_path_tgt, 0o600)
        os.environ["KRB5CCNAME"] = _path_tgt
    except Exception as e:
        _dag.log.error(f"{e}")
        raise
