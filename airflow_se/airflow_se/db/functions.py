
"""

"""
from copy import copy, deepcopy
from contextlib import closing
from datetime import datetime, timedelta
from sqlalchemy import desc
from typing import Optional, Union, Tuple, Any

from .base import Session
from .models import Audit, CacheLDAP

__all__ = [
    "get_max_ldap_cache",
    "set_new_ldap_cache",
    "delete_old_ldap_caches",
    "clear_ldap_caches",
    "create_new_audit_record",
    "get_part_messages_for_delivery",
    "mark_part_messages",
]


def create_new_audit_record(**kwargs) -> dict:
    with closing(Session()) as sess:
        with closing(Audit(**kwargs)) as aud:
            sess.add(aud)
            sess.commit()
            return aud.get_dict

def get_part_messages_for_delivery(part: int = 1000) -> list:
    with closing(Session()) as sess:
        rows = sess.query(Audit).filter(Audit.is_pushed.is_(False)).order_by(desc(Audit.id)).limit(part).all()
        sess.commit()
        return [row.get_dict if isinstance(row, Audit) else row for row in rows]

def mark_part_messages(ids: Union[list, set, tuple, int]):
    with closing(Session()) as sess:
        if isinstance(ids, (list, set, tuple)) and len(ids) > 0:
            sess.query(Audit).filter(Audit.id.in_(list(ids))).filter(Audit.is_pushed.is_(False))\
                .update({"is_pushed": True}, synchronize_session="fetch")
            sess.commit()
        elif isinstance(ids, int) and ids > 0:
            sess.query(Audit).filter(Audit.id == ids).filter(Audit.is_pushed.is_(False))\
                .update({"is_pushed": True}, synchronize_session="fetch")
            sess.commit()
        else:
            raise ValueError(f"parameter 'ids' must be 'list', 'set', 'tuple' or 'int', not be '{type(ids)}'!")

def get_max_ldap_cache(only_ts: bool = False) -> Union[Optional[datetime], Tuple[Optional[datetime], Optional[Any]]]:
    with closing(Session()) as sess:
        if only_ts:
            row = sess.query(CacheLDAP.ts).order_by(desc(CacheLDAP.ts)).first()
            sess.commit()
            return copy(row.ts) if row else None
        else:
            row = sess.query(CacheLDAP).order_by(desc(CacheLDAP.ts)).first()
            sess.commit()
            return (copy(row.ts), deepcopy(row.cache)) if row else (None, None)

def set_new_ldap_cache(cache: Union[list, tuple]) -> Optional[datetime]:
    with closing(Session()) as sess:
        with closing(CacheLDAP(cache=cache)) as ch:
            sess.add(ch)
            sess.commit()
            return copy(ch.ts)

def delete_old_ldap_caches(td: timedelta = timedelta(days=30)):
    dt: datetime = (datetime.now() - td) if (isinstance(td, timedelta) and (td >= timedelta(days=3)))\
        else (datetime.now() - timedelta(days=30))
    with closing(Session()) as sess:
        sess.query(CacheLDAP).filter(CacheLDAP.ts < dt).delete(synchronize_session="fetch")
        sess.commit()

def clear_ldap_caches():
    """
    Удаление всех записей в таблице!!! Осторожно!!!
    """
    with closing(Session()) as sess:
        sess.query(CacheLDAP).delete(synchronize_session="fetch")
        sess.commit()

