
"""
Модели БД
"""
from typing import Optional, Union, Tuple, Any
from contextlib import closing
from json import loads, dumps
from socket import getfqdn
from copy import copy, deepcopy
from datetime import datetime, timedelta
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Boolean,
    Index,
    Sequence,
    desc,
)

from .base import Base, Session, engine

__all__ = [
    "Audit",
    "CacheLDAP",
    "TGSList",
]


class Audit(Base):
    """
    События аудита
    """
    __tablename__ = "se_audit"

    id = Column(Integer, Sequence("seq_se_audit_id"), nullable=False, primary_key=True)
    ts = Column(DateTime, default=datetime.now, nullable=False)
    is_pushed = Column(Boolean, default=False, nullable=False)
    host = Column(String(255), default=getfqdn, nullable=False)
    remote_addr = Column(String(50), nullable=False)
    remote_login = Column(String(100), nullable=False)
    code_op = Column(String(100), nullable=False)
    app_id = Column(String(100), nullable=False)
    type_id = Column(String(50), nullable=False)
    subtype_id = Column(String(50), nullable=False)
    status_op = Column(String(50), nullable=False)
    extras_json = Column(Text, nullable=False)
    #B_SystemID = Column(String(50), nullable=False) #КЭ АС не обязательно для ППРБ аудит в событиях F0 https://confluence.sberbank.ru/pages/viewpage.action?pageId=3204448409
    #SystemID_FP = Column(String(50), nullable=False) #КЭ ФП не обязательно для ППРБ аудит в событиях F0 https://confluence.sberbank.ru/pages/viewpage.action?pageId=3204448409

    __table_args__ = (
        Index("uidx_se_audit_id", id, unique=True),
        Index("idx_se_audit_ts", ts, unique=False),
        Index("idx_se_audit_is_pushed", is_pushed, unique=False),
        {
            "comment": "Inner audit",
        },
    )

    def __init__(self, **kwargs):
        self.set_fields(**kwargs)

    @property
    def extras(self) -> Optional[dict]:
        if self.extras_json:
            return loads(self.extras_json)

    @extras.setter
    def extras(self, obj: Union[str, dict, None]):
        _obj: Union[str, dict, None] = None
        if isinstance(obj, str):
            try:
                _obj = loads(obj)
                if isinstance(_obj, dict):
                    _obj = dumps(_obj)
                else:
                    raise ValueError(f"JSON not a dictionary: {_obj}")
            except Exception as e1:
                raise ValueError(f"Missing parse JSON from string: {obj}\n{e1}")
        elif isinstance(obj, dict):
            try:
                _obj = dumps(obj)
            except Exception as e2:
                raise ValueError(f"Missing dumps JSON from dictionary: {obj}\n{e2}")
        elif obj is None:
            _obj = dumps({})
        else:
            raise ValueError(f"Invalid type \"{type(obj)}\" for parameter \"obj\": {obj=}")
        self.extras_json = _obj

    def set_fields(self, **kwargs):
        _ts = kwargs.get("ts") or self.ts
        _is_pushed = kwargs.get("is_pushed") or self.is_pushed
        _host = kwargs.get("host") or self.host
        _remote_addr = kwargs.get("remote_addr") or self.remote_addr
        _remote_login = kwargs.get("remote_login") or self.remote_login
        _code_op = kwargs.get("code_op") or self.code_op
        _app_id = kwargs.get("app_id") or self.app_id
        _type_id = kwargs.get("type_id") or self.type_id
        _subtype_id = kwargs.get("subtype_id") or self.subtype_id
        _status_op = kwargs.get("status_op") or self.status_op
        self.ts = _ts if isinstance(_ts, datetime) else datetime.now()
        self.is_pushed = _is_pushed if isinstance(_is_pushed, bool) else False
        self.host = _host if isinstance(_host, str) else getfqdn()
        self.remote_addr = _remote_addr if isinstance(_remote_addr, str) else ""
        self.remote_login = _remote_login if isinstance(_remote_login, str) else ""
        self.code_op = _code_op if isinstance(_code_op, str) else ""
        self.app_id = _app_id if isinstance(_app_id, str) else ""
        self.type_id = _type_id if isinstance(_type_id, str) else ""
        self.subtype_id = _subtype_id if isinstance(_subtype_id, str) else ""
        self.status_op = _status_op if isinstance(_status_op, str) else ""
        self.extras = kwargs.get("extras") or self.extras

    @property
    def get_dict(self) -> dict:
        return deepcopy({"id": self.id,
                         "ts": self.ts,
                         "is_pushed": self.is_pushed,
                         "host": self.host,
                         "remote_addr": self.remote_addr,
                         "remote_login": self.remote_login,
                         "code_op": self.code_op,
                         "app_id": self.app_id,
                         "type_id": self.type_id,
                         "subtype_id": self.subtype_id,
                         "status_op": self.status_op,
                         "extras": self.extras,
                         })

    def close(self):
        del self

    def __repr__(self) -> str:
        return f"Audit record: {self.get_dict}"


class CacheLDAP(Base):
    """
    Кэши LDAP
    """
    __tablename__ = "se_cache_ldap"

    id = Column(Integer, Sequence("seq_se_cache_ldap_id"), nullable=False, primary_key=True)
    ts = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False, primary_key=True)
    cache_json = Column(Text, nullable=False)

    __table_args__ = (
        Index("uidx_se_cache_ldap_id", id, unique=True),
        Index("idx_se_cache_ldap_ts", ts, unique=False),
        {
            "comment": "Caches LDAP",
        },
    )

    def __init__(self, **kwargs):
        self.set_fields(**kwargs)

    @property
    def cache(self) -> Optional[list]:
        if self.cache_json:
            return loads(self.cache_json)

    @cache.setter
    def cache(self, obj: Union[str, list, tuple, None]):
        _obj: Union[str, list, tuple, None] = None
        if isinstance(obj, str):
            try:
                _obj = loads(obj)
                if isinstance(_obj, (list, tuple)):
                    _obj = dumps(_obj)
                else:
                    raise ValueError(f"JSON not a list or tuple: {_obj}")
            except Exception as e1:
                raise ValueError(f"Missing parse JSON from string: {obj=}\n{e1}")
        elif isinstance(obj, (list, tuple)):
            try:
                _obj = dumps(obj)
            except Exception as e2:
                raise ValueError(f"Missing dumps JSON from list or tuple: {obj=}\n{e2}")
        elif obj is None:
            _obj = dumps([])
        else:
            raise ValueError(f"Invalid type \"{type(obj)}\" for parameter \"obj\": {obj=}")
        self.cache_json = _obj

    def set_fields(self, **kwargs):
        _ts = kwargs.get("ts") or self.ts
        self.ts = _ts if isinstance(_ts, datetime) else datetime.now()
        self.cache = kwargs.get("cache") or self.cache

    @property
    def get_dict(self) -> dict:
        return deepcopy({"id": self.id,
                         "ts": self.ts,
                         "cache": self.cache,
                         })

    def close(self):
        del self

    def __repr__(self) -> str:
        return f"Cache LDAP: {self.get_dict}"


class TGSList(Base):
    """Справочник TGS-ок"""
    __tablename__ = "se_tgs_list"

    tgs = Column(String(255), nullable=False, primary_key=True)
    is_valid = Column(Boolean, default=True, nullable=False)

    __table_args__ = (
        Index("idx_se_tgs_list_is_valid", is_valid, unique=False),
        {
            "comment": "TGS list",
        },
    )

    def __init__(self, tgs: str):
        self.tgs = tgs

    @staticmethod
    def get_tgs_list() -> list:
        with closing(Session()) as sess:
            rows = sess.query(TGSList).filter(TGSList.is_valid.is_(True)).all()
            sess.commit()
            return [row.tgs if isinstance(row, TGSList) else row for row in rows]

    @staticmethod
    def push_tgs(tgs: str):
        if tgs.strip() not in TGSList.get_tgs_list():
            with closing(Session()) as sess:
                with closing(TGSList(tgs.strip())) as tgs_obj:
                    sess.add(tgs_obj)
                    sess.commit()

    @staticmethod
    def del_tgs(tgs: str):
        if tgs.strip() in TGSList.get_tgs_list():
            with closing(Session()) as sess:
                sess.query(TGSList).filter(TGSList.tgs == tgs).update({"is_valid": False}, synchronize_session="fetch")
                sess.commit()

    def close(self):
        del self


# class InsteadOfXCom(Base):
#     """
#     вместо xComs
#     """
#     __tablename__ = "se_instead_of_xcom"
#
#     id         = Column(Integer, Sequence("seq_se_instead_of_xcom_id"), nullable=False, primary_key=True)
#     time_stamp = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
#     dag_id     = Column(String(256), nullable=False)
#     run_id     = Column(String(256), nullable=False)
#     key        = Column(String(256), nullable=False)
#     value      = Column(Text, nullable=False)
#
#     __table_args__ = (
#         Index("idx_se_instead_of_xcom_dag_id", dag_id, unique=True),
#         Index("idx_se_instead_of_xcom_dag_id", run_id, unique=False),
#         Index("idx_se_instead_of_xcom_time_stamp", time_stamp, unique=False),
#         {
#             "comment": "Instead Of XCom",
#         },
#     )
#
#     def __init__(self, **kwargs):
#         self.set_fields(**kwargs)
#
#     @property
#     def instead_of_xcom_value(self) -> Optional[list]:
#         if self.value:
#             return loads(self.value)
#
#     @instead_of_xcom_value.setter
#     def instead_of_xcom_value(self, obj: Union[str, list, tuple, None]):
#         _obj: Union[str, list, tuple, None] = None
#         if isinstance(obj, str):
#             try:
#                 _obj = loads(obj)
#                 if isinstance(_obj, (list, tuple)):
#                     _obj = dumps(_obj)
#                 else:
#                     raise ValueError(f"JSON value not a list or tuple: {_obj}")
#             except Exception as e1:
#                 raise ValueError(f"Missing parse JSON value from string: {obj=}\n{e1}")
#         elif isinstance(obj, (list, tuple)):
#             try:
#                 _obj = dumps(obj)
#             except Exception as e2:
#                 raise ValueError(f"Missing dumps JSON value from list or tuple: {obj=}\n{e2}")
#         elif obj is None:
#             _obj = dumps([])
#         else:
#             raise ValueError(f"Invalid type \"{type(obj)}\" for parameter \"obj\": {obj=}")
#         self.value = _obj
#
#     def set_fields(self, **kwargs):
#         _time_stamp = kwargs.get("time_stamp") or self.time_stamp
#         self.time_stamp = _time_stamp if isinstance(_time_stamp, datetime) else datetime.now()
#         self.dag_id     = kwargs.get("dag_id") or self.dag_id
#         self.run_id     = kwargs.get("run_id") or self.run_id
#         self.key        = kwargs.get("key") or self.key
#         self.value      = kwargs.get("value") or self.value
#
#     @property
#     def get_dict(self) -> dict:
#         return deepcopy({"id": self.id,
#                          "time_stamp": self.time_stamp,
#                          "dag_id": self.dag_id,
#                          "run_id": self.run_id,
#                          "key": self.key,
#                          "value": self.value,
#                          })
#
#     def close(self):
#         del self
#
#     def __repr__(self) -> str:
#         return f"Instead of xCom: {self.get_dict}"


try:
    Base.metadata.create_all(engine)
except Exception as ec:
    _ = ec

