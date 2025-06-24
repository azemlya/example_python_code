
"""
DataBase базовые объекты подключения
"""
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from airflow.configuration import conf

from airflow_se.commons import SQLA_ENGINE_CONF

__all__ = [
    "engine",
    "Base",
    "Session",
]

engine = create_engine(url=conf.get("database", "sql_alchemy_conn"), **SQLA_ENGINE_CONF)
metadata = MetaData(bind=engine, schema=conf.get("database", "sql_alchemy_schema"))
Session = sessionmaker(bind=engine)
Base = declarative_base(bind=engine, metadata=metadata)

