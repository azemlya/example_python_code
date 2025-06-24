
"""

"""
from typing import Optional
from flask import Flask, Config
from airflow.configuration import AirflowConfigParser

from .settings_class import Settings

__all__ = [
    "Settings",
    "get_settings",
]

__settings_instance: Optional[Settings] = None

def get_settings(
        flask_app: Optional[Flask] = None,
        flask_app_config: Optional[Config] = None,
        airflow_config: Optional[AirflowConfigParser] = None,
        silent: bool = False,
) -> Settings:
    global __settings_instance
    if isinstance(__settings_instance, Settings):
        return __settings_instance
    __settings_instance = Settings(
        flask_app=flask_app,
        flask_app_config=flask_app_config,
        airflow_config=airflow_config,
        silent=silent,
    )
    return __settings_instance

