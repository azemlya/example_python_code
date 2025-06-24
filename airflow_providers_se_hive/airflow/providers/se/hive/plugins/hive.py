from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.se.hive.macros.hive import closest_ds_partition, max_partition


class HivePluginSE(AirflowPlugin):
    """Hive plugin - delivering macros used by users that use the provider."""

    name = "hive_se"
    macros = [max_partition, closest_ds_partition]