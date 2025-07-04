from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.providers.se.hive.hooks.hive import HiveMetastoreHookSE
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HivePartitionSensorSE(BaseSensorOperator):
    """
    Waits for a partition to show up in Hive.

    Note: Because ``partition`` supports general logical operators, it
    can be inefficient. Consider using NamedHivePartitionSensorSE instead if
    you don't need the full flexibility of HivePartitionSensorSE.

    :param table: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :param partition: The partition clause to wait for. This is passed as
        is to the metastore Thrift client ``get_partitions_by_filter`` method,
        and apparently supports SQL like notation as in ``ds='2015-01-01'
        AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``
    :param metastore_se_conn_id: reference to the
        :ref: `metastore thrift service connection id <howto/connection:hive_metastore>`
    """

    template_fields: Sequence[str] = (
        "schema",
        "table",
        "partition",
    )
    ui_color = "#C5CAE9"

    def __init__(
        self,
        *,
        table: str,
        partition: str | None = "ds='{{ ds }}'",
        metastore_se_conn_id: str = "metastore_se_default",
        schema: str = "default",
        poke_interval: int = 60 * 3,
        **kwargs: Any,
    ):
        super().__init__(poke_interval=poke_interval, **kwargs)
        if not partition:
            partition = "ds='{{ ds }}'"
        self.metastore_se_conn_id = metastore_se_conn_id
        self.table = table
        self.partition = partition
        self.schema = schema

    def poke(self, context: Context) -> bool:
        if "." in self.table:
            self.schema, self.table = self.table.split(".")
        self.log.info("Poking for table %s.%s, partition %s", self.schema, self.table, self.partition)
        if not hasattr(self, "hook"):
            hook = HiveMetastoreHookSE(metastore_se_conn_id=self.metastore_se_conn_id)
        return hook.check_for_partition(self.schema, self.table, self.partition)