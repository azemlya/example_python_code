from typing import Optional, Union, Iterable, Dict

from airflow.models import BaseOperator
from ..hooks.greenplum import GreenplumHookSE

__all__ = [
    'GreenplumOperatorSE',
]


class GreenplumOperatorSE(BaseOperator):
    """
    Executes sql code in a specific Greenplum database

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in ".sql"
    :param greenplum_se_conn_id: The :ref:`greenplum conn id <howto/connection:greenplum>`
        reference to a specific greenplum database.
    :type greenplum_se_conn_id: str
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ("sql",)
    template_fields_renderers = {"sql": "sql"}
    template_ext = (".sql",)
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        sql: Union[str, Iterable[str]],
        greenplum_se_conn_id: str = "greenplum_se_default",
        autocommit: bool = False,
        parameters: Optional[Dict] = None,
        schema: Optional[str] = None,
        setrole: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.greenplum_se_conn_id = greenplum_se_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.schema = schema
        self.setrole = setrole
        self.hook: Optional[GreenplumHookSE] = None

    def execute(self, context):
        self.hook = GreenplumHookSE(greenplum_se_conn_id=self.greenplum_se_conn_id,
                                    schema=self.schema,
                                    setrole=self.setrole,
                                    context=context,
                                    )
        self.hook.run(sql=self.sql, autocommit=self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(f'Information message(notice) from Greenlum/Postgres connection: "{output.strip()}"')
