from __future__ import annotations

import os
import re
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from deprecated.classic import deprecated

from airflow.configuration import conf
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.se.hive.hooks.hive import HiveCliHookSE
from airflow.utils import operator_helpers
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.providers.se.hive.utils.krb_auth import krb_auth

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HiveOperatorSE(BaseOperator):
    """
    Executes hql code or hive script in a specific Hive database.

    :param hql: the hql to be executed. Note that you may also use
        a relative path from the dag file of a (template) hive
        script. (templated)
    :param hive_cli_se_conn_id: Reference to the
        :ref:`Hive CLI connection id <howto/connection:hive_cli>`. (templated)
    :param hiveconfs: if defined, these key value pairs will be passed
        to hive as ``-hiveconf "key"="value"``
    :param hiveconf_jinja_translate: when True, hiveconf-type templating
        ${var} gets translated into jinja-type templating {{ var }} and
        ${hiveconf:var} gets translated into jinja-type templating {{ var }}.
        Note that you may want to use this along with the
        ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
        object documentation for more details.
    :param script_begin_tag: If defined, the operator will get rid of the
        part of the script before the first occurrence of `script_begin_tag`
    :param run_as_owner: Run HQL code as a DAG's owner.
    :param mapred_queue: queue used by the Hadoop CapacityScheduler. (templated)
    :param mapred_queue_priority: priority within CapacityScheduler queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    :param hive_cli_params: parameters passed to hive CLO
    :param auth: optional authentication option passed for the Hive connection
    """

    template_fields: Sequence[str] = (
        "hql",
        "schema",
        "hive_cli_se_conn_id",
        "mapred_queue",
        "hiveconfs",
        "mapred_job_name",
        "mapred_queue_priority",
    )
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"hql": "hql"}
    ui_color = "#f0e4ec"

    def __init__(
        self,
        *,
        hql: str,
        hive_cli_se_conn_id: str = "hive_cli_se_default",
        schema: str = "default",
        hiveconfs: dict[Any, Any] | None = None,
        hiveconf_jinja_translate: bool = False,
        script_begin_tag: str | None = None,
        run_as_owner: bool = False,
        mapred_queue: str | None = None,
        mapred_queue_priority: str | None = None,
        mapred_job_name: str | None = None,
        hive_cli_params: str = "",
        auth: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.hql = hql
        self.hive_cli_se_conn_id = hive_cli_se_conn_id
        self.schema = schema
        self.hiveconfs = hiveconfs or {}
        self.hiveconf_jinja_translate = hiveconf_jinja_translate
        self.script_begin_tag = script_begin_tag
        self.run_as = None
        if run_as_owner:
            self.run_as = self.dag.owner
        self.mapred_queue = mapred_queue
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name
        self.hive_cli_params = hive_cli_params
        self.auth = auth

        job_name_template = conf.get_mandatory_value(
            "hive",
            "mapred_job_name_template",
            fallback="Airflow HiveOperatorSE task for {hostname}.{dag_id}.{task_id}.{execution_date}",
        )
        self.mapred_job_name_template: str = job_name_template

    @cached_property
    def hook(self) -> HiveCliHookSE:
        """Get Hive cli hook."""
        return HiveCliHookSE(
            hive_cli_se_conn_id=self.hive_cli_se_conn_id,
            run_as=self.run_as,
            mapred_queue=self.mapred_queue,
            mapred_queue_priority=self.mapred_queue_priority,
            mapred_job_name=self.mapred_job_name,
            hive_cli_params=self.hive_cli_params,
            auth=self.auth,
        )

    @deprecated(reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning)
    def get_hook(self) -> HiveCliHookSE:
        """Get Hive cli hook."""
        return self.hook

    def prepare_template(self) -> None:
        if self.hiveconf_jinja_translate:
            self.hql = re.sub(r"(\$\{(hiveconf:)?([ a-zA-Z0-9_]*)\})", r"{{ \g<3> }}", self.hql)
        if self.script_begin_tag and self.script_begin_tag in self.hql:
            self.hql = "\n".join(self.hql.split(self.script_begin_tag)[1:])

    def execute(self, context: Context) -> None:
        krb_auth(context)
        self.log.info("Executing: %s", self.hql)

        # set the mapred_job_name if it's not set with dag, task, execution time info
        if not self.mapred_job_name:
            ti = context["ti"]
            if ti.execution_date is None:
                raise RuntimeError("execution_date is None")
            self.hook.mapred_job_name = self.mapred_job_name_template.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                execution_date=ti.execution_date.isoformat(),
                hostname=ti.hostname.split(".")[0],
            )

        if self.hiveconf_jinja_translate:
            self.hiveconfs = context_to_airflow_vars(context)
        else:
            self.hiveconfs.update(context_to_airflow_vars(context))

        self.log.info("Passing HiveConf: %s", self.hiveconfs)
        self.hook.run_cli(hql=self.hql, schema=self.schema, hive_conf=self.hiveconfs)

    def dry_run(self) -> None:
        # Reset airflow environment variables to prevent
        # existing env vars from impacting behavior.
        self.clear_airflow_vars()

        self.hook.test_hql(hql=self.hql)

    def on_kill(self) -> None:
        if self.hook:
            self.hook.kill()

    def clear_airflow_vars(self) -> None:
        """Reset airflow environment variables to prevent existing ones from impacting behavior."""
        blank_env_vars = {
            value["env_var_format"]: "" for value in operator_helpers.AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
        }
        os.environ.update(blank_env_vars)