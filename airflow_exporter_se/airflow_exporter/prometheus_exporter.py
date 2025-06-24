"""
Метрики по DAGs и Tasks
"""
import logging

from typing import List, Tuple, Optional, Union, Generator, NamedTuple, Dict

from dataclasses import dataclass
from itertools import groupby as it_group_by
from urllib.parse import urlparse

import requests
from psutil import Process, process_iter, NoSuchProcess, AccessDenied, ZombieProcess, disk_usage
from sqlalchemy import func
from sqlalchemy import text

from flask import Response, current_app, request
from flask_appbuilder import BaseView as FABBaseView, expose as fab_expose

from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.state import State
from airflow.configuration import conf

# Importing base classes that we need to derive
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.samples import Sample

log = logging.getLogger(__name__)

try:
    from prometheus_flask_exporter import PrometheusMetrics as FlaskPrometheusMetrics

    flask_prometheus_metrics = FlaskPrometheusMetrics(app=current_app,
                                                      path="/metrics",
                                                      export_defaults=False,
                                                      defaults_prefix="airflow_se",
                                                      group_by="endpoint"
                                                      )

    flask_prometheus_metrics.register_default(
        flask_prometheus_metrics.counter(
            "http_request_total", "Total number of HTTP requests",
            labels={"method": lambda: request.method, "status": lambda r: r.status_code}
        )
    )
except:
    flask_prometheus_metrics = None


@dataclass
class DagInfo:
    dag_id: str
    is_paused: bool


def get_dag_info() -> List[DagInfo]:
    assert (Session is not None)

    sql_res = (
        Session.query(
            DagModel.dag_id, DagModel.is_paused
        )
        .group_by(DagModel.dag_id)
        .all()
    )

    res=[
        DagInfo(
            dag_id=i.dag_id,
            is_paused=i.is_paused
        )
        for i in sql_res
    ]

    return res

@dataclass
class DagStatusInfo:
    dag_id: str
    status: str
    cnt: int
    owner: str


def get_dag_status_info() -> List[DagStatusInfo]:
    """get dag info
    :return dag_info
    """
    assert (Session is not None)

    dag_status_query = Session.query(  # pylint: disable=no-member
        DagRun.dag_id, DagRun.state, func.count(DagRun.state).label('cnt')
    ).group_by(DagRun.dag_id, DagRun.state).subquery()

    sql_res = (
        Session.query(  # pylint: disable=no-member
            dag_status_query.c.dag_id, dag_status_query.c.state, dag_status_query.c.cnt,
            DagModel.owners
        )
        .join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id)
        .join(SerializedDagModel, SerializedDagModel.dag_id == dag_status_query.c.dag_id)
        .all()
    )

    res = [
        DagStatusInfo(
            dag_id=i.dag_id,
            status=i.state,
            cnt=i.cnt,
            owner=i.owners
        )
        for i in sql_res
    ]

    return res


def get_last_dagrun_info() -> List[DagStatusInfo]:
    """get last_dagrun info
    :return last_dagrun_info
    """
    assert (Session is not None)

    last_dagrun_query = Session.query(
        DagRun.dag_id, DagRun.state,
        func.row_number().over(partition_by=DagRun.dag_id,
                               order_by=DagRun.execution_date.desc()).label('row_number')
    ).subquery()

    sql_res = (
        Session.query(
            last_dagrun_query.c.dag_id, last_dagrun_query.c.state, last_dagrun_query.c.row_number,
            DagModel.owners
        )
        .filter(last_dagrun_query.c.row_number == 1)
        .join(DagModel, DagModel.dag_id == last_dagrun_query.c.dag_id)
        .join(SerializedDagModel, SerializedDagModel.dag_id == last_dagrun_query.c.dag_id)
        .all()
    )

    res = [
        DagStatusInfo(
            dag_id=i.dag_id,
            status=i.state,
            cnt=1,
            owner=i.owners
        )
        for i in sql_res
    ]

    return res


@dataclass
class TaskStatusInfo:
    dag_id: str
    task_id: str
    status: str
    cnt: int
    owner: str


def get_task_status_info() -> List[TaskStatusInfo]:
    """get task info
    :return task_info
    """
    assert (Session is not None)

    task_status_query = Session.query(  # pylint: disable=no-member
        TaskInstance.dag_id, TaskInstance.task_id,
        TaskInstance.state, func.count(TaskInstance.dag_id).label('cnt')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()

    sql_res = (
        Session.query(  # pylint: disable=no-member
            task_status_query.c.dag_id, task_status_query.c.task_id,
            task_status_query.c.state, task_status_query.c.cnt, DagModel.owners
        )
        .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
        .join(SerializedDagModel, SerializedDagModel.dag_id == task_status_query.c.dag_id)
        .order_by(task_status_query.c.dag_id)
        .all()
    )

    res = [
        TaskStatusInfo(
            dag_id=i.dag_id,
            task_id=i.task_id,
            status=i.state or 'none',
            cnt=i.cnt,
            owner=i.owners
        )
        for i in sql_res
    ]

    return res


@dataclass
class DagDurationInfo:
    dag_id: str
    duration: float


def get_dag_duration_info() -> List[DagDurationInfo]:
    """get duration of currently running DagRuns
    :return dag_info
    """
    assert (Session is not None)

    driver = Session.bind.driver  # pylint: disable=no-member
    durations = {
        'pysqlite': func.julianday(func.current_timestamp() - func.julianday(DagRun.start_date)) * 86400.0,
        'mysqldb': func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'mysqlconnector': func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'pyodbc': func.sum(func.datediff(text('second'), DagRun.start_date, func.now())),
        'default': func.now() - DagRun.start_date
    }
    duration = durations.get(driver, durations['default'])

    sql_res = (
        Session.query(  # pylint: disable=no-member
            DagRun.dag_id,
            func.max(duration).label('duration')
        )
        .group_by(DagRun.dag_id)
        .filter(DagRun.state == State.RUNNING)
        .join(SerializedDagModel, SerializedDagModel.dag_id == DagRun.dag_id)
        .all()
    )

    res = []

    for i in sql_res:
        if i.duration is not None:
            if driver in ('mysqldb', 'mysqlconnector', 'pysqlite'):
                dag_duration = i.duration
            else:
                dag_duration = i.duration.seconds

            res.append(DagDurationInfo(
                dag_id=i.dag_id,
                duration=dag_duration
            ))

    return res


def get_dag_labels(dag_id: str) -> Dict[str, str]:
    # reuse airflow webserver dagbag
    dag = current_app.dag_bag.get_dag(dag_id)

    if dag is None:
        return dict()

    labels = dag.params.get('labels', {})

    if hasattr(labels, 'value'):
        # Airflow version 2.2.*
        labels = {k: v for k, v in labels.value.items() if not k.startswith('__')}
    else:
        # Airflow version 2.0.*, 2.1.*
        labels = labels.get('__var', {})

    return labels


@dataclass
class ProcessInfo:
    pid: int
    name: str
    cpu_percent: float
    memory_percent: float


def get_processes_info_by_keywords(process_name_keywords: Union[str, List[str]]) -> List[ProcessInfo]:
    """get info about processes with keywords in their names
    :param process_name_keywords: keywords that should be in process names
    :return: list of ProcessInfo"""
    list_of_processes_info = []
    for process in process_iter(attrs=['pid', 'name', 'cpu_percent', 'memory_percent']):
        try:
            process_info = ProcessInfo(process.info['pid'],
                                       process.info['name'],
                                       process.info['cpu_percent'],
                                       process.info['memory_percent'])
            if isinstance(process_name_keywords, str) and process_name_keywords not in process_info.name:
                continue
            if isinstance(process_name_keywords, list):
                if not all(keyword in process_info.name for keyword in process_name_keywords):
                    continue
            list_of_processes_info.append(process_info)
        except (NoSuchProcess, AccessDenied, ZombieProcess) as e:
            log.error(f"Error while retrieving info for process with pid=\"{process.info['pid']}\": {e}")
    if not list_of_processes_info:
        if isinstance(process_name_keywords, list):
            process_name_keywords = ', '.join(process_name_keywords)
        log.error(f"Processes with keywords \"{process_name_keywords}\" in their name were not found")
    return list_of_processes_info


def calculate_total_resource_usage(processes_info: List[ProcessInfo], resource_type: str) -> Optional[float]:
    """calculate total resource usage by certain processes
    :param processes_info: list with info about certain processes
    :param resource_type: type of resource
    :return: total resource usage"""
    total_usage = None
    for process_info in processes_info:
        if not hasattr(process_info, resource_type):
            log.error(f"No '{resource_type}' attribute for object of ProcessInfo class")
            continue
        resource_usage = getattr(process_info, resource_type)
        if not isinstance(resource_usage, (float, int)):
            log.error(f"Type attribute '{resource_type}' does not match the expected (float, int).")
            continue
        total_usage = resource_usage if total_usage is None else total_usage + resource_usage
    return total_usage


def get_total_cpu_percent(list_of_processes_info: List[ProcessInfo]) -> Optional[float]:
    """get total percentage CPU utilization by certain processes
    :param list_of_processes_info: list with info about certain processes
    :return: total percentage CPU utilization"""
    return calculate_total_resource_usage(list_of_processes_info, 'cpu_percent')


def get_total_memory_percent(list_of_processes_info: List[ProcessInfo]) -> Optional[float]:
    """get total percentage memory utilization by certain processes
    :param list_of_processes_info: list with info about certain processes
    :return: total percentage memory utilization"""
    return calculate_total_resource_usage(list_of_processes_info, 'memory_percent')


def calculate_airflow_disk_util(logs_directory: str) -> Optional[float]:
    """calculate disk util by airflow
    :param logs_directory: directory with logs
    :return: percentage disk util"""
    try:
        disk_util = disk_usage(logs_directory)
        return disk_util.percent
    except FileNotFoundError:
        log.error(f"{logs_directory} not found")
        return None
    except OSError as e:
        log.error(f"An error occurred while calculating disk util: {e}")
        return None

def get_scheduler_status(host: str, port: int) -> Optional[float]:
    """get scheduler status
    :param host: host where airflow is deployed
    :param port: port where airflow is deployed
    :return: 1.0 if scheduler status is healthy or 0.0 otherwise or None"""
    url = f"https://{host}:{port}/api/v1/health"
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        health_data = response.json()

        scheduler_status = health_data.get("scheduler", {}).get("status", "unknown")

        if scheduler_status == "healthy":
            return 1.0
        return 0.0
    except requests.exceptions.HTTPError as http_err:
        log.error(f"There was HTTP error: {http_err}")
    except requests.exceptions.RequestException as req_err:
        log.error(f"There was query error: {req_err}")
    return None

def _add_gauge_metric(metric, labels, value):
    metric.samples.append(Sample(
        metric.name, labels,
        value,
        None
    ))


class MetricsCollector(object):
    """collection of metrics for prometheus"""

    @staticmethod
    def describe():
        return []

    @staticmethod
    def collect() -> Generator[Metric, None, None]:
        """collect metrics"""

        # Dag Metrics and collect all labels
        dag_info = get_dag_status_info()

        dag_status_metric = GaugeMetricFamily(
            'airflow_dag_status',
            'Shows the number of dag starts with this status',
            labels=['dag_id', 'owner', 'status']
        )

        for dag in dag_info:
            labels = get_dag_labels(dag.dag_id)

            _add_gauge_metric(
                dag_status_metric,
                {
                    'dag_id': dag.dag_id,
                    'owner': dag.owner,
                    'status': dag.status,
                    **labels
                },
                dag.cnt,
            )

        yield dag_status_metric

        # Count of active dags metric
        count_of_active_dags_metric = GaugeMetricFamily(
            'airflow_count_of_active_dags',
            'Shows the number of active dags (is_paused=false)',
            labels=[]
        )

        count_of_active_dags = 0.0

        dags = get_dag_info()
        for dag in dags:
            if not dag.is_paused:
                count_of_active_dags += 1

        _add_gauge_metric(
            count_of_active_dags_metric,
            {},
            count_of_active_dags
        )

        yield count_of_active_dags_metric

        # Last DagRun Metrics
        last_dagrun_info = get_last_dagrun_info()

        dag_last_status_metric = GaugeMetricFamily(
            'airflow_dag_last_status',
            'Shows the status of last dagrun',
            labels=['dag_id', 'owner', 'status']
        )

        for dag in last_dagrun_info:
            labels = get_dag_labels(dag.dag_id)

            for status in State.dag_states:
                _add_gauge_metric(
                    dag_last_status_metric,
                    {
                        'dag_id': dag.dag_id,
                        'owner': dag.owner,
                        'status': status,
                        **labels
                    },
                    int(dag.status == status)
                )

        yield dag_last_status_metric

        # DagRun metrics
        dag_duration_metric = GaugeMetricFamily(
            'airflow_dag_run_duration',
            'Maximum duration of currently running dag_runs for each DAG in seconds',
            labels=['dag_id']
        )
        for dag_duration in get_dag_duration_info():
            labels = get_dag_labels(dag_duration.dag_id)

            _add_gauge_metric(
                dag_duration_metric,
                {
                    'dag_id': dag_duration.dag_id,
                    **labels
                },
                dag_duration.duration
            )

        yield dag_duration_metric

        # Task metrics
        task_status_metric = GaugeMetricFamily(
            'airflow_task_status',
            'Shows the number of task starts with this status',
            labels=['dag_id', 'task_id', 'owner', 'status']
        )

        for dag_id, tasks in it_group_by(get_task_status_info(), lambda x: x.dag_id):
            labels = get_dag_labels(dag_id)

            for task in tasks:
                _add_gauge_metric(
                    task_status_metric,
                    {
                        'dag_id': task.dag_id,
                        'task_id': task.task_id,
                        'owner': task.owner,
                        'status': task.status,
                        **labels
                    },
                    task.cnt
                )

        yield task_status_metric

        # Webserver resources utilization metrics
        webserver_processes_info = get_processes_info_by_keywords("gunicorn")
        webserver_processes_info += get_processes_info_by_keywords(["airflow", "webserver"])

        if webserver_processes_info:
            # Webserver CPU utilization metrics
            webserver_cpu_util_metric = GaugeMetricFamily(
                'airflow_webserver_cpu_util',
                'Shows CPU utilization for webserver',
                labels=[]
            )

            webserver_cpu_util = get_total_cpu_percent(webserver_processes_info)

            if webserver_cpu_util is not None:
                _add_gauge_metric(
                    webserver_cpu_util_metric,
                    {},
                    webserver_cpu_util
                )

                yield webserver_cpu_util_metric

            # Webserver memory utilization metrics
            webserver_memory_util_metric = GaugeMetricFamily(
                'airflow_webserver_memory_util',
                'Shows memory utilization for webserver',
                labels=[]
            )

            webserver_memory_util = get_total_memory_percent(webserver_processes_info)

            if webserver_memory_util is not None:
                _add_gauge_metric(
                    webserver_memory_util_metric,
                    {},
                    webserver_memory_util
                )

                yield webserver_memory_util_metric

        # Scheduler resources utilization metrics
        scheduler_processes_info = get_processes_info_by_keywords(["airflow", "scheduler"])

        if scheduler_processes_info:
            # Scheduler CPU utilization metrics
            scheduler_cpu_util_metric = GaugeMetricFamily(
                'airflow_scheduler_cpu_util',
                'Shows CPU utilization for scheduler',
                labels=[]
            )

            scheduler_cpu_util = get_total_cpu_percent(scheduler_processes_info)

            if scheduler_cpu_util is not None:
                _add_gauge_metric(
                    scheduler_cpu_util_metric,
                    {},
                    scheduler_cpu_util
                )

                yield scheduler_cpu_util_metric

            # Scheduler memory utilization metrics
            scheduler_memory_util_metric = GaugeMetricFamily(
                'airflow_scheduler_memory_util',
                'Shows memory utilization for scheduler',
                labels=[]
            )

            scheduler_memory_util = get_total_memory_percent(scheduler_processes_info)

            if scheduler_memory_util is not None:
                _add_gauge_metric(
                    scheduler_memory_util_metric,
                    {},
                    scheduler_memory_util
                )

                yield scheduler_memory_util_metric

        # FS usage metrics
        logs_directory = conf.get("logging", "base_log_folder")

        disk_util_metric = GaugeMetricFamily(
            'airflow_disk_util',
            'Shows disk utilization for airflow',
            labels=['logs_directory']
        )

        disk_util = calculate_airflow_disk_util(logs_directory)

        if disk_util is not None:
            _add_gauge_metric(
                disk_util_metric,
                {
                    'logs_directory': logs_directory
                },
                disk_util
            )

            yield disk_util_metric

        # Availability of services metrics
        healthcheck_metric = GaugeMetricFamily(
            'airflow_healthcheck',
            'Shows scheduler status (healthy is 1.0)',
            labels=[]
        )

        base_url = urlparse(request.base_url)
        scheduler_status = get_scheduler_status(host=base_url.hostname, port=base_url.port)

        if scheduler_status is not None:
            _add_gauge_metric(
                healthcheck_metric,
                {},
                scheduler_status
            )
            yield healthcheck_metric


REGISTRY.register(MetricsCollector())


class RBACMetrics(FABBaseView):
    route_base = "/metrics"

    @fab_expose('/')
    def list(self):
        return Response(generate_latest(), mimetype='text')


# Metrics View for Flask app builder used in airflow with rbac enabled
RBACmetricsView = {
    "view": RBACMetrics(),
    "name": "Metrics SE",
    "category": "Sber Edition"
}


class AirflowExporterSEPlugin(AirflowPlugin):
    """plugin for show metrics"""
    name = "exporter_se_plugin"
    operators = []  # type: ignore
    hooks = []  # type: ignore
    executors = []  # type: ignore
    macros = []  # type: ignore
    admin_views = []  # type: ignore
    flask_blueprints = []  # type: ignore
    menu_links = []  # type: ignore
    appbuilder_views = [RBACmetricsView]
    appbuilder_menu_items = []  # type: ignore
