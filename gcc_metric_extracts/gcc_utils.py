from datetime import datetime, timedelta
from collections import namedtuple
from typing import Union
from collections import defaultdict
import polars as pl

from google.cloud import monitoring_v3, monitoring_dashboard_v1
from google.protobuf.json_format import MessageToDict

# type corresponds to google's data type (i.e. 'double_value', 'int64_value')
UsageLimit = namedtuple("UsageLimit", "used limit")
UsageMinMax = namedtuple("UsageMinMax", "used min max")


# todo: implement builder pattern for MQL
class MQLBuilder:

    def __init__(self):
        self.query = {}

    def fetch(self, object):
        self.query["fetch"] = object

        return self

    def metric(self, metric):
        self.query["metric"] = metric

        return self

    def filter(self, mql_filter):
        self.query["filter"] = mql_filter

    def group_by(self, group_by):
        self.query["group_by"] = "[]" + group_by


class MQLGenerator:
    def __init__(
        self, cluster: str, environment_name: str, agg: str = "60m", lookback: int = 30
    ):
        self.cluster = cluster
        self.environment_name = environment_name
        self.agg = agg
        # set the end date to be beginning of tomorrow
        today = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d 00:00")
        self.lookback = f"{lookback}d, d'{today}'"

        self.mql = {
            "scheduler_memory": self.scheduler_memory,
            "scheduler_cpu": self.scheduler_cpu,
            "worker_cpu": self.worker_cpu,
            "worker_memory": self.worker_memory,
            "worker_count": self.worker_count,
        }

    @property
    def scheduler_memory(self) -> UsageLimit:
        return UsageLimit(
            f"""fetch k8s_container
        | metric 'kubernetes.io/container/memory/used_bytes'
        | filter
            (resource.cluster_name == '{self.cluster}'
             && resource.pod_name =~ 'airflow-scheduler-.*')
        | group_by {self.agg}, [value_used_bytes_mean: mean(value.used_bytes)]
        | every {self.agg}
        | group_by [],
            [value_used_bytes_mean_aggregate: aggregate(value_used_bytes_mean)] | within {self.lookback}""",
            f"""fetch k8s_container
        | metric 'kubernetes.io/container/memory/limit_bytes'
        | filter
            (resource.cluster_name == '{self.cluster}'
            && resource.pod_name =~ 'airflow-scheduler-.*')
        | group_by {self.agg}, [value_limit_bytes_mean: mean(value.limit_bytes)]
        | every {self.agg}
        | group_by [],
            [value_limit_bytes_mean_aggregate: aggregate(value_limit_bytes_mean)] | within {self.lookback}""",
        )

    @property
    def scheduler_cpu(self) -> UsageLimit:
        return UsageLimit(
            f"""fetch k8s_container
            | metric 'kubernetes.io/container/cpu/core_usage_time'
            | filter
                (resource.cluster_name == '{self.cluster}'
                && resource.pod_name =~ 'airflow-scheduler-.*')
            | align rate({self.agg})
            | every {self.agg}
            | group_by [],
                [value_core_usage_time_aggregate: aggregate(value.core_usage_time)] | within {self.lookback}""",
            f"""fetch k8s_container
                | metric 'kubernetes.io/container/cpu/limit_cores'
                | filter
                    (resource.cluster_name == '{self.cluster}'
                    && resource.pod_name =~ 'airflow-scheduler-.*')
                | group_by {self.agg}, [value_limit_cores_mean: mean(value.limit_cores)]
                | every {self.agg}
                | group_by [],
                    [value_limit_cores_mean_aggregate: aggregate(value_limit_cores_mean)] | within {self.lookback}""",
        )

    @property
    def worker_cpu(self) -> UsageLimit:
        return UsageLimit(
            f"""fetch k8s_container
            | metric 'kubernetes.io/container/cpu/core_usage_time'
            | filter
                (resource.cluster_name == '{self.cluster}'
                && resource.pod_name =~ 'airflow-worker-.*'
                && resource.pod_name !~ 'airflow-worker-set-.*')
            | align rate({self.agg})
            | every {self.agg}
            | group_by [],
                [value_core_usage_time_aggregate: aggregate(value.core_usage_time)] | within {self.lookback}""",
            f"""fetch k8s_container
            | metric 'kubernetes.io/container/cpu/limit_cores'
            | filter
                (resource.cluster_name == '{self.cluster}'
                && resource.pod_name =~ 'airflow-worker-.*'
                && resource.pod_name !~ 'airflow-worker-set-.*')
            | group_by {self.agg}, [value_limit_cores_mean: mean(value.limit_cores)]
            | every {self.agg}
            | group_by [],
                [value_limit_cores_mean_aggregate: aggregate(value_limit_cores_mean)] | within {self.lookback}""",
        )

    @property
    def worker_memory(self) -> UsageLimit:
        return UsageLimit(
            f"""fetch k8s_container
            | metric 'kubernetes.io/container/memory/used_bytes'
            | filter
                (resource.cluster_name == 'us-central1-composer-datapl-af554142-gke'
                && resource.pod_name =~ 'airflow-worker-.*'
                && resource.pod_name !~ 'airflow-worker-set-.*')
                && (metric.memory_type == 'non-evictable')
            | group_by {self.agg}, [value_used_bytes_mean: mean(value.used_bytes)]
            | every {self.agg}
            | group_by [],
                [value_used_bytes_mean_aggregate: aggregate(value_used_bytes_mean)] | within {self.lookback}""",
            f"""fetch k8s_container
            | metric 'kubernetes.io/container/memory/limit_bytes'
            | filter
                (resource.cluster_name == 'us-central1-composer-datapl-af554142-gke'
                && resource.pod_name =~ 'airflow-worker-.*'
                && resource.pod_name !~ 'airflow-worker-set-.*')
            | group_by {self.agg}, [value_limit_bytes_mean: mean(value.limit_bytes)]
            | every {self.agg}
            | group_by [],
                [value_limit_bytes_mean_aggregate: aggregate(value_limit_bytes_mean)] | within {self.lookback}""",
        )

    @property
    def worker_count(self) -> UsageMinMax:
        return UsageMinMax(
            f"""fetch cloud_composer_environment
            | metric 'composer.googleapis.com/environment/num_celery_workers'
            | filter
                (resource.environment_name == '{self.environment_name}'
                && resource.location == 'us-central1')
            | group_by {self.agg}, [value_num_celery_workers_min: min(value.num_celery_workers)]
            | every {self.agg}
            | group_by [],
                [value_num_celery_workers_min_aggregate:
                aggregate(value_num_celery_workers_min)] | within {self.lookback}""",
            f"""fetch cloud_composer_environment
            | metric 'composer.googleapis.com/environment/worker/min_workers'
            | filter
                (resource.environment_name == 'composer-dataplex-testing'
                && resource.location == 'us-central1')
            | group_by {self.agg}, [value_min_workers_min: min(value.min_workers)]
            | every {self.agg}
            | group_by [], [value_min_workers_min_min: min(value_min_workers_min)] | within {self.lookback}""",
            f"""fetch cloud_composer_environment
            | metric 'composer.googleapis.com/environment/worker/max_workers'
            | filter
                (resource.environment_name == 'composer-dataplex-testing'
                && resource.location == 'us-central1')
            | group_by {self.agg}, [value_max_workers_min: min(value.max_workers)]
            | every {self.agg}
            | group_by [], [value_max_workers_min_max: max(value_max_workers_min)] | within {self.lookback}""",
        )


class AstroResourceMapper:
    SCHEDULER_RESOURCES = [
        {
            "size": {0: "small", 1: "medium", 2: "large"}.get(i),
            "cpu": 2**i,
            "memory": (2 ** (i + 1)) * (2**30),
        }
        for i in range(3)
    ]

    WORKER_RESOURCES = [
        {"size": f"A{5*(2**i)}", "cpu": 2**i, "memory": (2 ** (i + 1)) * (2**30)}
        for i in range(6)
    ]

    def __init__(self) -> None:
        pass

    def scheduler_size():
        pass

    def worker_size():
        pass


def get_dashboard(name: str):
    # Create a client
    client = monitoring_dashboard_v1.DashboardsServiceClient()

    # Initialize request argument(s)p
    request = monitoring_dashboard_v1.GetDashboardRequest(
        name=name,
    )

    # Make the request
    response = client.get_dashboard(request=request)

    return response


def get_dashboard_by_project_and_id(project_id: str, dashboard_id: str):
    client = monitoring_dashboard_v1.DashboardsServiceClient()

    # Initialize request argument(s)
    request = monitoring_dashboard_v1.GetDashboardRequest(
        name=f"projects/{project_id}/dashboards/{dashboard_id}",
    )

    # Make the request
    response = client.get_dashboard(request=request)

    return response


def get_mosaic_titles_and_queries(dashboard: monitoring_dashboard_v1.Dashboard):
    """
    util for retrieiving chart title and filters
    """
    return [
        (
            d.widget.title,
            [dataset.time_series_query for dataset in d.widget.xy_chart.data_sets],
        )
        for d in dashboard.mosaic_layout.tiles
    ]


def get_time_series_data_for_filter(
    project_id: str,
    filter,
    metric_client: monitoring_v3.MetricServiceClient = None,
    interval: timedelta | monitoring_v3.TimeInterval = timedelta(days=14),
):
    if isinstance(interval, timedelta):
        end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - interval
        interval = monitoring_v3.TimeInterval(
            mapping={"end_time": end, "start_time": start}
        )

    if not metric_client:
        metric_client = monitoring_v3.MetricServiceClient()

    time_series_data = metric_client.list_time_series(
        name=f"projects/{project_id}", filter=filter, interval=interval
    )

    return time_series_data


def process_time_series_data(time_series_data: monitoring_v3.ListTimeSeriesResponse):

    all_data = []
    for t in time_series_data:
        data = [
            {
                "timestamp": point.interval.end_time,
                "value": point.value.__dict__,
            }
            for point in t.points
        ]
        all_data.extend(data)
    return all_data


def time_series_query(
    project_id,
    query,
    page_size=100,
):
    metric_client = monitoring_v3.QueryServiceClient()

    query_requests = monitoring_v3.QueryTimeSeriesRequest(
        name=f"projects/{project_id}", query=query, page_size=page_size
    )

    time_series_data = metric_client.query_time_series(request=query_requests)

    return time_series_data


def time_series_query_df(project_id, query, page_size=100, value_col=None):
    raw_data_pb = time_series_query(
        project_id=project_id, query=query, page_size=page_size
    )
    raw_data_dict = MessageToDict(raw_data_pb._pb)
    data_type = f"{raw_data_dict['timeSeriesDescriptor']['pointDescriptors'][0]['valueType'].lower()}Value"

    dtype_maping = {
        "doubleValue": pl.Float64,
        "int64Value": pl.Int64,
        "stringValue": pl.String,
    }

    timestamp_value_tuples = [
        (point["timeInterval"]["endTime"], point["values"][0][data_type])
        for point in raw_data_dict["timeSeriesData"][0]["pointData"]
    ]

    df = pl.DataFrame(
        timestamp_value_tuples,
        schema={
            "timestamp": None,
            value_col if value_col else "value": dtype_maping.get(data_type),
        },
    )
    df = df.with_columns(
        pl.col("timestamp").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.6fZ")
    )

    return df


def get_df_from_mql_queries(
    project_id: str, metric: str, mql_queries: Union[UsageLimit, UsageMinMax]
):
    df_list = [
        time_series_query_df(
            project_id=project_id, query=query, value_col=f"{metric}_{query_key}"
        )
        for query_key, query in mql_queries._asdict().items()
    ]
    return pl.concat(df_list, how="align")


def generate_usage_report(
    project_id: str,
    environment_name: str,
    cluster: str,
    lookback: int = 30,
    mql: MQLGenerator = None,
):
    if not mql:
        mql = MQLGenerator(
            environment_name=environment_name, cluster=cluster, lookback=lookback
        )

    # todo: use google's async client instead

    data = [
        get_df_from_mql_queries(
            project_id=project_id, metric=metric, mql_queries=mql_queries
        )
        for metric, mql_queries in mql.mql.items()
    ]

    output = pl.concat(data, how="align")

    output.write_csv(f"{project_id}_{environment_name}_{cluster}_report.csv")

    return output


def gcc_utilization_to_astro(
    project_id: str,
    environment_name: str,
    cluster: str,
    lookback: int = 30,
    mql: MQLGenerator = None,
    zero_utilization_threshold=0.36,
):
    usage_df = generate_usage_report(
        project_id, environment_name, cluster, lookback, mql
    )
    zero_utilization = (
        (
            usage_df.filter(
                pl.col("worker_cpu_used") / pl.col("worker_cpu_limit")
                >= zero_utilization_threshold
            ).select(pl.len())
        )
        / usage_df.select(pl.len())
        * 100
    )

    worker_averages = usage_df.select(pl.col("^worker.*$")).mean()
    scheduler_maxes = usage_df.select(pl.col("^scheduler.*$")).max()

    utilization = worker_averages.with_columns(
        scheduler_maxes, pl.lit(zero_utilization).alias("zero_utlization_percentage")
    )
    return utilization
