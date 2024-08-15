from datetime import datetime, timedelta
from collections import namedtuple
from logging import getLogger
from typing import Union
import polars as pl

from google.cloud import monitoring_v3, monitoring_dashboard_v1
from google.protobuf.json_format import MessageToDict  # type: ignore

logger = getLogger(name=__name__)
UsageLimit = namedtuple("UsageLimit", "used limit")
UsageMinMax = namedtuple("UsageMinMax", "used min max")

# todo: implement builder pattern for MQL
# class MQLBuilder:
#     def __init__(self) -> None:
#         self.query = {}

#     def fetch(self, object) -> :
#         self.query["fetch"] = object

#         return self

#     def metric(self, metric):
#         self.query["metric"] = metric

#         return self

#     def filter(self, mql_filter):
#         self.query["filter"] = mql_filter

#     def group_by(self, group_by):
#         self.query["group_by"] = "[]" + group_by


class MQLGenerator:
    def __init__(
        self,
        cluster: str,
        environment_name: str,
        location: str,
        agg: str = "60m",
        lookback: int = 30,
    ) -> None:
        self.cluster = cluster
        self.environment_name = environment_name
        self.location = location
        self.agg = agg
        # set the end date to be beginning of tomorrow
        today = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d 00:00")
        self.lookback = f"{lookback}d, d'{today}'"

        self.mql: dict[str, Union[UsageLimit, UsageMinMax]] = {
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
            [value_used_bytes_mean_aggregate: aggregate(value_used_bytes_mean)] | within {self.lookback}""",  # noqa: E501
            f"""fetch k8s_container
        | metric 'kubernetes.io/container/memory/limit_bytes'
        | filter
            (resource.cluster_name == '{self.cluster}'
            && resource.pod_name =~ 'airflow-scheduler-.*')
        | group_by {self.agg}, [value_limit_bytes_mean: mean(value.limit_bytes)]
        | every {self.agg}
        | group_by [],
            [value_limit_bytes_mean_aggregate: aggregate(value_limit_bytes_mean)] | within {self.lookback}""",  # noqa: E501
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
                [value_core_usage_time_aggregate: aggregate(value.core_usage_time)] | within {self.lookback}""",  # noqa: E501
            f"""fetch k8s_container
                | metric 'kubernetes.io/container/cpu/limit_cores'
                | filter
                    (resource.cluster_name == '{self.cluster}'
                    && resource.pod_name =~ 'airflow-scheduler-.*')
                | group_by {self.agg}, [value_limit_cores_mean: mean(value.limit_cores)]
                | every {self.agg}
                | group_by [],
                    [value_limit_cores_mean_aggregate: aggregate(value_limit_cores_mean)] | within {self.lookback}""",  # noqa: E501
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
                [value_core_usage_time_aggregate: aggregate(value.core_usage_time)] | within {self.lookback}""",  # noqa: E501
            f"""fetch k8s_container
            | metric 'kubernetes.io/container/cpu/limit_cores'
            | filter
                (resource.cluster_name == '{self.cluster}'
                && resource.pod_name =~ 'airflow-worker-.*'
                && resource.pod_name !~ 'airflow-worker-set-.*')
            | group_by {self.agg}, [value_limit_cores_mean: mean(value.limit_cores)]
            | every {self.agg}
            | group_by [],
                [value_limit_cores_mean_aggregate: aggregate(value_limit_cores_mean)] | within {self.lookback}""",  # noqa: E501
        )

    @property
    def worker_memory(self) -> UsageLimit:
        return UsageLimit(
            f"""fetch k8s_container
            | metric 'kubernetes.io/container/memory/used_bytes'
            | filter
                (resource.cluster_name == '{self.cluster}'
                && resource.pod_name =~ 'airflow-worker-.*'
                && resource.pod_name !~ 'airflow-worker-set-.*')
                && (metric.memory_type == 'non-evictable')
            | group_by {self.agg}, [value_used_bytes_mean: mean(value.used_bytes)]
            | every {self.agg}
            | group_by [],
                [value_used_bytes_mean_aggregate: aggregate(value_used_bytes_mean)] | within {self.lookback}""",  # noqa: E501
            f"""fetch k8s_container
            | metric 'kubernetes.io/container/memory/limit_bytes'
            | filter
                (resource.cluster_name == '{self.cluster}'
                && resource.pod_name =~ 'airflow-worker-.*'
                && resource.pod_name !~ 'airflow-worker-set-.*')
            | group_by {self.agg}, [value_limit_bytes_mean: mean(value.limit_bytes)]
            | every {self.agg}
            | group_by [],
                [value_limit_bytes_mean_aggregate: aggregate(value_limit_bytes_mean)] | within {self.lookback}""",  # noqa: E501
        )

    @property
    def worker_count(self) -> UsageMinMax:
        return UsageMinMax(
            f"""fetch cloud_composer_environment
            | metric 'composer.googleapis.com/environment/num_celery_workers'
            | filter
                (resource.environment_name == '{self.environment_name}'
                && resource.location == '{self.location}')
            | group_by {self.agg}, [value_num_celery_workers_min: min(value.num_celery_workers)]
            | every {self.agg}
            | group_by [],
                [value_num_celery_workers_min_aggregate:
                aggregate(value_num_celery_workers_min)] | within {self.lookback}""",  # noqa: E501
            f"""fetch cloud_composer_environment
            | metric 'composer.googleapis.com/environment/worker/min_workers'
            | filter
                (resource.environment_name == '{self.environment_name}'
                && resource.location == '{self.location}')
            | group_by {self.agg}, [value_min_workers_min: min(value.min_workers)]
            | every {self.agg}
            | group_by [], [value_min_workers_min_min: min(value_min_workers_min)] | within {self.lookback}""",  # noqa: E501
            f"""fetch cloud_composer_environment
            | metric 'composer.googleapis.com/environment/worker/max_workers'
            | filter
                (resource.environment_name == '{self.environment_name}'
                && resource.location == '{self.location}')
            | group_by {self.agg}, [value_max_workers_min: min(value.max_workers)]
            | every {self.agg}
            | group_by [], [value_max_workers_min_max: max(value_max_workers_min)] | within {self.lookback}""",  # noqa: E501
        )


class AstroResourceMapper:
    def __init__(self, utilization_df: pl.DataFrame) -> None:
        self.utilization_df = utilization_df

    @property
    def schedulers(self) -> list[dict[str, Union[str, int]]]:
        size_dict = {0: "small", 1: "medium", 2: "large"}
        return [
            {
                "size": size_dict.get(i, 0),
                "cpu": 2**i,
                "memory": (2 ** (i + 1)) * (2**30),
            }
            for i in range(3)
        ]

    def scheduler_size(
        self,
        scheduler_memory_col: str = "scheduler_memory_used",
        scheduler_cpu_col: str = "scheduler_cpu_used",
    ) -> None:
        memory = self.utilization_df.select(scheduler_memory_col).item()
        cpu = self.utilization_df.select(scheduler_cpu_col).item()

        # by default set largest
        recommended_sched = self.schedulers[-1]["size"]
        # if mem and cpu less than small or med, set
        for sched in self.schedulers:
            if memory < sched["memory"] and cpu < sched["cpu"]:
                recommended_sched = sched["size"]
                break

        self.utilization_df = self.utilization_df.with_columns(
            pl.lit(recommended_sched).alias("astro_scheduler_size")
        )

    def worker_size(
        self,
        worker_memory_used: str = "worker_memory_used",
        worker_cpu_col: str = "worker_cpu_used",
    ) -> None:
        # worker cpu and memory into fractions of an a5
        self.utilization_df = self.utilization_df.with_columns(
            self.utilization_df.select(
                pl.col(worker_cpu_col), (pl.col(worker_memory_used) / (2 * (2**30)))
            )
            .max_horizontal()
            .alias("a5_workers")
        )

    def map_resources(self) -> pl.DataFrame:
        self.worker_size()

        self.scheduler_size()

        return self.utilization_df


def get_dashboard(name: str) -> monitoring_dashboard_v1.Dashboard:
    # Create a client
    client = monitoring_dashboard_v1.DashboardsServiceClient()

    # Initialize request argument(s)p
    request = monitoring_dashboard_v1.GetDashboardRequest(
        name=name,
    )

    # Make the request
    response = client.get_dashboard(request=request)

    return response


def get_dashboard_by_project_and_id(
    project_id: str, dashboard_id: str
) -> monitoring_dashboard_v1.Dashboard:
    client = monitoring_dashboard_v1.DashboardsServiceClient()

    # Initialize request argument(s)
    request = monitoring_dashboard_v1.GetDashboardRequest(
        name=f"projects/{project_id}/dashboards/{dashboard_id}",
    )

    # Make the request
    response = client.get_dashboard(request=request)

    return response


def time_series_query(
    project_id: str,
    query: str,
    page_size: int = 100,
) -> monitoring_v3.ListTimeSeriesResponse:
    metric_client = monitoring_v3.QueryServiceClient()

    query_requests = monitoring_v3.QueryTimeSeriesRequest(
        name=f"projects/{project_id}", query=query, page_size=page_size
    )

    time_series_data = metric_client.query_time_series(request=query_requests)

    return time_series_data


def time_series_query_df(
    project_id: str,
    query: str,
    page_size: int = 100,
    value_col: Union[str, None] = None,
) -> pl.DataFrame:
    raw_data_pb = time_series_query(
        project_id=project_id, query=query, page_size=page_size
    )
    raw_data_dict = MessageToDict(raw_data_pb._pb)
    if not raw_data_dict["timeSeriesDescriptor"]:
        logger.warning(f"No data found for query {value_col}")
        return None

    data_type = f"{raw_data_dict['timeSeriesDescriptor']['pointDescriptors'][0]['valueType'].lower()}Value"  # noqa: E501

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
            "timestamp": pl.String,
            value_col if value_col else "value": dtype_maping.get(data_type),
        },
    )
    df = df.with_columns(
        pl.col("timestamp").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.6fZ")
    )

    return df


def get_df_from_mql_queries(
    project_id: str, metric: str, mql_queries: Union[UsageLimit, UsageMinMax]
) -> pl.DataFrame:
    df_list = [
        time_series_query_df(
            project_id=project_id, query=query, value_col=f"{metric}_{query_key}"
        )
        for query_key, query in mql_queries._asdict().items()
    ]
    valid_dfs = list(filter(lambda df: df is not None, df_list))

    if not valid_dfs:
        raise Exception(
            "No data found for any metrics, check your input arguments and permissions"
        )

    return pl.concat(valid_dfs, how="align")
