from datetime import datetime, timedelta
from collections import namedtuple
import logging
import os
from typing import Union

import polars as pl

from google.cloud import monitoring_v3, monitoring_dashboard_v1
from google.protobuf.json_format import MessageToDict  # type: ignore

logger = logging.getLogger(name=__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)

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


class GccReportGenerator:
    def __init__(
        self,
        project_id: str,
        cluster: str,
        environment_name: str,
        location: str,
        agg: str = "1m",
        lookback: int = 30,
    ) -> None:
        self.project_id = project_id
        self.cluster = cluster
        self.environment_name = environment_name
        self.location = location
        self.agg = agg
        # set the end date to be beginning of tomorrow
        today = (datetime.today() + timedelta(days=1)).strftime("%Y/%m/%d 00:00")
        self.lookback = f"{lookback}d, d'{today}'"
        self.logger = logger

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

    def get_df_from_mql_queries(
        self, metric: str, mql_queries: Union[UsageLimit, UsageMinMax]
    ) -> pl.DataFrame:
        df_list = [
            time_series_query_df(
                project_id=self.project_id,
                query=query,
                value_col=f"{metric}_{query_key}",
            )
            for query_key, query in mql_queries._asdict().items()
        ]
        return pl.concat(df_list, how="align")

    # todo make async calls to dashboard endpoints
    def generate_usage_report(self) -> pl.DataFrame:
        self.logger.info(f"Retrieving metrics for environment {self.environment_name}")
        data = [
            self.get_df_from_mql_queries(metric=metric, mql_queries=mql_queries)
            for metric, mql_queries in self.mql.items()
        ]

        output = pl.concat(data, how="align")
        self.logger.info(
            f"Utilization metrics retrieved for environment {self.environment_name}"
        )
        output_file = (
            f"{self.project_id}_{self.environment_name}_{self.cluster}_raw.csv"
        )
        output.write_csv(output_file)
        self.logger.info(
            f"GCC utlization metrics saved to {os.path.join(os.getcwd(), output_file)}"
        )

        return output

    def gcc_utilization_summary(
        self, zero_utilization_threshold: float = 0.36
    ) -> pl.DataFrame:
        usage_df = self.generate_usage_report()
        self.logger.info("summarizing utilization metrics")
        zero_utilization = (
            (
                usage_df.filter(
                    pl.col("worker_cpu_used") / pl.col("worker_cpu_limit")
                    >= zero_utilization_threshold
                ).select(pl.len().alias("zero_utilization_percentage"))
            )
            / usage_df.select(pl.len())
            * 100
        )

        worker_averages = usage_df.select(pl.col("^worker.*$")).mean()
        scheduler_maxes = usage_df.select(pl.col("^scheduler.*$")).max()
        utilization = worker_averages.with_columns(*scheduler_maxes, *zero_utilization)
        self.logger.info(
            f"GCC Utilization metrics summarized for env {self.environment_name}"
        )
        output_file = (
            f"{self.project_id}_{self.environment_name}_{self.cluster}_report.csv"
        )

        utilization.write_csv(output_file)
        self.logger.info(
            f"GCC Utilization Summary saved to {os.path.join(os.getcwd(), output_file)}"
        )


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
