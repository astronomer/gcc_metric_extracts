import os
from dotenv import find_dotenv, load_dotenv
import logging
import pytest
from google.protobuf.json_format import MessageToDict
from gcc_metric_extracts.gcc_utils import (
    UsageMinMax,
    get_dashboard,
    GccReportGenerator,
    time_series_query,
    time_series_query_df,
)


env_file = find_dotenv(".env.tests")
if env_file:
    load_dotenv(env_file)


integration_test = pytest.mark.skipif(
    bool(os.getenv("INTEGRATION_TEST")) is False, reason="Integration Test"
)


@pytest.fixture
def gcc_rg():
    return GccReportGenerator(
        project_id=os.getenv("PROJECT_ID"),
        cluster=os.getenv("CLUSTER"),
        environment_name=os.getenv("ENVIRONMENT_NAME"),
        location=os.getenv("LOCATION"),
    )


@pytest.fixture
def missing_data_rg():
    class MisconfiguredRG(GccReportGenerator):
        def __init__(
            self,
            project_id: str,
            cluster: str,
            environment_name: str,
            location: str,
            agg: str = "1m",
            lookback: int = 30,
        ) -> None:
            super().__init__(
                project_id, cluster, environment_name, location, agg, lookback
            )

        @property
        def worker_count(self) -> UsageMinMax:
            return UsageMinMax(
                f"""fetch cloud_composer_environment
                | metric 'composer.googleapis.com/environment/num_celery_workers'
                | filter
                    (resource.environment_name == 'misconfigured_resource'
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
                    && resource.location == 'wrong_region')
                | group_by {self.agg}, [value_max_workers_min: min(value.max_workers)]
                | every {self.agg}
                | group_by [], [value_max_workers_min_max: max(value_max_workers_min)] | within {self.lookback}""",  # noqa: E501
            )

    return MisconfiguredRG(
        project_id=os.getenv("PROJECT_ID"),
        cluster=os.getenv("CLUSTER"),
        environment_name=os.getenv("ENVIRONMENT_NAME"),
        location=os.getenv("LOCATION"),
    )


@integration_test
def test_get_dashboard():
    response = get_dashboard(name=os.getenv("DASHBOARD"))
    assert response.mosaic_layout
    assert response


@integration_test
def test_time_series_query(gcc_rg):
    raw_data = time_series_query(
        project_id=os.getenv("PROJECT_ID"),
        query=gcc_rg.mql["scheduler_cpu"].used,
    )
    raw_dict = MessageToDict(raw_data._pb)
    print(raw_dict)

    print(gcc_rg.mql["scheduler_cpu"].used)
    data = time_series_query_df(
        project_id=os.getenv("PROJECT_ID"),
        query=gcc_rg.mql["scheduler_cpu"].used,
    )
    print(data)
    print(data.schema)

    assert not data.is_empty()


@integration_test
def test_generate_usage_report(gcc_rg):
    usage_df = gcc_rg.generate_usage_report()
    print(usage_df)
    assert usage_df.drop_nulls().shape == usage_df.shape


@integration_test
def test_gcc_utilization_to_astro(gcc_rg):
    averages = gcc_rg.gcc_utilization_summary()
    print(averages)


@integration_test
def test_misconfigured_metric(missing_data_rg, caplog):
    with caplog.at_level(logging.WARNING):
        missing_data_rg.gcc_utilization_summary()
    assert len(caplog.records) > 1
    assert len(["No data found " in x.message for x in caplog.records]) > 0
