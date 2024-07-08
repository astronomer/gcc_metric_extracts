import os
from dotenv import find_dotenv, load_dotenv
import pytest
from google.protobuf.json_format import MessageToDict
from gcc_metric_extracts.gcc_utils import (
    get_dashboard,
    GccReportGenerator,
    time_series_query,
    time_series_query_df,
    get_df_from_mql_queries,
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
def test_get_df_from_mql_queries(gcc_rg):
    mql_query_df = get_df_from_mql_queries(
        project_id=os.getenv("PROJECT_ID"),
        metric="worker_count",
        mql_queries=gcc_rg.mql["worker_count"],
    )
    print(mql_query_df)
    print(mql_query_df.shape)
    assert mql_query_df.drop_nulls().shape == mql_query_df.shape


@integration_test
def test_generate_usage_report(gcc_rg):
    usage_df = gcc_rg.generate_usage_report()
    print(usage_df)
    assert usage_df.drop_nulls().shape == usage_df.shape


@integration_test
def test_gcc_utilization_to_astro(gcc_rg):
    averages = gcc_rg.gcc_utilization_to_astro()
    print(averages)
