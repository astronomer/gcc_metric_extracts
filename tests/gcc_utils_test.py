import os
from dotenv import find_dotenv, load_dotenv
import pytest

from gcc_metric_extracts.gcc_utils import (
    get_dashboard,
    get_dashboard_by_project_and_id,
    get_mosaic_titles_and_queries,
    MQLGenerator,
    time_series_query_df,
    get_df_from_mql_queries,
    generate_usage_report,
    gcc_utilization_to_astro,
)


@pytest.fixture(scope="session", autouse=True)
def load_env():
    env_file = find_dotenv(".env.tests")
    load_dotenv(env_file)


@pytest.fixture
def mql():
    return MQLGenerator(
        cluster=os.getenv("CLUSTER"),
        environment_name=os.getenv("ENVIRONMENT_NAME"),
        lookback=5,
    )


integration_test = pytest.mark.skipif(
    bool(os.getenv("INTEGRATION_TEST")) is False, reason="Integration Test"
)


@integration_test
def test_get_dashboard():
    response = get_dashboard(name=os.getenv("DASHBOARD"))
    assert response.mosaic_layout
    assert response


@integration_test
def test_get_mosaic_queries():
    dashboard = get_dashboard_by_project_and_id(
        project_id=os.getenv("PROJECT_ID"), dashboard_id=os.getenv("DASHBOARD_ID")
    )
    print(dashboard)
    titles_and_filters = get_mosaic_titles_and_queries(dashboard)
    print(titles_and_filters)
    assert titles_and_filters


@integration_test
def test_time_series_query():
    mql = MQLGenerator(
        cluster=os.getenv("CLUSTER"),
        environment_name=os.getenv("ENVIRONMENT_NAME"),
        lookback="1d",
    )
    data = time_series_query_df(
        project_id=os.getenv("PROJECT_ID"),
        query=mql.mql["scheduler_cpu"].used,
    )
    print(data)
    print(data.schema)

    assert not data.is_empty()


@integration_test
def test_get_df_from_mql_queries(mql):
    mql_query_df = get_df_from_mql_queries(
        project_id=os.getenv("PROJECT_ID"),
        metric="worker_count",
        mql_queries=mql.mql["worker_count"],
    )
    print(mql_query_df)
    print(mql_query_df.shape)
    assert mql_query_df.drop_nulls().shape == mql_query_df.shape


def test_generate_usage_report(mql):
    usage_df = generate_usage_report(
        project_id=os.getenv("PROJECT_ID"),
        cluster=os.getenv("CLUSTER"),
        environment_name=os.getenv("ENVIRONMENT_NAME"),
    )
    print(usage_df)
    assert usage_df.drop_nulls().shape == usage_df.shape


def test_gcc_utilization_to_astro():
    averages = gcc_utilization_to_astro(
        project_id=os.getenv("PROJECT_ID"),
        cluster=os.getenv("CLUSTER"),
        environment_name=os.getenv("ENVIRONMENT_NAME"),
    )
    print(averages)
