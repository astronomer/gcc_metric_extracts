import argparse

from gcc_utils import GccReportGenerator

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--project_id",
        required=True,
        help="Google Cloud Project ID associated with the Cloud Composer environment",
    )
    parser.add_argument(
        "-e",
        "--environment_name",
        required=True,
        help="Name of the Cloud Composer Environment",
    )
    parser.add_argument(
        "-c",
        "--cluster",
        required=True,
        help="""Name of the GKE cluster running Cloud Composer Airflow components. \
                Available via the view cluster details link found in the\
                      Cloud Composer Environment Configuration tab""",
    )
    parser.add_argument(
        "-l",
        "--location",
        required=True,
        help="""Location of the configured environment i.e., us-central1""",
    )
    parser.add_argument(
        "--lookback",
        default=30,
        required=False,
        help="Number of days to look back to generate usage report",
    )

    args = parser.parse_args()

    rg = GccReportGenerator(**vars(args))
    rg.gcc_utilization_summary()
