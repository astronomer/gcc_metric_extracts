import argparse

from gcc_utils import gcc_utilization_to_astro

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
        "--lookback",
        default=30,
        required=False,
        help="Number of days to look back to generate usage report",
    )

    args = parser.parse_args()

    gcc_utilization_to_astro(**vars(args))
