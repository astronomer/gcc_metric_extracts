import argparse

from gcc_utils import GccReportGenerator, Gcc3ReportGenerator

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
        required=False,
        help="""Name of the GKE cluster running Cloud Composer Airflow components. \
                    Not required if using the --gcc3 flag. \
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

    # ←–– New flag to pick the GCC3 subclass
    parser.add_argument(
        "--gcc3",
        action="store_true",
        help="If set, use the Gcc3ReportGenerator (for GCC3 environments)",
    )

    args = parser.parse_args()

    # Pick the right class based on --gcc3
    if args.gcc3:
        rg = Gcc3ReportGenerator(
            project_id=args.project_id,
            cluster=args.cluster,
            environment_name=args.environment_name,
            location=args.location,
            agg="1m",
            lookback=args.lookback,
        )
    else:
        rg = GccReportGenerator(
            project_id=args.project_id,
            cluster=args.cluster,
            environment_name=args.environment_name,
            location=args.location,
            agg="1m",
            lookback=args.lookback,
        )

    # Finally run the summary
    rg.gcc_utilization_summary()