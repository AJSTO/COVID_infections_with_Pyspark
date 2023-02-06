import json
from jobs import covid_aggregates # importing job
# import importlib # this will be used if we will have more than one job to do
import argparse
from pyspark.sql import SparkSession


def _parse_arguments():
    """ Parse arguments provided by spark-submit commend"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    return parser.parse_args()


def main():
    """ Main function excecuted by spark-submit command"""
    args = _parse_arguments()

    with open("config.json", "r") as config_file:
        _config = json.load(config_file)

    spark = SparkSession.builder \
        .config("parentProject", "pyspark-covid") \
        .appName(_config.get("app_name")).getOrCreate()
    covid_aggregates.run_job(spark, _config)

    '''
    # THIS WILL BE USED IF APP WILL HAVE MORE THAN ONE JOBS:
    job_module = importlib.import_module(f"jobs.{args.job}")
    job_module.run_job(spark, _config)
    '''

if __name__ == "__main__":
    main()

# TO RUN PYSPARK JOB via console:
#  pytest && spark-submit --jars spark-3.1-bigquery-0.28.0-preview.jar \
#  --files config.json main.py --job covid_aggregates
