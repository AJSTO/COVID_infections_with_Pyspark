import json
import pyspark
from jobs import covid_aggregates
from pyspark.sql import SparkSession


def main():
    """
    Main function executed by spark-submit command.
    Creating Spark Session with connection to Bigquery.
    """
    with open("config.json", "r") as config_file:
        _config = json.load(config_file)

    spark = SparkSession.builder \
        .config("parentProject", _config.get("bigquery_project")) \
        .appName(_config.get("app_name")).getOrCreate()
    covid_aggregates.run_job(spark, _config)


if __name__ == "__main__":
    main()
