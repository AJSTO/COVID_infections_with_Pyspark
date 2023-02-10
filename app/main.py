import json
from jobs import covid_aggregates
from pyspark.sql import SparkSession


def main():
    """ Main function excecuted by spark-submit command"""
    with open("config.json", "r") as config_file:
        _config = json.load(config_file)

    spark = SparkSession.builder \
        .config("parentProject", _config.get("biquery_project")) \
        .appName(_config.get("app_name")).getOrCreate()
    covid_aggregates.run_job(spark, _config)


if __name__ == "__main__":
    main()

# TO RUN PYSPARK JOB via console:
#  pytest && spark-submit --jars bq_con/spark-3.1-bigquery-0.28.0-preview.jar --files config.json main.py --job covid_aggregates

'''
to create dataset in bigquery:
date string nullable
unit_id required
sum_of_infections nullable
'''
