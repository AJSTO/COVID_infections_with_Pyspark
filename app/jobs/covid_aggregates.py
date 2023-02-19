import pyspark
from pyspark.sql.functions import format_string, date_format, col


def _extract_data(spark, config):
    """
    Extract covid infections information from CSV file.

    Parameters
    ----------
    spark : object
        SparkSession built in main.py.
    config : json object
        Json file with information about Bigquery project, dataset, tables, credentials.

    Returns
    -------
    Pyspark Dataframe
        Extracted Pyspark Dataframe of covid infection informations.
    """
    return spark.read.csv(
        f"{config.get('source_data_path')}",
        sep=';',
        header=True,
        inferSchema=True
    )


def _infections_per_day_agg(covid_dataframe):
    """
    Create aggregate of infections per day per unit.

    Parameters
    ----------
    covid_dataframe: object
        Extracted Pyspark Dataframe of covid infections info.

    Returns
    -------
    Pyspark Dataframe
        Calculated aggregate of infections per day per unit.
    """
    voivodship_infection = covid_dataframe.groupBy('teryt_woj', 'data_rap_zakazenia') \
        .sum() \
        .select(
        'data_rap_zakazenia',
        'teryt_woj',
        'sum(liczba_zaraportowanych_zakazonych)'
    )
    voivodship_infection = voivodship_infection.dropna(
        how='any',
        thresh=None,
        subset=('teryt_woj', 'data_rap_zakazenia'),
    )
    # Replenishment of the unit id, if length equal to one adding 0 on front
    voivodship_infection = voivodship_infection.withColumn(
        "unit_id",
        format_string("%02d", "teryt_woj")
    )
    voivodship_infection = voivodship_infection.drop('teryt_woj')
    # Rename column name
    voivodship_infection = voivodship_infection.withColumnRenamed(
        "sum(liczba_zaraportowanych_zakazonych)", "sum_of_infections"
    )
    # Convert timestamp to string in format DD-MM-YYYY
    voivodship_infection = voivodship_infection.withColumn(
        "date", date_format(col('data_rap_zakazenia'), "dd-MM-yyyy")
    )
    county_infection = covid_dataframe.groupBy('teryt_pow', 'data_rap_zakazenia') \
        .sum() \
        .select(
        'data_rap_zakazenia',
        'teryt_pow',
        'sum(liczba_zaraportowanych_zakazonych)'
    )
    county_infection = county_infection.dropna(
        how='any',
        thresh=None,
        subset=('teryt_pow', 'data_rap_zakazenia'),
    )
    # Replenishment of the unit id, if length equal to three adding 0 on front
    county_infection = county_infection.withColumn(
        "unit_id",
        format_string("%04d", "teryt_pow")
    )
    county_infection = county_infection.drop('teryt_pow')
    # Rename column name
    county_infection = county_infection.withColumnRenamed(
        "sum(liczba_zaraportowanych_zakazonych)", "sum_of_infections"
    )
    # Convert timestamp to string in format DD-MM-YYYY
    county_infection = county_infection.withColumn(
        "date", date_format(col('data_rap_zakazenia'), "dd-MM-yyyy")
    )
    # Union two generated dataframes into one
    infections_per_day_per_unit = voivodship_infection.union(county_infection)
    infections_per_day_per_unit = infections_per_day_per_unit.select(
        'date', 'unit_id', 'sum_of_infections',
    )

    return infections_per_day_per_unit


def _load_agg(config, transformed_df):
    """
    Saving created table of aggregated infections per unit id per day.
    Mode of saving data - append. Writing pyspark Dataframe directly Bigquery table.

    Parameters
    ----------
    config : json object
        Json file with information about Bigquery project, dataset, tables.
    transformed_df: object
        Transformed Pyspark Dataframe of covid infection aggregates per day per unit id.
    """
    transformed_df.write.format("bigquery") \
        .option("credentialsFile", config.get('source_credentials')) \
        .option("writeMethod", "direct") \
        .mode('append') \
        .save(config.get('bigquery_table_path_short'))


def run_job(spark, config):
    """
    Running ETL job for getting daily aggregate per day per unit id.

    Parameters
    ----------
    spark : object
        SparkSession built in main.py.
    config : json object
        Json file with information about Bigquery project, dataset, tables.
    """
    _load_agg(
        config, _infections_per_day_agg(
            _extract_data(
                spark, config
            )
        )
    )
