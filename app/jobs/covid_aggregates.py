from pyspark.sql.functions import col, format_string, date_format


def _extract_data(spark, config):
    """ Load data from csv file """
    return spark.read.csv(
        f"{config.get('source_data_path')}",
        sep=';',
        header=True,
        inferSchema=True
    )


def _infections_per_day_agg(covid_df):
    """ Create aggregate of infections per day per unit """
    # Getting infections per day per voivodship id
    voivodship_infection = covid_df.groupBy('teryt_woj', 'data_rap_zakazenia') \
        .sum() \
        .select(
        'data_rap_zakazenia',
        'teryt_woj',
        'sum(liczba_zaraportowanych_zakazonych)'
    )
    # Dropping null values in voivodship column
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
    # Dropping teryt_woj column, prepared above unit_id column to merge with county table
    voivodship_infection = voivodship_infection.drop('teryt_woj')
    # Rename column name
    voivodship_infection = voivodship_infection.withColumnRenamed(
        "sum(liczba_zaraportowanych_zakazonych)", "sum_of_infections"
    )
    # Convert timestamp to string in format DD-MM-YYYY
    voivodship_infection = voivodship_infection.withColumn(
        "date", date_format(col('data_rap_zakazenia'), "dd-MM-yyyy")
    )
    # Dropping unnecessary column
    voivodship_infection = voivodship_infection.drop('data_rap_zakazenia')

    # Getting infections per day per voivodship id
    county_infection = covid_df.groupBy('teryt_pow', 'data_rap_zakazenia') \
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
    # Dropping teryt_pow column, prepared above unit_id column to merge with voivodship table
    county_infection = county_infection.drop('teryt_pow')
    # Rename column name
    county_infection = county_infection.withColumnRenamed(
        "sum(liczba_zaraportowanych_zakazonych)", "sum_of_infections"
    )
    # Convert timestamp to string in format DD-MM-YYYY
    county_infection = county_infection.withColumn(
        "date", date_format(col('data_rap_zakazenia'), "dd-MM-yyyy")
    )
    # Dropping unnecessary column
    county_infection = county_infection.drop('data_rap_zakazenia')
    # Union two generated dataframes into one
    infections_per_day_per_unit = voivodship_infection.union(county_infection)
    infections_per_day_per_unit = infections_per_day_per_unit.select(
        'date', 'unit_id', 'sum_of_infections'
    )

    return infections_per_day_per_unit


def _load_agg(config, transformed_df):
    """ Save aggregates to Bigquery """
    transformed_df.write.format("bigquery") \
        .option("credentialsFile", config.get('source_credentials')) \
        .option("writeMethod", "direct") \
        .mode('append') \
        .save(config.get('bigquery_table_path_short'))


def run_job(spark, config):
    """ Run covid aggregates job """
    _load_agg(config, _infections_per_day_agg(_extract_data(spark, config)))
