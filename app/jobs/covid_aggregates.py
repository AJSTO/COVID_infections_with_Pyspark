

def _extract_data(spark, config):
    """ Load data from csv file """
    return spark.read.csv(f"{config.get('source_data_path')}",sep =';', header=True, inferSchema=True)


def _infections_per_day_agg(covid_df):
    """ Create aggregate of infections per day """
    return (
        covid_df.groupBy('data_rap_zakazenia')
            .sum('liczba_zaraportowanych_zakazonych')
            .withColumnRenamed('sum(liczba_zaraportowanych_zakazonych)', "sum_of_infections")
            .withColumnRenamed('data_rap_zakazenia', "day")
            .select(
                    'day',
                    'sum_of_infections'
                   )
    )

#'''
def _load_agg(config, transformed_df):
    """ Save aggregates to Bigquery """
    transformed_df.write.format('bigquery') \
        .option("credentialsFile", "bq_con/credentials.json") \
        .option("table", "pyspark-covid.covid_dataset.covid_aggregates") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()

'''
def _load_agg(config, transformed_df):
    """ Save aggregates to csv """
    transformed_df.write.mode("overwrite") \
        .csv(f"{config.get('output_data_path')}/infections_agg_{datetime.now()}.csv")
'''
def run_job(spark, config):
    """ Run covid aggregates job """
    _load_agg(config, _infections_per_day_agg(_extract_data(spark, config)))