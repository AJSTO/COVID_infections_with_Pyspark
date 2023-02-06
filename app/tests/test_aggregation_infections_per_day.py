from jobs import covid_aggregates
import pandas as pd
from pyspark.sql import SparkSession


class TestCovidAggregates:
    def test_aggregation_infections_per_day(self, ):
        spark_session = SparkSession.builder \
            .appName('testing_agg').getOrCreate()
        test_data = spark_session.createDataFrame(
            [
                ('2022-10-11', 2, 201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 2, 201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 2, 201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 2, 201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 2, 201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 2, 201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
            ],
            [
                "data_rap_zakazenia", "teryt_woj", "teryt_pow", "plec", "kat_wiek", "producent",
                "dawka_ost", "numer_zarazenia", "odl_szczep_zar", "liczba_zaraportowanych_zakazonych",
            ],
        )

        expected_data = spark_session.createDataFrame(
            [("2022-10-11", 3), ("2022-10-12", 3)],
            ["day", "sum_of_infections"],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)
