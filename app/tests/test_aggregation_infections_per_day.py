from jobs import covid_aggregates
import pandas as pd
from pyspark.sql import SparkSession


class TestCovidAggregates:
    def test_aggregation_fill_zero_in_unit_id(self):
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
            [
                ("11-10-2022", '02', 3),
                ("12-10-2022", '02', 3),
                ("11-10-2022", '0201', 3),
                ("12-10-2022", '0201', 3)
            ],
            ["date", "unit_id", "sum_of_infections"],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

    def test_aggregation_without_filling_unit_id(self):
        spark_session = SparkSession.builder \
            .appName('testing_agg').getOrCreate()
        test_data = spark_session.createDataFrame(
            [
                ('2022-10-11', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
            ],
            [
                "data_rap_zakazenia", "teryt_woj", "teryt_pow", "plec", "kat_wiek", "producent",
                "dawka_ost", "numer_zarazenia", "odl_szczep_zar", "liczba_zaraportowanych_zakazonych",
            ],
        )

        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '22', 3),
                ("12-10-2022", '22', 3),
                ("11-10-2022", '2201', 3),
                ("12-10-2022", '2201', 3)
            ],
            ["date", "unit_id", "sum_of_infections"],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

    def test_aggregation_null_value_in_voivodship_id(self):
        spark_session = SparkSession.builder \
            .appName('testing_agg').getOrCreate()
        test_data = spark_session.createDataFrame(
            [
                ('2022-10-11', None, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', None, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
            ],
            [
                "data_rap_zakazenia", "teryt_woj", "teryt_pow", "plec", "kat_wiek", "producent",
                "dawka_ost", "numer_zarazenia", "odl_szczep_zar", "liczba_zaraportowanych_zakazonych",
            ],
        )

        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '22', 1),
                ("12-10-2022", '22', 3),
                ("11-10-2022", '2201', 3),
                ("12-10-2022", '2201', 3)
            ],
            ["date", "unit_id", "sum_of_infections"],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

    def test_aggregation_null_value_in_county_id(self):
        spark_session = SparkSession.builder \
            .appName('testing_agg').getOrCreate()
        test_data = spark_session.createDataFrame(
            [
                ('2022-10-11', 22, None, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, None, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
            ],
            [
                "data_rap_zakazenia", "teryt_woj", "teryt_pow", "plec", "kat_wiek", "producent",
                "dawka_ost", "numer_zarazenia", "odl_szczep_zar", "liczba_zaraportowanych_zakazonych",
            ],
        )

        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '22', 3),
                ("12-10-2022", '22', 3),
                ("11-10-2022", '2201', 2),
                ("12-10-2022", '2201', 2)
            ],
            ["date", "unit_id", "sum_of_infections"],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

    def test_aggregation_empty_date(self):
        spark_session = SparkSession.builder \
            .appName('testing_agg').getOrCreate()
        test_data = spark_session.createDataFrame(
            [
                (None, 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-11', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
                ('2022-10-12', 22, 2201, 'K', '55-64', 'Pfizer', 'przypominająca', 2, '180-270', 1),
            ],
            [
                "data_rap_zakazenia", "teryt_woj", "teryt_pow", "plec", "kat_wiek", "producent",
                "dawka_ost", "numer_zarazenia", "odl_szczep_zar", "liczba_zaraportowanych_zakazonych",
            ],
        )

        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '22', 2),
                ("12-10-2022", '22', 3),
                ("11-10-2022", '2201', 2),
                ("12-10-2022", '2201', 3)
            ],
            ["date", "unit_id", "sum_of_infections"],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)
