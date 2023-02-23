import pandas as pd
from pyspark.sql import SparkSession
from jobs import covid_aggregates
import pytest


class TestCovidAggregates:
    """
    This class is used to test the covid_aggregates module.
    """
    @pytest.fixture
    def spark_session(self):
        """
        Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            SparkSession: A SparkSession object with the application name "testing_agg".
        """
        return (
            SparkSession.builder.appName('testing_agg').getOrCreate()
        )

    @pytest.fixture
    def test_data_fill_zero(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame without complete unit id.

        Parameters
        ----------
        spark_session : fixture
            Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
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
        )

    @pytest.fixture
    def test_data_without_fulfilling_zeros(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame without properly formed unit id.

        Parameters
        ----------
            spark_session : SparkSession object.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
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
        )

    @pytest.fixture
    def test_data_null_value_in_voivodship(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame with some rows without voivodeship id.

        Parameters
        ----------
            spark_session : SparkSession object.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
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
        )

    @pytest.fixture
    def test_data_null_value_in_county(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame with some rows without county id.

        Parameters
        ----------
            spark_session : SparkSession object.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
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
        )

    @pytest.fixture
    def test_data_empty_date(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame with some rows without date.

        Parameters
        ----------
            spark_session : SparkSession object.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
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
        )

    def test_aggregation_fill_zero_in_unit_id(self, spark_session, test_data_fill_zero):
        """
        Pytest function to test the "_infections_per_day_agg" function in the "covid_aggregates" module, specifically
        to ensure that zero values are filled in for any missing unit ids.

        Parameters
        ----------
            spark_session: A fixture that provides a SparkSession object for testing Spark code.
            test_data_fill_zero: A fixture that provides test data for the Spark DataFrame.

        Returns
        -------
            None
        """
        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '02', 3),
                ("12-10-2022", '02', 3),
                ("11-10-2022", '0201', 3),
                ("12-10-2022", '0201', 3)
            ],
            [
                "date", "unit_id", "sum_of_infections"
            ],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data_fill_zero).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

        spark_session.stop()

    def test_aggregation_without_filling_unit_id(self, spark_session, test_data_without_fulfilling_zeros):
        """
        Pytest function to test the "_infections_per_day_agg" function in the "covid_aggregates" module, specifically
        to ensure that function deal with properly formattet unit id.

        Parameters
        ----------
            spark_session: A fixture that provides a SparkSession object for testing Spark code.
            test_data_fill_zero: A fixture that provides test data for the Spark DataFrame.

        Returns
        -------
            None
        """
        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '22', 3),
                ("12-10-2022", '22', 3),
                ("11-10-2022", '2201', 3),
                ("12-10-2022", '2201', 3)
            ],
            [
                "date", "unit_id", "sum_of_infections"
            ],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data_without_fulfilling_zeros).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

        spark_session.stop()

    def test_aggregation_null_value_in_voivodship_id(self, spark_session, test_data_null_value_in_voivodship):
        """
        Pytest function to test the "_infections_per_day_agg" function in the "covid_aggregates" module, specifically
        to ensure that function skips row if there is no given voivodeship id.

        Parameters
        ----------
            spark_session: A fixture that provides a SparkSession object for testing Spark code.
            test_data_null_value_in_voivodship: A fixture that provides test data for the Spark DataFrame.

        Returns
        -------
            None
        """
        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '22', 1),
                ("12-10-2022", '22', 3),
                ("11-10-2022", '2201', 3),
                ("12-10-2022", '2201', 3)
            ],
            [
                "date", "unit_id", "sum_of_infections"
            ],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data_null_value_in_voivodship).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

        spark_session.stop()

    def test_aggregation_null_value_in_county_id(self, spark_session, test_data_null_value_in_county):
        """
        Pytest function to test the "_infections_per_day_agg" function in the "covid_aggregates" module, specifically
        to ensure that function skips row if there is no given county id.

        Parameters
        ----------
            spark_session: A fixture that provides a SparkSession object for testing Spark code.
            test_data_null_value_in_county: A fixture that provides test data for the Spark DataFrame.

        Returns
        -------
            None
        """
        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '22', 3),
                ("12-10-2022", '22', 3),
                ("11-10-2022", '2201', 2),
                ("12-10-2022", '2201', 2)
            ],
            [
                "date", "unit_id", "sum_of_infections"
            ],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data_null_value_in_county).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

        spark_session.stop()

    def test_aggregation_empty_date(self, spark_session, test_data_empty_date):
        """
        Pytest function to test the "_infections_per_day_agg" function in the "covid_aggregates" module, specifically
        to ensure that function skips row if there is no given date.

        Parameters
        ----------
            spark_session: A fixture that provides a SparkSession object for testing Spark code.
            test_data_empty_date: A fixture that provides test data for the Spark DataFrame.

        Returns
        -------
            None
        """
        expected_data = spark_session.createDataFrame(
            [
                ("11-10-2022", '22', 2),
                ("12-10-2022", '22', 3),
                ("11-10-2022", '2201', 2),
                ("12-10-2022", '2201', 3)
            ],
            [
                "date", "unit_id", "sum_of_infections"
            ],
        ).toPandas()

        real_data = covid_aggregates._infections_per_day_agg(test_data_empty_date).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

        spark_session.stop()

# pytest --cov jobs.covid_aggregates --cov-report html