import datetime
import os
import sys
import pyspark
import pytest
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark_src import assert_pyspark_df_equal  # Import the function to test


# Test class for the assert_pyspark_df_equal function
class TestAssertPysparkDfEqual:
    def test_assert_pyspark_df_equal_success(
        self, spark_session: pyspark.sql.SparkSession
    ):
        """
        Test that two identical DataFrames are considered equal.
        """
        left_df = spark_session.createDataFrame(
            data=[
                [datetime.date(2020, 1, 1), "demo", 1.123, 10],
                [None, None, None, None],
            ],
            schema=StructType(
                [
                    StructField("col_a", DateType(), True),
                    StructField("col_b", StringType(), True),
                    StructField("col_c", DoubleType(), True),
                    StructField("col_d", LongType(), True),
                ]
            ),
        )
        right_df = spark_session.createDataFrame(
            data=[
                [datetime.date(2020, 1, 1), "demo", 1.123, 10],
                [None, None, None, None],
            ],
            schema=StructType(
                [
                    StructField("col_a", DateType(), True),
                    StructField("col_b", StringType(), True),
                    StructField("col_c", DoubleType(), True),
                    StructField("col_d", LongType(), True),
                ]
            ),
        )
        # Assert that the DataFrames are equal
        assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_one_is_not_pyspark_df(
        self, spark_session: pyspark.sql.SparkSession
    ):
        """
        Test that a non-DataFrame input raises an error.
        """
        left_df = spark_session.createDataFrame(
            data=[
                [datetime.date(2020, 1, 1), "demo", 1.123, 10],
                [None, None, None, None],
            ],
            schema=StructType(
                [
                    StructField("col_a", DateType(), True),
                    StructField("col_b", StringType(), True),
                    StructField("col_c", DoubleType(), True),
                    StructField("col_d", LongType(), True),
                ]
            ),
        )
        right_df = "Demo"
        with pytest.raises(
            AssertionError,
            match="Right expected type <class 'pyspark.sql.dataframe.DataFrame'>, found <class 'str'> instead",
        ):
            assert_pyspark_df_equal(left_df, right_df)

    # Test other cases for mismatches (e.g., different values, schemas, etc.)
    def test_assert_pyspark_df_equal_different_values(self, spark_session):
        """
        Test mismatch in DataFrame values.
        """
        left_df = spark_session.createDataFrame(
            [[1, "a"], [2, "b"]],
            ["id", "name"]
        )
        right_df = spark_session.createDataFrame(
            [[1, "a"], [3, "c"]],
            ["id", "name"]
        )
        with pytest.raises(
            AssertionError,
            match="Data mismatch",
        ):
            assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_different_row_count(self, spark_session):
        """
        Test mismatch in DataFrame row counts.
        """
        left_df = spark_session.createDataFrame(
            [[1, "a"]],
            ["id", "name"]
        )
        right_df = spark_session.createDataFrame(
            [[1, "a"], [2, "b"]],
            ["id", "name"]
        )
        with pytest.raises(
            AssertionError,
            match="Number of rows are not same",
        ):
            assert_pyspark_df_equal(left_df, right_df)
