import datetime
import os
import sys
import pytest
from pyspark.sql import SparkSession
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

from pyspark_src import assert_pyspark_df_equal, create_spark_session


@pytest.fixture(scope="session")
def spark_session():
    return create_spark_session()


class TestAssertPysparkDfEqual:
    def test_assert_pyspark_df_equal_success(self, spark_session: SparkSession):
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
        right_df = left_df
        assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_one_is_not_pyspark_df(self, spark_session: SparkSession):
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

    def test_assert_pyspark_df_equal_different_string_value(self, spark_session: SparkSession):
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
                [datetime.date(2020, 1, 1), "demo1", 1.123, 10],
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
        with pytest.raises(
            AssertionError,
            match="Data mismatch\\n\\nRow = 1 : Column = col_b\\n\\nACTUAL: demo\\nEXPECTED: demo1",
        ):
            assert_pyspark_df_equal(left_df, right_df)
