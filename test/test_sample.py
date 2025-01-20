import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Functions to be tested
def spark_session():
    """
    Get or create a Spark Session with specific configurations.
    """
    return (
        SparkSession.builder
        .master("local")
        .appName("unit-test")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.shuffle.service.enabled", "false")
        .getOrCreate()
    )

def get_df(input_data, schema):
    """
    Create a DataFrame from input data and schema.
    """
    spark = spark_session()
    input_df = spark.createDataFrame(input_data, schema)
    return input_df

def get_count(input_df):
    """
    Return the count of rows in the DataFrame.
    """
    return input_df.count()

# Pytest fixtures
@pytest.fixture(scope="module")
def spark():
    """
    Provide a SparkSession for all tests.
    """
    spark = spark_session()
    yield spark
    spark.stop()

# Test cases
def test_get_df(spark):
    """
    Test the get_df function with valid input data and schema.
    """
    input_data = [(1, 5), (2, 15), (3, 20)]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True)
    ])

    df = get_df(input_data, schema)

    # Assertions
    assert df.schema == schema, "Schema mismatch"
    assert df.count() == 3, "Row count mismatch"
    assert df.collect() == spark.createDataFrame(input_data, schema).collect(), "Data mismatch"

def test_get_count(spark):
    """
    Test the get_count function with a valid DataFrame.
    """
    input_data = [(1, 5), (2, 15), (3, 20)]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True)
    ])
    df = spark.createDataFrame(input_data, schema)

    count = get_count(df)

    # Assertions
    assert count == 3, "Row count mismatch"

def test_get_df_empty(spark):
    """
    Test the get_df function with an empty dataset.
    """
    input_data = []
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True)
    ])

    df = get_df(input_data, schema)

    # Assertions
    assert df.count() == 0, "Empty DataFrame should have 0 rows"

def test_get_count_empty(spark):
    """
    Test the get_count function with an empty DataFrame.
    """
    input_data = []
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", IntegerType(), True)
    ])
    df = spark.createDataFrame(input_data, schema)

    count = get_count(df)

    # Assertions
    assert count == 0, "Row count for empty DataFrame should be 0"
