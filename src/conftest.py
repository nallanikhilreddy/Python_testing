import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Fixture for creating a SparkSession for tests."""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-pyspark-testing") \
        .getOrCreate()
    yield spark
    spark.stop()
