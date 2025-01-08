import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Fixture to initialize a PySpark session for tests."""
    spark = SparkSession.builder \
        .main("local[*]") \
        .appName("PySpark Test Session") \
        .getOrCreate()
    yield spark
    spark.stop()
