import pytest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from nb_src_pyspark import get_count, create_spark_session

@pytest.fixture(scope="session")
def spark():
    return create_spark_session()

def test_get_count(spark):
    # Create a simple DataFrame for testing
    data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Get the count
    result = get_count(df)
    
    # Assert the count is as expected
    assert result == 3