import os
from pyspark.sql import SparkSession
 
# Ensure correct Python environment for PySpark
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\nikhi\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'
 
def create_spark_session():
    """
    Creates and returns a Spark session.
    """
    return SparkSession.builder \
        .appName("PySpark Unit Testing") \
        .master("local[*]") \
        .getOrCreate()
 
def get_count(df):
    """
    Returns the count of rows in the given DataFrame.
    """
    return df.count()
 
if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()
 
    # Sample DataFrame
    data = [("John", 34), ("Doe", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
 
    # Get the count and print it
    print(f"Row count: {get_count(df)}")
 
    # Stop Spark session
    spark.stop()