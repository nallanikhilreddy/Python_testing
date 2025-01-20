from pyspark.sql import SparkSession

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
    print("Spark version:", spark.version)
    input_df = spark.createDataFrame(input_data, schema)
    return input_df

def get_count(input_df):
    """
    Return the count of rows in the DataFrame.
    """
    return input_df.count()

# Test the functions
if __name__ == "__main__":
    input_data = [(1, 5), (2, 15), (3, 20)]
    schema = ["id", "value"]

    input_df = get_df(input_data, schema)
    count = get_count(input_df)
    print("Row count:", count)