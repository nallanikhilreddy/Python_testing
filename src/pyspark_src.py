from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Any


def create_spark_session():
    """
    Creates and returns a Spark session.
    """
    return SparkSession.builder \
        .appName("PySpark Unit Testing") \
        .master("local[*]") \
        .getOrCreate()


def _check_isinstance(left: Any, right: Any, cls):
    assert isinstance(left, cls), f"Left expected type {cls}, found {type(left)} instead"
    assert isinstance(right, cls), f"Right expected type {cls}, found {type(right)} instead"


def _check_columns(check_columns_in_order: bool, left_df: DataFrame, right_df: DataFrame):
    if check_columns_in_order:
        assert left_df.columns == right_df.columns, "df columns name mismatch"
    else:
        assert sorted(left_df.columns) == sorted(right_df.columns), "df columns name mismatch"


def _check_schema(check_columns_in_order: bool, left_df: DataFrame, right_df: DataFrame):
    if check_columns_in_order:
        assert left_df.dtypes == right_df.dtypes, "df schema type mismatch"
    else:
        assert sorted(left_df.dtypes, key=lambda x: x[0]) == sorted(right_df.dtypes, key=lambda x: x[0]), "df schema type mismatch"


def _check_df_content(left_df: DataFrame, right_df: DataFrame):
    left_df_list = left_df.collect()
    right_df_list = right_df.collect()

    for row_index in range(len(left_df_list)):
        for column_name in left_df.columns:
            left_cell = left_df_list[row_index][column_name]
            right_cell = right_df_list[row_index][column_name]
            if left_cell == right_cell or (left_cell is None and right_cell is None):
                continue
            msg = f"Data mismatch\n\nRow = {row_index + 1} : Column = {column_name}\n\nACTUAL: {left_cell}\nEXPECTED: {right_cell}\n"
            assert False, msg


def _check_row_count(left_df: DataFrame, right_df: DataFrame):
    left_df_count = left_df.count()
    right_df_count = right_df.count()
    assert left_df_count == right_df_count, f"Number of rows are not same.\n\nActual Rows: {left_df_count}\nExpected Rows: {right_df_count}\n"


def assert_pyspark_df_equal(
    left_df: DataFrame,
    right_df: DataFrame,
    check_dtype: bool = True,
    check_column_names: bool = False,
    check_columns_in_order: bool = False,
    order_by: list = None,
):
    """
    Compares two PySpark DataFrames for equality.

    Args:
        left_df (DataFrame): The first DataFrame.
        right_df (DataFrame): The second DataFrame.
        check_dtype (bool, optional): Compare schemas. Defaults to True.
        check_column_names (bool, optional): Compare column names. Defaults to False.
        check_columns_in_order (bool, optional): Check column order. Defaults to False.
        order_by (list, optional): List of columns to order by before comparing. Defaults to None.
    """
    _check_isinstance(left_df, right_df, DataFrame)

    if check_column_names:
        _check_columns(check_columns_in_order, left_df, right_df)

    if check_dtype:
        _check_schema(check_columns_in_order, left_df, right_df)

    _check_row_count(left_df, right_df)

    if order_by:
        left_df = left_df.orderBy(order_by)
        right_df = right_df.orderBy(order_by)

    _check_df_content(left_df, right_df)
