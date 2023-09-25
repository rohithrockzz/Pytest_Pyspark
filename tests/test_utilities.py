import pytest
from pyspark.sql import SparkSession
from src.utilities import rename_and_select_columns

# Define a pytest fixture to create a SparkSession
@pytest.fixture(scope="session")
def spark_session():
    """
    Create a SparkSession for testing purposes.

    Returns:
    pyspark.sql.SparkSession: A SparkSession object for testing.
    """
    spark = spark = SparkSession.builder.appName("YourAppName").getOrCreate()
    yield spark
    spark.stop()

# Define a pytest fixture to create sample data
@pytest.fixture(scope="function")
def sample_data(spark_session):
    """
    Create sample data as a Spark DataFrame for testing.

    Args:
    spark_session (pyspark.sql.SparkSession): The SparkSession for creating the DataFrame.

    Returns:
    pyspark.sql.DataFrame: A DataFrame containing sample data.
    """
    sample_df = [
        ("Alice", 25, "New York"),
        ("Bob", 30, "San Francisco"),
    ]
    columns = ["Name", "Age", "City"]
    df = spark_session.createDataFrame(sample_df, columns)
    return df

# Now, use these fixtures in your test function
def test_rename_and_select_columns(sample_data):
    """
    Test the rename_and_select_columns function.

    Args:
    sample_data (pyspark.sql.DataFrame): The sample DataFrame for testing.
    """
    # Define the column mapping directly within the test function
    column_mapping = {
        "example_table": {"Name": "Full Name", "Age": "Years"},
    }

    # Test renaming and selecting columns
    result_df = rename_and_select_columns(sample_data, "example_table", column_mapping)
   
    # Check if columns were renamed and selected correctly
    expected_columns = ["Full Name", "Years"]
    assert result_df.columns == expected_columns

    assert result_df.count() == 2
