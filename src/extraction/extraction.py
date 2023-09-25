"""Read data from silver table and selecting the required the column and renaming it"""

import json
from pyspark.sql import SparkSession

from src.utilities import rename_and_select_columns, read_data

# Initialize a Spark session
spark = SparkSession.builder.appName("MySparkApp") \
    .config("spark.local.dir", "local/temp") \
    .getOrCreate()# Load the column mapping from the JSON file
with open("src/extraction/column_mapping.json", encoding="utf-8") as file:
    column_mapping = json.load(file)

def extract_tables():
    """return 'tables' dictionary contains all the dataframes with selected and renamed columns"""

    # Create a list of table names based on the keys in the column_mapping JSON
    table_names = list(column_mapping.keys())

    # Initialize an empty dictionary to store the dataframes
    tables = {}
    # Load and transform each table dynamically
    for table_name in table_names:
        # Load the tables from CSV
        table_df = read_data(spark, f"/src/data/{table_name}.csv")

        # Apply the transformation function to the table
        processed_df = rename_and_select_columns(table_df, table_name, column_mapping)

        # Store the transformed dataframe in the 'tables' dictionary
        tables[table_name] = processed_df
    return tables
