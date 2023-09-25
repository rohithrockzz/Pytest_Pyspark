"""Utiltities functions"""
from typing import Dict
from pyspark.sql import DataFrame

def rename_and_select_columns(dataframe: DataFrame,
                               table_name: str,
                               column_mapping: Dict[str, Dict[str, str]]) -> DataFrame:
    """
    Rename and select specific columns in a DataFrame based on column_mapping.

    This function renames columns in a DataFrame based on a provided column mapping
    and selects specific columns from the DataFrame based on a list of column names.

    Parameters:
    df (pyspark.sql.DataFrame): The input DataFrame to be processed.
    table_name (str): The name of the table or source associated with the DataFrame.
    column_mapping (dict of dict): A dictionary of dictionaries containing column
        mapping information for different tables. The outer dictionary's keys are table
        names, and the inner dictionaries map old column names to new column names.
    Returns:
    pyspark.sql.DataFrame: The processed DataFrame with renamed and selected columns.
    """
    if table_name in column_mapping:
        mapping = column_mapping[table_name]
        # Rename columns based on the mapping
        for old_name, new_name in mapping.items():
            dataframe = dataframe.withColumnRenamed(old_name, new_name)       
        # Select specific columns from the DataFrame
        dataframe = dataframe.select(*mapping.values())
    return dataframe

def read_data(path):
    """To read the data from csv"""
    return spark.read.csv(path, header=True, inferSchema=True)
