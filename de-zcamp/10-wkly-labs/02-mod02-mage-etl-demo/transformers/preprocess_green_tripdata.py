import pandas as pd
from prettytable import PrettyTable


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def cleanup_data(df):
    """
    Cleans up the data by removing rows with NaN values in specified columns
    and rows where 'passenger_count' or 'trip_distance' is zero.

    Args:
        df (DataFrame): The input DataFrame to be cleaned.

    Returns:
        DataFrame: The cleaned DataFrame.
    """
    df.dropna(subset=['passenger_count', 'trip_distance'], inplace=True)
    df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
    return df

def summarize_data(df):
    """
    Summarizes key statistics of the DataFrame in a tabular format.

    This function creates a summary of the DataFrame, including its size, 
    and the counts of NaN and zero values in 'passenger_count' and 'trip_distance' columns.
    The summary is displayed as an ASCII table for better readability.

    Args:
        df (DataFrame): The DataFrame to be summarized.

    Output:
        Prints an ASCII table with summary statistics.
    """
    summary = {
        'Total Rows': df.shape[0],
        'Total Columns': df.shape[1],
        'NaN in Passenger Count': df['passenger_count'].isna().sum(),
        'NaN in Trip Distance': df['trip_distance'].isna().sum(),
        'Zero in Passenger Count': (df['passenger_count'] == 0).sum(),
        'Zero in Trip Distance': (df['trip_distance'] == 0).sum()
    }

    table = PrettyTable()
    table.field_names = ["Metric", "Value"]

    for key, value in summary.items():
        table.add_row([key, value])

    print(table)

def summarize_dftypes(df):
    """
    Summarizes the data types of columns in a pandas DataFrame.

    Args:
        df (DataFrame): The DataFrame whose data types are to be summarized.

    Output:
        Prints the name and data type of each column in the DataFrame.
    """
    print("DataFrame Column Types Summary:")
    print("--------------------------------")
    for col in df.columns:
        dtype = df[col].dtype
        print(f"{col.ljust(30)}: {str(dtype)}")
    print("--------------------------------")

def add_date_from_datetime(df):
    """
    Add date columns from datetime.

    Args:
        df (DataFrame): The DataFrame with datetime columns to be converted.

    Returns:
        DataFrame: The DataFrame with converted date columns.
    """
    try:
        df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    except Exception as e:
        print(f"Error converting datetime to date: {e}")
    return df

import re

def camel_to_snake(name):
    """
    Converts a string from CamelCase to snake_case.

    Args:
        name (str): The string in CamelCase format.

    Returns:
        str: The string converted to snake_case format.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def rename_columns_to_snake_case(df):
    """
    Renames DataFrame columns from CamelCase to snake_case.

    Args:
        df (DataFrame): The DataFrame with columns to be renamed.

    Returns:
        DataFrame: The DataFrame with renamed columns.
    """
    df.columns = [camel_to_snake(col) for col in df.columns]
    return df

@transformer
def transform(df, *args, **kwargs):
    """
    Transform function that refactors the data transformation process into smaller, focused functions.
    
    Args:
        df (DataFrame): The output from the upstream parent block.
        args: The output from any additional upstream blocks (if applicable).
    
    Returns:
        DataFrame: The transformed DataFrame.
    """
    print("=== Before Transform : ===")
    summarize_data(df)

    df = cleanup_data(df)
    df = add_date_from_datetime(df)
    df = rename_columns_to_snake_case(df)

    print("=== After Transform: ===")
    summarize_data(df)

    summarize_dftypes(df)

    return df

@test
def test_output(df, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
    assert "vendor_id" in df.columns, "DataFrame does not contain the column 'vendor_id'"
    assert (df['passenger_count'] > 0).all()
    assert (df['trip_distance'] > 0).all()
