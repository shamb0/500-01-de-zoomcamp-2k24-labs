import io
import pandas as pd
import requests
from prettytable import PrettyTable

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

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

@data_loader
def load_green_tripdata_data_from_api(*args, **kwargs):
    """
    Loads NYC Green Taxi Trip data from specified URLs into a pandas DataFrame.
    
    This function retrieves taxi trip data from multiple CSV files hosted online,
    concatenates them into a single DataFrame, and performs initial parsing and 
    type setting for the columns. It also summarizes the data loaded and its types.

    Args:
        *args: Additional arguments, if any.
        **kwargs: Keyword arguments, if any.

    Returns:
        pd.DataFrame: A DataFrame containing concatenated taxi trip data.
    """
    urls = [
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'
    ]

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    df = pd.concat([pd.read_csv(url, compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates) for url in urls])

    print("=== After Loading: ===")
    summarize_data(df)
    summarize_dftypes(df)

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
