from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame
import os
from dotenv import load_dotenv
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


def load_gcs_credentials():
    """
    Load Google Cloud Service credentials from environment variables.
    
    Raises:
        EnvError: If the 'GOOGLE_APPLICATION_CREDENTIALS' environment variable is not set.
    """
    load_dotenv()
    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS') is None:
        raise Exception("'GOOGLE_APPLICATION_CREDENTIALS' is not set.")


def read_parquet_from_gcs(bucket_name: str, project_id: str, table_name: str, partition_date: str) -> pa.Table:
    """
    Read a partitioned parquet dataset from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): The GCS bucket name.
        project_id (str): The GCS project ID.
        table_name (str): The table name in the GCS bucket.
        partition_date (str): The date for the partition to read.

    Returns:
        pa.Table: The table read from GCS.

    Raises:
        Exception: For any issues during the read process.
    """
    gcs_filesystem = pa.fs.GcsFileSystem(project_id=project_id)
    partition_path = f'{bucket_name}/{table_name}/lpep_pickup_date={partition_date}'

    dataset = pq.ParquetDataset(
        partition_path, 
        filesystem=gcs_filesystem
    )
    return dataset.read()


@data_loader
def load_data_from_gcs(*args, **kwargs) -> DataFrame:
    """
    Import a Table from a Google Cloud Storage (GCS) bucket which is in a partitioned format.

    Args:
        *args: Additional arguments, if any.
        **kwargs: Keyword arguments, if any.

    Returns:
        pd.DataFrame: A DataFrame containing concatenated taxi trip data.

    Raises:
        EnvError: If the 'GOOGLE_APPLICATION_CREDENTIALS' environment variable is not set.
        Exception: For any other issues during the export process.
    """

    gcs_bucket_name = 'shamb0_zcamp_2024_hcl_demo_v1_bucket'
    project_id = 'shamb0-zoomcamp-lab-01'
    gcs_table_name = 'green_trip_2020_q3'
    lpep_pickup_date='2020-09-30'

    try:

        load_gcs_credentials()

        table = read_parquet_from_gcs(gcs_bucket_name, project_id, gcs_table_name, lpep_pickup_date)
        
        # Convert and load into DataFrame
        df = table.to_pandas()
                
        summarize_data(df)

        summarize_dftypes(df)

    except Exception as e:
        raise Exception(f"Error exporting data to GCS Bucket: {e}")
           
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
