from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame
import os
from dotenv import load_dotenv

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def load_gcs_credentials():
    """
    Load Google Cloud Service credentials from environment variables.
    
    Raises:
        EnvError: If the 'GOOGLE_APPLICATION_CREDENTIALS' environment variable is not set.
    """
    load_dotenv()
    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS') is None:
        raise Exception("'GOOGLE_APPLICATION_CREDENTIALS' is not set.")

def validate_dataframe(df: DataFrame, required_field: str):
    """
    Validate if the required field exists in the DataFrame schema.

    Args:
        df (DataFrame): The DataFrame to validate.
        required_field (str): The field to check in the DataFrame schema.

    Raises:
        DataFrameError: If the required field is not present in the DataFrame.
    """
    if required_field not in df.columns:
        raise Exception(f"'{required_field}' field is missing in the DataFrame.")

@data_exporter
def export_data_to_gcs(df: DataFrame, **kwargs) -> None:
    """
    Export a DataFrame to a Google Cloud Storage (GCS) bucket in a partitioned format.

    Args:
        df (DataFrame): The DataFrame to be exported.
        **kwargs: Additional keyword arguments.

    Raises:
        EnvError: If the 'GOOGLE_APPLICATION_CREDENTIALS' environment variable is not set.
        DataFrameError: If the 'lpep_pickup_date' field is missing in the DataFrame.
        Exception: For any other issues during the export process.
    """

    gcs_bucket_name = 'shamb0_zcamp_2024_hcl_demo_v1_bucket'
    project_id = 'shamb0-zoomcamp-lab-01'
    partition_field = 'lpep_pickup_date'
    gcs_table_name = 'green_trip_2020_q3'
    export_root_path = f'{gcs_bucket_name}/{gcs_table_name}'
    
    try:
        load_gcs_credentials()
        validate_dataframe(df, partition_field)
            
        # Transforming the DataFrame into a PyArrow table
        table = pa.Table.from_pandas(df)
        gcs_filesystem = pa.fs.GcsFileSystem(project_id=project_id)

        pq.write_to_dataset(
            table, 
            root_path=export_root_path,
            partition_cols=[partition_field],
            filesystem=gcs_filesystem
        )

    except Exception as e:
        raise Exception(f"Error exporting data to GCS Bucket: {e}")



