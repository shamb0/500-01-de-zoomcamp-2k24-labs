from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
from prettytable import PrettyTable

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

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

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Export a DataFrame to a PostgreSQL table.

    This function connects to a PostgreSQL database using configurations specified
    in an external YAML file. It then exports the provided DataFrame to a specified
    table within the database. It supports various options such as whether to replace
    the table if it exists, handling unique constraints, etc.

    Args:
        df (DataFrame): The DataFrame to be exported.
        **kwargs: Additional keyword arguments.

    Note:
        Configurations are loaded from 'io_config.yaml' file within the repository path.
        The schema name and table name are predefined within the function.
    """
    summarize_data(df)

    schema_name = 'mage'
    table_name = 'green_tripdata_2020_q3'  # Table name for exporting data
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Using a context manager for database connection
    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            loader.export(
                df,
                schema_name,
                table_name,
                index=False,  # Do not write row names (index)
                if_exists='replace'  # Replace table if it exists
            )
    except Exception as e:
        print(f"Error exporting data to PostgreSQL: {e}")
