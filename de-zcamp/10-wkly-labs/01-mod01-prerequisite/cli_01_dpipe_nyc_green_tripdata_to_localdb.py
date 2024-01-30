import os
import logging
import pandas as pd
from dotenv import load_dotenv
import click
from .dpipe_io_sql_store import DPipeIOSqlStore
from .dpipe_io_source import DPipeIOSrc

# Using a global logger for consistency
logger = logging.getLogger(__name__)

def setup_logging():
    """
    Set up the logging configuration.
    """
    logging.basicConfig(level=logging.INFO)


def load_environment_variables():
    """
    Load environment variables from the .env file.

    Returns:
    tuple: Database URL and name.
    """
    load_dotenv()
    return os.getenv('DATABASE_URL'), os.getenv('DATABASE_NAME'), os.getenv('UTEST_GEN_DATA_ROOT')


def standardize_dataframe_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the structure of the DataFrame by renaming columns and converting datatypes.

    Args:
    df (pandas.DataFrame): The DataFrame to be standardized.

    Returns:
    pandas.DataFrame: The standardized DataFrame.
    """
    # Renaming columns to maintain consistency
    df.columns = df.columns.str.replace(" ", "_").str.lower()

    # Converting datetime columns to appropriate datatype
    datetime_columns = [
        'lpep_pickup_datetime', 
        'lpep_dropoff_datetime',
        'tpep_pickup_datetime', 
        'tpep_dropoff_datetime'
    ]
    
    # Converting datetime columns to appropriate datatype if they exist
    for column in datetime_columns:
        if column in df.columns:
            try:
                df[column] = pd.to_datetime(df[column])
            except Exception as e:
                # Handle exceptions if conversion fails
                logging.warning(f"Error converting column {column} to datetime: {e}")

    return df

def process_data_and_update_db(data_source: str, db_table_name: str, db_uri_path: str, desti_path: str):
    """
    Process data from the source and update the database.

    Args:
        data_source (str): Source of the data.
        db_table_name (str): Database table name.
        db_uri_path (str): Database URI path.
    """
    try:
        db_conn = DPipeIOSqlStore(db_uri_path)
        df = DPipeIOSrc.process_data(data_source, desti_path)
        db_conn.log_dataframe_schema(df, db_table_name)

        df = standardize_dataframe_structure(df)
        db_conn.log_dataframe_schema(df, db_table_name)

        db_conn.update_db_in_chunks(df, db_table_name)
    except Exception as e:
        logger.error(f"process_data_and_update_db => Pipeline Failed: {e}")


@click.command()
@click.option("--data-source", "-ds", default="Invalid", type=str)
@click.option("--db-table-name", "-dt", default="Invalid", type=str)
def cli(data_source, db_table_name):
    """
    Command Line Interface for running the data pipeline.

    Args:
        data_source (str): Source of the data.
        db_table_name (str): Database table name.
    """
    setup_logging()

    db_url, db_name, desti_path = load_environment_variables()
    db_uri_path = f"{db_url}/{db_name}"

    process_data_and_update_db(data_source, db_table_name, db_uri_path, desti_path)

if __name__ == "__main__":
    cli()
