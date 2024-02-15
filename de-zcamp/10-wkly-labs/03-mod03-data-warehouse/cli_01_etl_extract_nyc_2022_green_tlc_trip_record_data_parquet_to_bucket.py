"""
NYC Taxi Data Pipeline ETL Script

Purpose: Automate extraction and loading of NYC taxi trip data for analysis.

Usage: 
    python etl_script.py --color <color> --year <year> [--month <month>] [--bucket-id <bucket_id>]

Author: Shamb0
Date: February 2023
"""
import requests
import click
import pyarrow.parquet as pq
import os, sys, logging
from google.cloud import storage
from dotenv import load_dotenv
from tqdm import tqdm

# Configurations
taxi_data_urls = {
  "green": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet",
  # API links for other colors
}

months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]

def month_str_to_num(month_str):
    """Convert month string to number."""
    if month_str is None:
        return None
    if month_str.isdigit():
        month_num = int(month_str)
        if 1 <= month_num <= 12:
            return month_num
        else:
            raise ValueError(f"Invalid month number: {month_str}")    
    try:
        return months.index(month_str.lower()) + 1
    except ValueError:
        raise ValueError(f"Invalid month string: {month_str}")

def download_file(url, local_filename):
    """
    Download a file from a URL to a local file, displaying download progress.
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        with open(local_filename, 'wb') as f, tqdm(
                total=total_size, unit='iB', unit_scale=True) as bar:
            for chunk in r.iter_content(chunk_size=8192):
                bar.update(len(chunk))
                f.write(chunk)
    return local_filename

def ensure_directory_exists(directory):
    """
    Ensure that the directory exists.
    """
    os.makedirs(directory, exist_ok=True)

def download_and_process_data(color, year, month, data_root):
    """
    Download and process data for a given month.
    """
    url = taxi_data_urls[color].format(year=year, month=month)
    local_filename = os.path.join(data_root, f"{color}_data_{year}_{month:02d}.parquet")
    try:
        download_file(url, local_filename)
        data = pq.read_table(local_filename).to_pandas()
        summarize_data(data, f"{color}_data_{year}_{month:02d}.parquet")
        return data
    except Exception as e:
        logging.error(f"Error downloading or processing data: {e}")
        raise
    finally:
        os.remove(local_filename)  # Clean up the local file
        
def extract_month_data(color, year, month):
    """
    Extract taxi data for given filters.
    """
    selected_months = [month] if month else range(1, 13)
    data_root = os.getenv("UTEST_DATA_ROOT", "local_data")
    ensure_directory_exists(data_root)
    
    for m in selected_months:
        logging.info(f"=== Processing {color} taxi data for {year}-{m:02d} ===")
        try:
            data = download_and_process_data(color, year, m, data_root)
            logging.info(f"=== Processing {color} taxi data for {year}-{m:02d} Done! ===")
            yield m, data
        except Exception as e:
            logging.error(f"Error in extracting data for {year}-{m:02d}: {e}")
            continue
        
def summarize_data(data, source):
    """Prints a summary of the data, including size and structure."""
    logging.info(f"Data summary : {source}")
    logging.info(f"- Shape: {data.shape}")
    logging.info(f"- Columns: {', '.join(data.columns)}")

def upload_data_to_gcs(data, color, year, month, bucket_id):
    """Upload DataFrame to Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_id)
        filename = f"green_taxi/{color}_data_{year}_{month:02d}.parquet"
        blob = bucket.blob(filename)
        blob.upload_from_string(data.to_parquet(engine="pyarrow"))
        logging.info(f"Uploaded data to GCS bucket {bucket_id} as {filename}")
    except Exception as e:
        logging.error(f"Unable to load data to GCS, Error: {e}")
        raise

def save_data_locally(data, color, year, month):
    """Save DataFrame to local filesystem."""
    data_root = os.getenv("UTEST_GEN_DATA_ROOT", "local_data")
    ensure_directory_exists(data_root)
    try:
        local_filename = os.path.join(data_root, f"02_{color}_data_{year}_{month:02d}.parquet")
        data.to_parquet(local_filename, engine="pyarrow")
        logging.info(f"Saved data locally to {local_filename}")
    except Exception as e:
        logging.error(f"Unable to save data locally, Error: {e}")
        raise

def load_data(data, color, year, month, bucket_id=None):
    """Load DataFrame to cloud or local storage."""
    try:
        if bucket_id:
            # Check for GCS credentials
            if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                logging.error("GCS credentials not available")
                raise ValueError("GCS credentials not available")
            
            upload_data_to_gcs(data, color, year, month, bucket_id)
        else:
            save_data_locally(data, color, year, month)
    except Exception as e:
        logging.error(f"load_data failed, Error: {e}")
        raise
    
def setup_logging(logging_level):
    """Set up the logging configuration."""
    logging.basicConfig(
        level=getattr(logging, logging_level.upper(), logging.INFO)
    )
    
@click.command()
@click.option("--color", "-c", type=click.Choice(['green', 'yellow']), required=True, help="Taxi color")
@click.option("--year", "-y", type=int, required=True, help="Year of the dataset")
@click.option("--month", "-m", type=str, required=False, help="Specific month")
@click.option("--bucket-id", "-b", type=str, required=False, help="ID of the Google Cloud Storage bucket for upload")
@click.option("--logging-level", "-l", default="INFO", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]), required=False, help="Logging level")
def cli(color, year, month, bucket_id, logging_level):
    """Command-line interface for running the data pipeline."""
    load_dotenv()
    setup_logging(logging_level)
    try:
        month_num = month_str_to_num(month)
        extract_data = extract_month_data(
            color=color, 
            year=year, 
            month=month_num
        )
        for month, data in extract_data:
            load_data(
                data, 
                color, 
                year, 
                month, 
                bucket_id=bucket_id
            )
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":    
    cli()

