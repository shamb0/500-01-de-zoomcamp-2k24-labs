import pandas as pd
import logging
import requests
import os

class DPipeIOSrc:
    @staticmethod
    def _download_file(url, path=None):
        if path is None:
            raise ValueError("Path cannot be None")
        try:
            response = requests.get(url)
            if response.status_code == 200:
                with open(path, 'wb') as file:
                    file.write(response.content)
            else:
                raise requests.RequestException(f"Failed to download file from {url}")
        except Exception as e:
            logging.error(f"download_file => Error occurred: {e}")
            raise

    @staticmethod
    def _read_data_from_url(url, download_path=None, download_file_name=None):

        file_path = os.path.join(download_path, download_file_name) if download_path else download_file_name
        DPipeIOSrc._download_file(url, file_path)

        try:
            return pd.read_csv(file_path, compression='gzip' if file_path.endswith('.gz') else None)
        except Exception as e:
            logging.error(f"read_data_from_url => Error occurred: {e}")
            raise ValueError("Failed to read from url dumped file")
    
    @staticmethod
    def _read_data_from_local(path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"No file found at {path}")

        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError as e:
            logging.error(f"read_data_from_local => Error occurred: {e}")
            raise ValueError("Failed to read from local file")

    @staticmethod
    # Main Function to Process Data Based on Scenarios
    def process_data(source, download_path=None):
        logging.info("Running for the Config :: ...")
        logging.info(f"\t Source :: {source}")
        logging.info(f"\t DWL Path :: {download_path}")

        try:
            if source.startswith(('http://', 'https://')):
                download_file_name = source.split('/')[-1]
                return DPipeIOSrc._read_data_from_url(source, download_path, download_file_name)
            else:
                return DPipeIOSrc._read_data_from_local(source)
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            raise
