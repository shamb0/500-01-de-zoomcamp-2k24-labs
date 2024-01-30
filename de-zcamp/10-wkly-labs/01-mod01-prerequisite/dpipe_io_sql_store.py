import pandas as pd
import logging
from sqlalchemy import create_engine, exc, Table, MetaData
import time

class DPipeIOSqlStore:
    def __init__(self, db_uri_path) -> None:
        try:
            self.db_conn = create_engine(db_uri_path)
        except Exception as e:
            logging.error(f"_get_db_connection => Error occurred: {e}")
            raise Exception(f"Failed to create DB connection: {db_uri_path}")

    def _drop_table(self, table_name):
        """Drops the specified table from the database."""
        try:        
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.db_conn)
            if self._table_exists(table_name):
                table.drop(bind=self.db_conn, checkfirst=True)
        except Exception as e:
            logging.error(f"_drop_table => {table_name} Error occurred: {e}")
            raise

    def _table_exists(self, table_name):
        """Checks if the specified table exists in the database."""
        try:        
            with self.db_conn.connect() as conn:
                return self.db_conn.dialect.has_table(conn, table_name)
        except Exception as e:
            logging.error(f"_table_exists => {table_name} Error occurred: {e}")
            raise

    def update_db_in_chunks(
            self,
            df, 
            db_table_name, 
            chunk_size=10000, 
            max_retries=3,
            drop_existing=True,
    ):
        """
        Updates a Pandas DataFrame into a database in chunks with retry logic.

        Args:
            df (pandas.DataFrame): The DataFrame to update.
            db_table_name (str): Name of the database table to update.
            chunk_size (int): Number of rows per chunk.
            max_retries (int): Maximum number of retries on update failures.
            drop_existing (bool): Whether to drop the existing table before update (default: True).
        Returns:
            None

        Raises:
            Exception: If the update fails beyond retries.
        """

        if drop_existing and self._table_exists(db_table_name):
            self._drop_table(db_table_name)

        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)

        # Iterate over DataFrame chunks
        for i in range(num_chunks):
            df_chunk = df[i*chunk_size:(i+1)*chunk_size]
            update_start_time = time.time()        
            attempt = 0
            
            # Perform chunk update (implement database-specific query here)
            while attempt <= max_retries:
                try:
                    # Replace this with your actual update logic
                    df_chunk.to_sql(name=db_table_name, con=self.db_conn, if_exists='append')
                    logging.info(f"Chunk {i} updated successfully in {time.time() - update_start_time:.2f} seconds")
                    break
                except Exception as e:
                    logging.error(f"Chunk {i} update failed (attempt {attempt}/{max_retries}): {e}")
                    attempt += 1
                    time.sleep(5)

            if attempt > max_retries:
                logging.error(f"Unrecoverable error: Maximum retries reached for chunk {i}")
                raise Exception("Maximum retries reached")

        logging.info("Database update completed successfully.")

    def log_dataframe_schema(self, df, db_table_name):
        """
        Log the schema of the DataFrame and details of its columns.

        Args:
        df (pandas.DataFrame): The DataFrame whose schema is to be logged.
        db_table_name (str): Name of the database table corresponding to the DataFrame.
        db_conn: Database connection object.

        Returns:
        None
        """
        # Log the schema of the DataFrame as it would appear in the database
        logging.info(pd.io.sql.get_schema(df, name=db_table_name, con=self.db_conn))

        # Log details of each column in the DataFrame
        for col in df.columns:
            logging.info(f"Column '{col}': dtype: {df[col].dtype}, name: {df[col].name}")
