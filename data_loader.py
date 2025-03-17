import math
from tqdm import tqdm
import pandas as pd
import tempfile
import os


class DataLoader:
    def __init__(self, engine):
        self.engine = engine

    def load_dimension(self, df, table_name, chunk_size=50000):
        """
        Loads dimension data in chunks using tqdm for progress.
        This method replaces existing data so duplicates are avoided.
        """
        self._load_in_chunks(df, table_name, chunk_size)
        print(f"[DataLoader] Loaded dimension '{table_name}' with {len(df)} rows.")

    def load_fact(self, df, table_name, chunk_size=50000):
        """
        Loads fact data in chunks using tqdm for progress.
        This method replaces existing data so duplicates are avoided.
        """
        self._load_in_chunks(df, table_name, chunk_size)
        print(f"[DataLoader] Loaded fact '{table_name}' with {len(df)} rows.")

    def _load_in_chunks(self, df: pd.DataFrame, table_name: str, chunk_size: int):
        """
        Helper function that uses tqdm to show progress for large DataFrame inserts.
        If the data fits in one chunk, then the table is replaced (avoiding duplicates).
        For multi-chunk loads, the first chunk replaces the table and subsequent chunks append.
        """
        total_rows = len(df)
        total_chunks = math.ceil(total_rows / chunk_size)
        print(f"Loading {total_rows} rows into '{table_name}' in {total_chunks} chunk(s).")

        if total_chunks == 1:
            # Replace the table if it fits in one chunk
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            return

        start_index = 0
        with tqdm(total=total_rows, desc=f"Loading {table_name}", colour='green', leave=True) as pbar:
            while start_index < total_rows:
                end_index = start_index + chunk_size
                chunk_df = df.iloc[start_index:end_index]
                if start_index == 0:
                    # Replace the table on the first chunk
                    chunk_df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                else:
                    # Append subsequent chunks
                    chunk_df.to_sql(table_name, self.engine, if_exists='append', index=False)
                start_index = end_index
                pbar.update(len(chunk_df))

    def load_fact_using_copy(self, df, table_name):
        """
        Alternative method: Loads fact data using PostgreSQL's COPY command.
        This writes the DataFrame to a temporary CSV file and then uses psycopg2's COPY for a bulk load.
        IMPORTANT: The table is created (or replaced) using df.head(0) to avoid duplicates.
        """
        # Create (or replace) the table with the correct schema
        df.head(0).to_sql(table_name, self.engine, if_exists='replace', index=False)

        # Write the DataFrame to a temporary CSV file (without header and index)
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as tmp:
            df.to_csv(tmp.name, index=False, header=False)
            tmp_file = tmp.name

        # Use a raw connection to execute the COPY command
        conn = self.engine.raw_connection()
        try:
            cur = conn.cursor()
            with open(tmp_file, 'r') as f:
                cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)
            conn.commit()
            print(f"[DataLoader] Loaded fact '{table_name}' with {len(df)} rows using COPY.")
        except Exception as e:
            print(f"Error during COPY: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
            os.remove(tmp_file)
