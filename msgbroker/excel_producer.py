from config.config import FILE_DELIMITER
from msgbroker.producer_consumer import Producer

import os
import logging
import pandas as pd
from queue import Queue

class ExcelProducer(Producer):

    # Row Configuration (1-based, adjusted to 0-based inside the class)
    HUMAN_READABLE_ROW = 1  # Row 1 in Excel (ignored)
    REQUIRED_OPTIONAL_ROW = 2  # Row 2 in Excel (ignored)
    COLUMN_NAMES_ROW = 3  # Row 3 in Excel (actual column names)
    TABLE_NAME_ROW = 4  # Row 4 in Excel (table name)
    FIRST_RECORD_ROW = 5  # Row 5 in Excel (first actual record)

    def __init__(self, maxsize=1000, file_path="", logger=None, **kwargs):
        """
        Initialize the ExcelProducer.

        Args:
            file_path (str): Path to the Excel file or directory containing Excel files.
            logger (object): Logging utility.
        """
        super().__init__(logger, **kwargs)
        self.queue = Queue(maxsize=maxsize)
        self.file_path = file_path

    def produce_from_source(self):
        """
        Reads Excel files and enqueues each record to be consumed.
        """
        file_paths = self._get_excel_files(self.file_path)

        for file in file_paths:
            try:
                df = pd.read_excel(file, header=None)

                # Extract table name (A4 in Excel → row index TABLE_NAME_ROW - 1 in Pandas)
                table_name = df.iloc[self.TABLE_NAME_ROW - 1, 0]

                # Extract column names (Row 3 in Excel → row index COLUMN_NAMES_ROW - 1 in Pandas)
                column_names = df.iloc[self.COLUMN_NAMES_ROW - 1].tolist()

                # Extract data (Row 5 in Excel → row index FIRST_RECORD_ROW - 1 in Pandas)
                data = df.iloc[self.FIRST_RECORD_ROW - 1:].copy()
                data.columns = column_names
                data.reset_index(drop=True, inplace=True)

                # Notify consumer of new file
                self.produce({"marker": FILE_DELIMITER, "table_name": table_name, "column_names": column_names})

                for _, record in data.iterrows():
                    self.produce(record.to_dict())  # Push each row individually

                logging.info(f"Produced records from {file} for table: {table_name}")

            except Exception as e:
                logging.error(f"Error reading Excel file {file}: {e}")
                raise

    def produce(self, record):
        """
        Adds a record to the queue.
        """
        self.queue.put(record)

    def consume(self):
        """
        Retrieves a record from the queue.
        """
        return self.queue.get()

    def signal_done(self):
        """
        Signals that production is complete.
        """
        self.queue.put(None)

    def _get_excel_files(self, path):
        """
        Returns a list of Excel file paths from a given file or directory.
        """
        if os.path.isdir(path):
            return [os.path.join(path, f) for f in os.listdir(path) if f.endswith((".xls", ".xlsx"))]
        return [path]

    def get_context_id(self):
        """
        Retrieves the current context ID for the Producer.
        """
        return self.ctx_id

    def close(self):
        """
        Close the producer and clean up resources.
        """
        logging.info("ExcelProducer closed successfully.")




