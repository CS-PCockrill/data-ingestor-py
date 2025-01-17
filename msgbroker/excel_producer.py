from queue import Queue
import pandas as pd
import logging

from msgbroker.producer_consumer import Producer


class ExcelProducer(Producer):
    def __init__(self, file_path, header_row=3):
        """
        Initialize the ExcelProducer.

        Args:
            file_path (str): Path to the Excel file to process.
            header_row (int): The row number to use as headers (1-based indexing).
        """
        super().__init__()
        self.file_path = file_path
        self.header_row = header_row
        self.records = None

    def produce(self, message=None):
        """
        Produce records by reading from the Excel file.
        """
        try:
            df = pd.read_excel(self.file_path, header=None)
            headers = df.iloc[self.header_row - 1].tolist()
            data = df.iloc[self.header_row:]
            data.columns = headers
            data.reset_index(drop=True, inplace=True)
            self.records = data.to_dict(orient="records")
            logging.info(f"Successfully produced records from {self.file_path}")
        except Exception as e:
            logging.error(f"Error reading Excel file {self.file_path}: {e}")
            raise

    def close(self):
        """
        Close the producer and clean up resources.
        """
        self.records = None
        logging.info("ExcelProducer closed successfully.")
