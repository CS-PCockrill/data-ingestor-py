import os
from fileprocesser.processor import Processor
from msgbroker.excel_consumer import ExcelConsumer
from msgbroker.excel_producer import ExcelProducer
import logging

class DMSProcessor(Processor):
    def __init__(self, connection_manager, logger, config):
        """
        Initialize the DMSProcessor with configuration.

        Args:
            config (dict): Configuration settings for processing.
        """
        # Call the parent Processor's init (if applicable)
        super().__init__()

        # Initialize instance-specific attributes
        self.config = config

    def process_files(self, files, producer=None):
        """
        Process a list of Excel files.

        Args:
            files (list): List of file paths to Excel files.
        """
        for file_path in files:
            try:
                logging.info(f"Processing file: {file_path}")

                # Initialize the producer to load records from the Excel file
                producer = ExcelProducer(file_path, header_row=3)
                producer.produce()  # Load records

                # Define the output CSV file path
                output_csv = os.path.join(
                    self.config["outputDirectory"],
                    os.path.basename(file_path).replace(".xlsx", "-output.csv").replace(".xls", "-output.csv"),
                )
                os.makedirs(self.config["outputDirectory"], exist_ok=True)

                # Initialize the consumer to process records and write to CSV
                consumer = ExcelConsumer(producer, output_csv)
                consumer.consume()  # Process records
                consumer.finalize()  # Write to CSV

            except Exception as e:
                logging.error(f"Failed to process Excel file {file_path}: {e}")

            finally:
                # Ensure the producer releases resources
                producer.close()
