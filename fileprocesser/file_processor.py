import os
import logging
from threading import Lock
from prometheus_client import generate_latest

from config.config import METRICS
from fileprocesser.processor import Processor
from msgbroker.file_producer import FileProducer
from msgbroker.sql_consumer import SQLConsumer

class FileProcessor(Processor):

    def __init__(self, connection_manager, logger, config):
        """
        Initialize the FileProcessor instance with required components.

        Args:
            connection_manager (DBConnectionManager): Manages database connections.
            logger (SQLLogger): Handles logging operations.
            config (dict): Configuration settings for file processing.
        """
        super().__init__(logger)
        self.connection_manager = connection_manager
        self.logger = logger
        self.config = config
        self.conn = self.connection_manager.connect()
        self.table_name = config["tableName"]
        self.batch_size = config["sqlBatchSize"]
        self.worker_states = {}
        self.state_lock = Lock()  # Protect shared worker_states

        logging.info(f"FileProcessor initialized for table: {self.table_name}")

    def write_metrics_to_file(self, file_path="metrics_output.txt"):
        """
        Write all Prometheus metrics to a specified file when the program exits.

        Args:
            file_path (str): Path to the output file where metrics will be written.

        Raises:
            IOError: If writing to the file fails.
        """
        try:
            # Generate the latest metrics data
            metrics_output = generate_latest()
            with open(file_path, "w") as metrics_file:
                metrics_file.write(metrics_output.decode("utf-8"))  # Decode bytes for writing
            logging.info(f"Metrics successfully written to {file_path}")
        except Exception as e:
            logging.error(f"Failed to write metrics to file: {e}")
            raise

    def process_files(self, files, producer=None):
        """
        Process a list of files dynamically based on their file types.

        Args:
            files (list): List of file paths to process.
            producer (Producer): Optional producer for message queue.

        Behavior:
            - Determines the file type (JSON or XML) based on the file extension.
            - Fetches the schema and tag name for the file type.
            - Calls the `_process` method to handle parsing and insertion for each file.
        """
        for file_path in files:
            # Determine the file type based on the file extension
            file_type = "json" if file_path.endswith(".json") else "xml"

            # Retrieve the appropriate schema and tag name
            try:
                schema, tag_name = self._get_schema_and_tag(file_type)
            except ValueError as e:
                logging.error(f"Error determining schema and tag for file {file_path}: {e}")
                METRICS["errors"].inc()
                continue

            # Log the start of processing for the file
            logging.info(f"Processing file {file_path} as {file_type}.")

            # Process the file using the determined schema and tag and move to the output directory after successful processing
            try:
                # File-based processing
                file_producer = FileProducer(maxsize=1000)
                file_producer.set_source(file_path=file_path, file_type=file_type, schema_tag=tag_name)

                consumer = SQLConsumer(
                    logger=self.logger,
                    table_name=self.table_name,
                    producer=file_producer,
                    connection_manager=self.connection_manager,
                    key_column_mapping=schema,
                    batch_size=5
                )

                # Invoke the processing pipeline
                # Call parent class _process method
                super().process(producer=file_producer, consumer=consumer, key_column_mapping=schema)

                self._move_file_to_folder(file_path, self.config["outputDirectory"])
            except Exception as e:
                logging.error(f"File processing failed for {file_path}: {e}")
                METRICS["errors"].inc()
                raise

    def _get_schema_and_tag(self, file_type):
        """
        Dynamically retrieve the schema and tag name based on the file type.

        Args:
            file_type (str): File type, either 'json' or 'xml'.

        Returns:
            tuple: A tuple containing the schema and tag name for the specified file type.

        Raises:
            ValueError: If an unsupported file type is provided.
        """
        if file_type == "json":
            # Retrieve schema and tag name for JSON files
            return self.config.get("jsonSchema"), self.config.get("jsonTagName", "Records")
        elif file_type == "xml":
            # Retrieve schema and tag name for XML files
            return self.config.get("xmlSchema"), self.config.get("xmlTagName", "Record")
        else:
            # Handle unsupported file types
            raise ValueError(f"Unsupported file type: {file_type}")

    def _move_file_to_folder(self, file_path, folder_path):
        import shutil

        try:
            # Ensure the directory exists
            os.makedirs(folder_path, exist_ok=True)

            # Construct the destination path
            destination_path = os.path.join(folder_path, os.path.basename(file_path))

            # If the file already exists at the destination, overwrite it
            if os.path.exists(destination_path):
                os.replace(file_path, destination_path)
                logging.info(f"File overwritten at: {destination_path}")
            else:
                shutil.move(file_path, destination_path)
                logging.info(f"File moved to: {destination_path}")
        except Exception as e:
            logging.error(f"Failed to move or overwrite file {file_path} to {folder_path}: {e}")

    def close(self):
        """
        Close the database connection and release resources.
        """
        self.conn.close()
        logging.info("Database connection closed.")