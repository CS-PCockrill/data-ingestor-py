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

    def process_files(self, files):
        """
        Processes a list of files dynamically based on their file types.

        Args:
            files (list): List of file paths to process.

        Behavior:
            - Determines the file type (JSON or XML) based on the file extension.
            - Fetches the schema and tag name for the file type.
            - Dynamically sets up producer and consumer for each file.
            - Calls `process` for each file.
        """
        if not files:
            logging.warning("No files provided for processing.")
            return

        for file_path in files:
            try:
                # Configure producer and consumer dynamically
                producer, consumer, schema = self._configure_pipeline_for_file(file_path)

                # Process the file
                logging.info(f"Starting processing for file: {file_path}")
                self.process(producer=producer, consumer=consumer, key_column_mapping=schema)

                # Move the file to the output directory after successful processing
                self._move_file_to_folder(file_path, self.config["outputDirectory"])
                logging.info(f"Successfully processed and moved file: {file_path}")

            except Exception as e:
                logging.error(f"Failed to process file {file_path}: {e}")
                METRICS["errors"].inc()

    def _configure_pipeline_for_file(self, file_path):
        """
        Configures the producer and consumer pipeline for a given file.

        Args:
            file_path (str): Path to the input file.

        Returns:
            tuple: Configured producer, consumer, and schema.

        Raises:
            ValueError: If the file type is unsupported or schema/tag cannot be determined.
        """
        # Determine file type and schema
        file_type = "json" if file_path.endswith(".json") else "xml"
        schema, tag_name = self._get_schema_and_tag(file_type)

        # Configure producer
        producer = FileProducer(maxsize=1000)
        producer.set_source(file_path=file_path, file_type=file_type, schema_tag=tag_name)

        # Configure consumer
        consumer = SQLConsumer(
            logger=self.logger,
            table_name=self.table_name,
            producer=producer,
            connection_manager=self.connection_manager,
            key_column_mapping=schema,
            batch_size=5
        )

        return producer, consumer, schema

    def _get_schema_and_tag(self, file_type):
        """
        Retrieves the schema and tag name for a given file type.

        Args:
            file_type (str): Type of the file ('json' or 'xml').

        Returns:
            tuple: Schema (dict) and tag name (str).

        Raises:
            ValueError: If the file type is unsupported or schema/tag is not found.
        """
        if file_type == "json":
            schema = self.config.get("jsonSchema")
            tag_name = self.config.get("jsonTag", "records")
        elif file_type == "xml":
            schema = self.config.get("xmlSchema")
            tag_name = self.config.get("xmlTag", "record")
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        if not schema or not tag_name:
            raise ValueError(f"Schema or tag name missing for file type: {file_type}")

        return schema, tag_name

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