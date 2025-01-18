import argparse
import os.path

import logging
from contextlib import contextmanager

from db.connection_manager import PostgresConnectionManager
from fileprocesser.dms_processor import DMSProcessor
from fileprocesser.file_processor import FileProcessor
from fileprocesser.processor_factory import ProcessorFactory
from helpers import load_json_mapping
from logger.logger_factory import LoggerFactory
from logger.sqllogger import SQLLogger
from msgbroker.consumer_factory import ConsumerFactory
from msgbroker.excel_consumer import ExcelConsumer
from msgbroker.excel_producer import ExcelProducer
from msgbroker.file_producer import FileProducer
from msgbroker.producer_factory import ProducerFactory
from msgbroker.sql_consumer import SQLConsumer

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

def register_components():
    """
    Registers components (loggers, processors) for different interfaces in the ingestion system.

    This function sets up modular components for data ingestion by associating specific
    interfaces with their corresponding processors, loggers, control files, and supported file extensions.

    Structure:
    - INTERFACES defines the configuration for each interface.
    - Each interface has:
        - `interface_ids`: Unique identifiers for the interface.
        - `control_file_path`: Path to the control file (e.g., schema or metadata).
        - `processor_class`: Class responsible for processing files.
        - `file_extensions`: Supported file types for this interface.
        - `logger_class`: Logger class for tracking operations.

    How to Add New Interfaces:
    1. Add a new entry in the `INTERFACES` list with the following keys:
        - `interface_ids`: A set of unique strings identifying the new interface.
        - `control_file_path`: Path to the control file containing configuration for the processor.
        - `processor_class`: A custom processor class implementing required functionality.
        - `file_extensions`: List of file extensions supported by the processor.
        - `logger_class`: Logger class to use for the interface.
    2. Ensure the new `processor_class` and `logger_class` are implemented and compatible with
       the system's interfaces (e.g., inherit from base classes like `Processor` or `Logger`).
    3. Optional: Implement Producers/Consumers if the processor supports interchangeability.

    Example:
    To add a CSV processor:
    ```
    {
        "interface_ids": {"csv", "csv-ingestion"},
        "control_file_path": "interfaces/csv/control-file.json",
        "processor_class": CSVProcessor,
        "file_extensions": [".csv"],
        "logger_class": SQLLogger,
    }
    ```

    Notes on Producers/Consumers:
    - The Processor can also support interchangeable Producers and Consumers.
    - Producers generate or fetch data (e.g., pulling from APIs, reading files).
    - Consumers handle post-processing (e.g., inserting data into SQL tables or sending to a message queue).

    Returns:
        None
    """

    # Define the configuration for each interface
    INTERFACES = [
        {
            "interface_ids": {"mist", "mist-ams"},  # Unique IDs for this interface
            "control_file_path": "interfaces/mist-ams/control-file.json",  # Path to control/configuration file
            "processor_class": FileProcessor,  # Processor responsible for handling files for this interface
            "file_extensions": [".json", ".xml"],  # Supported file extensions for ingestion
            "logger_class": SQLLogger,  # Logger to track operations and log metadata
            "producer_class": FileProducer,
            "consumer_class": SQLConsumer,
        },
        {
            "interface_ids": {"dms", "dms-test"},  # Unique IDs for another interface
            "control_file_path": "interfaces/dms/control-file.json",  # Path to control/configuration file
            "processor_class": DMSProcessor,  # Processor for handling DMS-specific files
            "file_extensions": [".xlsx", ".xls"],  # Supported Excel file types
            "logger_class": SQLLogger,  # Logger for DMS
            "producer_class": ExcelProducer,
            "consumer_class": ExcelConsumer,
        },
    ]

    # Iterate over each interface configuration to register components
    for interface in INTERFACES:
        # Register the logger class for the specified interface IDs
        LoggerFactory.register_logger(
            interface["interface_ids"],  # Set of unique interface IDs
            interface["logger_class"],  # Logger class to register
        )

        for id in interface["interface_ids"]:
            # Register the producer with the factory
            ProducerFactory.register_producer(id, interface["producer_class"])
            # Register the consumer with the factory
            ConsumerFactory.register_consumer(id, interface["consumer_class"])

        # Register the processor class for the specified interface IDs
        ProcessorFactory.register_processor(
            interface["interface_ids"],  # Set of unique interface IDs
            interface["control_file_path"],  # Path to the control file
            interface["processor_class"],  # Processor class to register
            interface["file_extensions"],  # List of supported file extensions
        )

def parse_arguments():
    """
    Parse command-line arguments for the script.
    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Stream and process JSON/XML files.")
    parser.add_argument(
        "-file",
        required=False,
        help="Path to input JSON/XML file. If omitted, all files in the input directory will be processed.",
    )
    parser.add_argument(
        "-interface_id",
        required=True,
        help="Interface ID to identify the configuration and context.",
    )
    return parser.parse_args()

def validate_interface_id(interface_id, interface_ids):
    """
    Validate the provided interface ID.
    Args:
        interface_id (str): The interface ID to validate.
        interface_ids (dict): Valid interface IDs.
    Raises:
        ValueError: If the interface ID is invalid.
    """
    if interface_id not in interface_ids:
        logging.error(f"Interface ID '{interface_id}' not found in INTERFACE_IDS.")
        raise ValueError(f"Invalid Interface ID: {interface_id}")

def load_config(interface_id, interface_ids):
    """
    Load the configuration for the specified interface ID.
    Args:
        interface_id (str): The interface ID to use.
        interface_ids (dict): Mapping of interface IDs to config paths.
    Returns:
        dict: Loaded configuration.
    """
    config_path = interface_ids[interface_id]
    try:
        config = load_json_mapping(config_path)
        logging.info(f"Successfully loaded configuration from {config_path} with configuration:\n\t{config}")
        return config
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        raise

def get_files_to_process(config, file_arg, extensions):
    """
    Get a list of files to process.

    Args:
        config (dict): Configuration dictionary.
        file_arg (str): Specific file path from command-line arguments.
        extensions (list): List of valid file extensions.

    Returns:
        list: List of file paths to process.
    """
    input_dir = config["inputDirectory"]
    if file_arg:
        return [os.path.join(input_dir, file_arg)]
    return [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if any(f.endswith(ext) for ext in extensions)
    ]



@contextmanager
def resource_manager(processor, logger):
    """
    Context manager to handle resource cleanup.
    Args:
        processor (FileProcessor): File processor instance.
        logger (SQLLogger): Logger instance.
    Yields:
        None
    """
    try:
        yield
    finally:
        try:
            processor.close()
        except Exception as e:
            logging.warning(f"Failed to close the processor: {e}")
        try:
            logger.close()
        except Exception as e:
            logging.warning(f"Failed to close the logger: {e}")

# def main():
#     """
#     Main entry point for the script.
#     """
#     # 1. Register all components
#     register_components()
#
#     # 2. Parse command-line arguments
#     args = parse_arguments()
#
#     # 3. Validate interface ID and load configuration
#     interface_id = args.interface_id
#     if interface_id not in ProcessorFactory.get_interface_configs().keys():
#         logging.error(f"Interface ID '{interface_id}' is not registered.")
#         raise ValueError(f"Invalid Interface ID: {interface_id}")
#
#     # Load the configuration for the interface
#     control_file_path = ProcessorFactory.get_control_file_path(interface_id)
#     config = load_json_mapping(control_file_path)
#
#     # Determine source type and invoke processor
#     try:
#         # Create the processor
#         processor = ProcessorFactory.create_processor(interface_id, config)
#
#         # Check for supported file extensions to determine source type
#         file_extensions = ProcessorFactory.get_supported_file_extensions(interface_id)
#
#         if file_extensions:
#             # Handle file-based source
#             files = get_files_to_process(config, args.file, extensions=file_extensions)
#             if not files:
#                 logging.error(f"No files with valid extensions found in {config['inputDirectory']}.")
#                 raise ValueError(f"No files to process in {config['inputDirectory']}.")
#             processor.process_files(files)
#         else:
#             # Handle non-file-based source and destination
#             producer = ProducerFactory.create_producer(interface_id, **config.get("producerConfig", {}))
#             consumer = ConsumerFactory.create_consumer(interface_id, **config.get("consumerConfig", {}))
#             processor.process(producer=producer, consumer=consumer)
#
#         # Write metrics if supported
#         if hasattr(processor, "write_metrics_to_file"):
#             processor.write_metrics_to_file("prometheus_metrics.txt")
#
#     except Exception as e:
#         logging.error(f"An error occurred during processing: {e}")
#         raise
#     finally:
#         if hasattr(processor, "close"):
#             processor.close()
#
#     logging.info("Processing completed successfully.")


def main():
    """
    Main entry point for the script.
    """
    # 1. Register all components
    register_components()

    # 2. Parse command-line arguments
    args = parse_arguments()

    # 3. Validate interface ID and load configuration
    interface_id = args.interface_id
    if interface_id not in ProcessorFactory.get_interface_configs().keys():
        logging.error(f"Interface ID '{interface_id}' is not registered.")
        raise ValueError(f"Invalid Interface ID: {interface_id}")

    # Load the configuration for the interface
    control_file_path = ProcessorFactory.get_control_file_path(interface_id)
    config = load_json_mapping(control_file_path)

    # Update the producerConfig to include input directory or specific file
    config["producerConfig"].update({
        "file_path": os.path.join(config["inputDirectory"], args.file),
        "file_type": "json",
        "schema_tag": "jsonSchema",
    })

    # Update the consumerConfig dynamically
    config["consumerConfig"].update({
        "logger": None,
        "table_name": config.get("tableName"),
        "producer": None,
        "connection_manager": PostgresConnectionManager(config),
        "key_column_mapping": config.get("jsonSchema"),
        "batch_size": 5,
    })

    try:
        # Create processor
        processor = ProcessorFactory.create_processor(interface_id, config)

        # Create producer dynamically
        producer = ProducerFactory.create_producer(interface_id, **config["producerConfig"])

        # Update the consumerConfig to reference the created producer
        config["consumerConfig"].update({"producer": producer})
        config["consumerConfig"].update({"logger": processor.logger})

        # Create consumer dynamically
        consumer = ConsumerFactory.create_consumer(interface_id, **config["consumerConfig"])

        # Process records using the processor
        processor.process(producer=producer, consumer=consumer)

        # Write metrics if supported
        if hasattr(processor, "write_metrics_to_file"):
            processor.write_metrics_to_file("prometheus_metrics.txt")

    except Exception as e:
        logging.error(f"Processing failed: {e}")
        raise
    finally:
        if hasattr(processor, "close"):
            processor.close()


if __name__ == "__main__":
    main()
