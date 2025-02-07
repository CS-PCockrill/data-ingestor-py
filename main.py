import argparse
import os.path

import logging
from contextlib import contextmanager

from config.interfaces_config import INTERFACES
from db.connection_factory import DBConnectionFactory
from fileprocesser.file_processor import FileProcessor
from fileprocesser.processor_factory import ProcessorFactory
from helpers import load_json_mapping
from logger.logger_factory import LoggerFactory
from logger.sqllogger import SQLLogger
from msgbroker.consumer_factory import ConsumerFactory
from msgbroker.producer_factory import ProducerFactory

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

# Determine file_type and schema_tag dynamically based on file extension
def determine_file_type_and_schema(file_name):
    file_extension = os.path.splitext(file_name)[1].lower()

    # Define mappings of file extensions to types and schemas
    file_type_mapping = {
        ".json": {"file_type": "json", "schema_tag": "Records", "schema": "jsonSchema"},
        ".xml": {"file_type": "xml", "schema_tag": "Record", "schema": "xmlSchema"},
        # Add more mappings here if needed
    }

    # Retrieve file_type and schema_tag or raise an error for unsupported extensions
    if file_extension in file_type_mapping:
        return file_type_mapping[file_extension]["file_type"], file_type_mapping[file_extension]["schema_tag"], file_type_mapping[file_extension]["schema"]
    else:
        raise ValueError(f"Unsupported file extension: {file_extension}")

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

    file_name = args.file
    file_path = os.path.join(config["inputDirectory"], file_name)

    # Dynamically determine file_type and schema_tag
    file_type, schema_tag, schema = determine_file_type_and_schema(file_name)

    logging.info(f"Processing with producer configuration: {config['producerConfig']}")

    try:
        # Create processor. To create new processors create subclasses of fileprocessor.Processor, then register
        # new interfaces in config.interfaces_config.INTERFACES
        processor = ProcessorFactory.create_processor(interface_id, config)

        # Update producerConfig with computed values. Each key is a parameter in the respective Producer subclass
        # For example: FileProducer(maxsize=1000, file_path=file_path, file_type="json", schema_tag="Records")
        config["producerConfig"].update({
            "file_path": file_path,
            "logger": processor.logger,
        })

        # Create producer dynamically
        producer = ProducerFactory.create_producer(interface_id, **config["producerConfig"])

        # Update the consumerConfig to reference the created producer and logger. Each key is a parameter in the respective Consumer subclass.
        # For example: SQLConsumer(logger, table_name, producer, connection_manager, key_column_mapping, batch_size=5)
        config["consumerConfig"].update({"producer": producer})
        config["consumerConfig"].update({"logger": processor.logger})
        config["consumerConfig"].update({"connection_manager": DBConnectionFactory.get_connection_manager(config['dbType'], config)
})
        config["consumerConfig"].update({"key_column_mapping": config[schema]})

        # Create consumer. To create new consumers create subclasses of msgbroker.Consumer, then register new interfaces
        # in config.interfaces_config.INTERFACES
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
