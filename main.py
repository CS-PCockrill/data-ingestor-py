import argparse
import os.path

import logging
# INTERFACE_IDS is a key value list of interface IDs to their respective control config file path
from config.config import INTERFACE_IDS
from contextlib import contextmanager

from fileprocesser.dms_processor import DMSProcessor
from fileprocesser.file_processor import FileProcessor
from fileprocesser.processor import Processor
from fileprocesser.processor_factory import ProcessorFactory
from helpers import load_json_mapping
from logger.sqllogger import SQLLogger

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
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

def register_interfaces():
    ProcessorFactory.register_interface(
        interface_ids=["mist", "mist-ams"],
        control_file_path="interfaces/mist-ams/control-file.json",
        processor_class=FileProcessor,
        file_extensions=[".json", ".xml"],
        logger_class=SQLLogger,
    )

    ProcessorFactory.register_interface(
        interface_ids=["dms", "dms-test"],
        control_file_path="interfaces/dms/control-file.json",
        processor_class=DMSProcessor,
        file_extensions=[".xlsx", ".xls"],
        logger_class=SQLLogger,
    )


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

def main():
    """
    Main entry point for the script.
    """
    # 1. Register all interfaces
    register_interfaces()

    # 2. Parse command-line arguments
    args = parse_arguments()

    # 3. Validate interface ID and load configuration
    interface_id = args.interface_id
    if interface_id not in ProcessorFactory.get_interface_configs().keys():
        logging.error(f"Interface ID '{interface_id}' is not registered.")
        raise ValueError(f"Invalid Interface ID: {interface_id}")

    control_file_path = ProcessorFactory.get_control_file_path(interface_id)
    config = load_json_mapping(control_file_path)

    # 4. Retrieve the processor and file extensions
    try:
        processor = ProcessorFactory.create_processor(interface_id, config)
        file_extensions = ProcessorFactory.get_supported_file_extensions(interface_id)
    except ValueError as e:
        logging.error(f"Error retrieving processor or file extensions: {e}")
        raise

    # 5. Determine files to process
    files = get_files_to_process(config, args.file, extensions=file_extensions)
    if not files:
        logging.error(f"No files with valid extensions found in {config['inputDirectory']}.")
        raise ValueError(f"No files to process in {config['inputDirectory']}.")

    # 6. Process files using the selected processor
    try:
        processor.process_files(files)
        if hasattr(processor, "write_metrics_to_file"):
            processor.write_metrics_to_file("prometheus_metrics.txt")
    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")
        raise
    finally:
        if hasattr(processor, "close"):
            processor.close()

    logging.info("Processing completed successfully.")


if __name__ == "__main__":
    main()
