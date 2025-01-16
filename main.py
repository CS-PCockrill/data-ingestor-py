import argparse
import os.path

import logging
# INTERFACE_IDS is a key value list of interface IDs to their respective control config file path
from config.config import INTERFACE_IDS

from fileprocesser.fileprocessor import FileProcessor
from helpers import load_json_mapping
from logger.sqllogger import SQLLogger, LoggerContext
from db.connectionmanager import DBConnectionManager

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
    # Parse command-line arguments for input file and interface ID
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
    args = parser.parse_args()

    # Validate the provided interface ID
    if args.interface_id not in INTERFACE_IDS:
        logging.error(f"Interface ID '{args.interface_id}' not found in INTERFACE_IDS.")
        raise ValueError(f"Invalid Interface ID: {args.interface_id}")

    # Load the configuration for the specified interface ID
    config_path = INTERFACE_IDS[args.interface_id]
    try:
        config = load_json_mapping(config_path)
        logging.info(f"Successfully loaded configuration from {config_path}")
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        raise

    # Determine files to process: single file or all valid files in the input directory
    files = (
        [os.path.join(config["inputDirectory"], args.file)]  # Single file mode
        if args.file
        else [
            os.path.join(config["inputDirectory"], f)  # All files mode
            for f in os.listdir(config["inputDirectory"])
            if f.endswith(".json") or f.endswith(".xml")
        ]
    )

    # Validate that there are files to process
    if not files:
        logging.error(f"No .json or .xml files found in {config['inputDirectory']}.")
        raise ValueError(f"No files to process in {config['inputDirectory']}.")

    # Initialize components
    try:
        # Database connection manager
        connection_manager = DBConnectionManager(config)

        # Logger setup with context
        logger = SQLLogger(
            connection_manager,
            context=LoggerContext(
                config['interfaceType'],
                config['user'],
                config['tableName'],
                config['errorDefinitionSourceLocation'],
                config['logsTableName'],
            )
        )

        # File processor to handle file parsing and database insertion
        processor = FileProcessor(connection_manager, logger, config)

        # Process the files
        processor.process_files(files)

        # Write Prometheus metrics to file
        processor.write_metrics_to_file("prometheus_metrics.txt")

    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")
        raise

    finally:
        # Ensure resources are properly closed
        try:
            processor.close()
        except Exception as e:
            logging.warning(f"Failed to close the processor: {e}")
        try:
            logger.close()
        except Exception as e:
            logging.warning(f"Failed to close the logger: {e}")

    logging.info("Processing completed successfully.")


