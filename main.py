import argparse
import os.path

import logging
# INTERFACE_IDS is a key value list of interface IDs to their respective control config file path
from config.config import INTERFACE_IDS
import pandas as pd

from fileprocesser.fileprocessor import FileProcessor
from helpers import move_file_to_folder, load_json_mapping
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

def write_records_to_csv(records, output_file_path):
    """
    Write transformed records to a CSV file using Pandas.

    Args:
        records (list[dict]): List of dictionaries containing the data to write.
        output_file_path (str): Path to the output CSV file.
    """
    if not records:
        # Log a warning if no records are available
        logging.warning("No records to write to CSV.")
        return

    try:
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        # Convert records to a Pandas DataFrame
        df = pd.DataFrame(records)

        # Write DataFrame to a CSV file
        df.to_csv(output_file_path, index=False, sep='|', encoding='utf-8')
        logging.info(f"CSV file successfully written to: {output_file_path}")
    except Exception as e:
        # Handle errors during file writing
        logging.error(f"Failed to write CSV file: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream and process JSON/XML files.")
    parser.add_argument("-file", required=False, help="Path to input JSON/XML file.")
    parser.add_argument("-interface_id", required=True, help="Interface ID.")
    args = parser.parse_args()

    if args.interface_id not in INTERFACE_IDS:
        logging.error(f"Interface ID '{args.interface_id}' not found in INTERFACE_IDS.")
        raise ValueError(f"Invalid Interface ID: {args.interface_id}")

    config_path = INTERFACE_IDS[args.interface_id]
    try:
        config = load_json_mapping(config_path)
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        raise

    files = [os.path.join(config["inputDirectory"], args.file)] if args.file else [
        os.path.join(config["inputDirectory"], f)
        for f in os.listdir(config["inputDirectory"])
        if f.endswith(".json") or f.endswith(".xml")
    ]

    if not files:
        logging.error(f"No .json or .xml files found in {config['inputDirectory']}.")
        raise ValueError(f"No files to process in {config['inputDirectory']}.")

    # connection_manager = DBConnectionManager(config)
    logger = SQLLogger(DBConnectionManager(config), context=LoggerContext(config['interfaceType'], config['user'], config['tableName'], config['errorDefinitionSourceLocation'], config['logsTableName']))
    processor = FileProcessor(DBConnectionManager(config), logger, config)

    processor.process_files(files)

    # processor.close()
    # logger.close()

