import json
import argparse
import os.path
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values
import logging
# INTERFACE_IDS is a key value list of interface IDs to their respective control config file path
from config.config import INTERFACE_IDS
from queue import Queue
from threading import Thread
import pandas as pd

from helpers import move_file_to_folder, load_json_mapping
from logger.sqllogger import SQLLogger

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

def flatten_dict(data):
    """Flattens a nested dictionary and integrates repeated elements as individual rows."""
    # Initialize the base record, which stores non-nested key-value pairs
    base_record = {}
    # List to store records resulting from nested elements
    nested_records = []

    # Iterate through the dictionary items
    for key, value in data.items():
        if isinstance(value, list):
            # If the value is a list, iterate through the list
            for nested in value:
                if isinstance(nested, dict):
                    # Copy the base record and merge nested dictionaries
                    new_record = base_record.copy()
                    new_record.update(nested)
                    nested_records.append(new_record)
        elif isinstance(value, dict):
            # If the value is a dictionary, merge it with the base record
            base_record.update(value)
        else:
            # If the value is neither a list nor a dictionary, add it to the base record
            base_record[key] = value

    # If there are no nested records, return the base record as a single-item list
    if not nested_records:
        return [base_record]

    # Update each nested record with the base record values
    for record in nested_records:
        record.update(base_record)

    return nested_records


def parse_json_file(file_path, schema_tag="Records"):
    """Parses and flattens JSON records from a file."""
    try:
        # Open and load the JSON file
        with open(file_path, "r") as file:
            data = json.load(file)
            logger.info(f"Successfully loaded JSON file: {file_path}")
    except FileNotFoundError:
        # Handle case where the file does not exist
        logger.error(f"JSON file not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        # Handle invalid JSON syntax
        logger.error(f"Error parsing JSON file: {e}")
        raise

    # Extract records using the schema tag
    records = data.get(schema_tag, data)

    # Flatten and yield each record
    if isinstance(records, list):
        for record in records:
            for flattened in flatten_dict(record):
                yield flattened
    elif isinstance(records, dict):
        for flattened in flatten_dict(records):
            yield flattened


def parse_xml_file(file_path, schema_tag="Record"):
    """Parses and flattens XML records from a file."""
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()
        logger.info(f"Successfully parsed XML file: {file_path}")
    except FileNotFoundError:
        # Handle case where the file does not exist
        logger.error(f"XML file not found: {file_path}")
        raise
    except ET.ParseError as e:
        # Handle invalid XML syntax
        logger.error(f"Error parsing XML file: {e}")
        raise

    def parse_element(element):
        """Recursively parses an XML element into a dictionary."""
        record = {}
        for child in element:
            if len(child) > 0:
                # Handle nested elements by appending to a list
                if child.tag not in record:
                    record[child.tag] = []
                record[child.tag].append(parse_element(child))
            else:
                # Add leaf node text to the record
                record[child.tag] = child.text.strip() if child.text else None
        return record

    # Extract records and flatten them
    for record_element in root.findall(f".//{schema_tag}"):
        raw_record = parse_element(record_element)
        flattened_records = flatten_dict(raw_record)
        for record in flattened_records:
            yield record


def process_file(file_path, schema_tag, file_type="json", output_queue=None):
    """
    Processes a file (JSON or XML), flattens records, and optionally queues them.

    Args:
        file_path (str): Path to the input file.
        schema_tag (str): The schema tag name for JSON/XML records.
        file_type (str): Either 'json' or 'xml' to specify file type.
        output_queue (queue.Queue): Optional queue to stream records to a consumer.
    """
    # Choose the parser based on file type
    parser = parse_json_file if file_type == "json" else parse_xml_file

    # Iterate through parsed records
    for record in parser(file_path, schema_tag=schema_tag):
        if output_queue:
            # Put records into the queue for parallel processing
            logger.debug(f"Adding record to queue: {record}")
            output_queue.put(record)
        else:
            # Yield records sequentially for single-threaded processing
            logger.debug(f"Yielding record: {record}")
            yield record

    # Signal the consumer that processing is complete
    if output_queue:
        logger.debug("Signaling consumer that processing is complete.")
        output_queue.put(None)


def transform_and_validate_records(records, key_column_mapping):
    """Transform and validate a list of records based on a key-column mapping."""
    transformed_records = []
    for record in records:
        transformed_record = {}
        missing_keys = set()

        # Map keys from the record to the specified schema
        for json_key, db_column in key_column_mapping.items():
            if json_key in record:
                transformed_record[db_column] = record[json_key]
            else:
                missing_keys.add(json_key)

        if missing_keys:
            # Log a warning for missing keys
            logger.warning(f"Record missing keys: {missing_keys}")

        transformed_records.append(transformed_record)

    # Log the total number of transformed records
    logger.info(f"Transformed {len(transformed_records)} records.")
    return transformed_records


def write_records_to_csv(records, output_file_path):
    """
    Write transformed records to a CSV file using Pandas.

    Args:
        records (list[dict]): List of dictionaries containing the data to write.
        output_file_path (str): Path to the output CSV file.
    """
    if not records:
        # Log a warning if no records are available
        logger.warning("No records to write to CSV.")
        return

    try:
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        # Convert records to a Pandas DataFrame
        df = pd.DataFrame(records)

        # Write DataFrame to a CSV file
        df.to_csv(output_file_path, index=False, sep='|', encoding='utf-8')
        logger.info(f"CSV file successfully written to: {output_file_path}")
    except Exception as e:
        # Handle errors during file writing
        logger.error(f"Failed to write CSV file: {e}")
        raise

def connect_to_postgres(config):
    """Connect to the PostgreSQL database."""
    try:
        # Establish connection to the PostgreSQL database using credentials from the config
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        logging.info("Successfully connected to PostgreSQL.")
        return conn  # Return the active connection object
    except Exception as e:
        # Log the error if connection fails
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def batch_insert_records(logger, conn, table_name, records):
    """
    Perform a batch insert of multiple records into the database with custom logging.

    Args:
        logger (SQLLogger): Instance of the SQLLogger for custom logging.
        conn: Database connection object.
        table_name (str): Name of the target table.
        records (list): List of records to insert.
    """
    if not records:
        # Log a warning if there are no records to insert
        logger.log_job(symbol="GS2001W", success=False)
        return

    try:
        with conn.cursor() as cur:
            # Prepare the SQL INSERT query
            columns = records[0].keys()
            query = 'INSERT INTO {} ({}) VALUES %s'.format(
                table_name,
                ', '.join('"{}"'.format(col.lower()) for col in columns)
            )
            values = [[record[col] for col in columns] for record in records]

            # Log the execution of the query
            job_id = logger.log_job(symbol="GS1001I", query=query, success=True)

            # Execute batch insert
            execute_values(cur, query, values)
            conn.commit()

            # Log successful insertion
            logger.log_job(symbol="GS1001I", job_id=job_id, query=query, success=True)
    except Exception as e:
        # Log the error and rollback
        logger.log_job(symbol="GS2002E", job_id=job_id, query=query, metadata={"exception": str(e)},
                       success=False)
        conn.rollback()
        raise


def consumer_transform_and_insert(logger, queue, conn, table_name, key_column_mapping):
    """
    Transforms records from the queue and inserts them into PostgreSQL in batches with custom logging.

    Args:
        logger (SQLLogger): Instance of the SQLLogger for custom logging.
        queue (Queue): Queue containing records to process.
        conn: Database connection object.
        table_name (str): Name of the target table.
        key_column_mapping (dict): Mapping of JSON keys to database column names.
        # job_id (int): Job ID for logging context.
    """
    batch = []  # Collect records for batch insertion
    while True:
        record = queue.get()
        if record is None:  # Check for the termination signal
            queue.task_done()
            break

        # Log receipt of the record
        # job_id = logger.log_job(
        #     error_symbol="GS2005I",  # Informational code
        #     success=True,
        # )

        # Transform the record based on the key-column mapping
        transformed_record = {
            db_column: record.get(json_key)
            for json_key, db_column in key_column_mapping.items()
        }
        batch.append(transformed_record)
        queue.task_done()

        # Perform batch insert if batch size reaches threshold
        if len(batch) >= 5:
            batch_insert_records(logger, conn, table_name, batch)
            batch.clear()

    # Insert remaining records in the batch
    if batch:
        batch_insert_records(logger, conn, table_name, batch)

    # Log completion of the consumer
    # logger.log_job(
    #     error_symbol="GS2006I",  # Informational code
    #     success=True,
    # )


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

    input_directory = config["inputDirectory"]
    output_directory = config["outputDirectory"]
    files_to_process = (
        [os.path.join(input_directory, args.file)]
        if args.file
        else [
            os.path.join(input_directory, f)
            for f in os.listdir(input_directory)
            if f.endswith(".json") or f.endswith(".xml")
        ]
    )

    if not files_to_process:
        logging.error(f"No .json or .xml files found in {input_directory}.")
        raise ValueError(f"No files to process in {input_directory}.")

    logger = SQLLogger(config, error_table="error_definitions")
    conn = connect_to_postgres(config)

    for file_path in files_to_process:
        file_type = "json" if file_path.endswith(".json") else "xml"
        schema_tag = config.get(f"{file_type}TagName", "Records")
        key_column_mapping = config.get(f"{file_type}Schema")

        # job_id = logger.log_job(
        #     error_symbol="GS1001I",
        #     success=True
        # )

        record_queue = Queue(maxsize=100)

        producer = Thread(
            target=lambda: [
                record_queue.put(record)
                for record in process_file(file_path, schema_tag, file_type)
            ]
        )
        consumer = Thread(
            target=consumer_transform_and_insert,
            args=(logger, record_queue, conn, config["tableName"], key_column_mapping),
        )

        producer.start()
        consumer.start()

        producer.join()
        record_queue.put(None)  # Signal end of records
        consumer.join()

        move_file_to_folder(file_path, output_directory)

    conn.close()
    logger.close()
    # logger.log_job(
    #     error_symbol="GS1002I",
    #     success=True
    # )
