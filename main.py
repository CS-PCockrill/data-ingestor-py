import json
import csv
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

def flatten_dict(data):
    """Flattens a nested dictionary and integrates repeated elements as individual rows."""
    base_record = {}
    nested_records = []

    for key, value in data.items():
        if isinstance(value, list):
            for nested in value:
                if isinstance(nested, dict):
                    new_record = base_record.copy()
                    new_record.update(nested)
                    nested_records.append(new_record)
        elif isinstance(value, dict):
            base_record.update(value)
        else:
            base_record[key] = value

    if not nested_records:
        return [base_record]

    for record in nested_records:
        record.update(base_record)

    return nested_records


def parse_json_file(file_path, schema_tag="Records"):
    """
    Parses and flattens JSON records from a file using Pandas.

    Args:
        file_path (str): Path to the JSON file.
        schema_tag (str): Key to extract records from the JSON file (default: "Records").

    Returns:
        pd.DataFrame: Flattened data as a Pandas DataFrame.
    """
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            logging.info(f"Successfully loaded JSON file: {file_path}")
    except FileNotFoundError:
        logging.error(f"JSON file not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON file: {e}")
        raise

    # Extract records
    records = data.get(schema_tag, data)

    # Flatten the records into a DataFrame
    if isinstance(records, list):
        df = pd.json_normalize(records)
    elif isinstance(records, dict):
        df = pd.json_normalize([records])  # Wrap in a list to handle a single record
    else:
        logging.error(f"Unexpected data format: {type(records)}")
        raise ValueError("The JSON structure is not compatible with Pandas processing.")

    return df


# def parse_json_file(file_path, schema_tag="Records"):
#     """Parses and flattens JSON records from a file."""
#     try:
#         with open(file_path, "r") as file:
#             data = json.load(file)
#             logging.info(f"Successfully loaded JSON file: {file_path}")
#     except FileNotFoundError:
#         logging.error(f"JSON file not found: {file_path}")
#         raise
#     except json.JSONDecodeError as e:
#         logging.error(f"Error parsing JSON file: {e}")
#         raise
#
#     records = data.get(schema_tag, data)
#     if isinstance(records, list):
#         for record in records:
#             for flattened in flatten_dict(record):
#                 yield flattened
#     elif isinstance(records, dict):
#         for flattened in flatten_dict(records):
#             yield flattened

def parse_xml_file(file_path, schema_tag="Record"):
    """Parses and flattens XML records from a file."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        logging.info(f"Successfully parsed XML file: {file_path}")
    except FileNotFoundError:
        logging.error(f"XML file not found: {file_path}")
        raise
    except ET.ParseError as e:
        logging.error(f"Error parsing XML file: {e}")
        raise

    def parse_element(element):
        record = {}
        for child in element:
            if len(child) > 0:
                if child.tag not in record:
                    record[child.tag] = []
                record[child.tag].append(parse_element(child))
            else:
                record[child.tag] = child.text.strip() if child.text else None
        return record

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
    parser = parse_json_file if file_type == "json" else parse_xml_file

    # Iterate over flattened records and handle queueing or serial processing
    for record in parser(file_path, schema_tag=schema_tag):
        if output_queue:
            # Put records into the queue for streaming/parallel processing
            output_queue.put(record)
        else:
            # Yield records sequentially (streaming)
            yield record

    # Signal queue consumers that the file processing is complete
    if output_queue:
        output_queue.put(None)

def transform_and_validate_records(records, key_column_mapping):
    """Transform and validate a list of records based on a key-column mapping."""
    transformed_records = []
    for record in records:
        transformed_record = {}
        missing_keys = set()

        for json_key, db_column in key_column_mapping.items():
            if json_key in record:
                transformed_record[db_column] = record[json_key]
            else:
                missing_keys.add(json_key)

        if missing_keys:
            logging.warning(f"Record missing keys: {missing_keys}")

        transformed_records.append(transformed_record)

    logging.info(f"Transformed {len(transformed_records)} records.")
    return transformed_records

def write_records_to_csv(records, output_file_path):
    """Write transformed records to a CSV file."""
    if not records:
        logging.warning("No records to write to CSV.")
        return

    headers = list(records[0].keys())

    try:
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        with open(output_file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            for record in records:
                writer.writerow(record)
        logging.info(f"CSV file successfully written to: {output_file_path}")
    except Exception as e:
        logging.error(f"Failed to write CSV file: {e}")
        raise

def connect_to_postgres(config):
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        logging.info("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def batch_insert_records(conn, table_name, records):
    """Perform a batch insert of multiple records into the database."""
    if not records:
        logging.warning("No records to insert.")
        return

    try:
        with conn.cursor() as cur:
            columns = records[0].keys()
            query = 'INSERT INTO {} ({}) VALUES %s'.format(
                table_name,
                ', '.join('"{}"'.format(col.lower()) for col in columns)
            )
            values = [[record[col] for col in columns] for record in records]
            logging.info(f"Executing query: {query}\nValues: {values}\nTable Name: {table_name}")
            execute_values(cur, query, values)
            conn.commit()
            logging.info(f"Successfully inserted {len(records)} records into {table_name}.")
    except Exception as e:
        logging.error(f"Failed to insert records: {e}")
        conn.rollback()
        raise

def consumer_transform_and_insert(queue, conn, table_name, key_column_mapping):
    """Transforms records from the queue and inserts them into PostgreSQL in batches."""
    batch = []
    while True:
        record = queue.get()
        if record is None:  # End of data
            queue.task_done()
            break

        logging.info(f"Received record: {record}")
        # Transform record
        transformed_record = {}
        for json_key, db_column in key_column_mapping.items():
            transformed_record[db_column] = record.get(json_key)

        batch.append(transformed_record)
        queue.task_done()

        # Perform batch insert when enough records are collected
        if len(batch) >= 5:
            batch_insert_records(conn, table_name, batch)
            batch.clear()

    # Insert any remaining records
    if batch:
        batch_insert_records(conn, table_name, batch)
    logging.info("Consumer finished processing.")



if __name__ == "__main__":

    # Command-line arguments
    parser = argparse.ArgumentParser(description="Stream and process JSON/XML files.")
    parser.add_argument("-file", required=False, help="Path to input JSON/XML file.")
    parser.add_argument("-interface_id", required=True, help="Interface ID.")
    args = parser.parse_args()

    # Validate interface ID
    if args.interface_id not in INTERFACE_IDS:
        logging.error(f"Interface ID '{args.interface_id}' not found in INTERFACE_IDS.")
        raise ValueError(f"Invalid Interface ID: {args.interface_id}")

    # Load the configuration file for the specified interface
    config_path = INTERFACE_IDS[args.interface_id]
    try:
        config = load_json_mapping(config_path)
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        raise

    # Determine files to process
    input_directory = config["inputDirectory"]
    output_directory = config["outputDirectory"]

    if args.file:
        # Process a single file
        files_to_process = [os.path.join(input_directory, args.file)]
    else:
        # Process all .json and .xml files in the input directory
        files_to_process = [
            os.path.join(input_directory, f)
            for f in os.listdir(input_directory)
            if f.endswith(".json") or f.endswith(".xml")
        ]

    if not files_to_process:
        logging.error(f"No .json or .xml files found in {input_directory}.")
        raise ValueError(f"No files to process in {input_directory}.")

    # Initialize database connection
    conn = connect_to_postgres(config)

    def process_file_thread(file_path):
        """Threaded function to process a single file."""
        file_type = "json" if file_path.endswith(".json") else "xml"
        schema_tag = config.get(f"{file_type}TagName", "Records")
        key_column_mapping = config.get(f"{file_type}Schema")

        record_queue = Queue(maxsize=10)

        def producer():
            """Producer thread to parse file and add records to the queue."""
            logging.info(f"Starting producer for {file_path}...")
            for record in process_file(file_path, schema_tag=schema_tag, file_type=file_type):
                record_queue.put(record)
            record_queue.put(None)  # Signal end of records
            logging.info(f"Producer finished processing {file_path}.")

        def consumer():
            """Consumer thread to transform and insert records into the database."""
            logging.info(f"Starting consumer for {file_path}...")
            consumer_transform_and_insert(record_queue, conn, config["tableName"], key_column_mapping)

        # Start producer and consumer threads
        producer_thread = Thread(target=producer)
        consumer_thread = Thread(target=consumer)

        producer_thread.start()
        consumer_thread.start()

        producer_thread.join()
        record_queue.join()
        consumer_thread.join()

        # Move the processed file to the output directory
        move_file_to_folder(file_path, output_directory)

    # Start a thread for each file
    threads = []
    for file_path in files_to_process:
        thread = Thread(target=process_file_thread, args=(file_path,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Close the database connection
    conn.close()
    logging.info("All files processed successfully.")
