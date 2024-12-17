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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

def load_json_mapping(file_path):
    """Load key-value mapping from a JSON file into a dictionary."""
    try:
        with open(file_path, 'r') as file:
            mapping = json.load(file)
            logging.info(f"Successfully loaded JSON mapping from {file_path}")
            return mapping
    except FileNotFoundError:
        logging.error(f"Mapping file not found at {file_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON mapping file at {file_path}: {e}")
        raise

import os
import csv
import json
import logging
import psycopg2
import xml.etree.ElementTree as ET
from queue import Queue
from threading import Thread
from psycopg2.extras import execute_values

BATCH_SIZE = 1000  # Number of records per batch

def flatten_dict(data):
    """Flattens a nested dictionary."""
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

def parse_file(file_path, schema_tag, file_type, output_queue):
    """Parses a JSON or XML file and streams flattened records into a queue."""
    try:
        if file_type == "json":
            with open(file_path, "r") as file:
                data = json.load(file)
                records = data.get(schema_tag, data)
                for record in (records if isinstance(records, list) else [records]):
                    for flattened in flatten_dict(record):
                        output_queue.put(flattened)
        elif file_type == "xml":
            tree = ET.parse(file_path)
            root = tree.getroot()
            for record_element in root.findall(f".//{schema_tag}"):
                raw_record = {}
                for child in record_element:
                    raw_record[child.tag] = child.text
                for flattened in flatten_dict(raw_record):
                    output_queue.put(flattened)
    finally:
        output_queue.put(None)  # Signal end of parsing

def consumer_transform_and_insert(queue, conn, table_name, key_column_mapping):
    """Transforms records from the queue and inserts them into PostgreSQL in batches."""
    batch = []
    while True:
        record = queue.get()
        if record is None:  # End of data
            break

        # Transform record
        transformed_record = {}
        for json_key, db_column in key_column_mapping.items():
            transformed_record[db_column] = record.get(json_key)

        batch.append(transformed_record)

        # Perform batch insert when enough records are collected
        if len(batch) >= BATCH_SIZE:
            batch_insert_records(conn, table_name, batch)
            batch.clear()

    # Insert any remaining records
    if batch:
        batch_insert_records(conn, table_name, batch)

def batch_insert_records(conn, table_name, records):
    """Batch insert records into PostgreSQL."""
    if not records:
        return

    try:
        with conn.cursor() as cur:
            columns = records[0].keys()
            query = 'INSERT INTO {} ({}) VALUES %s'.format(
                table_name, ', '.join(f'"{col}"' for col in columns)
            )
            values = [[record[col] for col in columns] for record in records]
            execute_values(cur, query, values)
            conn.commit()
            logging.info(f"Inserted {len(records)} records into {table_name}.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to insert records: {e}")

def connect_to_postgres(config):
    """Connect to the PostgreSQL database."""
    try:
        return psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise

if __name__ == "__main__":
    import argparse

    # Command-line arguments
    parser = argparse.ArgumentParser(description="Stream and process JSON/XML files.")
    parser.add_argument("-file", required=True, help="Path to input JSON/XML file.")
    parser.add_argument("-interface_id", required=True, help="Interface ID.")
    args = parser.parse_args()

    # Validate interface ID
    if args.interface_id not in INTERFACE_IDS:
        logging.error(f"Interface ID '{args.interface_id}' not found in INTERFACE_IDS.")
        raise ValueError(f"Invalid Interface ID: {args.interface_id}")

    # Load the configuration file for the specified interface
    config_path = INTERFACE_IDS[args.interface_id]
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Failed to load configuration file: {e}")
        raise

    # Determine file type and tags
    file_path = os.path.join(config["inputDirectory"], args.file)
    file_type = "json" if file_path.endswith(".json") else "xml" if file_path.endswith(".xml") else None
    if not file_type:
        logging.error("Unsupported file type. The input file must have a .json or .xml extension.")
        raise ValueError("Unsupported file type. The input file must have a .json or .xml extension.")

    schema_tag = config.get(f"{file_type}TagName", "Records")
    key_column_mapping = config.get(f"{file_type}Schema")

    # Initialize the database connection and queue
    conn = connect_to_postgres(config)
    record_queue = Queue(maxsize=BATCH_SIZE * 2)

    # Start producer (parsing file) and consumer (transform and insert)
    producer_thread = Thread(target=parse_file, args=(file_path, schema_tag, file_type, record_queue))
    consumer_thread = Thread(target=consumer_transform_and_insert, args=(
        record_queue, conn, config["tableName"], key_column_mapping
    ))

    logging.info(f"Starting processing for interface ID: {args.interface_id}")
    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

    conn.close()
    logging.info("Processing completed successfully.")
