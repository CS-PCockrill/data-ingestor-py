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
    """Parses and flattens JSON records from a file."""
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

    records = data.get(schema_tag, data)
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

def process_file(file_path, schema_tag, file_type="json"):
    """Processes a file (JSON or XML) and flattens the records."""
    if file_type == "json":
        yield from parse_json_file(file_path, schema_tag=schema_tag)
    elif file_type == "xml":
        yield from parse_xml_file(file_path, schema_tag=schema_tag)
    else:
        logging.error(f"Unsupported file type: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")

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
            execute_values(cur, query, values)
            conn.commit()
            logging.info(f"Successfully inserted {len(records)} records into {table_name}.")
    except Exception as e:
        logging.error(f"Failed to insert records: {e}")
        conn.rollback()
        raise

if __name__ == "__main__":
    # flags for a JSON or XML input file (-file)
    # and a configuration file with table information, schema, and connection details (-config)
    parser = argparse.ArgumentParser(description="Process JSON or XML file based on input flags.")
    parser.add_argument("-file", required=True, help="Path to the input file (JSON or XML).")
    parser.add_argument("-config", required=True, help="Path to the configuration file (JSON).")
    parser.add_argument("-interface_id", required=True, help="Interface ID")
    args = parser.parse_args()

    file_path = args.file
    file_type = "json" if file_path.lower().endswith(".json") else "xml" if file_path.lower().endswith(".xml") else None
    if not file_type:
        logging.error("Unsupported file type. The input file must have a .json or .xml extension.")
        raise ValueError("Unsupported file type. The input file must have a .json or .xml extension.")

    if not args.interface_id in INTERFACE_IDS:
        logging.error(f"Interface ID not found in key set: {args.interface_id}, {INTERFACE_IDS[args.interface_id]}")
        raise ValueError(f"Interface ID not found in key set: {args.interface_id}, {INTERFACE_IDS[args.interface_id]}")

    logging.info(f"Interface ID: {args.interface_id}, {INTERFACE_IDS[args.interface_id]}")
    # config_path = args.config
    config = load_json_mapping(INTERFACE_IDS[args.interface_id])

    conn = connect_to_postgres(config)

    transformed_records = transform_and_validate_records(
        process_file(file_path, schema_tag=config[f"{file_type}TagName"], file_type=file_type),
        config[f"{file_type}Schema"]
    )

    write_records_to_csv(transformed_records, os.path.join(config["outputDirectory"], "output.csv"))

    batch_insert_records(conn, config["tableName"], transformed_records)

    conn.close()
    logging.info("Database connection closed.")