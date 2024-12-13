import json
import csv
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

def load_json_mapping(file_path):
    """Load key-value mapping from a JSON file into a dictionary."""
    try:
        with open(file_path, 'r') as file:
            mapping = json.load(file)
            return mapping
    except FileNotFoundError:
        raise Exception(f"Mapping file not found at {file_path}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error parsing JSON mapping file at {file_path}: {e}")

def flatten_dict(data):
    """
    Flattens a nested dictionary and integrates repeated elements as individual rows.

    Args:
        data (dict): The dictionary to flatten.

    Returns:
        list[dict]: A list of flattened dictionaries, one for each repeated element.
    """
    base_record = {}
    nested_records = []

    for key, value in data.items():
        if isinstance(value, list):
            # Handle repeated elements (lists)
            for nested in value:
                if isinstance(nested, dict):
                    # Create a new record for each repeated element
                    new_record = base_record.copy()
                    new_record.update(nested)  # Integrate the nested fields
                    nested_records.append(new_record)
        elif isinstance(value, dict):
            # Flatten nested dictionaries into base_record
            base_record.update(value)
        else:
            # Add primitive fields to base_record
            base_record[key] = value

    # If no nested records were created, return the base record as a single entry
    if not nested_records:
        return [base_record]

    # If nested records exist, copy base fields into each
    for record in nested_records:
        record.update(base_record)

    return nested_records

def parse_json_file(file_path, schema_tag="Records"):
    """
    Parses and flattens JSON records from a file.

    Args:
        file_path (str): Path to the JSON file.
        schema_tag: Tag in JSON file to extract records from.

    Yields:
        dict: A flattened record.
    """
    with open(file_path, "r") as file:
        data = json.load(file)

    records = data.get(schema_tag, data)  # Adjust based on your schema
    if isinstance(records, list):
        for record in records:
            for flattened in flatten_dict(record):
                yield flattened
    elif isinstance(records, dict):
        for flattened in flatten_dict(records):
            yield flattened

def parse_xml_file(file_path, schema_tag="Record"):
    """
    Parses and flattens XML records from a file.

    Args:
        file_path (str): Path to the XML file.
        schema_tag (str): The tag name for records to extract.

    Yields:
        dict: A flattened record.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    def parse_element(element):
        """Recursively parses an XML element into a dictionary."""
        record = {}
        for child in element:
            if len(child) > 0:  # Child has nested elements
                if child.tag not in record:
                    record[child.tag] = []
                record[child.tag].append(parse_element(child))
            else:  # Child is a simple element
                record[child.tag] = child.text.strip() if child.text else None
        return record

    for record_element in root.findall(f".//{schema_tag}"):
        raw_record = parse_element(record_element)
        flattened_records = flatten_dict(raw_record)
        for record in flattened_records:
            yield record

def process_file(file_path, schema_tag, file_type="json"):
    """
    Processes a file (JSON or XML) and flattens the records.

    Args:
        file_path (str): Path to the file.
        schema_tag (str): The tag name for records to extract
        file_type (str): Type of the file ('json' or 'xml').

    Yields:
        dict: A flattened record.
    """
    if file_type == "json":
        yield from parse_json_file(file_path, schema_tag=schema_tag)
    elif file_type == "xml":
        yield from parse_xml_file(file_path, schema_tag=schema_tag)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

def transform_and_validate_records(records, key_column_mapping):
    """
    Transform and validate a list of records based on a key-column mapping.

    Parameters:
    - records (list[dict]): The list of records to transform and validate.
    - key_column_mapping (dict): A dictionary mapping JSON/XML keys to DB column names.

    Returns:
    - list[dict]: A list of transformed records with keys as DB column names.
    """
    transformed_records = []

    for record in records:
        transformed_record = {}
        missing_keys = set()

        # Map keys using the key-column mapping
        for json_key, db_column in key_column_mapping.items():
            if json_key in record:
                transformed_record[db_column] = record[json_key]
            else:
                missing_keys.add(json_key)

        # Log missing keys if any
        if missing_keys:
            print(f"Record missing keys: {missing_keys}")

        # Add the transformed record to the result
        transformed_records.append(transformed_record)

    return transformed_records

def write_records_to_csv(records, output_file_path):
    """
    Write transformed records to a CSV file.

    Parameters:
    - records (list[dict]): The list of transformed records.
    - output_file_path (str): The file path for the output CSV.
    """
    if not records:
        print("No records to write to CSV.")
        return

    # Get the headers from the keys of the first record
    headers = list(records[0].keys())

    try:
        with open(output_file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)

            # Write the header
            writer.writeheader()

            # Write each record
            for record in records:
                writer.writerow(record)

        print(f"CSV file successfully written to: {output_file_path}")
    except Exception as e:
        print(f"Failed to write CSV file: {e}")

def connect_to_postgres(config):
    """
    Connect to the PostgreSQL database.

    Parameters:
    - config (dict): Database configuration with keys: host, port, database, user, password.

    Returns:
    - conn: A connection object to the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        print("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        raise


def generate_insert_query(table_name, record):
    """
    Generate an `INSERT` query for PostgreSQL based on the record.

    Parameters:
    - table_name (str): Name of the target database table.
    - record (dict): A dictionary where keys are column names and values are data to insert.

    Returns:
    - query (str): The generated `INSERT` query.
    - values (tuple): The tuple of values corresponding to the placeholders in the query.
    """
    columns = record.keys()
    placeholders = ", ".join(["%s"] * len(record))
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    values = tuple(record.values())
    return query, values


def batch_insert_records(conn, table_name, records):
    """
    Perform a batch insert of multiple records into the database.

    Parameters:
    - conn: PostgreSQL connection object.
    - table_name (str): Name of the target database table.
    - records (list[dict]): A list of dictionaries representing the records to insert.

    Returns:
    - None
    """
    if not records:
        print("No records to insert.")
        return

    try:
        with conn.cursor() as cur:
            # Extract columns from the first record
            columns = records[0].keys()
            query = 'INSERT INTO {} ({}) VALUES %s'.format(
                table_name,
                ', '.join('"{}"'.format(col) for col in columns)
            )

            # Prepare values for all records
            values = [[record[col] for col in columns] for record in records]

            # Use execute_values for efficient batch insertion
            execute_values(cur, query, values)
            conn.commit()
            print(f"Successfully inserted {len(records)} records into {table_name}")
    except Exception as e:
        print(f"Failed to insert records: {e}")
        conn.rollback()
        raise


# Example Usage
if __name__ == "__main__":
    # Example JSON
    json_file_path = "test-loader.json"

    # Example XML
    xml_file_path = "test-loader.xml"

    # Control Config File
    config_path = "control-file.json"
    config = load_json_mapping(config_path)
    print(config)

    # Connect to PostgreSQL
    conn = connect_to_postgres(config)

    # Transform and validate records
    transformed_records = transform_and_validate_records(process_file(json_file_path, schema_tag=config["jsonTagName"], file_type="json"), config['jsonSchema'])
    print("Transformed records (JSON):")
    for record in transformed_records:
        print(record)


    # Transform and validate records
    transformed_records = transform_and_validate_records(process_file(xml_file_path, schema_tag=config["xmlTagName"], file_type="xml"), config['xmlSchema'])
    print("Transformed records (XML):")
    for record in transformed_records:
        print(record)


    write_records_to_csv(transformed_records, "output.csv")

    # Insert Records
    batch_insert_records(conn, "SFLW_RECS", transformed_records)

    # Close the connection
    conn.close()

