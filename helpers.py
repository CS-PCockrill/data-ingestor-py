import logging
import os
import json
import pandas as pd

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


