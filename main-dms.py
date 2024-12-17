import pandas as pd
import argparse
import logging
import json
import os
from config.config import INTERFACE_IDS

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

def load_excel_file(file_path):
    """
    Load an Excel file into a pandas DataFrame.

    Args:
        file_path (str): Path to the Excel file.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    try:
        df = pd.read_excel(file_path, header=None)
        print(f"Successfully loaded Excel file: {file_path}")
        return df
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        raise


def get_headers_and_data(df, header_row=3):
    """
    Extract headers from a specified row and the data starting from the next row.

    Args:
        df (pd.DataFrame): DataFrame loaded from the Excel file.
        header_row (int): Row number to extract headers (1-based indexing).

    Returns:
        tuple: (list of column headers, DataFrame of data rows)
    """
    try:
        # Get headers from the specified row
        headers = df.iloc[header_row - 1].tolist()
        print(f"Extracted headers: {headers}")

        # Extract data starting from the row after the header
        data = df.iloc[header_row:]
        data.columns = headers  # Set headers as column names
        data.reset_index(drop=True, inplace=True)  # Reset index
        return headers, data
    except Exception as e:
        print(f"Error extracting headers and data: {e}")
        raise


def write_data_to_csv(data, output_file):
    """
    Write a DataFrame to a CSV file.

    Args:
        data (pd.DataFrame): DataFrame containing the data to write.
        output_file (str): Path to the output CSV file.
    """
    try:
        data.to_csv(output_file, index=False)
        print(f"Data successfully written to CSV file: {output_file}")
    except Exception as e:
        print(f"Error writing data to CSV: {e}")
        raise


# Example Usage
if __name__ == "__main__":
    # flags for a JSON or XML input file (-file)
    # and a configuration file with table information, schema, and connection details (-config)
    parser = argparse.ArgumentParser(description="Process Excel file based on input flags.")
    parser.add_argument("-file", required=True, help="Path to the input file (Excel).")
    parser.add_argument("-interface_id", required=True, help="Interface ID")
    args = parser.parse_args()

    if not args.interface_id in INTERFACE_IDS:
        logging.error(f"Interface ID not found in key set: {args.interface_id}, {INTERFACE_IDS[args.interface_id]}")
        raise ValueError(f"Interface ID not found in key set: {args.interface_id}, {INTERFACE_IDS[args.interface_id]}")

    logging.info(f"Interface ID: {args.interface_id}, {INTERFACE_IDS[args.interface_id]}")
    config = load_json_mapping(INTERFACE_IDS[args.interface_id])

    file_path = str(os.path.join(config["inputDirectory"], args.file))
    file_type = "xlsx" if file_path.lower().endswith(".xlsx") else "xls" if file_path.lower().endswith(".xls") else None
    if not file_type:
        logging.error("Unsupported file type. The input file must have a .xlsx or .xls extension.")
        raise ValueError("Unsupported file type. The input file must have a .xlsx or .xls extension.")

    output_csv = "output-dms.csv"  # Replace with desired CSV output path

    # Load Excel file
    df = load_excel_file(file_path)

    # Extract headers and data
    headers, data = get_headers_and_data(df, header_row=3)

    # Write data to CSV
    write_data_to_csv(data, os.path.join(config["outputDirectory"], output_csv))
