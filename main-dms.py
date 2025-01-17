import pandas as pd
import argparse
import logging
import os
from config.config import INTERFACE_IDS
from helpers import load_json_mapping


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
    Write a DataFrame to a CSV file with a '|' delimiter.

    Args:
        data (pd.DataFrame): DataFrame containing the data to write.
        output_file (str): Path to the output CSV file.
    """
    try:
        # Write the DataFrame to a CSV file using '|' as the delimiter
        data.to_csv(output_file, index=False, sep='|')
        print(f"Data successfully written to CSV file with '|' delimiter: {output_file}")
    except Exception as e:
        print(f"Error writing data to CSV: {e}")
        raise



if __name__ == "__main__":
    # Command-line arguments
    parser = argparse.ArgumentParser(description="Process Excel file(s) based on input flags.")
    parser.add_argument("-file", required=False, help="Path to the input file (Excel).")
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

    # Get input and output directories
    input_directory = config["inputDirectory"]
    output_directory = config["outputDirectory"]

    # Determine files to process
    if args.file:
        # Process a single file
        files_to_process = [os.path.join(input_directory, args.file)]
    else:
        # Process all .xlsx and .xls files in the input directory
        files_to_process = [
            os.path.join(input_directory, f)
            for f in os.listdir(input_directory)
            if f.endswith(".xlsx") or f.endswith(".xls")
        ]

    if not files_to_process:
        logging.error(f"No .xlsx or .xls files found in {input_directory}.")
        raise ValueError(f"No files to process in {input_directory}.")

    # Process each file sequentially
    for file_path in files_to_process:
        try:
            logging.info(f"Processing file: {file_path}")

            # Determine file type
            file_type = "xlsx" if file_path.lower().endswith(".xlsx") else "xls"
            if not file_type:
                logging.error(f"Unsupported file type: {file_path}")
                continue

            # Load Excel file
            df = load_excel_file(file_path)

            # Extract headers and data
            headers, data = get_headers_and_data(df, header_row=3)

            # Define output CSV path
            output_csv = os.path.join(output_directory, os.path.basename(file_path).replace(f".{file_type}", "-output.csv"))

            # Ensure output directory exists
            os.makedirs(output_directory, exist_ok=True)

            # Write data to CSV
            write_data_to_csv(data, output_csv)
            # move_file_to_folder(file_path, output_directory)

            logging.info(f"File successfully processed and saved to: {output_csv}")
        except Exception as e:
            logging.error(f"Failed to process file {file_path}: {e}")
