import pandas as pd


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
    excel_file = "dms-example.xlsx"  # Replace with your Excel file path
    output_csv = "output-dms.csv"  # Replace with desired CSV output path

    # Load Excel file
    df = load_excel_file(excel_file)

    # Extract headers and data
    headers, data = get_headers_and_data(df, header_row=3)

    # Write data to CSV
    write_data_to_csv(data, output_csv)
