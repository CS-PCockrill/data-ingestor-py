import logging
import os
import json

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

def move_file_to_folder(file_path, folder_path):
    import shutil

    shutil.move(file_path, folder_path)

    # Move the processed file to the output directory
    try:
        os.makedirs(folder_path, exist_ok=True)  # Ensure the directory exists
        shutil.move(file_path, folder_path)
        logging.info(f"File moved to: {folder_path}")
    except Exception as e:
        logging.error(f"Failed to move file {file_path} to {folder_path}: {e}")