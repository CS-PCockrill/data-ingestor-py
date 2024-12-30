import logger
import os
import json

def load_json_mapping(file_path):
    """Load key-value mapping from a JSON file into a dictionary."""
    try:
        with open(file_path, 'r') as file:
            mapping = json.load(file)
            logger.info(f"Successfully loaded JSON mapping from {file_path}")
            return mapping
    except FileNotFoundError:
        logger.error(f"Mapping file not found at {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON mapping file at {file_path}: {e}")
        raise


def move_file_to_folder(file_path, folder_path):
    import shutil

    try:
        # Ensure the directory exists
        os.makedirs(folder_path, exist_ok=True)

        # Construct the destination path
        destination_path = os.path.join(folder_path, os.path.basename(file_path))

        # If the file already exists at the destination, overwrite it
        if os.path.exists(destination_path):
            os.replace(file_path, destination_path)
            logger.info(f"File overwritten at: {destination_path}")
        else:
            shutil.move(file_path, destination_path)
            logger.info(f"File moved to: {destination_path}")
    except Exception as e:
        logger.error(f"Failed to move or overwrite file {file_path} to {folder_path}: {e}")
