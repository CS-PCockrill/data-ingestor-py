import json
import logging
import os
import uuid
from queue import Queue
import xml.etree.ElementTree as ET

from config.config import METRICS
from msgbroker.producer_consumer import Producer


class FileProducer(Producer):
    """
    Producer that reads data from files (JSON/XML) and pushes records to a queue.
    """

    def __init__(self, maxsize=1000, file_path=None, file_type="json", schema_tag="Records", logger=None, **kwargs):
        super().__init__(logger=logger, **kwargs)
        self.queue = Queue(maxsize=maxsize)
        self.file_path = file_path
        self.file_type = file_type
        self.schema_tag = schema_tag
        self.artifact_name = file_path

    def set_source(self, file_path, file_type, schema_tag):
        """
        Sets the source for the file-based producer.

        Args:
            file_path (str): Path to the input file.
            file_type (str): File type, either 'json' or 'xml'.
            schema_tag (str): Schema tag to extract records.
        """
        self.file_path = file_path
        self.file_type = file_type
        self.schema_tag = schema_tag

        if os.path.isfile(file_path):
            self.artifact_name = os.path.basename(file_path)
        elif os.path.isdir(file_path):
            self.artifact_name = file_path  # Directory name for logging purposes
        else:
            raise ValueError(f"Invalid file path: {file_path}")

    def _get_files(self):
        """
        Retrieves the list of files to process.

        Returns:
            list: List of file paths to process.
        """
        if os.path.isfile(self.file_path):
            return [self.file_path]
        elif os.path.isdir(self.file_path):
            return [
                os.path.join(self.file_path, f)
                for f in os.listdir(self.file_path)
                if f.endswith(".json") or f.endswith(".xml")
            ]
        else:
            raise ValueError(f"Invalid file path: {self.file_path}")

    def produce_from_source(self):
        """
        Reads files and produces records into the queue.
        """
        if not self.file_path:
            raise ValueError("File path not set for FileProducer")

        files_to_process = self._get_files()
        if not files_to_process:
            raise ValueError(f"No valid JSON or XML files found in {self.file_path}")

        for file in files_to_process:
            # Determine file type based on the extension if not explicitly set
            file_type = "json" if file.endswith(".json") else "xml"
            schema_tag = "Records" if file_type == "json" else "Record"

            self.logger.set_context_id(str(uuid.uuid4()))
            logging.info(f"Processing file: {file} with Context ID: {self.logger.get_context_id()}")

            logging.info(f"Processing file: {file} as {file_type}")
            for record in self._process_file(file, schema_tag, file_type):
                self.produce(record)
                METRICS["records_read"].inc()

        self.signal_done()

    def produce(self, record):
        """
        Adds a record to the queue.
        """
        self.queue.put(record)

    def consume(self):
        """
        Retrieves a record from the queue.
        """
        return self.queue.get()

    def signal_done(self):
        """
        Signals that production is complete.
        """
        self.queue.put(None)

    def close(self):
        """
        Clears the queue and releases resources.
        """
        while not self.queue.empty():
            self.queue.get()
            self.queue.task_done()

    def get_context_id(self):
        """
        Retrieves the current context ID for the Producer.
        """
        return self.ctx_id

    def _process_file(self, file_path, schema_tag, file_type="json"):
        """
        Parses and flattens JSON/XML records from a file.

        Args:
            file_path (str): Path to the input file.
            schema_tag (str): Schema tag to extract records.
            file_type (str): File type ('json' or 'xml').

        Yields:
            dict: Flattened records extracted from the file.
        """
        parser = self.parse_json_file if file_type == "json" else self.parse_xml_file
        for record in parser(file_path, schema_tag):
            yield record

    def _flatten_dict(self, data):
        """
        Flattens a nested dictionary and handles repeated elements as individual rows.

        This is critical for processing hierarchical data structures into a normalized format
        suitable for database operations.

        Args:
            data (dict): The nested dictionary to be flattened.

        Returns:
            list[dict]: A list of flattened dictionaries derived from the input data.

        Example:
            Input: {"key1": "value1", "key2": [{"subkey1": "value2"}, {"subkey1": "value3"}]}
            Output: [{"key1": "value1", "subkey1": "value2"}, {"key1": "value1", "subkey1": "value3"}]
        """
        # Initialize the base record, containing non-nested key-value pairs
        base_record = {}
        # List to store records resulting from nested elements
        nested_records = []

        # Iterate over the dictionary items
        for key, value in data.items():
            if isinstance(value, list):
                # If the value is a list, iterate through its elements
                for nested in value:
                    if isinstance(nested, dict):
                        # Copy base record and merge with nested dictionary
                        new_record = base_record.copy()
                        new_record.update(nested)
                        nested_records.append(new_record)
            elif isinstance(value, dict):
                # If the value is a dictionary, merge it with the base record
                base_record.update(value)
            else:
                # Add scalar values to the base record
                base_record[key] = value

        # If no nested records exist, return the base record as a single-item list
        if not nested_records:
            logging.debug("No nested records found; returning base record.")
            return [base_record]

        # Update each nested record with values from the base record
        for record in nested_records:
            record.update(base_record)

        logging.debug(f"Flattened dictionary to {len(nested_records)} records.")
        return nested_records

    def parse_json_file(self, file_path, schema_tag="Records"):
        """
        Parses and flattens JSON records from a file.

        Args:
            file_path (str): Path to the JSON file.
            schema_tag (str): Key to extract records from the JSON structure (default: "Records").

        Yields:
            dict: Flattened records extracted from the JSON file.

        Raises:
            FileNotFoundError: If the JSON file is not found.
            json.JSONDecodeError: If the JSON file contains invalid syntax.
        """
        try:
            # Open and load the JSON file into a Python dictionary
            with open(file_path, "r") as file:
                data = json.load(file)
                logging.info(f"Successfully loaded JSON file: {file_path}")
        except FileNotFoundError:
            # Log and re-raise error if the file is missing
            logging.error(f"JSON file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            # Log and re-raise error for invalid JSON syntax
            logging.error(f"Error parsing JSON file: {e}")
            raise

        # Extract records using the schema tag or fallback to the root of the JSON structure
        records = data.get(schema_tag, data)

        # Flatten and yield each record
        if isinstance(records, list):
            for record in records:
                for flattened in self._flatten_dict(record):
                    yield flattened
        elif isinstance(records, dict):
            for flattened in self._flatten_dict(records):
                yield flattened

    def parse_xml_file(self, file_path, schema_tag="Record"):
        """
        Parses and flattens XML records from a file.

        Args:
            file_path (str): Path to the XML file.
            schema_tag (str): Tag to extract records from the XML structure (default: "Record").

        Yields:
            dict: Flattened records extracted from the XML file.

        Raises:
            FileNotFoundError: If the XML file is not found.
            ET.ParseError: If the XML file contains invalid syntax.
        """
        try:
            # Parse the XML file and obtain the root element
            tree = ET.parse(file_path)
            root = tree.getroot()
            logging.info(f"Successfully parsed XML file: {file_path}")
        except FileNotFoundError:
            # Log and re-raise error if the file is missing
            logging.error(f"XML file not found: {file_path}")
            raise
        except ET.ParseError as e:
            # Log and re-raise error for invalid XML syntax
            logging.error(f"Error parsing XML file: {e}")
            raise

        def parse_element(element):
            """
            Recursively parses an XML element into a dictionary.

            Args:
                element (xml.etree.ElementTree.Element): The XML element to parse.

            Returns:
                dict: Parsed representation of the element.
            """
            record = {}
            for child in element:
                if len(child) > 0:
                    # Handle nested elements by appending them to a list
                    if child.tag not in record:
                        record[child.tag] = []
                    record[child.tag].append(parse_element(child))
                else:
                    # Add leaf node text to the record
                    record[child.tag] = child.text.strip() if child.text else None
            return record

        # Extract and flatten records
        for record_element in root.findall(f".//{schema_tag}"):
            raw_record = parse_element(record_element)
            flattened_records = self._flatten_dict(raw_record)
            for record in flattened_records:
                yield record