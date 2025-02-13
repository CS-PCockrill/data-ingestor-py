import json
import logging
import os
import uuid
from queue import Queue
import xml.etree.ElementTree as ET

from config.config import METRICS, FILE_DELIMITER
from msgbroker.producer_consumer import Producer


class FileProducer(Producer):
    """
    Producer that reads data from files (JSON/XML) and pushes records to a queue.
    """

    FILE_TYPES = {"json", "xml"}  # Supported file types

    def __init__(self, global_context=None, maxsize=1000, config=None, file_path=None, file_type=None, schema_tag=None, logger=None, **kwargs):
        super().__init__(logger=logger, **kwargs)
        self.global_context = global_context
        self.queue = Queue(maxsize=maxsize)
        self.config = config
        self.file_path = file_path
        self.file_type = file_type  # Auto-detected if None
        self.schema_tag = schema_tag  # Auto-detected if None
        self.artifact_name = file_path

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
        Reads files from a directory (or single file) and produces records into the queue.
        Dynamically determines the schema per file and passes key-column mapping as metadata.
        """
        if not self.file_path:
            raise ValueError("File path not set for FileProducer")

        files_to_process = self._get_files()
        if not files_to_process:
            raise ValueError(f"No valid JSON or XML files found in {self.file_path}")

        for file in files_to_process:
            # Determine file type dynamically
            file_type = "json" if file.endswith(".json") else "xml"

            # Retrieve the appropriate schema mapping per file
            schema_key = "jsonSchema" if file_type == "json" else "xmlSchema"
            key_column_mapping = self.config[schema_key]

            context_id = str(uuid.uuid4())
            self.logger.set_context_id(context_id)
            logging.info(
                f"Processing file: {file} with Context ID: {self.logger.get_context_id()} and Schema: {key_column_mapping}")

            # Pass metadata first
            self.global_context.set("key_column_mapping", key_column_mapping)
            self.global_context.set("filename", file.split("/")[-1])
            self.global_context.set("context_id", context_id)
            self.produce({
                "marker": FILE_DELIMITER,
            })

            # Process and enqueue records
            for record in self._process_file(file, file_type):
                transformed_record = {
                    db_column: record.get(json_key)
                    for json_key, db_column in key_column_mapping.items()
                }
                self.produce(transformed_record)
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

    def _process_file(self, file_path, file_type):
        """
        Parses and flattens JSON/XML records from a file.

        Args:
            file_path (str): Path to the input file.
            file_type (str): File type ('json' or 'xml').

        Yields:
            dict: Flattened records extracted from the file.
        """
        if file_type == "json":
            parser = self._parse_json_file
        elif file_type == "xml":
            parser = self._parse_xml_file
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        for record in parser(file_path):
            yield record

    def _detect_json_schema_tag(self, data):
        """
        Detects the key containing an array of records in a JSON structure.

        Args:
            data (dict): JSON data loaded into a dictionary.

        Returns:
            str: The detected schema tag or an empty string if not found.
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):  # Look for the first array in the structure
                    return key
        return ""  # Default to empty string if no array is found

    def _detect_xml_schema_tag(self, root):
        """
        Detects the most likely schema tag (e.g., "Record") by finding the most common
        direct child element under the root.

        Args:
            root (xml.etree.ElementTree.Element): The root element of the XML document.

        Returns:
            str: The detected schema tag or "Record" as a fallback.
        """
        tag_counts = {}
        for child in root:
            tag_counts[child.tag] = tag_counts.get(child.tag, 0) + 1

        # Get the most common child element under the root
        detected_tag = max(tag_counts, key=tag_counts.get, default="Row")
        logging.info(f"Detected XML schema tag: {detected_tag}")
        return detected_tag

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

    def _parse_json_file(self, file_path):
        """
        Parses and flattens JSON records from a file, dynamically detecting the schema tag.

        Args:
            file_path (str): Path to the JSON file.

        Yields:
            dict: Flattened records extracted from the JSON file.
        """
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
                logging.info(f"Successfully loaded JSON file: {file_path}")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"Error loading JSON file {file_path}: {e}")
            raise

        # Auto-detect schema tag if not provided
        schema_tag = self.schema_tag or self._detect_json_schema_tag(data)
        logging.info(f"Detected schema tag: {schema_tag}")

        records = data.get(schema_tag, data)

        if isinstance(records, list):
            for record in records:
                yield from self._flatten_dict(record)
        elif isinstance(records, dict):
            yield from self._flatten_dict(records)

    def _parse_xml_file(self, file_path):
        """
        Parses and flattens XML records from a file.

        Args:
            file_path (str): Path to the XML file.

        Yields:
            dict: Flattened records extracted from the XML file.
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            logging.info(f"Successfully parsed XML file: {file_path}")
        except (FileNotFoundError, ET.ParseError) as e:
            logging.error(f"Error loading XML file {file_path}: {e}")
            raise

        # Detect schema tag if not provided
        schema_tag = self.schema_tag or self._detect_xml_schema_tag(root)
        logging.info(f"Using XML schema tag: {schema_tag}")

        for record_element in root.findall(f".//{schema_tag}"):
            raw_record = self._parse_xml_element(record_element)
            yield from self._flatten_dict(raw_record)

    def _parse_xml_element(self, element):
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
                # Handle nested lists correctly
                if child.tag not in record:
                    record[child.tag] = []
                record[child.tag].append(self._parse_xml_element(child))
            else:
                # Add text values to the dictionary
                record[child.tag] = child.text.strip() if child.text else None
        return record