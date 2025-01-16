import json
import logging
import xml.etree.ElementTree as ET
from psycopg2.extras import execute_values
from queue import Queue
from threading import Thread
from prometheus_client import Counter, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential


FILE_DB_WRITE_SUCCESS = Counter("file_processor_db_write_success", "Number of successful DB writes")
FILE_DB_WRITE_FAILURE = Counter("file_processor_write_failure", "Number of failed DB writes")
FILE_PROCESSING_TIME = Histogram("file_processing_time_seconds", "Time taken to process files")

class FileProcessor:
    worker_states = {}

    def __init__(self, connection_manager, logger, config):
        """
        Initialize the FileProcessor.

        Args:
            logger (SQLLogger): Logger instance for logging operations.
            config (dict): Configuration dictionary with database and processing details.
        """
        self.connection_manager = connection_manager
        self.logger = logger
        self.config = config
        self.conn = self.connection_manager.connect()
        self.table_name = config["tableName"]
        self.batch_size = config["sqlBatchSize"]


    def _flatten_dict(self, data):
        """Flattens a nested dictionary and integrates repeated elements as individual rows."""
        # Initialize the base record, which stores non-nested key-value pairs
        base_record = {}
        # List to store records resulting from nested elements
        nested_records = []

        # Iterate through the dictionary items
        for key, value in data.items():
            if isinstance(value, list):
                # If the value is a list, iterate through the list
                for nested in value:
                    if isinstance(nested, dict):
                        # Copy the base record and merge nested dictionaries
                        new_record = base_record.copy()
                        new_record.update(nested)
                        nested_records.append(new_record)
            elif isinstance(value, dict):
                # If the value is a dictionary, merge it with the base record
                base_record.update(value)
            else:
                # If the value is neither a list nor a dictionary, add it to the base record
                base_record[key] = value

        # If there are no nested records, return the base record as a single-item list
        if not nested_records:
            return [base_record]

        # Update each nested record with the base record values
        for record in nested_records:
            record.update(base_record)

        return nested_records

    def _parse_json_file(self, file_path, schema_tag="Records"):
        """Parses and flattens JSON records from a file."""
        try:
            # Open and load the JSON file
            with open(file_path, "r") as file:
                data = json.load(file)
                logging.info(f"Successfully loaded JSON file: {file_path}")
        except FileNotFoundError:
            # Handle case where the file does not exist
            logging.error(f"JSON file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            # Handle invalid JSON syntax
            logging.error(f"Error parsing JSON file: {e}")
            raise

        # Extract records using the schema tag
        records = data.get(schema_tag, data)

        # Flatten and yield each record
        if isinstance(records, list):
            for record in records:
                for flattened in self._flatten_dict(record):
                    yield flattened
        elif isinstance(records, dict):
            for flattened in self._flatten_dict(records):
                yield flattened

    def _parse_xml_file(self, file_path, schema_tag="Record"):
        """Parses and flattens XML records from a file."""
        try:
            # Parse the XML file
            tree = ET.parse(file_path)
            root = tree.getroot()
            logging.info(f"Successfully parsed XML file: {file_path}")
        except FileNotFoundError:
            # Handle case where the file does not exist
            logging.error(f"XML file not found: {file_path}")
            raise
        except ET.ParseError as e:
            # Handle invalid XML syntax
            logging.error(f"Error parsing XML file: {e}")
            raise

        def parse_element(element):
            """Recursively parses an XML element into a dictionary."""
            record = {}
            for child in element:
                if len(child) > 0:
                    # Handle nested elements by appending to a list
                    if child.tag not in record:
                        record[child.tag] = []
                    record[child.tag].append(parse_element(child))
                else:
                    # Add leaf node text to the record
                    record[child.tag] = child.text.strip() if child.text else None
            return record

        # Extract records and flatten them
        for record_element in root.findall(f".//{schema_tag}"):
            raw_record = parse_element(record_element)
            flattened_records = self._flatten_dict(raw_record)
            for record in flattened_records:
                yield record

    def _process_file(self, file_path, schema_tag, file_type="json", output_queue=None):
        """
        Processes a file (JSON or XML), flattens records, and optionally queues them.

        Args:
            file_path (str): Path to the input file.
            schema_tag (str): The schema tag name for JSON/XML records.
            file_type (str): Either 'json' or 'xml' to specify file type.
            output_queue (queue.Queue): Optional queue to stream records to a consumer.
        """
        # Choose the parser based on file type
        parser = self._parse_json_file if file_type == "json" else self._parse_xml_file

        # Iterate through parsed records
        for record in parser(file_path, schema_tag=schema_tag):
            if output_queue:
                # Put records into the queue for parallel processing
                logging.debug(f"Adding record to queue: {record}")
                output_queue.put(record)
            else:
                # Yield records sequentially for single-threaded processing
                logging.debug(f"Yielding record: {record}")
                yield record

        # Signal the consumer that processing is complete
        if output_queue:
            logging.debug("Signaling consumer that processing is complete.")
            output_queue.put(None)

    def _transform_and_validate_records(self, records, key_column_mapping):
        """Transform and validate a list of records based on a key-column mapping."""
        transformed_records = []
        for record in records:
            transformed_record = {}
            missing_keys = set()

            # Map keys from the record to the specified schema
            for json_key, db_column in key_column_mapping.items():
                if json_key in record:
                    transformed_record[db_column] = record[json_key]
                else:
                    missing_keys.add(json_key)

            if missing_keys:
                # Log a warning for missing keys
                logging.warning(f"Record missing keys: {missing_keys}")

            transformed_records.append(transformed_record)

        # Log the total number of transformed records
        logging.info(f"Transformed {len(transformed_records)} records.")
        return transformed_records

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _batch_insert_records(self, records, artifact_name, conn):
        """
        Perform a batch insert using the specified connection.
        """
        if not records:
            self.logger.log_job(
                symbol="GS2001W",
                job_name=f"{self.table_name} BATCH INS",
                success=False,
                artifact_name=artifact_name,
            )
            return

        job_id = self.logger.log_job(
            "",
            "",
            symbol="GS1002I",
            job_name=f"{self.table_name} BATCH INS",
            artifact_name=artifact_name,
        )

        try:
            with conn.cursor() as cur:
                columns = records[0].keys()
                query = 'INSERT INTO {} ({}) VALUES %s'.format(
                    self.table_name,
                    ', '.join('"{}"'.format(col.lower()) for col in columns)
                )
                values = [[record[col] for col in columns] for record in records]

                execute_values(cur, query, values)
                self.logger.log_job(
                    query,
                    values,
                    symbol="GS1002I",
                    job_name=f"{self.table_name} BATCH INS",
                    job_id=job_id,
                    query=query,
                    values=values,
                    artifact_name=artifact_name,
                    success=True,
                )
        except Exception as e:
            self.logger.log_job(
                str(e),
                symbol="GS2002E",
                job_name=f"{self.table_name} BATCH INS",
                job_id=job_id,
                query=query,
                artifact_name=artifact_name,
                success=False,
                error_message=str(e),
            )
            raise

    def _consume_and_insert(self, queue, key_column_mapping, artifact_name, worker_id, conn):
        batch = []
        try:
            while True:
                record = queue.get()
                if record is None:
                    queue.task_done()
                    break

                batch.append(self._transform_record(record, key_column_mapping))
                queue.task_done()

                if len(batch) >= self.batch_size:
                    self._batch_insert_records(batch, artifact_name, conn)
                    batch.clear()

            if batch:
                self._batch_insert_records(batch, artifact_name, conn)

            # Mark success
            self.worker_states[worker_id]["error"] = False
        except Exception as e:
            logging.error(f"Worker {worker_id} encountered an error: {e}")
            self.worker_states[worker_id]["error"] = True
        finally:
            queue.put(None)  # Signal completion

    def _process(self, file_path, file_type, schema_tag, key_column_mapping):
        queue = Queue(maxsize=100)

        # Start producer
        producer = Thread(
            target=lambda: [
                queue.put(record)
                for record in self._process_file(file_path, schema_tag, file_type)
            ]
        )
        producer.start()

        # Start consumer workers
        for worker_id in range(self.config["numWorkers"]):
            conn = self.connection_manager.connect()
            worker_name = f"worker_{worker_id}"
            self.worker_states[worker_name] = {"conn": conn, "error": False}

            consumer = Thread(
                target=self._consume_and_insert,
                args=(queue, key_column_mapping, file_path, worker_name, conn),
            )
            consumer.start()

        producer.join()
        queue.put(None)  # Signal end of records

        # Wait for all workers to finish
        for state in self.worker_states.values():
            state["conn"].commit() if not state["error"] else state["conn"].rollback()
            state["conn"].close()

        # Log final status
        if any(state["error"] for state in self.worker_states.values()):
            logging.error("2PC: Transaction rollback due to errors.")
        else:
            logging.info("2PC: All transactions committed successfully.")

    def _get_schema_and_tag(self, file_type):
        """
        Get the schema and tag name dynamically based on the file type.

        Args:
            file_type (str): File type, either 'json' or 'xml'.

        Returns:
            tuple: A tuple containing the schema and tag name for the specified file type.
        """
        if file_type == "json":
            return self.config.get("jsonSchema"), self.config.get("jsonTagName", "Records")
        elif file_type == "xml":
            return self.config.get("xmlSchema"), self.config.get("xmlTagName", "Record")
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def _transform_record(self, record, key_column_mapping):
        transformed_record = {
            db_column: record.get(json_key)
            for json_key, db_column in key_column_mapping.items()
        }
        transformed_record["processed"] = False
        return transformed_record

    def process_files(self, files):
        """
        Process a list of files dynamically based on their file types.

        Args:
            files (list): List of file paths to process.
        """
        for file_path in files:
            file_type = "json" if file_path.endswith(".json") else "xml"
            schema, tag_name = self._get_schema_and_tag(file_type)
            logging.info(f"Processing file {file_path} as {file_type}.")
            self._process(file_path, file_type, tag_name, schema)

    def close(self):
        """
        Close the database connection and release resources.
        """
        self.conn.close()
        logging.info("Database connection closed.")