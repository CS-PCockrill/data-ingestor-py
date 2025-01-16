import json
import logging
from abc import ABC, abstractmethod
from queue import Queue
import xml.etree.ElementTree as ET
from prometheus_client import generate_latest, Counter, Histogram, Summary
from threading import Lock

from psycopg2.extras import execute_values
from tenacity import stop_after_attempt, wait_exponential, retry


class Producer(ABC):
    def __init__(self):
        self.artifact_name = None  # Common field for all producers

    @abstractmethod
    def produce(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

class Consumer(ABC):
    def __init__(self, producer):
        self.producer = producer
        self.artifact_name = producer.artifact_name  # Inherit artifact name from producer

    @abstractmethod
    def consume(self):
        """
        Abstract method to consume records.
        """
        pass

    @abstractmethod
    def process_record(self, record):
        """
        Abstract method to process a consumed record.
        """
        pass

    @abstractmethod
    def finalize(self):
        """
        Abstract method to handle finalization after consuming all records.
        """
        pass


class FileProducer(Producer):
    """
    Producer that reads data from files (JSON/XML) and pushes records to a queue.
    """
    def __init__(self, maxsize=1000):
        self.queue = Queue(maxsize=maxsize)
        self.file_path = None
        self.file_type = None
        self.schema_tag = None

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
        self.artifact_name = file_path.split('/')[-1]

    def produce_from_source(self):
        """
        Reads the file and produces records into the queue.
        """
        if not self.file_path or not self.file_type or not self.schema_tag:
            raise ValueError("Source not set for FileProducer")
        for record in self._process_file(self.file_path, self.schema_tag, self.file_type):
            self.produce(record)
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
        parser = self._parse_json_file if file_type == "json" else self._parse_xml_file
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

    def _parse_json_file(self, file_path, schema_tag="Records"):
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

    def _parse_xml_file(self, file_path, schema_tag="Record"):
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



class SQLConsumer(Consumer):
    METRICS = {
        "records_read": Counter(
            "file_processor_records_read",
            "Total number of records read from files."
        ),
        "records_processed": Counter(
            "file_processor_records_processed",
            "Total number of records successfully inserted into the database."
        ),
        "errors": Counter(
            "file_processor_errors",
            "Total number of errors encountered during processing."
        ),
        "file_processing_time": Summary(
            "file_processor_processing_time_seconds",
            "Time taken to process a file, including all worker operations."
        ),
        "batch_insert_time": Histogram(
            "file_processor_batch_insert_time_seconds",
            "Time taken to perform batch inserts into the database."
        )
    }

    def __init__(self, logger, producer, connection_manager, key_column_mapping, batch_size=100):
        """
        Initializes the SQLConsumer.

        Args:
            producer (Producer): The producer instance to consume records from.
            connection_manager: Database connection manager.
            key_column_mapping (dict): Mapping of JSON keys to database column names.
            batch_size (int): Number of records to process in a single batch.
        """
        super().__init__(producer)
        self.logger = logger
        self.connection_manager = connection_manager
        self.key_column_mapping = key_column_mapping
        self.batch_size = batch_size
        self.batch = []
        self.lock = Lock()
        self.conn = self.connection_manager.connect()
        self.error = False

    def consume(self):
        """
        Consumes records from the producer and processes them in batches.
        """
        job_id = self.logger.log_job(
            symbol="GS2001W",
            job_name=f"Consume Records for {self.producer.artifact_name}",
            artifact_name=self.producer.artifact_name,
            success=False,
        )

        try:
            while True:
                record = self.producer.consume()
                if record is None:
                    break  # Signal that production is complete

                self.process_record(record)

                if len(self.batch) >= self.batch_size:
                    self._insert_batch(job_id)

        except Exception as e:
            self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Consume Records for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
                error_message=str(e),
                job_id=job_id,
                success=False,
            )
            logging.error(f"SQLConsumer encountered an error while consuming records: {e}")
            self.METRICS["errors"].inc()
            self.error = True
        finally:
            # Insert any remaining records in the batch
            if self.batch:
                self._insert_batch(job_id)

            # Mark the job as completed
            self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Consume Records for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
                job_id=job_id,
                success=not self.error,
            )

    def process_record(self, record):
        """
        Transforms and adds a record to the batch.

        Args:
            record (dict): Record to process.
        """
        try:
            transformed_record = {
                db_column: record.get(json_key)
                for json_key, db_column in self.key_column_mapping.items()
            }
            transformed_record["processed"] = False
            with self.lock:
                self.batch.append(transformed_record)
            self.logger.log_job(
                symbol="GS2001W",
                job_name="Transform Record",
                artifact_name=self.producer.artifact_name,
                success=True,
            )
        except Exception as e:
            self.logger.log_job(
                symbol="GS2001W",
                job_name="Transform Record",
                artifact_name=self.producer.artifact_name,
                error_message=str(e),
                success=False,
            )
            logging.error(f"Error processing record: {e}")
            self.METRICS["errors"].inc()

    @METRICS["batch_insert_time"].time()
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _insert_batch(self, job_id):
        """
        Inserts the current batch of records into the database.

        Args:
            job_id (str): The ID of the current log job for batch insertion.
        """
        try:
            if not self.batch:
                self.logger.log_job(
                    symbol="GS2001W",
                    job_name=f"Batch Insert for {self.producer.artifact_name}",
                    artifact_name=self.producer.artifact_name,
                    success=False,
                    error_message="No records to insert.",
                )
                logging.warning("Insert batch called with no records to process.")
                return

            with self.conn.cursor() as cur:
                columns = self.batch[0].keys()
                query = 'INSERT INTO {} ({}) VALUES %s'.format(
                    self.producer.artifact_name,
                    ', '.join('"{}"'.format(col.lower()) for col in columns)
                )
                values = [[record[col] for col in columns] for record in self.batch]

                execute_values(cur, query, values)
                self.conn.commit()

                self.logger.log_job(
                    query=query,
                    values=values,
                    symbol="GS2001W",
                    job_name=f"Batch Insert for {self.producer.artifact_name}",
                    artifact_name=self.producer.artifact_name,
                    job_id=job_id,
                    success=True,
                )
                logging.info(f"Batch of {len(self.batch)} records inserted successfully.")
                self.METRICS["records_processed"].inc(len(self.batch))
                self.batch.clear()
        except Exception as e:
            self.logger.log_job(
                query=query,
                values=values,
                symbol="GS2001W",
                job_name=f"Batch Insert for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
                job_id=job_id,
                success=False,
                error_message=str(e),
            )
            logging.error(f"Failed to insert batch: {e}")
            self.conn.rollback()
            self.METRICS["errors"].inc()
            self.error = True
            raise

    def finalize(self):
        """
        Finalizes the consumer's operations, committing or rolling back transactions.
        """
        try:
            if self.error:
                self.conn.rollback()
                self.logger.log_job(
                    symbol="GS2001W",
                    job_name=f"Finalize Consumer for {self.producer.artifact_name}",
                    artifact_name=self.producer.artifact_name,
                    success=False,
                    error_message="Rollback due to errors encountered.",
                )
                logging.error(f"Finalizing consumer for {self.producer.artifact_name} with rollback due to errors.")
            else:
                self.conn.commit()
                self.logger.log_job(
                    symbol="GS2001W",
                    job_name=f"Finalize Consumer for {self.producer.artifact_name}",
                    artifact_name=self.producer.artifact_name,
                    success=True,
                )
                logging.info(f"Finalizing consumer for {self.producer.artifact_name} with commit.")
        except Exception as e:
            self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Finalize Consumer for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
                success=False,
                error_message=str(e),
            )
            logging.error(f"Error finalizing consumer for {self.producer.artifact_name}: {e}")
            self.METRICS["errors"].inc()
        finally:
            self.conn.close()
            logging.info(f"Database connection closed for consumer of {self.producer.artifact_name}.")
