import os
import logging
from threading import Thread, Lock, Event
from prometheus_client import generate_latest, Counter, Histogram, Summary

from config.config import METRICS
from msgbroker.producerconsumer import FileProducer, SQLConsumer

class FileProcessor:

    def __init__(self, connection_manager, logger, config):
        """
        Initialize the FileProcessor instance with required components.

        Args:
            connection_manager (DBConnectionManager): Manages database connections.
            logger (SQLLogger): Handles logging operations.
            config (dict): Configuration settings for file processing.
        """
        self.connection_manager = connection_manager
        self.logger = logger
        self.config = config
        self.conn = self.connection_manager.connect()
        self.table_name = config["tableName"]
        self.batch_size = config["sqlBatchSize"]
        self.worker_states = {}
        self.state_lock = Lock()  # Protect shared worker_states

        logging.info(f"FileProcessor initialized for table: {self.table_name}")

    def write_metrics_to_file(self, file_path="metrics_output.txt"):
        """
        Write all Prometheus metrics to a specified file when the program exits.

        Args:
            file_path (str): Path to the output file where metrics will be written.

        Raises:
            IOError: If writing to the file fails.
        """
        try:
            # Generate the latest metrics data
            metrics_output = generate_latest()
            with open(file_path, "w") as metrics_file:
                metrics_file.write(metrics_output.decode("utf-8"))  # Decode bytes for writing
            logging.info(f"Metrics successfully written to {file_path}")
        except Exception as e:
            logging.error(f"Failed to write metrics to file: {e}")
            raise

    # def _flatten_dict(self, data):
    #     """
    #     Flattens a nested dictionary and handles repeated elements as individual rows.
    #
    #     This is critical for processing hierarchical data structures into a normalized format
    #     suitable for database operations.
    #
    #     Args:
    #         data (dict): The nested dictionary to be flattened.
    #
    #     Returns:
    #         list[dict]: A list of flattened dictionaries derived from the input data.
    #
    #     Example:
    #         Input: {"key1": "value1", "key2": [{"subkey1": "value2"}, {"subkey1": "value3"}]}
    #         Output: [{"key1": "value1", "subkey1": "value2"}, {"key1": "value1", "subkey1": "value3"}]
    #     """
    #     # Initialize the base record, containing non-nested key-value pairs
    #     base_record = {}
    #     # List to store records resulting from nested elements
    #     nested_records = []
    #
    #     # Iterate over the dictionary items
    #     for key, value in data.items():
    #         if isinstance(value, list):
    #             # If the value is a list, iterate through its elements
    #             for nested in value:
    #                 if isinstance(nested, dict):
    #                     # Copy base record and merge with nested dictionary
    #                     new_record = base_record.copy()
    #                     new_record.update(nested)
    #                     nested_records.append(new_record)
    #         elif isinstance(value, dict):
    #             # If the value is a dictionary, merge it with the base record
    #             base_record.update(value)
    #         else:
    #             # Add scalar values to the base record
    #             base_record[key] = value
    #
    #     # If no nested records exist, return the base record as a single-item list
    #     if not nested_records:
    #         logging.debug("No nested records found; returning base record.")
    #         return [base_record]
    #
    #     # Update each nested record with values from the base record
    #     for record in nested_records:
    #         record.update(base_record)
    #
    #     logging.debug(f"Flattened dictionary to {len(nested_records)} records.")
    #     return nested_records
    #
    # def _parse_json_file(self, file_path, schema_tag="Records"):
    #     """
    #     Parses and flattens JSON records from a file.
    #
    #     Args:
    #         file_path (str): Path to the JSON file.
    #         schema_tag (str): Key to extract records from the JSON structure (default: "Records").
    #
    #     Yields:
    #         dict: Flattened records extracted from the JSON file.
    #
    #     Raises:
    #         FileNotFoundError: If the JSON file is not found.
    #         json.JSONDecodeError: If the JSON file contains invalid syntax.
    #     """
    #     try:
    #         # Open and load the JSON file into a Python dictionary
    #         with open(file_path, "r") as file:
    #             data = json.load(file)
    #             logging.info(f"Successfully loaded JSON file: {file_path}")
    #     except FileNotFoundError:
    #         # Log and re-raise error if the file is missing
    #         logging.error(f"JSON file not found: {file_path}")
    #         raise
    #     except json.JSONDecodeError as e:
    #         # Log and re-raise error for invalid JSON syntax
    #         logging.error(f"Error parsing JSON file: {e}")
    #         raise
    #
    #     # Extract records using the schema tag or fallback to the root of the JSON structure
    #     records = data.get(schema_tag, data)
    #
    #     # Flatten and yield each record
    #     if isinstance(records, list):
    #         for record in records:
    #             for flattened in self._flatten_dict(record):
    #                 yield flattened
    #     elif isinstance(records, dict):
    #         for flattened in self._flatten_dict(records):
    #             yield flattened
    #
    # def _parse_xml_file(self, file_path, schema_tag="Record"):
    #     """
    #     Parses and flattens XML records from a file.
    #
    #     Args:
    #         file_path (str): Path to the XML file.
    #         schema_tag (str): Tag to extract records from the XML structure (default: "Record").
    #
    #     Yields:
    #         dict: Flattened records extracted from the XML file.
    #
    #     Raises:
    #         FileNotFoundError: If the XML file is not found.
    #         ET.ParseError: If the XML file contains invalid syntax.
    #     """
    #     try:
    #         # Parse the XML file and obtain the root element
    #         tree = ET.parse(file_path)
    #         root = tree.getroot()
    #         logging.info(f"Successfully parsed XML file: {file_path}")
    #     except FileNotFoundError:
    #         # Log and re-raise error if the file is missing
    #         logging.error(f"XML file not found: {file_path}")
    #         raise
    #     except ET.ParseError as e:
    #         # Log and re-raise error for invalid XML syntax
    #         logging.error(f"Error parsing XML file: {e}")
    #         raise
    #
    #     def parse_element(element):
    #         """
    #         Recursively parses an XML element into a dictionary.
    #
    #         Args:
    #             element (xml.etree.ElementTree.Element): The XML element to parse.
    #
    #         Returns:
    #             dict: Parsed representation of the element.
    #         """
    #         record = {}
    #         for child in element:
    #             if len(child) > 0:
    #                 # Handle nested elements by appending them to a list
    #                 if child.tag not in record:
    #                     record[child.tag] = []
    #                 record[child.tag].append(parse_element(child))
    #             else:
    #                 # Add leaf node text to the record
    #                 record[child.tag] = child.text.strip() if child.text else None
    #         return record
    #
    #     # Extract and flatten records
    #     for record_element in root.findall(f".//{schema_tag}"):
    #         raw_record = parse_element(record_element)
    #         flattened_records = self._flatten_dict(raw_record)
    #         for record in flattened_records:
    #             yield record
    #
    # def _process_file(self, file_path, schema_tag, file_type="json", output_queue=None):
    #     """
    #     Processes a file (JSON or XML), flattens records, and optionally queues them for processing.
    #
    #     Args:
    #         file_path (str): Path to the input file.
    #         schema_tag (str): The schema tag name for JSON/XML records.
    #         file_type (str): File type, either 'json' or 'xml' (default: 'json').
    #         output_queue (queue.Queue, optional): Queue to stream records for parallel processing.
    #
    #     Yields:
    #         dict: Flattened records if `output_queue` is not specified.
    #
    #     Behavior:
    #         - If `output_queue` is provided, records are pushed to the queue for consumption.
    #         - If `output_queue` is None, records are yielded sequentially for single-threaded use.
    #     """
    #     # Select the appropriate parser based on the file type
    #     parser = self._parse_json_file if file_type == "json" else self._parse_xml_file
    #
    #     # Iterate through parsed records
    #     for record in parser(file_path, schema_tag=schema_tag):
    #         if output_queue:
    #             # Push records into the queue for parallel processing
    #             logging.debug(f"Adding record to queue: {record}")
    #             output_queue.put(record)
    #         else:
    #             # Yield records for sequential processing
    #             logging.debug(f"Yielding record: {record}")
    #             yield record
    #
    #     # Notify the consumer that processing is complete
    #     if output_queue:
    #         logging.debug("Signaling consumer that processing is complete.")
    #         output_queue.put(None)

    def _transform_and_validate_records(self, records, key_column_mapping):
        """
        Transforms and validates a list of records based on a key-column mapping.

        Args:
            records (list): List of input records to process.
            key_column_mapping (dict): Mapping of JSON keys to database column names.

        Returns:
            list: List of transformed records ready for database insertion.
        """
        transformed_records = []
        for record in records:
            transformed_record = {}
            missing_keys = set()

            # Map keys from the record to the specified schema
            for json_key, db_column in key_column_mapping.items():
                if json_key in record:
                    transformed_record[db_column] = record[json_key]
                else:
                    # Track missing keys for logging purposes
                    missing_keys.add(json_key)

            if missing_keys:
                # Log any keys that were expected but not found in the input record
                logging.warning(f"Record missing keys: {missing_keys}")

            transformed_records.append(transformed_record)

        # Log the total number of successfully transformed records
        logging.info(f"Transformed {len(transformed_records)} records.")
        return transformed_records

    # @METRICS["batch_insert_time"].time()  # Track time taken for batch inserts
    # @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))  # Retry with exponential backoff
    # def _batch_insert_records(self, records, artifact_name, conn):
    #     """
    #     Perform a batch insert into the database.
    #
    #     Args:
    #         records (list): List of records to insert.
    #         artifact_name (str): Artifact name for logging purposes.
    #         conn (connection): Database connection to use for the insert.
    #
    #     Raises:
    #         Exception: Rethrows any exceptions encountered during the batch insert.
    #     """
    #     if not records:
    #         # Log a warning if no records are provided for insertion
    #         self.logger.log_job(
    #             symbol="GS2001W",
    #             job_name=f"{self.table_name} BATCH INS",
    #             success=False,
    #             artifact_name=artifact_name,
    #         )
    #         return
    #
    #     # Log the start of a batch insert operation
    #     job_id = self.logger.log_job(
    #         "",
    #         "",
    #         symbol="GS1002I",
    #         job_name=f"{self.table_name} BATCH INS",
    #         artifact_name=artifact_name,
    #     )
    #
    #     try:
    #         with conn.cursor() as cur:
    #             # Dynamically construct the SQL query for the batch insert
    #             columns = records[0].keys()
    #             query = 'INSERT INTO {} ({}) VALUES %s'.format(
    #                 self.table_name,
    #                 ', '.join('"{}"'.format(col.lower()) for col in columns)
    #             )
    #             values = [[record[col] for col in columns] for record in records]
    #
    #             # Perform the batch insert
    #             execute_values(cur, query, values)
    #
    #             # Log success and update metrics
    #             self.logger.log_job(
    #                 query,
    #                 values,
    #                 symbol="GS1002I",
    #                 job_name=f"{self.table_name} BATCH INS",
    #                 job_id=job_id,
    #                 query=query,
    #                 values=values,
    #                 artifact_name=artifact_name,
    #                 success=True,
    #             )
    #             self.METRICS["records_processed"].inc(len(records))  # Increment processed records metric
    #
    #     except Exception as e:
    #         # Log failure and increment error metrics
    #         self.logger.log_job(
    #             str(e),
    #             symbol="GS2002E",
    #             job_name=f"{self.table_name} BATCH INS",
    #             job_id=job_id,
    #             query=query,
    #             artifact_name=artifact_name,
    #             success=False,
    #             error_message=str(e),
    #         )
    #         self.METRICS["errors"].inc()
    #         raise

    # def _consume_and_insert(self, consumer, key_column_mapping, artifact_name, worker_id, conn):
    #     """
    #     Consumes records from the queue, transforms them, and inserts them into the database.
    #
    #     Args:
    #         consumer (Consumer): Consumer instance to fetch records.
    #         key_column_mapping (dict): Mapping of JSON keys to database column names.
    #         artifact_name (str): Artifact name for logging.
    #         worker_id (str): Unique identifier for the worker.
    #         conn (connection): Database connection to use.
    #
    #     Behavior:
    #         - Batches records from the queue based on `self.batch_size`.
    #         - Performs a batch insert when the batch size is met.
    #     """
    #     batch = []
    #     try:
    #         while True:
    #             record = consumer.consume()
    #             if record is None:
    #                 # Signal the end of consumption
    #                 break
    #
    #             # Transform and add the record to the batch
    #             batch.append(self._transform_record(record, key_column_mapping))
    #
    #             # Perform batch insert when batch size is met
    #             if len(batch) >= self.batch_size:
    #                 self._batch_insert_records(batch, artifact_name, conn)
    #                 batch.clear()
    #
    #         # Insert any remaining records
    #         if batch:
    #             self._batch_insert_records(batch, artifact_name, conn)
    #
    #         # Mark worker success
    #         with self.state_lock:
    #             self.worker_states[worker_id]["error"] = False
    #     except Exception as e:
    #         logging.error(f"Worker {worker_id} encountered an error: {e}")
    #         with self.state_lock:
    #             self.worker_states[worker_id]["error"] = True
    #
    #         # Increment error metrics
    #         self.METRICS["errors"].inc()

    @METRICS["file_processing_time"].time()  # Track total file processing time
    def _process(self, producer, consumer, key_column_mapping=None):
        """
        Processes records using producer and consumer components.

        Args:
            producer (Producer): The producer instance for generating records.
            consumer (Consumer): The consumer instance for processing records.
            key_column_mapping (dict): Mapping of JSON keys to database column names (optional).

        Behavior:
            - Uses the provided producer to generate records.
            - Passes records from the producer to the consumer for processing.
        """
        all_workers_done = Event()

        try:
            # Start the producer task
            def producer_task():
                try:
                    producer.produce_from_source()  # The producer generates and queues records
                except Exception as e:
                    logging.error(f"Producer encountered an error: {e}")
                    METRICS["errors"].inc()
                    raise
                finally:
                    # Signal the end of production
                    all_workers_done.set()

            producer_thread = Thread(target=producer_task)
            producer_thread.start()

            # Start the consumer task
            def consumer_task():
                try:
                    consumer.consume()  # The consumer processes records from the producer
                except Exception as e:
                    logging.error(f"Consumer encountered an error: {e}")
                    METRICS["errors"].inc()
                    raise
                finally:
                    consumer.finalize()  # Finalize consumer after consumption

            consumer_thread = Thread(target=consumer_task)
            consumer_thread.start()

            # Wait for producer and consumer to finish
            producer_thread.join()
            all_workers_done.wait()
            consumer.signal_done()  # Notify the consumer of end-of-production
            consumer_thread.join()

            logging.info("Processing completed successfully.")
        except Exception as e:
            logging.error(f"Failed to process records: {e}")
            METRICS["errors"].inc()
            raise
        finally:
            producer.close()  # Ensure producer resources are released

    def _get_schema_and_tag(self, file_type):
        """
        Dynamically retrieve the schema and tag name based on the file type.

        Args:
            file_type (str): File type, either 'json' or 'xml'.

        Returns:
            tuple: A tuple containing the schema and tag name for the specified file type.

        Raises:
            ValueError: If an unsupported file type is provided.
        """
        if file_type == "json":
            # Retrieve schema and tag name for JSON files
            return self.config.get("jsonSchema"), self.config.get("jsonTagName", "Records")
        elif file_type == "xml":
            # Retrieve schema and tag name for XML files
            return self.config.get("xmlSchema"), self.config.get("xmlTagName", "Record")
        else:
            # Handle unsupported file types
            raise ValueError(f"Unsupported file type: {file_type}")

    def _transform_record(self, record, key_column_mapping):
        """
        Transforms a single record based on the provided key-column mapping.

        Args:
            record (dict): Input record to be transformed.
            key_column_mapping (dict): Mapping of JSON keys to database column names.

        Returns:
            dict: Transformed record with keys mapped to database columns and a 'processed' flag.
        """
        # Transform record by mapping keys
        transformed_record = {
            db_column: record.get(json_key)
            for json_key, db_column in key_column_mapping.items()
        }
        # Add a flag to indicate the record's processed status
        transformed_record["processed"] = False
        return transformed_record

    def process_files(self, files, producer=None):
        """
        Process a list of files dynamically based on their file types.

        Args:
            files (list): List of file paths to process.
            producer (Producer): Optional producer for message queue.

        Behavior:
            - Determines the file type (JSON or XML) based on the file extension.
            - Fetches the schema and tag name for the file type.
            - Calls the `_process` method to handle parsing and insertion for each file.
        """
        for file_path in files:
            # Determine the file type based on the file extension
            file_type = "json" if file_path.endswith(".json") else "xml"

            # Retrieve the appropriate schema and tag name
            try:
                schema, tag_name = self._get_schema_and_tag(file_type)
            except ValueError as e:
                logging.error(f"Error determining schema and tag for file {file_path}: {e}")
                METRICS["errors"].inc()
                continue

            # Log the start of processing for the file
            logging.info(f"Processing file {file_path} as {file_type}.")

            # Process the file using the determined schema and tag and move to the output directory after successful processing
            try:
                # File-based processing
                file_producer = FileProducer(maxsize=1000)
                file_producer.set_source(file_path=file_path, file_type=file_type, schema_tag=tag_name)

                consumer = SQLConsumer(
                    logger=self.logger,
                    producer=file_producer,
                    connection_manager=self.connection_manager,
                    key_column_mapping=schema,
                    batch_size=100
                )

                # Invoke the processing pipeline
                self._process(producer=file_producer, consumer=consumer, key_column_mapping=schema)

                # sql_consumer = SQLConsumer(producer, self.connection_manager, key_column_mapping)
                # self._process(producer=file_producer, consumer_cls=SQLConsumer, key_column_mapping=schema)
                self._move_file_to_folder(file_path, self.config["outputDirectory"])
            except Exception as e:
                logging.error(f"File processing failed for {file_path}: {e}")
                METRICS["errors"].inc()

    def _move_file_to_folder(self, file_path, folder_path):
        import shutil

        try:
            # Ensure the directory exists
            os.makedirs(folder_path, exist_ok=True)

            # Construct the destination path
            destination_path = os.path.join(folder_path, os.path.basename(file_path))

            # If the file already exists at the destination, overwrite it
            if os.path.exists(destination_path):
                os.replace(file_path, destination_path)
                logging.info(f"File overwritten at: {destination_path}")
            else:
                shutil.move(file_path, destination_path)
                logging.info(f"File moved to: {destination_path}")
        except Exception as e:
            logging.error(f"Failed to move or overwrite file {file_path} to {folder_path}: {e}")

    def close(self):
        """
        Close the database connection and release resources.
        """
        self.conn.close()
        logging.info("Database connection closed.")