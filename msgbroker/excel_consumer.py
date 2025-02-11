import datetime
import logging
import json
from config.config import METRICS, FILE_DELIMITER
from msgbroker.producer_consumer import Consumer
from threading import Lock
from psycopg2.extras import execute_values  # Efficient bulk insert for PostgreSQL

class ExcelConsumer(Consumer):

    def __init__(self, logger, producer, connection_manager, batch_size=100, **kwargs):
        """
        Initialize the ExcelConsumer.

        Args:
            producer (ExcelProducer): The producer instance providing records.
            output_file (str): Path to the output CSV file.
        """
        super().__init__(producer)
        self.connection_manager = connection_manager
        self.logger = logger
        self.consumed_records = []
        self.batch = []
        self.lock = Lock()
        self.conn = self.connection_manager.connect()
        self.query_builder = self.connection_manager.get_query_builder("") # Get QueryBuilder from the connection manager
        self.error = False
        self.batch_size = batch_size
        self.table_name = None
        self.column_names = None
        self.query_builder = None

    def consume(self):
        """
        Consumes records from the producer and processes them in batches.
        """
        job_id = self.logger.log_job(
            symbol="GS2001W",
            job_name=f"Consume Records for {self.producer.file_path}",
            artifact_name=self.producer.file_path,
            start_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            success=False,
            status="IN PROGRESS"
        )

        try:
            while True:
                record = self.producer.consume()
                if record is None:
                    if self.batch:
                        self._insert_batch()
                    break  # No more records

                # Handle file delimiter marker to ensure we handle any remaining records before moving to the next file,
                # and prevent any side effects with mismatched table name and columns.
                if "marker" in record and record["marker"] == FILE_DELIMITER:
                    if self.batch:
                        self._insert_batch()  # Ensure previous batch is committed

                    self.table_name = record["table_name"]
                    self.column_names = record["column_names"]
                    self.query_builder = self.connection_manager.get_query_builder(self.table_name)
                    logging.info(f"Switching to new file: Table={self.table_name}, Columns={self.column_names}")
                    continue  # Skip marker and move to next record

                self.batch.append(record)
                if len(self.batch) >= self.batch_size:
                    self._insert_batch()

        except Exception as e:
            self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Consume Records for {self.producer.file_path}",
                artifact_name=self.producer.file_path,
                error_message=str(e),
                end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                job_id=job_id,
                success=False,
                status="ERROR"
            )
            logging.error(f"ExcelConsumer encountered an error while consuming records: {e}")
            METRICS["errors"].inc()
            self.error = True

        finally:
            if self.batch:
                self._insert_batch()

            self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Consume Records for {self.producer.file_path}",
                artifact_name=self.producer.file_path,
                job_id=job_id,
                success=not self.error,
                end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                status="COMPLETED"
            )

    def _insert_batch(self):
        """
        Inserts a batch of records into the database.
        """
        if not self.batch or not self.table_name or not self.column_names:
            return

        try:
            query = self.query_builder.build_insert_query(self.column_names)
            values = [[record.get(col, None) for col in self.column_names] for record in self.batch]

            job_id = self.logger.log_job(
                query=query,
                values=json.dumps(values),
                symbol="GS2001W",
                job_name=f"Batch Insert for {self.producer.file_path}",
                artifact_name=self.producer.file_path,
                success=True,
                status="SUCCESS",
                end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            )

            logging.info("Values in batch: %s", values)
            logging.info("Insert query: %s", query)

            try:
                self.connection_manager.execute_batch_insert(self.conn, query, values)
            finally:
                logging.info(f"Successfully inserted batch of {len(self.batch)} records into {self.table_name}.")

                self.logger.log_job(
                    query=query,
                    values=json.dumps(values),
                    symbol="GS2001W",
                    job_name=f"Batch Insert for {self.producer.file_path}",
                    artifact_name=self.producer.file_path,
                    job_id=job_id,
                    success=True,
                    status="SUCCESS",
                    end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                )

                METRICS["records_processed"].inc(len(self.batch))
                self.batch.clear() # Reset batch after insert

        except Exception as e:
            logging.error(f"Error inserting batch into {self.table_name}: {e}")
            self.error = True
            raise

    def process_record(self, record):
        """
        Process an individual record by adding it to the consumed records list.
        """
        self.consumed_records.append(record)

    def finalize(self):
        """
        Finalize the consumption by writing records to the output CSV file.
        """

        # try:
        #     pd.DataFrame(self.consumed_records).to_csv(self.output_file, index=False, sep="|")
        #     logging.info(f"Data successfully written to CSV: {self.output_file}")
        # except Exception as e:
        #     logging.error(f"Error writing to CSV file {self.output_file}: {e}")
        #     raise
        # finally:
        #     self.consumed_records = []  # Clear records after writing
