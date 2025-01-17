import logging
from threading import Lock

from psycopg2.extras import execute_values
from tenacity import stop_after_attempt, retry, wait_exponential

from config.config import METRICS
from msgbroker.producer_consumer import Consumer


class SQLConsumer(Consumer):

    def __init__(self, logger, table_name, producer, connection_manager, key_column_mapping, batch_size=5):
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
        self.table_name = table_name
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
                    self._insert_batch()

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
            METRICS["errors"].inc()
            self.error = True

        finally:
            # Insert any remaining records in the batch
            if self.batch:
                self._insert_batch()

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
            METRICS["errors"].inc()

    @METRICS["batch_insert_time"].time()
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _insert_batch(self):
        """
        Inserts the current batch of records into the database.
        """
        try:
            if not self.batch:
                job_id = self.logger.log_job(
                    symbol="GS2001W",
                    job_name=f"Batch Insert for {self.producer.artifact_name}",
                    artifact_name=self.producer.artifact_name,
                    success=False,
                    error_message="No records to insert.",
                )
                logging.warning("Insert batch called with no records to process.")
                return

            job_id = self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Batch Insert for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
            )

            with self.conn.cursor() as cur:
                columns = self.batch[0].keys()
                query = 'INSERT INTO {} ({}) VALUES %s'.format(
                    self.table_name,
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
                METRICS["records_processed"].inc(len(self.batch))
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
            METRICS["errors"].inc()
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
            METRICS["errors"].inc()
        finally:
            self.conn.close()
            logging.info(f"Database connection closed for consumer of {self.producer.artifact_name}.")