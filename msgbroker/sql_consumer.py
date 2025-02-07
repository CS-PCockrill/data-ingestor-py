import datetime
import json
import logging
from threading import Lock

from psycopg2.extras import execute_values
from tenacity import stop_after_attempt, retry, wait_exponential

from config.config import METRICS, FILE_DELIMITER
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
        self.query_builder = self.connection_manager.get_query_builder(table_name)  # Get QueryBuilder from the connection manager
        self.error = False

    # def consume(self):
    #     """
    #     Consumes records from the producer and processes them in batches.
    #     """
    #     job_id = self.logger.log_job(
    #         symbol="GS2001W",
    #         job_name=f"Consume Records for {self.producer.artifact_name}",
    #         artifact_name=self.producer.artifact_name,
    #         start_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    #         success=False,
    #         status="IN PROGRESS"
    #     )
    #
    #     try:
    #         while True:
    #             record = self.producer.consume()
    #             if record is None:
    #                 if self.batch:
    #                     self._insert_batch()
    #                 break  # Signal that production is complete
    #
    #
    #             self.batch.append(record)
    #             if len(self.batch) >= self.batch_size:
    #                 self._insert_batch()
    #
    #     except Exception as e:
    #         self.logger.log_job(
    #             symbol="GS2001W",
    #             job_name=f"Consume Records for {self.producer.artifact_name}",
    #             artifact_name=self.producer.artifact_name,
    #             error_message=str(e),
    #             end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    #             job_id=job_id,
    #             success=False,
    #             status="ERROR"
    #         )
    #         logging.error(f"SQLConsumer encountered an error while consuming records: {e}")
    #         METRICS["errors"].inc()
    #         self.error = True
    #
    #     finally:
    #         # Insert any remaining records in the batch
    #         if self.batch:
    #             self._insert_batch()
    #
    #         # Mark the job as completed
    #         self.logger.log_job(
    #             symbol="GS2001W",
    #             job_name=f"Consume Records for {self.producer.artifact_name}",
    #             artifact_name=self.producer.artifact_name,
    #             job_id=job_id,
    #             success=not self.error,
    #             end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    #             status="COMPLETED"
    #         )

    def consume(self):
        """
        Consumes records from the producer and processes them in batches.
        Dynamically updates key-column mapping when a new file is processed.
        """
        job_id = self.logger.log_job(
            symbol="GS2001W",
            job_name=f"Consume Records for {self.producer.artifact_name}",
            artifact_name=self.producer.artifact_name,
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
                    break  # Signal that production is complete

                # Detect metadata marker and update key-column mapping
                if "marker" in record and record["marker"] == FILE_DELIMITER:
                    if self.batch:
                        self._insert_batch()  # Flush batch before schema switch

                    # Update key-column mapping dynamically
                    self.key_column_mapping = record["key_column_mapping"]

                    self.query_builder = self.connection_manager.get_query_builder(self.table_name)
                    logging.info(
                        f"Updated table name: {self.table_name}, New Key-Column Mapping: {self.key_column_mapping}")
                    continue  # Skip processing the metadata record

                # Append record to batch
                self.batch.append(record)
                if len(self.batch) >= self.batch_size:
                    self._insert_batch()

        except Exception as e:
            self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Consume Records for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
                error_message=str(e),
                end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                job_id=job_id,
                success=False,
                status="ERROR"
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
                end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                status="COMPLETED"
            )

    def process_record(self, record):
        """
        Transforms and adds a record to the batch.

        Args:
            record (dict): Record to process.
        """
        try:
            job_id = self.logger.log_job(
                symbol="GS2001W",
                job_name="Transform Record",
                start_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                artifact_name=self.producer.artifact_name,
                status="IN PROGRESS",
            )
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
                end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                artifact_name=self.producer.artifact_name,
                job_id=job_id,
                success=True,
                status="SUCCESS",
            )
        except Exception as e:
            self.logger.log_job(
                symbol="GS2001W",
                job_name="Transform Record",
                end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                artifact_name=self.producer.artifact_name,
                error_message=str(e),
                job_id=job_id,
                success=False,
                status="ERROR"
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
                    status="WARNING",
                    start_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                )
                logging.warning("Insert batch called with no records to process.")
                return

            job_id = self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Batch Insert for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
                status="IN PROGRESS",
                start_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            )

            with self.conn.cursor() as cur:
                # Generate the INSERT query using the QueryBuilder
                columns = self.batch[0].keys()
                query = self.query_builder.build_insert_query(columns)
                values = [[record[col] for col in columns] for record in self.batch]

                logging.info("Values in batch: %s", values)
                logging.info("Values in query: %s", query)

                # Execute the query with the batch values
                execute_values(cur, query, values)
                self.conn.commit()
                logging.info(f"Successfully inserted batch of {len(self.batch)} records into {self.table_name}.")

                self.logger.log_job(
                    query=query,
                    values=json.dumps(values),
                    symbol="GS2001W",
                    job_name=f"Batch Insert for {self.producer.artifact_name}",
                    artifact_name=self.producer.artifact_name,
                    job_id=job_id,
                    success=True,
                    status="SUCCESS",
                    end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),

                )
                METRICS["records_processed"].inc(len(self.batch))
                self.batch.clear()

        except Exception as e:
            self.logger.log_job(
                query=query,
                values=json.dumps(values),
                symbol="GS2001W",
                job_name=f"Batch Insert for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
                job_id=job_id,
                success=False,
                error_message=str(e),
                status="ERROR",
                end_time = datetime.datetime.now(datetime.timezone.utc).isoformat(),

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
                    status="ERROR",
                    end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),

                )
                logging.error(f"Finalizing consumer for {self.producer.artifact_name} with rollback due to errors.")
            else:
                self.conn.commit()
                self.logger.log_job(
                    symbol="GS2001W",
                    job_name=f"Finalize Consumer for {self.producer.artifact_name}",
                    artifact_name=self.producer.artifact_name,
                    success=True,
                    status="SUCCESS",
                    end_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),

                )
                logging.info(f"Finalizing consumer for {self.producer.artifact_name} with commit.")
        except Exception as e:
            self.logger.log_job(
                symbol="GS2001W",
                job_name=f"Finalize Consumer for {self.producer.artifact_name}",
                artifact_name=self.producer.artifact_name,
                success=False,
                error_message=str(e),
                status="ERROR",
                end_time = datetime.datetime.now(datetime.timezone.utc).isoformat(),

            )
            logging.error(f"Error finalizing consumer for {self.producer.artifact_name}: {e}")
            METRICS["errors"].inc()
        finally:
            self.conn.close()
            logging.info(f"Database connection closed for consumer of {self.producer.artifact_name}.")