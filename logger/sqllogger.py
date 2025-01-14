import logging
import datetime
import psycopg2
import cx_Oracle
import json
import socket
import uuid


class SQLLogger:
    def __init__(self, db_config, error_table):
        self.db_config = db_config
        self.db_type = db_config.get("type").lower()
        self.conn = None
        self.cursor = None
        self.error_table = error_table
        self.error_cache = {}
        self.cache_hits = 0
        self.cache_misses = 0

        if self.db_type not in ["oracle", "postgresql"]:
            logging.error("Unsupported database type. Use 'oracle' or 'postgresql'.")
            raise ValueError("Unsupported database type. Use 'oracle' or 'postgresql'.")

        logging.info("Initializing SQLLogger...")
        self._connect()

    def _connect(self):
        try:
            if self.db_type == "postgresql":
                self.conn = psycopg2.connect(
                    host=self.db_config["host"],
                    port=self.db_config["port"],
                    database=self.db_config["database"],
                    user=self.db_config["user"],
                    password=self.db_config["password"],
                )
            elif self.db_type == "oracle":
                self.conn = cx_Oracle.connect(
                    user=self.db_config["user"],
                    password=self.db_config["password"],
                    dsn=f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}",
                )
            self.cursor = self.conn.cursor()
            logging.info(f"Successfully connected to {self.db_type} database.")
        except Exception as e:
            logging.error(f"Failed to connect to {self.db_type} database: {e}")
            raise

    def _get_error_definition(self, error_symbol):
        if error_symbol in self.error_cache:
            self.cache_hits += 1
            return self.error_cache[error_symbol]

        query = f"SELECT * FROM {self.error_table} WHERE SYMB = :1" if self.db_type == "oracle" else \
                f"SELECT * FROM {self.error_table} WHERE SYMB = %s"
        try:
            self.cursor.execute(query, (error_symbol,))
            result = self.cursor.fetchone()
            if result:
                self.cache_misses += 1
                # Assume columns are mapped to a dictionary (adjust as needed for actual schema)
                error_def = {
                    "severity": result["SVRT"],
                    "description": result["DSCR"],
                }
                self.error_cache[error_symbol] = error_def
                return error_def
            else:
                logging.warning(f"Error code '{error_symbol}' not found.")
                return None
        except Exception as e:
            logging.error(f"Failed to fetch error definition for '{error_symbol}': {e}")
            raise

    def _format_message(self, description, *args):
        try:
            return description.format(*args)
        except IndexError as e:
            logging.error(f"Failed to format message '{description}' with arguments {args}: {e}")
            raise

    def log_job(
        self,
        symbol,
        job_id=None,
        job_name="Job",
        job_type=None,
        query=None,
        user_id=None,
        metadata=None,
        success=None,
        *args,
    ):
        """
        Log a job state with the specified error code and arguments.

        Args:
            symbol (str): Symbol for the error code (e.g., "GS6782E").
            *args: Positional arguments for the error message.
            job_id (int): Existing job ID to update (if None, creates a new job log).
            job_name (str): Name of the job.
            job_type (str): Type/category of the job.
            query (str): SQL query associated with the job.
            user_id (str): ID of the user who initiated the job.
            metadata (dict): Additional metadata for the log.
            success (bool): Final status of the job (True for success, False for failure).
        """
        current_time = datetime.datetime.now(datetime.timezone.utc)
        error = self._get_error_definition(symbol)
        if not error:
            logging.error(f"Cannot log job: Unknown error code '{symbol}'.")
            return

        severity = error["severity"]
        description = error["description"]
        message = self._format_message(description, *args)
        metadata_str = json.dumps(metadata) if metadata else None
        host_name = socket.gethostname()

        if job_id is None:
            # Insert a new job log
            insert_query = """
            INSERT INTO ss_logs (
                job_name, job_type, status, start_time, message, query, user_id,
                host_name, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            if self.db_type == "oracle":
                insert_query = """
                INSERT INTO ss_logs (
                    job_name, job_type, status, start_time, message, query, user_id,
                    host_name, metadata
                )
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9) RETURNING id INTO :10
                """
            try:
                self.cursor.execute(
                    insert_query,
                    (
                        job_name,
                        job_type,
                        "IN PROGRESS",
                        current_time,
                        message,
                        query,
                        user_id,
                        host_name,
                        metadata_str,
                    ),
                )
                job_id = self.cursor.fetchone()[0]
                self.conn.commit()
                logging.info(f"New job logged with ID {job_id}: {message}")
                return job_id
            except Exception as e:
                logging.error(f"Failed to insert job log: {e}")
                raise
        else:
            # Update an existing job log
            status = "SUCCESS" if success else "FAILURE"
            update_query = """
            UPDATE ss_logs
            SET status = %s, end_time = %s, message = %s, query = %s, metadata = %s
            WHERE id = %s
            """
            if self.db_type == "oracle":
                update_query = """
                UPDATE ss_logs
                SET status = :1, end_time = :2, message = :3, query = :4, metadata = :5
                WHERE id = :6
                """
            try:
                self.cursor.execute(
                    update_query,
                    (status, current_time, message, query, metadata_str, job_id),
                )
                self.conn.commit()
                logging.info(f"Job ID {job_id} updated to {status}: {message}")
            except Exception as e:
                logging.error(f"Failed to update job log with ID {job_id}: {e}")
                raise

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info(
            f"SQL Logger connection closed. Cache Hits: {self.cache_hits}, Cache Misses: {self.cache_misses}."
        )
