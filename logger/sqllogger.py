import logging
import datetime
import psycopg2
import cx_Oracle
import json
import socket

class LoggerContext:
    def __init__(self, interface_type, user_id, table_name, error_table_name):
        self.interface_type = interface_type
        self.user_id = user_id
        self.table_name = table_name
        self.error_table = error_table_name


class SQLLogger:
    def __init__(self, db_config, context):
        self.db_config = db_config
        self.context = context
        self.conn = self._connect()
        self.cursor = self.conn.cursor()

    def _connect(self):
        try:
            conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
            )
            logging.info("Successfully connected to PostgreSQL.")
            return conn
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    def log_job(
            self,
            *args,
            symbol,
            job_id=None,
            job_name="Job",
            job_type=None,
            query=None,
            values=None,
            artifact_name=None,
            success=None,
            error_message=None,
    ):
        """
        Log a job to the database. Inserts a new job if `job_id` is None, otherwise updates the existing job.

        Args:
            *args: Additional arguments for error message formatting.
            symbol (str): Error symbol/code.
            job_id (int): ID of the job to update (None for new jobs).
            job_name (str): Name of the job.
            job_type (str): Type/category of the job.
            query (str): SQL query associated with the job.
            values (list or dict): Values for the query.
            artifact_name (str): Name of the artifact being processed.
            success (bool): Job success status (True/False).
            error_message (str): Error details if any.

        Returns:
            int: ID of the logged or updated job.
        """
        current_time = datetime.datetime.now(datetime.timezone.utc)
        host_name = socket.gethostname()

        # Resolve severity and message using the provided symbol and arguments
        severity, message = self._resolve_error(symbol, *args)

        if job_id is None:
            # Insert a new job log
            insert_query = """
            INSERT INTO ss_logs (
                job_name, job_type, symb, severity, status, start_time, message,
                error_message, query, values, artifact_name, user_id, host_name, table_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            parameters = (
                job_name,
                job_type or self.context.interface_type,
                symbol,
                severity,
                "IN PROGRESS",
                current_time,
                message,
                error_message,
                query,
                json.dumps(values) if values else None,
                artifact_name,
                self.context.user_id,
                host_name,
                self.context.table_name,
            )
            try:
                logging.debug(f"Executing query: {insert_query}")
                logging.debug(f"Parameters: {parameters}")
                self.cursor.execute(insert_query, parameters)
                job_id = self.cursor.fetchone()[0]
                self.conn.commit()
                logging.info(f"Job logged with ID {job_id}: {message}")
                return job_id
            except Exception as e:
                logging.error(f"Failed to log job: {e}")
                raise
        else:
            # Update an existing job log
            update_query = """
            UPDATE ss_logs
            SET status = %s, end_time = %s, message = %s, error_message = %s, query = %s, values = %s, 
                user_id = %s, table_name = %s
            WHERE id = %s
            """
            status = "SUCCESS" if success else "FAILURE"
            parameters = (
                status,
                current_time,
                message,
                error_message,
                query,
                json.dumps(values) if values else None,
                self.context.user_id,
                self.context.table_name,
                job_id,
            )
            try:
                logging.debug(f"Executing query: {update_query}")
                logging.debug(f"Parameters: {parameters}")
                self.cursor.execute(update_query, parameters)
                self.conn.commit()
                logging.info(f"Job ID {job_id} updated to {status}: {message}")
            except Exception as e:
                logging.error(f"Failed to update job log with ID {job_id}: {e}")
                raise

    def _resolve_error(self, symbol, *args):
        query = f"SELECT svrt, dscr FROM {self.context.error_table} WHERE symb = %s"
        try:
            self.cursor.execute(query, (symbol,))
            result = self.cursor.fetchone()
            if result:
                severity, description = result
                return severity, description.format(*args)
            else:
                logging.warning(f"Error symbol '{symbol}' not found.")
                return "W", f"Unknown error: {symbol}"
        except Exception as e:
            logging.error(f"Failed to resolve error definition for '{symbol}': {e}")
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("SQL Logger connection closed.")
