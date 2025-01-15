import logging
import datetime
import psycopg2
import cx_Oracle
import json
import socket

class LoggerContext:
    def __init__(self, interface_type, user_id, table_name):
        self.interface_type = interface_type
        self.user_id = user_id
        self.table_name = table_name


class SQLLogger:
    def __init__(self, db_config, error_table, context):
        self.db_config = db_config
        self.error_table = error_table
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

    def log_job(self, *args, symbol, job_id=None, job_name="Job", job_type=None, query=None, values=None, artifact_name=None, success=None, error_message=None):
        current_time = datetime.datetime.now(datetime.timezone.utc)
        host_name = socket.gethostname()
        severity, message = self._resolve_error(symbol, *args)

        if job_id is None:
            insert_query = """
            INSERT INTO ss_logs (
                job_name, job_type, symb, severity, status, start_time, message,
                error_message, query, values, artifact_name, user_id, host_name, table_name,
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            try:
                self.cursor.execute(
                    insert_query,
                    (
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
                    ),
                )
                job_id = self.cursor.fetchone()[0]
                self.conn.commit()
                logging.info(f"Job logged with ID {job_id}: {message}")
                return job_id
            except Exception as e:
                logging.error(f"Failed to log job: {e}")
                raise
        else:
            update_query = """
            UPDATE ss_logs
            SET status = %s, message = %s, error_message = %s, query = %s, values = %s, user_id = %s, table_name = %s
            WHERE id = %s
            """
            status = "SUCCESS" if success else "FAILURE"
            try:
                self.cursor.execute(
                    update_query,
                    (
                        status,
                        message,
                        error_message,
                        query,
                        json.dumps(values) if values else None,
                        self.context.user_id,
                        self.context.table_name,
                        job_id,
                    ),
                )
                self.conn.commit()
                logging.info(f"Job ID {job_id} updated to {status}: {message}")
            except Exception as e:
                logging.error(f"Failed to update job log with ID {job_id}: {e}")
                raise

    def _resolve_error(self, symbol, *args):
        query = f"SELECT svrt, dscr FROM {self.error_table} WHERE symb = %s"
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


# class SQLLogger:
#     def __init__(self, db_config, error_table):
#         self.db_config = db_config
#         self.db_type = db_config.get("type").lower()
#         self.conn = None
#         self.cursor = None
#         self.error_table = error_table
#         self.error_cache = {}
#         self.cache_hits = 0
#         self.cache_misses = 0
#
#         if self.db_type not in ["oracle", "postgresql"]:
#             logging.error("Unsupported database type. Use 'oracle' or 'postgresql'.")
#             raise ValueError("Unsupported database type. Use 'oracle' or 'postgresql'.")
#
#         logging.info("Initializing SQLLogger...")
#         self._connect()
#
#     def _connect(self):
#         try:
#             if self.db_type == "postgresql":
#                 self.conn = psycopg2.connect(
#                     host=self.db_config["host"],
#                     port=self.db_config["port"],
#                     database=self.db_config["database"],
#                     user=self.db_config["user"],
#                     password=self.db_config["password"],
#                 )
#             elif self.db_type == "oracle":
#                 self.conn = cx_Oracle.connect(
#                     user=self.db_config["user"],
#                     password=self.db_config["password"],
#                     dsn=f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}",
#                 )
#             self.cursor = self.conn.cursor()
#             logging.info(f"Successfully connected to {self.db_type} database.")
#         except Exception as e:
#             logging.error(f"Failed to connect to {self.db_type} database: {e}")
#             raise
#
#     def _get_error_definition(self, error_symbol):
#         """
#         Fetch the error definition from the database or cache.
#
#         Args:
#             error_symbol (str): The error code to look up.
#
#         Returns:
#             dict: A dictionary containing severity and description, or None if not found.
#         """
#         if error_symbol in self.error_cache:
#             self.cache_hits += 1
#             return self.error_cache[error_symbol]
#
#         # SQL query for fetching the error definition
#         query = f"SELECT * FROM {self.error_table} WHERE SYMB = :1" if self.db_type == "oracle" else \
#             f"SELECT * FROM {self.error_table} WHERE SYMB = %s"
#         try:
#             # Execute the query with the error_symbol
#             self.cursor.execute(query, (error_symbol,))
#             result = self.cursor.fetchone()
#
#             if result:
#                 self.cache_misses += 1
#
#                 # Map the result to a dictionary using the column names
#                 column_names = [desc[0] for desc in self.cursor.description]
#                 error_def = dict(zip(column_names, result))
#
#                 # Cache the result for future lookups
#                 self.error_cache[error_symbol] = {
#                     "severity": error_def["svrt"],
#                     "description": error_def["dscr"],
#                 }
#                 return self.error_cache[error_symbol]
#             else:
#                 # Log a warning if the error code is not found
#                 logging.warning(f"Error code '{error_symbol}' not found in {self.error_table}.")
#                 return None
#         except Exception as e:
#             # Log and raise an exception if the query fails
#             logging.error(f"Failed to fetch error definition for '{error_symbol}': {e}")
#             raise
#
#     def _format_message(self, description, *args):
#         try:
#             return description.format(*args)
#         except IndexError as e:
#             logging.error(f"Failed to format message '{description}' with arguments {args}: {e}")
#             raise
#
#     def log_job(
#             self,
#             *args,
#             symbol,
#             job_id=None,
#             job_name="Job",
#             job_type=None,
#             query=None,
#             values=None,
#             artifact_name=None,
#             user_id=None,
#             error_message=None,
#             table_name=None,
#             success=None,
#     ):
#         """
#         Log a job state with the specified error code and arguments.
#
#         Args:
#             symbol (str): Symbol for the error code (e.g., "GS6782E").
#             *args: Positional arguments for the error message.
#             job_id (int): Existing job ID to update (if None, creates a new job log).
#             job_name (str): Name of the job.
#             job_type (str): Type/category of the job.
#             query (str): SQL query associated with the job.
#             values (list): Query values or parameters.
#             artifact_name (str): Name of the artifact being processed.
#             user_id (str): ID of the user who initiated the job.
#             error_message (str): Error message or description.
#             success (bool): Final status of the job (True for success, False for failure).
#             table_name (str): Table name that this operation is being executed against.
#         """
#         current_time = datetime.datetime.now(datetime.timezone.utc)
#         error = self._get_error_definition(symbol)
#         if not error:
#             logging.error(f"Cannot log job: Unknown error code '{symbol}'.")
#             return
#
#         severity = error["severity"]
#         description = error["description"]
#         message = self._format_message(description, *args)
#         host_name = socket.gethostname()
#         values_str = json.dumps(values) if values else None
#
#         if job_id is None:
#             # Insert a new job log
#             insert_query = """
#             INSERT INTO ss_logs (
#                 job_name, job_type, symb, severity, status, start_time, message,
#                 error_message, query, values, table_name, artifact_name, user_id, host_name
#             )
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
#             """
#             if self.db_type == "oracle":
#                 insert_query = """
#                 INSERT INTO ss_logs (
#                     job_name, job_type, symb, severity, status, start_time, message,
#                     error_message, query, values, table_name, artifact_name, user_id, host_name
#                 )
#                 VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13) RETURNING id INTO :14
#                 """
#             try:
#                 self.cursor.execute(
#                     insert_query,
#                     (
#                         job_name,
#                         job_type,
#                         symbol,
#                         severity,
#                         "IN PROGRESS",
#                         current_time,
#                         message,
#                         error_message,
#                         query,
#                         values_str,
#                         table_name,
#                         artifact_name,
#                         user_id,
#                         host_name,
#                     ),
#                 )
#                 job_id = self.cursor.fetchone()[0]
#                 self.conn.commit()
#                 logging.info(f"New job logged with ID {job_id}: {message}")
#                 return job_id
#             except Exception as e:
#                 logging.error(f"Failed to insert job log: {e}")
#                 raise
#         else:
#             # Update an existing job log
#             status = "SUCCESS" if success else "FAILURE"
#             update_query = """
#             UPDATE ss_logs
#             SET status = %s, end_time = %s, message = %s, query = %s, values = %s,
#                 artifact_name = %s, error_message = %s
#             WHERE id = %s
#             """
#             if self.db_type == "oracle":
#                 update_query = """
#                 UPDATE ss_logs
#                 SET status = :1, end_time = :2, message = :3, query = :4, values = :5,
#                     artifact_name = :6, error_message = :7
#                 WHERE id = :8
#                 """
#             try:
#                 self.cursor.execute(
#                     update_query,
#                     (
#                         status,
#                         current_time,
#                         message,
#                         query,
#                         values_str,
#                         artifact_name,
#                         error_message,
#                         job_id,
#                     ),
#                 )
#                 self.conn.commit()
#                 logging.info(f"Job ID {job_id} updated to {status}: {message}")
#             except Exception as e:
#                 logging.error(f"Failed to update job log with ID {job_id}: {e}")
#                 raise
#
#     def close(self):
#         if self.cursor:
#             self.cursor.close()
#         if self.conn:
#             self.conn.close()
#         logging.info(
#             f"SQL Logger connection closed. Cache Hits: {self.cache_hits}, Cache Misses: {self.cache_misses}."
#         )
