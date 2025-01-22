import logging
import datetime
import json
import socket

import logging.handlers
from prometheus_client import Counter, Histogram

from logger.logger import Logger
from errors.error_resolver import ErrorResolver

# Prometheus metrics for observability
LOG_DB_WRITE_SUCCESS = Counter("log_db_write_success", "Number of successful DB writes")
LOG_DB_WRITE_FAILURE = Counter("log_db_write_failure", "Number of failed DB writes")
LOG_PROCESSING_TIME = Histogram("log_processing_time_seconds", "Time taken to process logs")


def setup_fallback_logger():
    """
    Configures a fallback logger to handle logging in case of database failures.

    Returns:
        logging.Logger: A logger instance that writes logs to a rotating file.
    """
    # Create a logger instance for fallback purposes
    fallback_logger = logging.getLogger("fallback_logger")

    # Configure a rotating file handler to limit file size and manage backups
    handler = logging.handlers.RotatingFileHandler(
        "fallback_logs.json",  # Logs will be written to this file
        maxBytes=5 * 1024 * 1024,  # Maximum size of 5 MB per file
        backupCount=3,  # Maintain up to 3 backup files
    )

    # Use a simple timestamp-based log format for readability
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    fallback_logger.addHandler(handler)

    # Set the fallback logger to log INFO-level and higher messages
    fallback_logger.setLevel(logging.INFO)

    logging.debug("Fallback logger successfully configured.")  # Debug-level log for setup confirmation
    return fallback_logger


class LoggerContext:
    """
    Encapsulates contextual information for logging operations.

    Attributes:
        interface_type (str): The name of the interface we are loading
        user_id (int): The ID of the user initiating the log.
        table_name (str): Name of the primary database table being logged.
        error_table (str): Name of the table for logging errors.
        logs_table (str): Name of the table for storing general logs.
        logger_schema (dict): The schema of the logger table to log to the database.
    """

    def __init__(self, interface_type, user_id, table_name, error_table_name, logs_table_name, logger_schema):
        self.interface_type = interface_type
        self.user_id = user_id
        self.table_name = table_name
        self.error_table = error_table_name
        self.logs_table = logs_table_name
        self.logger_schema = logger_schema

        # Debugging initialization
        logging.debug(
            f"LoggerContext initialized with: interface_type={interface_type}, "
            f"user_id={user_id}, table_name={table_name}, "
            f"error_table={error_table_name}, logs_table={logs_table_name}, logger_schema={logger_schema}"
        )


class SQLLogger(Logger):
    """
    Handles database logging operations with fallback mechanisms for error resilience.

    Attributes:
        connection_manager (DBConnectionManager): Manages database connections.
        context (LoggerContext): Provides contextual information for logging operations.
    """

    def __init__(self, connection_manager, context):
        super().__init__()
        self.connection_manager = connection_manager
        self.conn = self.connection_manager.connect()
        self.context = context
        self.query_builder = self.connection_manager.get_query_builder(context.logs_table)
        self.query_builder.set_schema(context.logger_schema)
        self.error_resolver = ErrorResolver(self.connection_manager.connect(), context.error_table)
        self.fallback_logger = setup_fallback_logger()
        logging.debug("SQLLogger initialized successfully.")

    def _build_parameters(self, **kwargs):
        """
        Dynamically constructs a dictionary of parameters based on the provided schema.

        Args:
            current_time (datetime): Current timestamp.
            host_name (str): Name of the host machine.
            **kwargs: Dynamic fields for the parameters.

        Returns:
            dict: Constructed parameters for SQL operations.
        """
        parameters = {}

        for key, db_column in self.query_builder.schema.items():
            if key in kwargs:
                parameters[db_column] = kwargs[key]

        logging.debug(f"Constructed parameters: {parameters}")
        return parameters

    def log_job(self, *args, symbol, **kwargs):
        """
        Logs a job entry to the database, either by inserting a new entry or updating an existing one.

        Args:
            *args: Positional arguments for the error resolver.
            symbol (str): Unique identifier for the log type (e.g., error or info).
            **kwargs: Additional metadata fields for the log.

        Returns:
            int: Job ID of the logged entry.

        Raises:
            Exception: Logs errors and triggers fallback logging on failure.
        """
        try:
            severity, message = self.error_resolver.resolve(symbol, *args)
            current_time = datetime.datetime.now(datetime.timezone.utc)
            host_name = socket.gethostname()

            job_id = kwargs.get("job_id")

            if job_id is None:
                # Insert operation
                insert_params = self._build_parameters(
                    symbol=symbol, severity=severity, message=message, **kwargs
                )
                insert_query = self.query_builder.build_insert_query(insert_params.keys(), batch=False)
                logging.info(f"Inserting {symbol} into {host_name} table...\nINSERT QUERY: {insert_query}")
                job_id = self._execute_query(insert_query, tuple(insert_params.values()))
            else:
                # Update operation
                update_params = self._build_parameters(
                    symbol=symbol, severity=severity, message=message, **kwargs
                )
                update_query = self.query_builder.build_update_query(update_params.keys())
                logging.info(f"Updating {symbol} into {host_name} table...\nUPDATE QUERY: {update_query}")
                self._execute_query(update_query, tuple(update_params.values()) + (job_id,))

            LOG_DB_WRITE_SUCCESS.inc()
            return job_id
        except Exception as e:
            LOG_DB_WRITE_FAILURE.inc()
            logging.error(f"Failed to log job: {e}")
            self._fallback_log(symbol, str(e), kwargs)

    def _execute_query(self, query, parameters):
        """
        Executes an SQL query using a connection-level cursor.

        Args:
            query (str): SQL query string to execute.
            parameters (tuple): Query parameters for the SQL query.

        Returns:
            Any: Result of the query execution (e.g., inserted ID for insert queries).

        Raises:
            Exception: Propagates database exceptions for error handling.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, parameters)
                if query.strip().lower().startswith("insert"):
                    return cursor.fetchone()[0]
                self.conn.commit()
            logging.info(f"Successfully executed query: {query}")
        except Exception as e:
            logging.error(f"Error executing query: {query}, Parameters: {parameters}, Error: {e}")
            raise

    def _fallback_log(self, symbol, message, kwargs):
        """
        Logs job details to the fallback logger in case of a database failure.

        Args:
            symbol (str): Unique identifier for the log type.
            message (str): Error message to log.
            kwargs: Additional metadata fields for the log.
        """
        log_entry = self._format_log_entry(
            symbol=symbol,
            message=message,
            additional_info=kwargs,
        )
        logging.warning(f"Fallback log entry: {log_entry}")
        self.fallback_logger.info(log_entry)

    def _format_log_entry(self, **kwargs):
        """
        Formats a log entry as a structured JSON object.

        This method creates a consistent log structure that includes metadata such as timestamp,
        hostname, and context details. Additional fields can be appended via keyword arguments.

        Args:
            **kwargs: Additional fields to include in the log entry, such as error details,
                      database query, or processing context.

        Returns:
            str: A JSON-encoded string representing the structured log entry.

        Raises:
            TypeError: If `kwargs` contains non-serializable values.
        """
        try:
            # Construct the core log entry structure
            log_entry = {
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),  # ISO8601 format for timestamp
                "host": socket.gethostname(),  # Hostname of the machine running the logger
                "context": {
                    "interface_type": self.context.interface_type,  # Name of interface were logging for
                    "user_id": self.context.user_id,  # User associated with the log entry
                    "table_name": self.context.table_name,  # Primary table related to the log
                },
                **kwargs,  # Include additional dynamic fields
            }

            # Serialize the log entry to JSON
            serialized_entry = json.dumps(log_entry)
            logging.debug(f"Formatted log entry: {serialized_entry}")
            return serialized_entry
        except TypeError as e:
            logging.error(f"Failed to format log entry due to non-serializable data: {kwargs}. Error: {e}")
            raise

    def close(self):
        """
        Closes the database connection when the logger is no longer in use.
        """
        if self.conn:
            self.conn.close()
            logging.info("SQL Logger connection closed.")

