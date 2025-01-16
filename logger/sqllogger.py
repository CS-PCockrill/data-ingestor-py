import logging
import datetime
import json
import socket

import logging.handlers
from prometheus_client import Counter, Histogram
from db.querybuilder import QueryBuilder
from errors.errorresolver import ErrorResolver

LOG_DB_WRITE_SUCCESS = Counter("log_db_write_success", "Number of successful DB writes")
LOG_DB_WRITE_FAILURE = Counter("log_db_write_failure", "Number of failed DB writes")
LOG_PROCESSING_TIME = Histogram("log_processing_time_seconds", "Time taken to process logs")

def setup_fallback_logger():
    fallback_logger = logging.getLogger("fallback_logger")
    handler = logging.handlers.RotatingFileHandler(
        "fallback_logs.json",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,  # Keep 3 backup files
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    fallback_logger.addHandler(handler)
    fallback_logger.setLevel(logging.INFO)
    return fallback_logger

class LoggerContext:
    def __init__(self, interface_type, user_id, table_name, error_table_name, logs_table_name):
        self.interface_type = interface_type
        self.user_id = user_id
        self.table_name = table_name
        self.error_table = error_table_name
        self.logs_table = logs_table_name


class SQLLogger:
    def __init__(self, connection_manager, context):
        self.connection_manager = connection_manager
        self.conn = self.connection_manager.connect()
        self.context = context
        self.query_builder = QueryBuilder(context.logs_table)
        # Use a connection-level cursor in ErrorResolver
        self.error_resolver = ErrorResolver(self.conn, context.error_table)
        self.fallback_logger = setup_fallback_logger()

    def _execute_query(self, query, parameters):
        """
        Helper method to execute a query with a local cursor.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, parameters)
            if query.strip().lower().startswith("insert"):
                # Return the generated ID for insert queries
                return cursor.fetchone()[0]
            # Commit changes for all queries
            self.conn.commit()

    def _insert_job(self, parameters):
        insert_query = self.query_builder.build_insert_query([
            "job_name", "job_type", "symb", "severity", "status", "start_time",
            "message", "error_message", "query", "values", "artifact_name",
            "user_id", "host_name", "table_name"
        ])
        try:
            return self._execute_query(insert_query, parameters)
        except Exception as e:
            logging.error(f"Failed to insert job: {e}")
            raise

    def _update_job(self, parameters):
        update_query = self.query_builder.build_update_query([
            "status", "end_time", "message", "error_message",
            "query", "values", "user_id", "table_name"
        ])
        try:
            self._execute_query(update_query, parameters)
        except Exception as e:
            logging.error(f"Failed to update job: {e}")
            raise

    @LOG_PROCESSING_TIME.time()
    def log_job(self, *args, symbol, **kwargs):
        try:
            severity, message = self.error_resolver.resolve(symbol, *args)
            current_time = datetime.datetime.now(datetime.timezone.utc)
            host_name = socket.gethostname()

            job_id = kwargs.get("job_id")
            parameters = (
                kwargs.get("job_name", "Job"),
                kwargs.get("job_type", self.context.interface_type),
                symbol,
                severity,
                "IN PROGRESS" if job_id is None else ("SUCCESS" if kwargs.get("success") else "FAILURE"),
                current_time,
                message,
                kwargs.get("error_message"),
                kwargs.get("query"),
                json.dumps(kwargs.get("values")) if kwargs.get("values") else None,
                kwargs.get("artifact_name"),
                self.context.user_id,
                host_name,
                self.context.table_name,
            )

            if job_id is None:
                # Debug logs for insert operation
                logging.debug(f"Inserting job with parameters: {parameters}")
                job_id = self._insert_job(parameters)
            else:
                # Debug logs for update operation
                update_parameters = parameters + (job_id,)
                logging.debug(f"Updating job with parameters: {update_parameters}")
                self._update_job(update_parameters)

            # Increment success counter
            LOG_DB_WRITE_SUCCESS.inc()
            return job_id
        except Exception as e:
            # Increment failure counter
            LOG_DB_WRITE_FAILURE.inc()

            # Log detailed error information
            logging.error(f"Logging job failed: {e}")
            logging.debug(f"Symbol: {symbol}, Parameters: {kwargs}")

            # Fallback logging for additional error context
            self._fallback_log(symbol, str(e), kwargs)

    def _format_log_entry(self, **kwargs):
        log_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "host": socket.gethostname(),
            "context": {
                "interface_type": self.context.interface_type,
                "user_id": self.context.user_id,
                "table_name": self.context.table_name,
            },
            **kwargs,
        }
        return json.dumps(log_entry)

    def _fallback_log(self, symbol, message, kwargs):
        log_entry = self._format_log_entry(
            symbol=symbol,
            message=message,
            additional_info=kwargs,
        )
        self.fallback_logger.info(log_entry)

    def close(self):
        if self.conn:
            self.conn.close()
        logging.info("SQL Logger connection closed.")
