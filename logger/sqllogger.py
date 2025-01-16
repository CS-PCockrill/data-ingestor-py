import logging
import datetime
import json
import socket
from errors.errorresolver import ErrorResolver

class QueryBuilder:
    def __init__(self, table_name):
        self.table_name = table_name

    def build_insert_query(self, columns):
        column_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        return f"INSERT INTO {self.table_name} ({column_list}) VALUES ({placeholders}) RETURNING id"

    def build_update_query(self, columns, condition="id = %s"):
        set_clause = ", ".join([f"{col} = %s" for col in columns])
        return f"UPDATE {self.table_name} SET {set_clause} WHERE {condition}"

class LoggerContext:
    def __init__(self, interface_type, user_id, table_name, error_table_name):
        self.interface_type = interface_type
        self.user_id = user_id
        self.table_name = table_name
        self.error_table = error_table_name


class SQLLogger:
    def __init__(self, connection_manager, context):
        self.connection_manager = connection_manager
        self.conn = self.connection_manager.connect()
        self.cursor = self.conn.cursor()
        self.context = context
        self.query_builder = QueryBuilder("ss_logs")
        self.error_resolver = ErrorResolver(self.cursor, context.error_table)

    def _insert_job(self, parameters):
        insert_query = self.query_builder.build_insert_query([
            "job_name", "job_type", "symb", "severity", "status", "start_time",
            "message", "error_message", "query", "values", "artifact_name",
            "user_id", "host_name", "table_name"
        ])
        try:
            self.cursor.execute(insert_query, parameters)
            job_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return job_id
        except Exception as e:
            logging.error(f"Failed to insert job: {e}")
            raise

    def _update_job(self, parameters):
        update_query = self.query_builder.build_update_query([
            "status", "end_time", "message", "error_message",
            "query", "values", "user_id", "table_name"
        ])
        try:
            self.cursor.execute(update_query, parameters)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to update job: {e}")
            raise

    def log_job(self, *args, symbol, **kwargs):
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
            return self._insert_job(parameters)
        else:
            self._update_job(parameters + (job_id,))

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("SQL Logger connection closed.")
