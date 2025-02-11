import logging

from abc import ABC, abstractmethod

import psycopg2
import cx_Oracle
from psycopg2.extras import execute_values

from db.oracle_query_builder import OracleQueryBuilder
from db.postgres_query_builder import PostgresQueryBuilder


class ConnectionManager(ABC):
    def __init__(self, db_config):
        """
        Initialize with database configuration.

        Args:
            db_config (dict): Database configuration dictionary.
        """
        self.db_config = db_config
        # self.query_builder = self._create_query_builder()

    @abstractmethod
    def connect(self):
        """
        Abstract method to establish and return a database connection.
        """
        pass

    @abstractmethod
    def close(self, conn):
        """
        Abstract method to close the database connection.

        Args:
            conn: The database connection to close.
        """
        pass

    @abstractmethod
    def execute_batch_insert(self, conn, query, values):
        """
        Executes a batch insert operation. Must be implemented in subclass.
        """
        pass


class PostgresConnectionManager(ConnectionManager):
    def __init__(self, db_config, schema=None):
        super().__init__(db_config)
        self.query_builder = PostgresQueryBuilder(db_config["consumerConfig"]["table_name"])
        self.schema = schema or {}

    def connect(self):
        try:
            conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
            )
            logging.info("Successfully created a new PostgreSQL connection.")
            return conn
        except Exception as e:
            logging.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def close(self, conn):
        if conn:
            try:
                conn.close()
                logging.info("PostgreSQL connection closed.")
            except Exception as e:
                logging.error(f"Failed to close PostgreSQL connection: {e}")

    def get_query_builder(self, table_name):
        self.query_builder = PostgresQueryBuilder(table_name)
        return self.query_builder

    def execute_batch_insert(self, conn, query, values):
        """
        Executes a batch insert operation using `execute_values` for PostgreSQL.
        """
        with conn.cursor() as cur:
            execute_values(cur, query, values)
            conn.commit()
            logging.info(f"Successfully inserted {len(values)} records into PostgreSQL.")

class OracleConnectionManager(ConnectionManager):
    def __init__(self, db_config):
        super().__init__(db_config)
        self.query_builder = OracleQueryBuilder(db_config["consumerConfig"]["table_name"])

    def connect(self):
        try:
            dsn = cx_Oracle.makedsn(
                self.db_config["host"],
                self.db_config["port"],
                sid=self.db_config["sid"],  # SID or service name
            )
            conn = cx_Oracle.connect(
                user=self.db_config["user"],
                password=self.db_config["password"],
                dsn=dsn,
            )
            logging.info("Successfully created a new Oracle connection.")
            return conn
        except Exception as e:
            logging.error(f"Failed to connect to Oracle: {e}")
            raise

    def close(self, conn):
        if conn:
            try:
                conn.close()
                logging.info("Oracle connection closed.")
            except Exception as e:
                logging.error(f"Failed to close Oracle connection: {e}")

    def get_query_builder(self, table_name):
        self.query_builder = OracleQueryBuilder(table_name)
        return self.query_builder

    def execute_batch_insert(self, conn, query, values):
        """
        Executes a batch insert operation using `executemany` for Oracle.
        """
        with conn.cursor() as cur:
            cur.executemany(query, values)  # Oracle's batch execution
            conn.commit()
            logging.info(f"Successfully inserted {len(values)} records into Oracle.")
