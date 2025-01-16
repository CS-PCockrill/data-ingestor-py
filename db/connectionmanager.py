import logging
import psycopg2


class DBConnectionManager:
    def __init__(self, db_config):
        self.db_config = db_config

    def connect(self):
        """
        Create and return a new database connection for every call.

        Returns:
            psycopg2.extensions.connection: A new database connection instance.
        """
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
            logging.error(f"Failed to connect to database: {e}")
            raise

    @staticmethod
    def close(conn):
        """
        Close the provided database connection.

        Args:
            conn (psycopg2.extensions.connection): The database connection to close.
        """
        if conn:
            try:
                conn.close()
                logging.info("Database connection closed.")
            except Exception as e:
                logging.error(f"Failed to close database connection: {e}")
