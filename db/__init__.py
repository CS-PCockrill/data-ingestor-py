import logging
import psycopg2


class DBConnectionManager:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None

    def connect(self):
        if not self.conn:
            try:
                self.conn = psycopg2.connect(
                    host=self.db_config["host"],
                    port=self.db_config["port"],
                    database=self.db_config["database"],
                    user=self.db_config["user"],
                    password=self.db_config["password"],
                )
                logging.info("Successfully connected to PostgreSQL.")
            except Exception as e:
                logging.error(f"Failed to connect to database: {e}")
                raise
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")
