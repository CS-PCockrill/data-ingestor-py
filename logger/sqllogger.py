import logging
import datetime
import psycopg2
import cx_Oracle


class SQLLogger:
    """
    A custom SQL logger that logs job states to a SQL database.
    Tracks the lifecycle of a job (IN PROGRESS -> SUCCESS/FAILURE).
    """

    def __init__(self, db_config):
        """
        Initialize the logger with database configuration.
        Args:
            db_config (dict): Database configuration with keys:
                              - "type" ("oracle" or "postgresql")
                              - "host"
                              - "port"
                              - "database"
                              - "user"
                              - "password"
        """
        self.db_config = db_config
        self.db_type = db_config.get("type").lower()
        self.conn = None
        self.cursor = None

        if self.db_type not in ["oracle", "postgresql"]:
            raise ValueError("Unsupported database type. Use 'oracle' or 'postgresql'.")

        self._connect()
        self._setup_table()

    def _connect(self):
        """Connect to the database."""
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
        except Exception as e:
            logging.error(f"Failed to connect to {self.db_type} database: {e}")
            raise

    def _setup_table(self):
        """Ensure the job_logs table exists."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS SS_LOGS (
            id SERIAL PRIMARY KEY,
            job_name VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            message TEXT,
            metadata JSONB
        )
        """
        if self.db_type == "oracle":
            create_table_query = """
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE SS_LOGS (
                    id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
                    job_name VARCHAR2(255) NOT NULL,
                    status VARCHAR2(50) NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    message CLOB,
                    metadata CLOB
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN RAISE; END IF;
            END;
            """
        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to create job_logs table: {e}")
            raise

    def log_job_start(self, job_name, metadata=None):
        """
        Log the start of a job.

        Args:
            job_name (str): Name of the job.
            metadata (dict): Additional information about the job.

        Returns:
            int: The ID of the inserted log record.
        """
        start_time = datetime.datetime.now(datetime.UTC)
        insert_query = """
        INSERT INTO ss_logs (job_name, status, start_time, metadata)
        VALUES (%s, %s, %s, %s) RETURNING id
        """
        if self.db_type == "oracle":
            insert_query = """
            INSERT INTO ss_logs (job_name, status, start_time, metadata)
            VALUES (:1, :2, :3, :4) RETURNING id INTO :5
            """
        metadata_str = str(metadata) if metadata else None
        try:
            if self.db_type == "oracle":
                self.cursor.execute(insert_query, (job_name, "IN PROGRESS", start_time, metadata_str))
                job_id = self.cursor.fetchone()[0]
            else:
                self.cursor.execute(insert_query, (job_name, "IN PROGRESS", start_time, metadata_str))
                job_id = self.cursor.fetchone()[0]
            self.conn.commit()
            logging.info(f"Job '{job_name}' started with ID {job_id}.")
            return job_id
        except Exception as e:
            logging.error(f"Failed to log job start: {e}")
            raise

    def log_job_end(self, job_id, success=True, message=None):
        """
        Log the end of a job.

        Args:
            job_id (int): ID of the job log record.
            success (bool): Whether the job succeeded.
            message (str): Additional details about the job outcome.
        """
        end_time = datetime.datetime.now(datetime.UTC)
        status = "SUCCESS" if success else "FAILURE"
        update_query = """
        UPDATE SS_LOGS
        SET status = %s, end_time = %s, message = %s
        WHERE id = %s
        """
        if self.db_type == "oracle":
            update_query = """
            UPDATE SS_LOGS
            SET status = :1, end_time = :2, message = :3
            WHERE id = :4
            """
        try:
            self.cursor.execute(update_query, (status, end_time, message, job_id))
            self.conn.commit()
            logging.info(f"Job with ID {job_id} marked as {status}.")
        except Exception as e:
            logging.error(f"Failed to log job end: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("SQL Logger connection closed.")