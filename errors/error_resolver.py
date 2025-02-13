import logging
import cx_Oracle

class ErrorResolverOracle:
    """
    Resolves error symbols into corresponding severity and descriptions using an Oracle database table.

    Attributes:
        conn (cx_Oracle.Connection): The active Oracle database connection.
        error_table_name (str): Name of the table containing error definitions.
    """
    def __init__(self, conn, error_table_name):
        """
        Initialize the ErrorResolver with a database connection and error table name.

        Args:
            conn (cx_Oracle.Connection): Active Oracle database connection.
            error_table_name (str): Name of the table containing error definitions.
        """
        self.conn = conn
        self.error_table_name = error_table_name

    def resolve(self, symbol, *args):
        """
        Resolve an error symbol into its severity and formatted description.

        Args:
            symbol (str): The error symbol to resolve (e.g., "GS2002E").
            *args: Optional arguments to format into the description.

        Returns:
            tuple: A tuple containing severity (str) and formatted description (str).

        Raises:
            Exception: If there is a failure during database query execution.
        """
        query = f"SELECT svrt, dscr FROM {self.error_table_name} WHERE symb = :1"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (symbol,))  # Execute query with bind variable
                result = cursor.fetchone()  # Fetch the first matching row

                if result:
                    severity, description = result
                    return severity, description.format(*args)
                else:
                    logging.warning(f"Error symbol '{symbol}' not found in {self.error_table_name}.")
                    return "W", f"Unknown error: {symbol}"

        except cx_Oracle.DatabaseError as e:
            logging.error(f"Failed to resolve error definition for '{symbol}': {e}")
            self.conn.rollback()  # Rollback in case of an error
            raise

class ErrorResolver:
    """
    Resolves error symbols into corresponding severity and descriptions using a database table.

    Attributes:
        conn (psycopg2.extensions.connection): The active database connection.
        error_table_name (str): Name of the table containing error definitions.
    """
    def __init__(self, conn, error_table_name):
        """
        Initialize the ErrorResolver with a database connection and error table name.

        Args:
            conn (psycopg2.extensions.connection): Active database connection.
            error_table_name (str): Name of the table containing error definitions.
        """
        self.conn = conn
        self.error_table_name = error_table_name

    def resolve(self, symbol, *args):
        """
        Resolve an error symbol into its severity and formatted description.

        Args:
            symbol (str): The error symbol to resolve (e.g., "GS2002E").
            *args: Optional arguments to format into the description.

        Returns:
            tuple: A tuple containing severity (str) and formatted description (str).

        Raises:
            Exception: If there is a failure during database query execution.
        """
        # logging.info("Error table name: %s | Symbol: %s", self.error_table_name, symbol)
        query = f"SELECT svrt, dscr FROM {self.error_table_name} WHERE symb = %s"
        try:
            # Using a context manager for the database cursor
            with self.conn.cursor() as cursor:
                cursor.execute(query, (symbol,))  # Execute the query with the provided symbol
                result = cursor.fetchone()  # Fetch the first matching row

                if result:
                    # Unpack the result into severity and description
                    severity, description = result
                    # Format the description with additional arguments, if any
                    # self.conn.commit()
                    return severity, description.format(*args)
                else:
                    # Log a warning if the symbol is not found
                    logging.warning(f"Error symbol '{symbol}' not found in {self.error_table_name}.")
                    # self.conn.commit()
                    return "W", f"Unknown error: {symbol}"


        except Exception as e:
            # Log and propagate exceptions for visibility and debugging
            # self.conn.rollback()
            logging.error(f"Failed to resolve error definition for '{symbol}': {e}")
            raise


