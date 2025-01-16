import logging

class ErrorResolver:
    def __init__(self, conn, error_table_name):
        self.conn = conn
        self.error_table_name = error_table_name

    def resolve(self, symbol, *args):
        query = f"SELECT svrt, dscr FROM {self.error_table_name} WHERE symb = %s"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (symbol,))
                result = cursor.fetchone()
                if result:
                    severity, description = result
                    return severity, description.format(*args)
                else:
                    logging.warning(f"Error symbol '{symbol}' not found.")
                    return "W", f"Unknown error: {symbol}"
        except Exception as e:
            logging.error(f"Failed to resolve error definition for '{symbol}': {e}")
            raise

