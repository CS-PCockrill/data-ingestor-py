import logging

class ErrorResolver:
    def __init__(self, cursor, error_table):
        self.cursor = cursor
        self.error_table = error_table

    def resolve(self, symbol, *args):
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
