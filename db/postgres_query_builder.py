from db.query_builder import QueryBuilder

class PostgresQueryBuilder(QueryBuilder):
    def __init__(self, table_name, schema=None):
        """
        Initializes the PostgresQueryBuilder with table name and optional schema.

        Args:
            table_name (str): Name of the database table for query generation.
            schema (dict, optional): Schema mapping logical keys to database column names.
        """
        super().__init__(table_name)
        self.table_name = table_name
        self.schema = schema or {}

    def build_insert_query(self, columns, batch=True):
        """
        Generate an INSERT query with column names wrapped in double quotes
        for SQL safety. Use placeholders for batch values or positional arguments
        based on the `batch` flag.

        Args:
            columns (list): List of column names for the INSERT statement.
            batch (bool): If True, uses a single placeholder (%s) for batch inserts.
                         If False, uses positional placeholders for single inserts.

        Returns:
            str: A SQL INSERT query string.
        """
        # Wrap each column name in double quotes
        col_list = ", ".join(f'"{col.lower()}"' for col in columns)

        if batch:
            # Use a single placeholder for batch values
            values_placeholder = "%s"
        else:
            # Use positional placeholders for single values
            values_placeholder = f"({', '.join(['%s'] * len(columns))})"

        # Generate the query
        return f"INSERT INTO {self.table_name} ({col_list}) VALUES {values_placeholder} RETURNING id;"

    def build_update_query(self, columns, condition="id = %s"):
        assignments = ", ".join(f'"{col.lower()}" = %s' for col in columns if col != "job_id")
        return f"UPDATE {self.table_name} SET {assignments} WHERE {condition}"

    def set_schema(self, schema):
        """
        Updates the schema for the query builder.

        Args:
            schema (dict): Schema mapping logical keys to database column names.
        """
        self.schema = schema

    def map_to_columns(self, data):
        """
        Maps logical keys in the input data to their corresponding database column names.

        Args:
            data (dict): Input data with logical keys.

        Returns:
            dict: Data with database column names as keys.
        """
        if not self.schema:
            return data
        return {self.schema.get(key, key): value for key, value in data.items()}
