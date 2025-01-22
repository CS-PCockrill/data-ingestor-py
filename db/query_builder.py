
class QueryBuilder:
    def __init__(self, table_name):
        self.table_name = table_name

    def build_insert_query(self, columns):
        raise NotImplementedError("build_insert_query must be implemented in subclasses.")

    def build_update_query(self, columns, condition="id = :id"):
        raise NotImplementedError("build_update_query must be implemented in subclasses.")

    def set_schema(self, schema):
        """
        Updates the schema for the query builder.

        Args:
            schema (dict): Schema mapping logical keys to database column names.
        """
        pass

    def map_to_columns(self, data):
        """
        Maps logical keys in the input data to their corresponding database column names.

        Args:
            data (dict): Input data with logical keys.

        Returns:
            dict: Data with database column names as keys.
        """
        pass