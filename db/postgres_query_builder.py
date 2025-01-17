from db.query_builder import QueryBuilder

class PostgresQueryBuilder(QueryBuilder):
    def build_insert_query(self, columns):
        """
        Generate an INSERT query with column names wrapped in double quotes
        for SQL safety and a placeholder for batch values.
        """
        # Wrap each column name in double quotes
        col_list = ", ".join(f'"{col}"' for col in columns)
        # Generate the query
        return f"INSERT INTO {self.table_name} ({col_list}) VALUES %s RETURNING id;"

        # column_list = ", ".join(columns)
        # placeholders = ", ".join(["%s"] * len(columns))  # Postgres uses `%s` for placeholders
        # return f"INSERT INTO {self.table_name} ({column_list}) VALUES ({placeholders}) RETURNING id"

    def build_update_query(self, columns, condition="id = %s"):
        set_clause = ", ".join([f"{col} = %s" for col in columns])
        return f"UPDATE {self.table_name} SET {set_clause} WHERE {condition}"

