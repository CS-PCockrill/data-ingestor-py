from db.query_builder import QueryBuilder

class OracleQueryBuilder(QueryBuilder):
    def build_insert_query(self, columns):
        column_list = ", ".join(columns)
        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])  # Oracle uses `:1, :2, :3`
        return f"INSERT INTO {self.table_name} ({column_list}) VALUES ({placeholders})"

    def build_update_query(self, columns, condition="id = :1"):
        set_clause = ", ".join([f"{col} = :{i+1}" for i, col in enumerate(columns, 1)])
        return f"UPDATE {self.table_name} SET {set_clause} WHERE {condition}"