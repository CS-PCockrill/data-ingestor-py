
class QueryBuilder:
    def __init__(self, table_name):
        self.table_name = table_name

    def build_insert_query(self, columns):
        raise NotImplementedError("build_insert_query must be implemented in subclasses.")

    def build_update_query(self, columns, condition="id = :id"):
        raise NotImplementedError("build_update_query must be implemented in subclasses.")