from db.connection_manager import PostgresConnectionManager, OracleConnectionManager

class DBConnectionFactory:
    @staticmethod
    def get_connection_manager(db_type, db_config):
        """
        Get the appropriate connection manager based on the database type.

        Args:
            db_type (str): The database type ('postgres' or 'oracle').
            db_config (dict): The database configuration.

        Returns:
            ConnectionManager: An instance of the appropriate connection manager.

        Raises:
            ValueError: If the database type is not supported.
        """
        if db_type == "postgres":
            return PostgresConnectionManager(db_config)
        elif db_type == "oracle":
            return OracleConnectionManager(db_config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
