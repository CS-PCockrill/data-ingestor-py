from db.connection_manager import DBConnectionManager
from logger.sqllogger import LoggerContext


class ProcessorFactory:
    _processor_classes = {}
    _file_extensions = {}
    _interface_configs = {}
    _logger_classes = {}

    @classmethod
    def register_interface(cls, interface_ids, control_file_path, processor_class, file_extensions, logger_class=None):
        """
        Register one or more interfaces with the same configuration.

        Args:
            interface_ids (list or set): A list or set of interface IDs to register.
            control_file_path (str): Path to the control file for this interface.
            processor_class (type): The processor class to use for this interface.
            file_extensions (list): List of supported file extensions for this interface.
            logger_class (type): The logger class to use for this interface (optional).
        """
        if not isinstance(interface_ids, (list, set)):
            raise ValueError("interface_ids must be a list or set.")

        for interface_id in interface_ids:
            cls._interface_configs[interface_id] = control_file_path
            cls._processor_classes[interface_id] = processor_class
            cls._file_extensions[interface_id] = file_extensions
            cls._logger_classes[interface_id] = logger_class

    @classmethod
    def get_control_file_path(cls, interface_id):
        """
        Retrieve the control file path for a given interface ID.

        Args:
            interface_id (str): The interface ID.

        Returns:
            str: The control file path.

        Raises:
            ValueError: If the interface ID is not registered.
        """
        if interface_id not in cls._interface_configs:
            raise ValueError(f"Interface ID '{interface_id}' is not registered.")
        return cls._interface_configs[interface_id]

    @classmethod
    def create_logger(cls, interface_id, connection_manager, logger_context):
        """
        Create an instance of the logger for a given interface ID.

        Args:
            interface_id (str): The interface ID.
            connection_manager: The database connection manager.
            logger_context: The context for logging.

        Returns:
            Logger: An instance of the logger.

        Raises:
            ValueError: If the interface ID or logger class is not registered.
        """
        if interface_id not in cls._logger_classes:
            raise ValueError(f"No logger registered for interface ID '{interface_id}'.")
        logger_class = cls._logger_classes[interface_id]
        if logger_class is None:
            raise ValueError(f"No logger class defined for interface ID '{interface_id}'.")
        return logger_class(connection_manager, logger_context)

    @classmethod
    def create_processor(cls, interface_id, config):
        """
        Create an instance of the processor for a given interface ID.

        Args:
            interface_id (str): The interface ID.
            config (dict): Configuration dictionary for the processor.

        Returns:
            Processor: An instance of the processor.

        Raises:
            ValueError: If the interface ID is not registered.
        """
        if interface_id not in cls._processor_classes:
            raise ValueError(f"Interface ID '{interface_id}' is not registered.")
        processor_class = cls._processor_classes[interface_id]

        # Initialize the connection manager and logger
        connection_manager = DBConnectionManager(config)
        logger_context = LoggerContext(
            config['interfaceType'],
            config['user'],
            config['tableName'],
            config['errorDefinitionSourceLocation'],
            config['logsTableName'],
        )
        logger = cls.create_logger(interface_id, connection_manager, logger_context)

        # Create and return the processor instance with logger
        return processor_class(connection_manager, logger, config)

    @classmethod
    def get_supported_file_extensions(cls, interface_id):
        """
        Retrieve the supported file extensions for a given interface ID.

        Args:
            interface_id (str): The interface ID.

        Returns:
            list: List of supported file extensions.

        Raises:
            ValueError: If the interface ID is not registered.
        """
        if interface_id not in cls._file_extensions:
            raise ValueError(f"Interface ID '{interface_id}' is not registered.")
        return cls._file_extensions[interface_id]

    @classmethod
    def get_interface_configs(cls):
        """
        Retrieve all registered interface configurations.

        Returns:
            dict: Dictionary of interface configurations.
        """
        return cls._interface_configs
