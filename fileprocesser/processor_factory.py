import logging

from db.connection_factory import DBConnectionFactory
from fileprocesser.processor import Processor
from logger.logger_factory import LoggerFactory
from logger.sqllogger import LoggerContext


class ProcessorFactory:
    _processor_classes = {}
    _file_extensions = {}
    _interface_configs = {}

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
    def register_processor(cls, interface_ids, control_file_path, processor_class, file_extensions):
        """
        Register one or more interface IDs with a specific processor class.

        Args:
            interface_ids (list or set): Interface IDs to register.
            control_file_path (str): Path to the control file.
            processor_class (type): Processor class for the interfaces.
            file_extensions (list): Supported file extensions for the interfaces.

        Raises:
            ValueError: If `interface_ids` is not a list or set, or if `processor_class` is invalid.
        """
        # Validate that interface_ids is a list or set
        if not isinstance(interface_ids, (list, set)):
            raise ValueError("interface_ids must be a list or set.")

        # Validate that processor_class is a subclass of Processor
        if not issubclass(processor_class, Processor):
            error_message = (
                f"Invalid processor class '{processor_class.__name__}'. "
                f"Expected a subclass of Processor."
            )
            logging.error(error_message)
            raise ValueError(error_message)

        # Validate file extensions type
        if not isinstance(file_extensions, (list, set)):
            raise ValueError("file_extensions must be a list or set.")

        # Register the processor class for each interface ID
        for interface_id in interface_ids:
            cls._interface_configs[interface_id] = control_file_path
            cls._processor_classes[interface_id] = processor_class
            cls._file_extensions[interface_id] = file_extensions
            logging.info(
                f"Registered processor class '{processor_class.__name__}' for interface ID '{interface_id}' "
                f"with supported file extensions: {file_extensions}"
            )

    @classmethod
    def create_processor(cls, interface_id, config):
        """
        Create a processor instance for the given interface ID.

        Args:
            interface_id (str): The interface ID.
            config (dict): Configuration dictionary.

        Returns:
            Processor: A processor instance.

        Raises:
            ValueError: If no processor is registered for the interface ID.
        """
        if interface_id not in cls._processor_classes:
            raise ValueError(f"Interface ID '{interface_id}' is not registered.")
        processor_class = cls._processor_classes[interface_id]

        # Connection Manager and Logger
        db_type = config.get("dbType", "postgres").lower()
        connection_manager = DBConnectionFactory.get_connection_manager(db_type, config)
        logger_context = LoggerContext(
            config["interfaceType"],
            config["user"],
            config["tableName"],
            config["errorDefinitionSourceLocation"],
            config["logsTableName"],
            config["logsSchema"]
        )
        logger = LoggerFactory.create_logger(interface_id, connection_manager, logger_context)

        # Create the processor
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
