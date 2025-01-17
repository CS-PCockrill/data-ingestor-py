from logger.logger import Logger
import logging

class LoggerFactory:
    _logger_classes = {}

    @classmethod
    def register_logger(cls, interface_ids, logger_class):
        """
        Register one or more interface IDs with a specific logger class.

        Args:
            interface_ids (list or set): Interface IDs to register.
            logger_class (type): Logger class for the interfaces.

        Raises:
            ValueError: If `interface_ids` is not a list or set, or if `logger_class` is invalid.
        """
        # Validate interface_ids type
        if not isinstance(interface_ids, (list, set)):
            raise ValueError("interface_ids must be a list or set.")

        # Validate that the logger_class is a subclass of the base Logger class
        if not issubclass(logger_class, Logger):
            error_message = (
                f"Invalid logger class '{logger_class.__name__}'. "
                f"Expected a subclass of Logger."
            )
            logging.error(error_message)
            raise ValueError(error_message)

        # Register the logger class for each interface ID
        for interface_id in interface_ids:
            cls._logger_classes[interface_id] = logger_class
            logging.info(
                f"Registered logger class '{logger_class.__name__}' for interface ID '{interface_id}'."
            )

    @classmethod
    def create_logger(cls, interface_id, connection_manager, logger_context):
        """
        Create a logger instance for the given interface ID.

        Args:
            interface_id (str): The interface ID.
            connection_manager: Database connection manager.
            logger_context: Context information for the logger.

        Returns:
            Logger: A logger instance.

        Raises:
            ValueError: If no logger is registered for the interface ID.
        """
        if interface_id not in cls._logger_classes:
            raise ValueError(f"No logger registered for interface ID '{interface_id}'.")
        logger_class = cls._logger_classes[interface_id]
        return logger_class(connection_manager, logger_context)
