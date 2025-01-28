from abc import ABC, abstractmethod

class Logger(ABC):
    def __init__(self):
        self.context_id = None

    @abstractmethod
    def log_job(self, *args, symbol, **kwargs):
        """
        Abstract method for logging a message.

        Args:
            *args: Positional arguments for the error resolver.
            symbol (str): Unique identifier for the log type (e.g., error or info).
            **kwargs: Additional metadata fields for the log.
            :param schema:
        """
        pass

    @abstractmethod
    def close(self):
        """
        Abstract method for closing logger resources.
        """
        pass

    @abstractmethod
    def set_context_id(self, context_id):
        self.context_id = context_id

    @abstractmethod
    def get_context_id(self):
        return self.context_id
