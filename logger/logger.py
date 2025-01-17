from abc import ABC, abstractmethod

class Logger(ABC):
    @abstractmethod
    def log_job(self, *args, symbol, **kwargs):
        """
        Abstract method for logging a message.

        Args:
            *args: Positional arguments for the error resolver.
            symbol (str): Unique identifier for the log type (e.g., error or info).
            **kwargs: Additional metadata fields for the log.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Abstract method for closing logger resources.
        """
        pass
