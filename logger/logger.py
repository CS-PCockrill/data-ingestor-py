from abc import ABC, abstractmethod

class Logger(ABC):
    @abstractmethod
    def log_job(self, message, level="info"):
        """
        Abstract method for logging a message.

        Args:
            message (str): The message to log.
            level (str): The logging level (e.g., "info", "error").
        """
        pass

    @abstractmethod
    def close(self):
        """
        Abstract method for closing logger resources.
        """
        pass
