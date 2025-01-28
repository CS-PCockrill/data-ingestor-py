from abc import ABC, abstractmethod

class Producer(ABC):
    def __init__(self, logger, **kwargs):
        """
        Abstract base class for producers.

        Args:
            **kwargs: Dynamic attributes for the producer.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)

        self.logger = logger
        self.artifact_name = None  # Common field for all producers
        self.ctx_id = None

    @abstractmethod
    def produce(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_context_id(self):
        """
        Retrieves the current context ID for the Producer.
        """
        return self.ctx_id

class Consumer(ABC):
    def __init__(self, producer, **kwargs):
        """
                Abstract base class for producers.

                Args:
                    **kwargs: Dynamic attributes for the producer.
                """
        for key, value in kwargs.items():
            setattr(self, key, value)

        self.producer = producer
        self.artifact_name = producer.artifact_name  # Inherit artifact name from producer

    @abstractmethod
    def consume(self):
        """
        Abstract method to consume records.
        """
        pass

    @abstractmethod
    def process_record(self, record):
        """
        Abstract method to process a consumed record.
        """
        pass

    @abstractmethod
    def finalize(self):
        """
        Abstract method to handle finalization after consuming all records.
        """
        pass








