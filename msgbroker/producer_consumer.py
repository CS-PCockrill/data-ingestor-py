from abc import ABC, abstractmethod

class Producer(ABC):
    def __init__(self):
        self.artifact_name = None  # Common field for all producers

    @abstractmethod
    def produce(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

class Consumer(ABC):
    def __init__(self, producer):
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







