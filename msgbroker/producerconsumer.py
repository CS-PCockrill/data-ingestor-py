from abc import ABC, abstractmethod
from queue import Queue

from fileprocesser.fileprocessor import FileProcessor


class Producer(ABC):
    @abstractmethod
    def produce(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

class Consumer(ABC):
    @abstractmethod
    def consume(self):
        pass

    @abstractmethod
    def signal_done(self):
        pass


class QueueProducer:
    def __init__(self, maxsize=1000):
        self.queue = Queue(maxsize=maxsize)
        self.file_path = None
        self.file_type = None
        self.schema_tag = None

    def set_source(self, file_path, file_type, schema_tag):
        self.file_path = file_path
        self.file_type = file_type
        self.schema_tag = schema_tag

    def produce_from_source(self):
        if not self.file_path or not self.file_type or not self.schema_tag:
            raise ValueError("Source not set for QueueProducer")
        for record in FileProcessor._process_file(self.file_path, self.schema_tag, self.file_type):
            self.produce(record)

    def produce(self, record):
        self.queue.put(record)

    def consume(self):
        return self.queue.get()

    def signal_done(self):
        self.queue.put(None)

    def close(self):
        while not self.queue.empty():
            self.queue.get()
            self.queue.task_done()


class QueueConsumer(Consumer):
    def __init__(self, producer):
        self.queue = producer.queue

    def consume(self):
        return self.queue.get()

    def signal_done(self):
        self.queue.put(None)
