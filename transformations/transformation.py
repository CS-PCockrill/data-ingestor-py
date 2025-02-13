from abc import ABC, abstractmethod

class Transformation(ABC):
    """Base class for transforming records before passing along to Target in Consumers"""

    def __init__(self, global_context=None):
        self.global_context = global_context

    @abstractmethod
    def transform(self, record):
        """Transforms the record into the correct format or add fields, etc..."""
        pass
