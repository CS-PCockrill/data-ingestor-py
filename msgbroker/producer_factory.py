class ProducerFactory:
    """
    Factory for dynamically creating producers based on interface ID.
    """
    _registry = {}

    @classmethod
    def register_producer(cls, interface_id, producer_class):
        """
        Registers a producer class for a specific interface ID.

        Args:
            interface_id (str): Identifier for the interface.
            producer_class (type): Class of the producer to register.
        """
        cls._registry[interface_id] = producer_class

    @classmethod
    def create_producer(cls, interface_id, *args, **kwargs):
        """
        Creates a producer instance based on the registered class.

        Args:
            interface_id (str): Identifier for the interface.

        Returns:
            Producer: An instance of the registered producer class.

        Raises:
            ValueError: If the interface ID is not registered.
        """
        if interface_id not in cls._registry:
            raise ValueError(f"Producer for interface '{interface_id}' not registered.")
        producer_class = cls._registry[interface_id]
        return producer_class(*args, **kwargs)
