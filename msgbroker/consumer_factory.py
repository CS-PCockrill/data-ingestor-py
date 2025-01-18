class ConsumerFactory:
    """
    Factory for dynamically creating consumers based on interface ID.
    """
    _registry = {}

    @classmethod
    def register_consumer(cls, interface_id, consumer_class):
        """
        Registers a consumer class for a specific interface ID.

        Args:
            interface_id (str): Identifier for the interface.
            consumer_class (type): Class of the consumer to register.
        """
        cls._registry[interface_id] = consumer_class

    @classmethod
    def create_consumer(cls, interface_id, *args, **kwargs):
        """
        Creates a consumer instance based on the registered class.

        Args:
            interface_id (str): Identifier for the interface.

        Returns:
            Consumer: An instance of the registered consumer class.

        Raises:
            ValueError: If the interface ID is not registered.
        """
        if interface_id not in cls._registry:
            raise ValueError(f"Consumer for interface '{interface_id}' not registered.")
        consumer_class = cls._registry[interface_id]
        return consumer_class(*args, **kwargs)
