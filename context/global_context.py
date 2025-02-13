
class GlobalContext:
    def __init__(self, context_id=None, filename=None):
        self.context_id = context_id
        self.filename = filename

    def set(self, key, value):
        """Dynamically set a new key-value pair"""
        setattr(self, key, value)
        
    def get(self, key, default=None):
        """Dynamically retrieve a key-value pair"""
        return getattr(self, key, default)