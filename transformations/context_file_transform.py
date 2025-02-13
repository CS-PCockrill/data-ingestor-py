from transformations.transformation import Transformation

class ContextFileTransform(Transformation):

    def __init__(self, global_context=None):
        super().__init__(global_context)
        self.global_context = global_context

    def transform(self, record):
        record['context_id'] = self.global_context.get('context_id')
        record['filename'] = self.global_context.get('filename')
        return record