class RestException(Exception):
    def __init__(self, error_result):
        super().__init__(error_result['message'])
        self.error_result = error_result
