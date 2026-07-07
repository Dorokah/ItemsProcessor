class RestException(Exception):
    def __init__(self, algorithm_error_result):
        super().__init__(algorithm_error_result['message'])
        self.algo_error_result = algorithm_error_result
