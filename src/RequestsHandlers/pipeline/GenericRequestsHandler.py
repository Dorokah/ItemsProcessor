import json
import statistics
from opentracing_instrumentation import traced_function
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider, image_provider
from src.utils.adapter_logger import AlgorithmAdapterLogger


class GenericRequestsHandler(RequestHandler):

    def __init__(self, algorithm_adapter_logger: AlgorithmAdapterLogger):
        super().__init__(algorithm_adapter_logger)
        self.pipeline_name = config_provider.get_pipeline_name()
        self.next_queue_name = config_provider.get_next_service_name()
        self.results_queue_name = config_provider.get_publish_queue_name()

    @traced_function
    def request_pre_processing(self, body):
        algorithm_request = json.loads(body)
        self.adapter_logger.log_trace_id()
        self.validate_mandatory_fields(algorithm_request)
        self.adapter_logger.log_received_request(algorithm_request)
        algorithm_request, image_bytes, image_metadata = image_provider.get_image_and_metadata(algorithm_request)
        self.adapter_logger.log_image_metadata(image_metadata)
        self.adapter_logger.log_get_image_duration(algorithm_request['getImageDuration'])
        algorithm_request = self.extract_algo_request_optionals(algorithm_request)
        return algorithm_request, image_bytes

    def process_algorithm_result(self, algorithm_response):
        algorithm_result = json.loads(algorithm_response.content)
        self.validate_algorithm_response(algorithm_response.status_code, algorithm_result)
        algorithm_result['statusType'] = 'success'
        return algorithm_result

    def get_algorithm_post_data(self, algo_request, **kwargs):
        run_config = algo_request['runConfig']
        return {'requestId': algo_request['requestId'],
                **run_config}

    def get_publish_queue(self):
        boxes_amount = self.adapter_logger.get_boxes_amount()
        if boxes_amount == 0:
            return self.results_queue_name
        return self.next_queue_name
