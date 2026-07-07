import json
import statistics
from opentracing_instrumentation import traced_function
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider, image_provider
from src.utils.adapter_logger import AdapterLogger


class GenericRequestsHandler(RequestHandler):

    def __init__(self, adapter_logger: AdapterLogger):
        super().__init__(adapter_logger)
        self.pipeline_name = config_provider.get_pipeline_name()
        self.next_queue_name = config_provider.get_next_service_name()
        self.results_queue_name = config_provider.get_publish_queue_name()

    @traced_function
    def request_pre_processing(self, body):
        request = json.loads(body)
        self.adapter_logger.log_trace_id()
        self.validate_mandatory_fields(request)
        self.adapter_logger.log_received_request(request)
        request, image_bytes, image_metadata = image_provider.get_image_and_metadata(request)
        self.adapter_logger.log_image_metadata(image_metadata)
        self.adapter_logger.log_get_image_duration(request['getImageDuration'])
        request = self.extract_request_optionals(request)
        return request, image_bytes

    def process_result(self, response):
        result = json.loads(response.content)
        self.validate_response(response.status_code, result)
        result['statusType'] = 'success'
        return result

    def get_post_data(self, request, **kwargs):
        run_config = request['runConfig']
        return {'requestId': request['requestId'],
                **run_config}

    def get_publish_queue(self):
        boxes_amount = self.adapter_logger.get_boxes_amount()
        if boxes_amount == 0:
            return self.results_queue_name
        return self.next_queue_name
