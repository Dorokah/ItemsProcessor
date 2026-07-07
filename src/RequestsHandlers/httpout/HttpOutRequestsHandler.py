import json
import time
import traceback
import requests
from opentracing_instrumentation import traced_function
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import result_builder
from src.utils.tracer import traced_consumer
from src.utils.adapter_logger import AdapterLogger


# noinspection DuplicatedCode
class HttpOutRequestsHandler(RequestHandler):

    def __init__(self, adapter_logger: AdapterLogger):
        super().__init__(adapter_logger)

    @traced_consumer
    def handle_request(self, connection, ch, method, properties, body):
        start_timestamp = time.time()
        self.adapter_logger.reset_aggregated_log()
        try:
            self.send_results(body, properties, start_timestamp)
        except Exception as exception:
            self.adapter_logger.log_error(str(exception),
                                          start_timestamp,
                                          traceback=str(traceback.format_exc()))
        self.ack(ch, connection, method)

    def send_results(self, body, properties, start_timestamp):
        result = json.loads(body)
        result_url = properties.headers['X-Result-Url']
        self.log_received_httpout_request(result)
        self.adapter_logger.add_field('resultUrl', result_url)
        self.adapter_logger.log_trace_id()
        self.send_result(result_url, self.get_result(result))
        self.adapter_logger.log_success_logstash(start_timestamp)

    def log_received_httpout_request(self, result):
        self.adapter_logger.add_field('pipelineName', result['pipelineName'])
        self.adapter_logger.add_field('entityId', result['entityId'])
        self.adapter_logger.add_field('requestId', result['requestId'])
        if 'imageFullUrl' in result:
            self.adapter_logger.add_field('imageFullUrl', result['imageFullUrl'])
        self.adapter_logger.send_to_logstash("Result pulled")

    @staticmethod
    def get_result(result):
        # todo: this is hardcoded only because of lego team
        if 'pipelineName' in result:
            pipeline_name = result['pipelineName'].lower()
            if pipeline_name == "pipeline":
                return {}
        raise Exception("Got unknown result")

    @traced_function
    def send_result(self, result_url, result):
        result_post_start_timestamp = time.time()
        response = requests.post(result_url, json=result, timeout=self.service_timeout)
        result_post_duration = time.time() - result_post_start_timestamp
        status_code = str(response.status_code)
        self.adapter_logger.info(f"Status Code: {status_code}")
        self.adapter_logger.add_field('receivedStatusCode', status_code)
        self.adapter_logger.add_field('resultPostDuration', result_post_duration)

    # ------ Placeholders to match base class abstract methods ------ #
    def perform_adapter_actions(self, body, start_timestamp):
        pass

    def get_publish_queue(self):
        pass

    def get_post_data(self, request, **kwargs):
        pass

    def process_result(self, response):
        pass

    def request_pre_processing(self, body):
        pass
