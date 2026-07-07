import json
import time
import traceback
import requests
from opentracing_instrumentation import traced_function
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import result_builder
from src.utils.tracer import traced_consumer
from src.utils.adapter_logger import AlgorithmAdapterLogger


# noinspection DuplicatedCode
class HttpOutRequestsHandler(RequestHandler):

    def __init__(self, algorithm_adapter_logger: AlgorithmAdapterLogger):
        super().__init__(algorithm_adapter_logger)

    @traced_consumer
    def handle_algo_request(self, connection, ch, method, properties, body):
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
        algo_result = json.loads(body)
        result_url = properties.headers['X-Result-Url']
        self.log_received_httpout_request(algo_result)
        self.adapter_logger.add_field('resultUrl', result_url)
        self.adapter_logger.log_trace_id()
        self.send_result(result_url, self.get_result(algo_result))
        self.adapter_logger.log_success_logstash(start_timestamp)

    def log_received_httpout_request(self, algo_result):
        self.adapter_logger.add_field('algorithmName', algo_result['algorithmName'])
        self.adapter_logger.add_field('entityId', algo_result['entityId'])
        self.adapter_logger.add_field('requestId', algo_result['requestId'])
        if 'imageFullUrl' in algo_result:
            self.adapter_logger.add_field('imageFullUrl', algo_result['imageFullUrl'])
        self.adapter_logger.send_to_logstash("Algo Result pulled")

    @staticmethod
    def get_result(algo_result):
        # todo: this is hardcoded only because of lego team
        if 'algorithmName' in algo_result:
            algorithm_name = algo_result['algorithmName'].lower()
            if algorithm_name == "pipeline":
                return {}
        raise Exception("Got unknown algorithm result")

    @traced_function
    def send_result(self, result_url, algo_result):
        result_post_start_timestamp = time.time()
        response = requests.post(result_url, json=algo_result, timeout=self.algo_timeout)
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

    def get_algorithm_post_data(self, algo_request, **kwargs):
        pass

    def process_algorithm_result(self, algorithm_response):
        pass

    def request_pre_processing(self, body):
        pass
