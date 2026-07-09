import abc
import time
import traceback
from http import HTTPStatus
from src.utils import config_provider, result_builder
from src.utils.exceptions import RestException
from src.utils.tracer import inject_trace_headers, trace_span, traced_consumer
from src.utils.service_logger import ServiceLogger


class RequestHandler(abc.ABC):
    def __init__(self, service_logger: ServiceLogger):
        self.service_logger = service_logger
        self.is_results_logging_enabled = config_provider.get_enable_results_logging()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        body = message.value()
        try:
            result_body = self.perform_actions(body, start_timestamp)
        except RestException as service_exception:
            self.service_logger.log_error(str(service_exception), start_timestamp)
            result_body = result_builder.create_error_result(self.service_logger.get_aggregated_log())
        except Exception as exception:
            self.service_logger.log_error(str(exception),
                                          start_timestamp,
                                          traceback=str(traceback.format_exc()))
            result_body = result_builder.create_error_result(self.service_logger.get_aggregated_log())

        publish_topic = self.get_publish_queue()
        if publish_topic:
            self.service_logger.info(f"Sending to topic: {publish_topic}")
            self.publish_and_ack(producer, publish_topic, consumer, message, result_body)

    def perform_actions(self, body, request_start_timestamp):
        request = self.request_pre_processing(body)
        result = self.perform_service_call(request)
        self.service_logger.log_success_logstash(request_start_timestamp)
        return result_builder.create_success_result(result, self.service_logger.get_aggregated_log())

    def perform_service_call(self, request):
        with trace_span("RequestHandler.perform_service_call"):
            post_start_timestamp = time.time()
            response = {}
            post_duration = time.time() - post_start_timestamp
            self.service_logger.log_post_duration(post_duration)
            result = self.process_result(response)
            if self.is_results_logging_enabled:
                self.service_logger.log_results(result['results'])
            return result

    def publish_and_ack(self, producer, topic_name, consumer, message, result_body):
        val_bytes = result_body.encode('utf-8') if isinstance(result_body, str) else result_body
        key_bytes = message.key()

        producer.produce(topic=topic_name, value=val_bytes, key=key_bytes, headers=inject_trace_headers(message.headers()))
        producer.poll(0)

    # ----- Optional Implementations -----#

    def get_publish_queue(self):
        raise NotImplementedError()

    def request_pre_processing(self, body):
        raise NotImplementedError()

    def get_post_data(self, request, **kwargs):
        raise NotImplementedError()

    def process_result(self, response):
        raise NotImplementedError()

    # ----- Static methods -----#

    @staticmethod
    def validate_response(status_code, result):
        if status_code != HTTPStatus.OK:
            raise RestException(result)

