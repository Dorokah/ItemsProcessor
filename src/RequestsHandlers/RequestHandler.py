import abc
import functools
import time
import traceback
from http import HTTPStatus
import requests
from opentracing import tags, Format, global_tracer
from opentracing_instrumentation import traced_function
from src.utils import config_provider, result_builder
from src.utils.exceptions import RestException
from src.utils.tracer import traced_consumer, get_trace_id, get_span_id
from src.utils.adapter_logger import AlgorithmAdapterLogger


class RequestHandler(abc.ABC):
    def __init__(self, adapter_logger: AlgorithmAdapterLogger):
        self.adapter_logger = adapter_logger
        self.is_results_logging_enabled = config_provider.get_enable_results_logging()
        self.is_first_service = config_provider.get_is_first_service()
        self.tracer = global_tracer()

    @traced_consumer
    def handle_algo_request(self, producer, consumer, message):
        start_timestamp = time.time()
        self.adapter_logger.reset_aggregated_log()
        body = message.value()
        try:
            result_body = self.perform_adapter_actions(body, start_timestamp)
        except RestException as algorithm_exception:
            self.adapter_logger.log_error(str(algorithm_exception), start_timestamp)
            result_body = result_builder.create_error_result(self.adapter_logger.get_aggregated_log())
        except Exception as exception:
            self.adapter_logger.log_error(str(exception),
                                          start_timestamp,
                                          traceback=str(traceback.format_exc()))
            result_body = result_builder.create_error_result(self.adapter_logger.get_aggregated_log())
        
        publish_topic = self.get_publish_queue()
        if publish_topic:
            self.adapter_logger.info(f"Sending to topic: {publish_topic}")
            self.publish_and_ack(producer, publish_topic, consumer, message, result_body)

    def perform_adapter_actions(self, body, request_start_timestamp):
        request, image_bytes = self.request_pre_processing(body)
        algorithm_result = self.perform_algo_call(request, image_bytes)
        self.adapter_logger.log_success_logstash(request_start_timestamp)
        return result_builder.create_success_result(algorithm_result, self.adapter_logger.get_aggregated_log())

    @traced_function
    def perform_algo_call(self, algo_request, image_bytes):
        algorithm_post_start_timestamp = time.time()
        algorithm_response = {'boundingBox': ''}
        algorithm_post_duration = time.time() - algorithm_post_start_timestamp
        self.adapter_logger.log_post_duration(algorithm_post_duration)
        algo_result = self.process_algorithm_result(algorithm_response)
        if self.is_results_logging_enabled:
            self.adapter_logger.log_results(algo_result['results'])
        return algo_result

    def publish_and_ack(self, producer, topic_name, consumer, message, result_body):
        headers = message.headers() or []
        if self.is_first_service:
            headers_dict = {k: v.decode('utf-8') if isinstance(v, bytes) else v for k, v in headers}
            self.tracer.inject(self.tracer.active_span.context, Format.TEXT_MAP, headers_dict)
            headers = [(k, str(v).encode('utf-8')) for k, v in headers_dict.items()]

        val_bytes = result_body.encode('utf-8') if isinstance(result_body, str) else result_body
        key_bytes = message.key()

        producer.produce(topic=topic_name, value=val_bytes, key=key_bytes, headers=headers)
        producer.poll(0)

    # ----- Optional Implementations -----#

    def get_publish_queue(self):
        raise NotImplementedError()

    def request_pre_processing(self, body):
        raise NotImplementedError()

    def get_algorithm_post_data(self, algo_request, **kwargs):
        raise NotImplementedError()

    def process_algorithm_result(self, algorithm_response):
        raise NotImplementedError()

    # ----- Static methods -----#

    @staticmethod
    def validate_algorithm_response(status_code, algorithm_result):
        if status_code != HTTPStatus.OK:
            raise RestException(algorithm_result)

    @staticmethod
    def validate_mandatory_fields(algorithm_request):
        if 'imageUrl' not in algorithm_request:
            raise Exception("Request does not have entityId or imageFullUrl field.")
