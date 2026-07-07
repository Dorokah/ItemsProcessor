import logging
import time
from opentracing import global_tracer
from src.utils import config_provider, logstash_logger


class AdapterLogger:
    def __init__(self):
        logging.basicConfig(format=f'%(asctime)s | %(levelname)s: %(message)s', datefmt='[%I:%M:%S]')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.service_config = self._get_service_config()
        self.aggregated_log = {}
        self.logstash_logger = logstash_logger.LogstashLogger(logging,
                                                              self.service_config.get('logstashEnable', False),
                                                              self.service_config.get('logstashHost', ''),
                                                              self.service_config.get('logstashPort', 0),
                                                              self.service_config.get('elasticIndex', ''))
        self.tracer = global_tracer()

    @staticmethod
    def _get_service_config():
        config = {
            'kafkaBootstrapServers': config_provider.get_kafka_bootstrap_servers(),
            'kafkaGroupId': config_provider.get_kafka_group_id(),
            'serviceName': config_provider.get_service_name(),
            'logstashHost': config_provider.get_logstash_host(),
            'logstashPort': config_provider.get_logstash_port(),
            'elasticIndex': config_provider.get_elastic_index_name(),
            'serviceVersion': config_provider.get_service_version(),
            'consumeQueue': config_provider.get_consume_queue_name(),
            'isFirstService': config_provider.get_is_first_service(),
            'pipelineName': config_provider.get_pipeline_name(),
            'httpOutQueue': config_provider.get_results_queue_name(),
            'logstashEnable': config_provider.get_logstash_enable(),
            'tracingEnable': config_provider.get_tracing_enable(),
            'moduleName': f"{config_provider.get_request_handler_class_name()['moduleName']}",
            'className': f"{config_provider.get_request_handler_class_name()['className']}",
            'serviceTimeout': config_provider.get_service_post_timeout()
        }
        return {k: v for k, v in config.items() if v != ''}

    def log_service_config(self):
        self.logger.info("Service config:\n" + "\n".join("{}: {}".format(k, v) for k, v in self.service_config.items()))

    def reset_aggregated_log(self):
        self.aggregated_log = {'serviceName': self.service_config.get('serviceName', 'Undefined'),
                               'serviceVersion': self.service_config.get('serviceVersion', 'Undefined'),
                               'pipelineName': self.service_config.get('pipelineName', 'Pipeline'),
                               'requestId': 'n/a',
                               'entityId': 'n/a'}

    def log_received_request(self, request, message="Received request"):
        self.aggregated_log['requestId'] = request['requestId']
        if 'entityId' in request:
            self.aggregated_log['entityId'] = request['entityId']
        if 'imageFullUrl' in request:
            self.aggregated_log['imageFullUrl'] = request['imageFullUrl']
            logging.info(f"requestId: {request['requestId']} imageFullUrl: {request['imageFullUrl']}")
        logging.info(f"{message} requestId: {self.aggregated_log['requestId']}")
        self.logstash_logger.log(message, self.aggregated_log)

    def send_to_logstash(self, message=""):
        self.logstash_logger.log(message, self.aggregated_log)

    def log_image_metadata(self, image_metadata):
        self.aggregated_log['imageResolution'] = f"{image_metadata['imageWidth']}x{image_metadata['imageHeight']}"
        self.aggregated_log = {**self.aggregated_log, **image_metadata}

    def log_get_image_duration(self, get_image_duration):
        self.aggregated_log['getImageDuration'] = get_image_duration

    def log_boxes_amount(self, boxes_amount):
        self.aggregated_log['boundingBoxesAmount'] = boxes_amount

    def get_boxes_amount(self):
        if 'boundingBoxesAmount' in self.aggregated_log:
            return self.aggregated_log['boundingBoxesAmount']
        return 0

    def log_post_duration(self, post_duration):
        self.aggregated_log['servicePostDuration'] = post_duration

    def log_threshold(self, threshold):
        self.aggregated_log['serviceThreshold'] = threshold

    def log_common_response_data(self, response):
        self.aggregated_log['gpuId'] = int(response.headers['Gpu-Id'])
        self.aggregated_log['serviceDuration'] = float(response.headers['Service-Duration'])
        self.aggregated_log['serviceFullDuration'] = float(response.headers['Service-Full-Duration'])

    def log_batch_data(self, response):
        if 'Service-Batch-Duration' in response.headers:
            self.aggregated_log['serviceBatchDuration'] = float(response.headers['Service-Batch-Duration'])
        if 'Batch-Size' in response.headers:
            self.aggregated_log['batchSize'] = int(response.headers['Batch-Size'])

    def log_trace_id(self):
        span = self.tracer.active_span
        if hasattr(span, 'trace_id'):
            trace_id = format(span.trace_id, 'x')
            self.aggregated_log['traceId'] = trace_id
            return trace_id
        return "No_trace_ID"

    def is_request_unsuccessful(self):
        return self.aggregated_log['statusType'] == 'processingError'

    def log_results(self, results):
        self.aggregated_log['results'] = results

    def log_sigterm_received(self):
        self.logger.info("Sigterm Received")

    def log_detection_average_score(self, boxes, score_field_name, average_field_name):
        bounding_boxes_amount = len(boxes)
        if bounding_boxes_amount != 0:
            score_sum = 0.0
            for box in boxes:
                score_sum += box[score_field_name]
            self.aggregated_log[average_field_name] = score_sum / bounding_boxes_amount

    def log_success_logstash(self, request_start_timestamp):
        success_msg = "Successfully processed"
        self.logger.info(f"{success_msg} requestId: {self.aggregated_log['requestId']}")
        self.aggregated_log['statusType'] = 'processingSuccess'
        self.aggregated_log['adapterDuration'] = time.time() - request_start_timestamp
        if self.service_config.get('tracingEnable', False) and self.service_config.get('tracingResultsLogsEnable', False):
            self.tracer.active_span.log_kv({**self.aggregated_log, 'message': success_msg})
        self.logstash_logger.log(success_msg, self.aggregated_log)

    def log_error(self, exception_message, request_start_timestamp, traceback=None):
        self.logger.error(f"processingError requestId: {self.aggregated_log['requestId']}")
        self.logger.error(exception_message)
        self.aggregated_log['statusType'] = 'processingError'
        self.aggregated_log['exception'] = exception_message
        if traceback:
            self.logger.error(traceback)
            self.aggregated_log['adapterErrorTraceback'] = traceback
        self.aggregated_log['adapterDuration'] = time.time() - float(request_start_timestamp)
        if self.service_config.get('tracingEnable', False):
            self.tracer.active_span.log_kv({**self.aggregated_log, 'message': exception_message})
        self.logstash_logger.log(exception_message, self.aggregated_log, False)

    def get_aggregated_log(self):
        return self.aggregated_log

    def info(self, message):
        self.logger.info(message)

    def add_field(self, field_name, value):
        self.aggregated_log[field_name] = value

    def set_logstash_handler(self):
        self.logstash_logger.set_logstash_handler()
