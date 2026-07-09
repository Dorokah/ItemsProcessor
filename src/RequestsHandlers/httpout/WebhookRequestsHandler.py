import json
import requests
import time
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import extract_span_links, link_current_span, trace_message, trace_span, traced_consumer


class WebhookRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.webhook_url = config_provider.get_webhook_url()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        self._handle_message(message)

    def handle_batch(self, producer, consumer, messages):
        start_timestamp = time.time()
        batch_results = []
        if config_provider.get_tracing_enable():
            with trace_span(
                "WebhookRequestsHandler.batch",
                links=extract_span_links(messages),
                attributes={"messaging.batch.message_count": len(messages)}
            ):
                batch_links = [link_current_span({"link.type": "batch"})]
                for message in messages:
                    with trace_message(
                        message,
                        "WebhookRequestsHandler.post_status",
                        extra_links=batch_links
                    ):
                        batch_results.append(self._handle_message(message, log_to_elastic=False))
                self._log_batch_result(messages, batch_results, start_timestamp)
        else:
            for message in messages:
                batch_results.append(self._handle_message(message, log_to_elastic=False))
            self._log_batch_result(messages, batch_results, start_timestamp)

    def _handle_message(self, message, log_to_elastic=True):
        body = message.value()
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()
        try:
            status_payload = json.loads(body)
            pokemon_id = status_payload.get('id', '')
            status = status_payload.get('status', '')
            request_id = f"webhook-{pokemon_id}"

            # Setup logging metadata fields
            self.service_logger.add_field('requestId', request_id)
            self.service_logger.add_field('entityId', pokemon_id)
            self.service_logger.info(f"Received status update for Pokemon ID {pokemon_id}: {status}")

            self.service_logger.info(f"POSTing status to Webhook URL: {self.webhook_url}")
            # Perform POST request
            response = requests.post(self.webhook_url, json=status_payload, timeout=10)

            self.service_logger.info(f"Webhook response status: {response.status_code}")
            if log_to_elastic:
                self.service_logger.log_success_logstash(start_timestamp)
            return {
                "requestId": request_id,
                "entityId": pokemon_id,
                "status": status,
                "webhookStatusCode": response.status_code
            }

        except Exception as e:
            error_message = f"Error forwarding webhook status: {e}"
            if log_to_elastic:
                self.service_logger.log_error(error_message, start_timestamp)
            return {
                "requestId": self.service_logger.get_aggregated_log().get('requestId', 'webhook-unknown'),
                "entityId": self.service_logger.get_aggregated_log().get('entityId', 'unknown'),
                "error": error_message
            }

    def _log_batch_result(self, messages, batch_results, start_timestamp):
        errors = [result for result in batch_results if result.get("error")]
        successful_results = [result for result in batch_results if not result.get("error")]

        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()
        self.service_logger.add_field('requestId', self._build_batch_request_id(messages))
        self.service_logger.add_field('entityId', 'webhook-batch')
        self.service_logger.add_field('batchSize', len(messages))
        self.service_logger.add_field('batchMessageOffsets', [self._message_position(message) for message in messages])
        self.service_logger.add_field('webhookSuccessCount', len(successful_results))
        self.service_logger.add_field('webhookFailureCount', len(errors))
        self.service_logger.add_field('webhookEntityIds', [result.get('entityId') for result in batch_results])
        self.service_logger.add_field(
            'webhookStatusCodes',
            [result.get('webhookStatusCode') for result in successful_results]
        )

        if errors:
            self.service_logger.add_field('webhookBatchErrors', errors)
            self.service_logger.log_error(f"Webhook batch completed with {len(errors)} failures", start_timestamp)
        else:
            self.service_logger.log_success_logstash(start_timestamp)

    @classmethod
    def _build_batch_request_id(cls, messages):
        return "webhook-batch-" + "_".join(cls._message_position(message) for message in messages)

    @staticmethod
    def _message_position(message):
        return f"{message.topic()}-{message.partition()}-{message.offset()}"

    # ------ Placeholders to match base class abstract methods ------ #
    def perform_actions(self, body, start_timestamp):
        pass

    def get_publish_queue(self):
        return None

    def get_post_data(self, request, **kwargs):
        pass

    def process_result(self, response):
        pass

    def request_pre_processing(self, body):
        pass
