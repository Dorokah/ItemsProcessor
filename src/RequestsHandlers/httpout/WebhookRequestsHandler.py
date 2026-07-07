import json
import requests
import time
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import traced_consumer


class WebhookRequestsHandler(RequestHandler):
    def __init__(self, adapter_logger):
        super().__init__(adapter_logger)
        self.webhook_url = config_provider.get_webhook_url()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        body = message.value()
        start_timestamp = time.time()
        self.adapter_logger.reset_aggregated_log()
        self.adapter_logger.log_trace_id()
        try:
            status_payload = json.loads(body)
            pokemon_id = status_payload.get('id', '')
            status = status_payload.get('status', '')

            # Setup logging metadata fields
            self.adapter_logger.add_field('requestId', f"webhook-{pokemon_id}")
            self.adapter_logger.add_field('entityId', pokemon_id)
            self.adapter_logger.info(f"Received status update for Pokemon ID {pokemon_id}: {status}")

            self.adapter_logger.info(f"POSTing status to Webhook URL: {self.webhook_url}")
            # Perform POST request
            response = requests.post(self.webhook_url, json=status_payload, timeout=10)

            self.adapter_logger.info(f"Webhook response status: {response.status_code}")
            self.adapter_logger.log_success_logstash(start_timestamp)

        except Exception as e:
            self.adapter_logger.logger.error(f"Error forwarding webhook status: {e}")

    # ------ Placeholders to match base class abstract methods ------ #
    def perform_adapter_actions(self, body, start_timestamp):
        pass

    def get_publish_queue(self):
        return None

    def get_post_data(self, request, **kwargs):
        pass

    def process_result(self, response):
        pass

    def request_pre_processing(self, body):
        pass
