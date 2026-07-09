import json
import time

import requests

from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import inject_trace_headers, traced_consumer


class SurnameEnrichmentRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.surname_api_url = config_provider.get_surname_api_url().rstrip("/")
        self.publish_topic = config_provider.get_publish_queue_name()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        body = message.value()
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()

        try:
            surname_event = json.loads(body)
            pokemon_id = str(surname_event.get("id", ""))
            surname = str(surname_event.get("surname", "")).strip()

            if not pokemon_id:
                raise ValueError("Missing 'id' in surname event")
            if not surname:
                raise ValueError("Missing 'surname' in surname event")

            self.service_logger.add_field("requestId", f"surname-enrichment-{pokemon_id}")
            self.service_logger.add_field("entityId", pokemon_id)
            self.service_logger.add_field("surname", surname)
            self.service_logger.info(
                f"Requesting French surname for Pokemon ID {pokemon_id}: {surname}"
            )

            response = requests.get(
                f"{self.surname_api_url}/translate",
                params={"surname": surname},
                timeout=10,
            )
            response.raise_for_status()
            api_result = response.json()
            french_name = api_result["frenchName"]

            enriched_event = {
                **surname_event,
                "frenchName": french_name,
            }

            self.service_logger.add_field("frenchName", french_name)
            self.service_logger.info(
                f"Publishing enriched surname for Pokemon ID {pokemon_id}: {french_name}"
            )

            producer.produce(
                topic=self.publish_topic,
                key=pokemon_id.encode("utf-8"),
                value=json.dumps(enriched_event).encode("utf-8"),
                headers=inject_trace_headers(message.headers()),
            )
            producer.poll(0)

            self.service_logger.log_success_logstash(start_timestamp)

        except Exception as exc:
            self.service_logger.log_error(str(exc), start_timestamp)

    def perform_actions(self, body, start_timestamp):
        pass

    def get_publish_queue(self):
        return self.publish_topic

    def get_post_data(self, request, **kwargs):
        pass

    def process_result(self, response):
        pass

    def request_pre_processing(self, body):
        pass
