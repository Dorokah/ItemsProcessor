import json
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import (
    inject_text_map_headers,
    inject_trace_headers,
    traced_consumer,
    start_item_span,
    get_message_json_trace_id,
)


class SurnameEnrichmentRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.surname_api_url = config_provider.get_surname_api_url().rstrip("/")
        self.publish_topic = config_provider.get_publish_queue_name()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        self.handle_batch(producer, consumer, [message])

    def handle_batch(self, producer, consumer, messages):
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()
        successful_ids = []
        failed_records = []

        self.service_logger.add_field("requestId", f"surname-enrichment-batch-{int(start_timestamp * 1000)}")
        self.service_logger.add_field("batchSize", len(messages))
        self.service_logger.info(f"Enriching surname Kafka batch with {len(messages)} messages")

        for message in messages:
            pokemon_id = "unknown"
            try:
                surname_event = json.loads(message.value())
                pokemon_id = str(surname_event.get("id", ""))
                surname = str(surname_event.get("surname", "")).strip()

                if not pokemon_id:
                    raise ValueError("Missing 'id' in surname event")
                if not surname:
                    raise ValueError("Missing 'surname' in surname event")

                json_trace_id = get_message_json_trace_id(message)
                with start_item_span(
                    "enrich_pokemon_surname_item",
                    pokemon_id,
                    json_trace_id=json_trace_id,
                    message=message,
                ):
                    api_url = f"{self.surname_api_url}/translate?{urlencode({'surname': surname})}"
                    api_request = UrlRequest(
                        api_url,
                        headers=inject_text_map_headers(
                            item_id=pokemon_id,
                            json_trace_id=json_trace_id,
                        ),
                    )
                    try:
                        with urlopen(api_request, timeout=10) as response:
                            api_result = json.loads(response.read().decode("utf-8"))
                    except HTTPError as exc:
                        raise RuntimeError(exc.read().decode("utf-8")) from exc
                    french_name = api_result["frenchName"]

                    enriched_event = {
                        **surname_event,
                        "frenchName": french_name,
                    }

                    producer.produce(
                        topic=self.publish_topic,
                        key=pokemon_id.encode("utf-8"),
                        value=json.dumps(enriched_event).encode("utf-8"),
                        headers=inject_trace_headers(
                            item_id=pokemon_id,
                            json_trace_id=json_trace_id,
                        ),
                    )
                    producer.poll(0)
                successful_ids.append(pokemon_id)

            except Exception as exc:
                failed_records.append((pokemon_id, str(exc)))

        self.service_logger.add_field("kafkaProducedCount", len(successful_ids))
        self.service_logger.add_field("entityIds", successful_ids)
        if failed_records:
            self.service_logger.add_field("failureCount", len(failed_records))
            self.service_logger.add_field("failureEntityIds", [pokemon_id for pokemon_id, _ in failed_records])
            self.service_logger.add_field("failureMessages", [error for _, error in failed_records])
            self.service_logger.log_error(
                f"Surname enrichment batch had {len(failed_records)} failed messages",
                start_timestamp,
            )
        else:
            self.service_logger.info(
                f"Published {len(successful_ids)} enriched surname messages to Kafka topic {self.publish_topic}"
            )
            self.service_logger.log_success_logstash(start_timestamp)

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
