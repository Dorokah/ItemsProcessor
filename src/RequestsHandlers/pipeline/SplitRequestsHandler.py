import json
import time
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import (
    extract_span_links,
    inject_trace_headers,
    link_current_span,
    trace_message,
    trace_span,
    traced_consumer,
)


class SplitRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.publish_topic = config_provider.get_publish_queue_name()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        self._split_message(producer, message)

    def handle_batch(self, producer, consumer, messages):
        if config_provider.get_tracing_enable():
            with trace_span(
                "SplitRequestsHandler.batch",
                links=extract_span_links(messages),
                attributes={"messaging.batch.message_count": len(messages)}
            ):
                batch_links = [link_current_span({"link.type": "batch"})]
                for message in messages:
                    self._split_message(producer, message, batch_links)
        else:
            for message in messages:
                self._split_message(producer, message)

    def _split_message(self, producer, message, batch_links=None):
        body = message.value()
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()
        self.service_logger.add_field('requestId', self._build_request_id(message))
        try:
            pokedex = json.loads(body)
            if not isinstance(pokedex, list):
                self.service_logger.info("Incoming pokedex payload is not a JSON list!")
                return

            total_pokemon = len(pokedex)
            self.service_logger.add_field('entityId', 'pokedex')
            self.service_logger.add_field('splitCount', total_pokemon)
            self.service_logger.info(f"Splitting pokedex of size {total_pokemon} into individual messages")
            for idx, pokemon in enumerate(pokedex):
                pokemon_str = json.dumps(pokemon)
                key_str = str(pokemon.get('id', ''))

                with trace_message(
                    message,
                    "SplitRequestsHandler.split_item",
                    extra_links=batch_links,
                    attributes={"pokemon.id": key_str, "split.index": idx}
                ):
                    producer.produce(
                        topic=self.publish_topic,
                        value=pokemon_str.encode('utf-8'),
                        key=key_str.encode('utf-8'),
                        headers=inject_trace_headers(message.headers())
                    )
                if idx % 100 == 0:
                    producer.poll(0)

            producer.flush()
            self.service_logger.log_success_logstash(start_timestamp)

        except Exception as e:
            self.service_logger.log_error(str(e), start_timestamp)

    @staticmethod
    def _build_request_id(message):
        return f"splitter-{message.topic()}-{message.partition()}-{message.offset()}"
