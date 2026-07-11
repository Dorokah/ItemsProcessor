import json
import time
import uuid
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import traced_consumer, inject_trace_headers, start_item_span, get_active_trace_id, now_ms


class SplitRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.publish_topic = config_provider.get_publish_queue_name()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        self.handle_batch(producer, consumer, [message])

    def handle_batch(self, producer, consumer, messages):
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()
        produced_count = 0
        input_count = len(messages)

        try:
            splitter_id = f"splitter-{uuid.uuid4().hex[:12]}"
            self.service_logger.add_field('requestId', f"split-batch-{int(start_timestamp * 1000)}")
            self.service_logger.add_field('splitterId', splitter_id)
            self.service_logger.add_field('batchSize', input_count)
            self.service_logger.info(f"Splitting Kafka batch with {input_count} pokedex payloads")

            for message in messages:
                json_trace_id = get_active_trace_id()
                if json_trace_id:
                    self.service_logger.add_field('jsonTraceId', json_trace_id)
                pokedex = json.loads(message.value())
                if not isinstance(pokedex, list):
                    raise ValueError("Incoming pokedex payload is not a JSON list")

                for pokemon in pokedex:
                    pokemon_str = json.dumps(pokemon)
                    key_str = str(pokemon.get('id', ''))
                    split_ts_ms = now_ms()
                    with start_item_span(
                        "split_pokemon_item",
                        key_str,
                        splitter_id=splitter_id,
                        json_trace_id=json_trace_id,
                        new_trace=True,
                        split_ts_ms=split_ts_ms,
                    ):
                        producer.produce(
                            topic=self.publish_topic,
                            value=pokemon_str.encode('utf-8'),
                            key=key_str.encode('utf-8'),
                            headers=inject_trace_headers(
                                item_id=key_str,
                                splitter_id=splitter_id,
                                json_trace_id=json_trace_id,
                                split_ts_ms=split_ts_ms,
                            )
                        )
                    produced_count += 1
                    if produced_count % 100 == 0:
                        producer.poll(0)

            self.service_logger.add_field('kafkaProducedCount', produced_count)
            self.service_logger.info(f"Produced {produced_count} Pokemon messages to Kafka topic {self.publish_topic}")
            self.service_logger.log_success_logstash(start_timestamp)

        except Exception as e:
            self.service_logger.add_field('kafkaProducedCount', produced_count)
            self.service_logger.log_error(str(e), start_timestamp)
