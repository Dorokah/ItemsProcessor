import json
import time
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import traced_consumer, inject_trace_headers


class SplitRequestsHandler(RequestHandler):
    def __init__(self, adapter_logger):
        super().__init__(adapter_logger)
        self.publish_topic = config_provider.get_publish_queue_name()

    @traced_consumer
    def handle_algo_request(self, producer, consumer, message):
        body = message.value()
        start_timestamp = time.time()
        self.adapter_logger.reset_aggregated_log()
        try:
            pokedex = json.loads(body)
            if not isinstance(pokedex, list):
                self.adapter_logger.info("Incoming pokedex payload is not a JSON list!")
                return
            
            total_pokemon = len(pokedex)
            self.adapter_logger.info(f"Splitting pokedex of size {total_pokemon} into individual messages")
            for idx, pokemon in enumerate(pokedex):
                pokemon_str = json.dumps(pokemon)
                key_str = str(pokemon.get('id', ''))
                
                producer.produce(
                    topic=self.publish_topic,
                    value=pokemon_str.encode('utf-8'),
                    key=key_str.encode('utf-8'),
                    headers=inject_trace_headers(message.headers())
                )
                if idx % 100 == 0:
                    producer.poll(0)

            producer.flush()
            self.adapter_logger.log_success_logstash(start_timestamp)
            
        except Exception as e:
            self.adapter_logger.log_error(str(e), start_timestamp)
