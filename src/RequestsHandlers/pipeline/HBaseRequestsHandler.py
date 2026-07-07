import happybase
import json
import time
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import traced_consumer, inject_trace_headers


class HBaseRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.hbase_host = config_provider.get_hbase_host()
        self.hbase_port = config_provider.get_hbase_port()
        self.table_name = config_provider.get_hbase_table_name()
        self.publish_topic = config_provider.get_publish_queue_name()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        body = message.value()
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()
        try:
            pokemon = json.loads(body)
            pokemon_id = str(pokemon.get('id', ''))
            if not pokemon_id:
                raise Exception("Missing 'id' in Pokemon record")

            self.service_logger.add_field('requestId', f"hbase-{pokemon_id}")
            self.service_logger.add_field('entityId', pokemon_id)
            self.service_logger.info(f"Writing Pokemon ID {pokemon_id} to HBase")

            # Connect to HBase
            connection = happybase.Connection(host=self.hbase_host, port=self.hbase_port)
            connection.open()

            # Ensure table and schema exist
            tables = connection.tables()
            families = {
                'info': dict(),
                'name': dict(),
                'type': dict(),
                'base': dict(),
                'profile': dict(),
                'evolution': dict()
            }
            table_name_bytes = self.table_name.encode('utf-8')
            if table_name_bytes not in tables:
                self.service_logger.info(f"Creating HBase table: {self.table_name}")
                connection.create_table(self.table_name, families)

            table = connection.table(self.table_name)

            # Flatten nested data structures
            data = {}

            # info family
            if 'species' in pokemon:
                data[b'info:species'] = str(pokemon['species']).encode('utf-8')
            if 'description' in pokemon:
                data[b'info:description'] = str(pokemon['description']).encode('utf-8')
            if 'image' in pokemon and isinstance(pokemon['image'], dict):
                for k, val in pokemon['image'].items():
                    data[f'info:image_{k}'.encode('utf-8')] = str(val).encode('utf-8')

            # name family
            if 'name' in pokemon and isinstance(pokemon['name'], dict):
                for lang, val in pokemon['name'].items():
                    data[f'name:{lang}'.encode('utf-8')] = str(val).encode('utf-8')

            # type family
            if 'type' in pokemon and isinstance(pokemon['type'], list):
                for idx, val in enumerate(pokemon['type']):
                    data[f'type:{idx}'.encode('utf-8')] = str(val).encode('utf-8')

            # base family
            if 'base' in pokemon and isinstance(pokemon['base'], dict):
                for stat, val in pokemon['base'].items():
                    data[f'base:{stat}'.encode('utf-8')] = str(val).encode('utf-8')

            # profile family
            if 'profile' in pokemon and isinstance(pokemon['profile'], dict):
                for k, val in pokemon['profile'].items():
                    if isinstance(val, list):
                        for idx, list_val in enumerate(val):
                            data[f'profile:{k}_{idx}'.encode('utf-8')] = str(list_val).encode('utf-8')
                    else:
                        data[f'profile:{k}'.encode('utf-8')] = str(val).encode('utf-8')

            # Put to HBase
            row_key = pokemon_id.encode('utf-8')
            table.put(row_key, data)
            connection.close()

            # Publish success status
            status_payload = {
                "id": pokemon_id,
                "status": "success",
                "message": f"Successfully wrote Pokemon ID {pokemon_id} to HBase"
            }
            if self.publish_topic:
                producer.produce(
                    topic=self.publish_topic,
                    value=json.dumps(status_payload).encode('utf-8'),
                    key=pokemon_id.encode('utf-8'),
                    headers=inject_trace_headers(message.headers())
                )
                producer.poll(0)

            self.service_logger.log_success_logstash(start_timestamp)

        except Exception as e:
            # Try to report failure if possible
            pokemon_id = "unknown"
            try:
                pokemon_id = str(json.loads(body).get('id', 'unknown'))
            except:
                pass
            status_payload = {
                "id": pokemon_id,
                "status": "failed",
                "message": str(e)
            }
            if self.publish_topic:
                producer.produce(
                    topic=self.publish_topic,
                    value=json.dumps(status_payload).encode('utf-8'),
                    key=pokemon_id.encode('utf-8'),
                    headers=inject_trace_headers(message.headers())
                )
                producer.poll(0)
            self.service_logger.log_error(str(e), start_timestamp)
