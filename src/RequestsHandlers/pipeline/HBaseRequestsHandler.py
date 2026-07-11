import happybase
import json
import time
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import (
    traced_consumer,
    inject_trace_headers,
    start_item_span,
    get_message_splitter_id,
    get_message_json_trace_id,
)


class HBaseRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.hbase_host = config_provider.get_hbase_host()
        self.hbase_port = config_provider.get_hbase_port()
        self.table_name = config_provider.get_hbase_table_name()
        self.hbase_timeout_ms = config_provider.get_hbase_timeout_ms()
        self.publish_topic = config_provider.get_publish_queue_name()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        self.handle_batch(producer, consumer, [message])

    def handle_batch(self, producer, consumer, messages):
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()

        parsed_records = []
        failed_records = []

        for message in messages:
            try:
                pokemon = json.loads(message.value())
                pokemon_id = str(pokemon.get('id', ''))
                if not pokemon_id:
                    raise Exception("Missing 'id' in Pokemon record")
                parsed_records.append((message, pokemon_id, pokemon, self._pokemon_to_hbase_data(pokemon)))
            except Exception as exc:
                pokemon_id = "unknown"
                try:
                    pokemon_id = str(json.loads(message.value()).get('id', 'unknown'))
                except Exception:
                    pass
                failed_records.append((message, pokemon_id, str(exc)))

        pokemon_ids = [pokemon_id for _, pokemon_id, _, _ in parsed_records]
        self.service_logger.add_field('requestId', f"hbase-batch-{'_'.join(pokemon_ids[:5])}" if pokemon_ids else "hbase-batch-empty")
        self.service_logger.add_field('batchSize', len(messages))
        self.service_logger.add_field('hbaseBatchSize', len(parsed_records))
        self.service_logger.add_field('entityIds', pokemon_ids)
        self.service_logger.info(f"Writing HBase batch with {len(parsed_records)} valid records from {len(messages)} messages")

        connection = None
        try:
            connection = happybase.Connection(
                host=self.hbase_host,
                port=self.hbase_port,
                timeout=self.hbase_timeout_ms,
            )
            connection.open()
            self._ensure_table(connection)
            table = connection.table(self.table_name)
            batch = table.batch(batch_size=max(len(parsed_records), 1))

            for message, pokemon_id, _, data in parsed_records:
                with start_item_span(
                    "queue_hbase_pokemon_put",
                    pokemon_id,
                    splitter_id=get_message_splitter_id(message),
                    json_trace_id=get_message_json_trace_id(message),
                    message=message,
                ):
                    batch.put(pokemon_id.encode('utf-8'), data)

            batch.send()

            for message, pokemon_id, _, _ in parsed_records:
                with start_item_span(
                    "publish_hbase_status",
                    pokemon_id,
                    splitter_id=get_message_splitter_id(message),
                    json_trace_id=get_message_json_trace_id(message),
                    message=message,
                ):
                    self._publish_status(
                        producer,
                        message,
                        pokemon_id,
                        "success",
                        f"Successfully wrote Pokemon ID {pokemon_id} to HBase",
                    )

            for message, pokemon_id, error_message in failed_records:
                with start_item_span(
                    "publish_hbase_status",
                    pokemon_id,
                    splitter_id=get_message_splitter_id(message),
                    json_trace_id=get_message_json_trace_id(message),
                    message=message,
                ):
                    self._publish_status(producer, message, pokemon_id, "failed", error_message)

            if failed_records:
                self.service_logger.add_field('hbaseFailureCount', len(failed_records))
                self.service_logger.add_field('hbaseFailureEntityIds', [pokemon_id for _, pokemon_id, _ in failed_records])
                self.service_logger.log_error(
                    f"HBase batch had {len(failed_records)} invalid records",
                    start_timestamp,
                )
            else:
                self.service_logger.add_field('hbaseSuccessCount', len(parsed_records))
                self.service_logger.log_success_logstash(start_timestamp)

        except Exception as exc:
            all_records = [(message, pokemon_id) for message, pokemon_id, _, _ in parsed_records]
            for message, pokemon_id in all_records:
                with start_item_span(
                    "publish_hbase_status",
                    pokemon_id,
                    splitter_id=get_message_splitter_id(message),
                    json_trace_id=get_message_json_trace_id(message),
                    message=message,
                ):
                    self._publish_status(producer, message, pokemon_id, "failed", str(exc))

            self.service_logger.add_field('hbaseFailureCount', len(all_records) + len(failed_records))
            self.service_logger.add_field('hbaseFailureEntityIds', [pokemon_id for _, pokemon_id in all_records] + [pokemon_id for _, pokemon_id, _ in failed_records])
            self.service_logger.log_error(str(exc), start_timestamp)
        finally:
            if connection:
                connection.close()

    def _ensure_table(self, connection):
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
            try:
                connection.create_table(self.table_name, families)
            except Exception as e:
                if 'TableExistsException' in str(e):
                    self.service_logger.info(f"HBase table {self.table_name} already exists (created concurrently).")
                else:
                    raise e

    def _pokemon_to_hbase_data(self, pokemon):
        data = {}

        if 'species' in pokemon:
            data[b'info:species'] = str(pokemon['species']).encode('utf-8')
        if 'description' in pokemon:
            data[b'info:description'] = str(pokemon['description']).encode('utf-8')
        if 'image' in pokemon and isinstance(pokemon['image'], dict):
            for k, val in pokemon['image'].items():
                data[f'info:image_{k}'.encode('utf-8')] = str(val).encode('utf-8')

        if 'name' in pokemon and isinstance(pokemon['name'], dict):
            for lang, val in pokemon['name'].items():
                data[f'name:{lang}'.encode('utf-8')] = str(val).encode('utf-8')

        if 'type' in pokemon and isinstance(pokemon['type'], list):
            for idx, val in enumerate(pokemon['type']):
                data[f'type:{idx}'.encode('utf-8')] = str(val).encode('utf-8')

        if 'base' in pokemon and isinstance(pokemon['base'], dict):
            for stat, val in pokemon['base'].items():
                data[f'base:{stat}'.encode('utf-8')] = str(val).encode('utf-8')

        if 'profile' in pokemon and isinstance(pokemon['profile'], dict):
            for k, val in pokemon['profile'].items():
                if isinstance(val, list):
                    for idx, list_val in enumerate(val):
                        data[f'profile:{k}_{idx}'.encode('utf-8')] = str(list_val).encode('utf-8')
                else:
                    data[f'profile:{k}'.encode('utf-8')] = str(val).encode('utf-8')

        return data

    def _publish_status(self, producer, message, pokemon_id, status, status_message):
        if not self.publish_topic:
            return

        status_payload = {
            "id": pokemon_id,
            "status": status,
            "message": status_message
        }
        producer.produce(
            topic=self.publish_topic,
            value=json.dumps(status_payload).encode('utf-8'),
            key=pokemon_id.encode('utf-8'),
            headers=inject_trace_headers(
                message.headers(),
                item_id=pokemon_id,
                splitter_id=get_message_splitter_id(message),
                json_trace_id=get_message_json_trace_id(message),
            )
        )
        producer.poll(0)
