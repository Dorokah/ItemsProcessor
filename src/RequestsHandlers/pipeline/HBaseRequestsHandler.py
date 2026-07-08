import happybase
import json
import time
import opentracing
from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import traced_consumer, inject_trace_headers, trace_message


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
                try:
                    connection.create_table(self.table_name, families)
                except Exception as e:
                    if 'TableExistsException' in str(e):
                        self.service_logger.info(f"HBase table {self.table_name} already exists (created concurrently).")
                    else:
                        raise e

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

    def handle_batch(self, producer, consumer, messages):
        self.service_logger.info(f"Starting HBase batch process for {len(messages)} messages")
        
        valid_writes = []     # List of tuples: (row_key, data, pokemon_id, message)
        failed_records = []   # List of tuples: (pokemon_id, error_msg, message)
        
        # 1. Parse and validate each message in its own trace context
        for message in messages:
            with trace_message(message, "HBaseRequestsHandler.prepare"):
                body = message.value()
                try:
                    pokemon = json.loads(body)
                    pokemon_id = str(pokemon.get('id', ''))
                    if not pokemon_id:
                        raise Exception("Missing 'id' in Pokemon record")
                    
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

                    row_key = pokemon_id.encode('utf-8')
                    valid_writes.append((row_key, data, pokemon_id, message))
                except Exception as e:
                    pokemon_id = "unknown"
                    try:
                        pokemon_id = str(json.loads(body).get('id', 'unknown'))
                    except:
                        pass
                    failed_records.append((pokemon_id, str(e), message))

        # 2. Write valid records to HBase in a single transaction/batch
        successful_records = []
        start_hbase_time = time.time()
        
        if valid_writes:
            # Build references to trace the batch write from all messages in the batch
            references = []
            if config_provider.get_tracing_enable():
                tracer = opentracing.global_tracer()
                for _, _, _, message in valid_writes:
                    headers = message.headers()
                    if headers:
                        headers_dict = {
                            k: (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                            for k, v in headers
                        }
                        context = tracer.extract(opentracing.Format.TEXT_MAP, headers_dict)
                        if context:
                            references.append(opentracing.follows_from(context))

            # Span context for batch write
            batch_write_span = None
            if config_provider.get_tracing_enable():
                tracer = opentracing.global_tracer()
                batch_write_span = tracer.start_active_span("HBaseRequestsHandler.batch_write", references=references)

            try:
                self.service_logger.info(f"Connecting to HBase for batch write of {len(valid_writes)} records")
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
                    try:
                        connection.create_table(self.table_name, families)
                    except Exception as e:
                        if 'TableExistsException' in str(e):
                            self.service_logger.info(f"HBase table {self.table_name} already exists (created concurrently).")
                        else:
                            raise e

                table = connection.table(self.table_name)
                
                with table.batch() as b:
                    for row_key, data, pokemon_id, message in valid_writes:
                        b.put(row_key, data)
                
                connection.close()
                successful_records = valid_writes
            except Exception as e:
                self.service_logger.logger.error(f"HBase batch write failed: {e}")
                # All these writes are now failed
                for row_key, data, pokemon_id, message in valid_writes:
                    failed_records.append((pokemon_id, f"HBase batch write failure: {e}", message))
            finally:
                if batch_write_span:
                    batch_write_span.close()

        # 3. Publish success status for successful writes, wrapped in individual trace contexts
        for row_key, data, pokemon_id, message in successful_records:
            with trace_message(message, "HBaseRequestsHandler.publish_status"):
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

                self.service_logger.reset_aggregated_log()
                self.service_logger.log_trace_id()
                self.service_logger.add_field('requestId', f"hbase-{pokemon_id}")
                self.service_logger.add_field('entityId', pokemon_id)
                self.service_logger.log_success_logstash(start_hbase_time)

        # 4. Publish failed status for failed writes, wrapped in individual trace contexts
        for pokemon_id, err_msg, message in failed_records:
            with trace_message(message, "HBaseRequestsHandler.publish_status"):
                status_payload = {
                    "id": pokemon_id,
                    "status": "failed",
                    "message": err_msg
                }
                if self.publish_topic:
                    producer.produce(
                        topic=self.publish_topic,
                        value=json.dumps(status_payload).encode('utf-8'),
                        key=pokemon_id.encode('utf-8'),
                        headers=inject_trace_headers(message.headers())
                    )
                    producer.poll(0)

                self.service_logger.reset_aggregated_log()
                self.service_logger.log_trace_id()
                self.service_logger.add_field('requestId', f"hbase-{pokemon_id}")
                self.service_logger.add_field('entityId', pokemon_id)
                self.service_logger.log_error(err_msg, start_hbase_time)

        # Flush any remaining messages to Kafka before finishing the batch
        if self.publish_topic:
            producer.flush()
