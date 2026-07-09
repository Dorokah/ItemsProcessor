import json
import time

import happybase

from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import traced_consumer, start_item_span, get_message_json_trace_id


class HBaseSurnameUpdateRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.hbase_host = config_provider.get_hbase_host()
        self.hbase_port = config_provider.get_hbase_port()
        self.table_name = config_provider.get_hbase_table_name()
        self.hbase_timeout_ms = config_provider.get_hbase_timeout_ms()

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
                enriched_event = json.loads(message.value())
                pokemon_id = str(enriched_event.get("id", ""))
                surname = str(enriched_event.get("surname", "")).strip()
                french_name = str(enriched_event.get("frenchName", "")).strip()

                if not pokemon_id:
                    raise ValueError("Missing 'id' in enriched surname event")
                if not surname:
                    raise ValueError("Missing 'surname' in enriched surname event")
                if not french_name:
                    raise ValueError("Missing 'frenchName' in enriched surname event")

                parsed_records.append((message, pokemon_id, surname, french_name))
            except Exception as exc:
                failed_records.append(str(exc))

        pokemon_ids = [pokemon_id for _, pokemon_id, _, _ in parsed_records]
        self.service_logger.add_field("requestId", f"hbase-surname-batch-{'_'.join(pokemon_ids[:5])}" if pokemon_ids else "hbase-surname-batch-empty")
        self.service_logger.add_field("batchSize", len(messages))
        self.service_logger.add_field("hbaseBatchSize", len(parsed_records))
        self.service_logger.add_field("entityIds", pokemon_ids)
        self.service_logger.info(
            f"Updating HBase surname fields for {len(parsed_records)} records from {len(messages)} messages"
        )

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
            for message, pokemon_id, surname, french_name in parsed_records:
                with start_item_span(
                    "queue_hbase_surname_put",
                    pokemon_id,
                    json_trace_id=get_message_json_trace_id(message),
                    message=message,
                ):
                    batch.put(
                        pokemon_id.encode("utf-8"),
                        {
                            b"name:surname": surname.encode("utf-8"),
                            b"name:frenchSurname": french_name.encode("utf-8"),
                        },
                    )
            batch.send()

            if failed_records:
                self.service_logger.add_field("hbaseFailureCount", len(failed_records))
                self.service_logger.add_field("hbaseBatchErrors", failed_records)
                self.service_logger.log_error(
                    f"HBase surname batch had {len(failed_records)} invalid records",
                    start_timestamp,
                )
            else:
                self.service_logger.add_field("hbaseSuccessCount", len(parsed_records))
                self.service_logger.log_success_logstash(start_timestamp)

        except Exception as exc:
            self.service_logger.add_field("hbaseFailureCount", len(parsed_records) + len(failed_records))
            self.service_logger.log_error(str(exc), start_timestamp)
        finally:
            if connection:
                connection.close()

    def _ensure_table(self, connection):
        table_name_bytes = self.table_name.encode("utf-8")
        if table_name_bytes in connection.tables():
            return

        connection.create_table(
            self.table_name,
            {
                "info": dict(),
                "name": dict(),
                "type": dict(),
                "base": dict(),
                "profile": dict(),
                "evolution": dict(),
            },
        )

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
