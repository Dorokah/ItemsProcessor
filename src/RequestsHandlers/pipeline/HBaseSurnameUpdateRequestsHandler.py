import json
import time

import happybase

from src.RequestsHandlers.RequestHandler import RequestHandler
from src.utils import config_provider
from src.utils.tracer import traced_consumer


class HBaseSurnameUpdateRequestsHandler(RequestHandler):
    def __init__(self, service_logger):
        super().__init__(service_logger)
        self.hbase_host = config_provider.get_hbase_host()
        self.hbase_port = config_provider.get_hbase_port()
        self.table_name = config_provider.get_hbase_table_name()
        self.hbase_timeout_ms = config_provider.get_hbase_timeout_ms()

    @traced_consumer
    def handle_request(self, producer, consumer, message):
        body = message.value()
        start_timestamp = time.time()
        self.service_logger.reset_aggregated_log()
        self.service_logger.log_trace_id()

        try:
            enriched_event = json.loads(body)
            pokemon_id = str(enriched_event.get("id", ""))
            surname = str(enriched_event.get("surname", "")).strip()
            french_name = str(enriched_event.get("frenchName", "")).strip()

            if not pokemon_id:
                raise ValueError("Missing 'id' in enriched surname event")
            if not surname:
                raise ValueError("Missing 'surname' in enriched surname event")
            if not french_name:
                raise ValueError("Missing 'frenchName' in enriched surname event")

            self.service_logger.add_field("requestId", f"hbase-surname-update-{pokemon_id}")
            self.service_logger.add_field("entityId", pokemon_id)
            self.service_logger.add_field("surname", surname)
            self.service_logger.add_field("frenchName", french_name)
            self.service_logger.info(
                f"Updating HBase surname fields for Pokemon ID {pokemon_id}"
            )

            connection = happybase.Connection(
                host=self.hbase_host,
                port=self.hbase_port,
                timeout=self.hbase_timeout_ms,
            )
            connection.open()
            self._ensure_table(connection)

            table = connection.table(self.table_name)
            table.put(
                pokemon_id.encode("utf-8"),
                {
                    b"name:surname": surname.encode("utf-8"),
                    b"name:frenchSurname": french_name.encode("utf-8"),
                },
            )
            connection.close()

            self.service_logger.log_success_logstash(start_timestamp)

        except Exception as exc:
            self.service_logger.log_error(str(exc), start_timestamp)

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
