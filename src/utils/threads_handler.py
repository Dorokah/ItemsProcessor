import threading
import time
from src.utils.service_logger import ServiceLogger
from src.utils import config_provider
from src.utils.tracer import start_batch_span
from importlib import import_module


class ThreadsHandler:
    def __init__(self, producer, consumer, logger, request_handler_import_pkg, request_handler_import_class):
        self.threads = []
        self.service_logger = logger
        self.producer = producer
        self.consumer = consumer
        self.is_sigterm_received = False
        self.rh_module = import_module(request_handler_import_pkg)
        self.rh_import_class = request_handler_import_class
        self.max_worker_threads = config_provider.get_max_worker_threads()

    def _do_work(self, message):
        service_logger = ServiceLogger()
        request_handler_class = getattr(self.rh_module, self.rh_import_class)
        requests_handler = request_handler_class(service_logger)
        requests_handler.handle_request(self.producer, self.consumer, message)
        if self.is_sigterm_received:
            service_logger.log_sigterm_received()

    def _do_batch_work(self, messages):
        service_logger = ServiceLogger()
        request_handler_class = getattr(self.rh_module, self.rh_import_class)
        requests_handler = request_handler_class(service_logger)

        with start_batch_span("consume_kafka_batch", messages):
            if hasattr(requests_handler, "handle_batch"):
                requests_handler.handle_batch(self.producer, self.consumer, messages)
            else:
                for message in messages:
                    requests_handler.handle_request(self.producer, self.consumer, message)

        self.producer.flush()
        if self.is_sigterm_received:
            service_logger.log_sigterm_received()

    def on_message(self, message):
        self.on_batch([message])

    def on_batch(self, messages):
        if not messages:
            return

        self._remove_finished_threads()
        while len(self.threads) >= self.max_worker_threads and not self.is_sigterm_received:
            self.service_logger.info(
                f"Max worker threads reached ({self.max_worker_threads}); waiting before consuming more work"
            )
            time.sleep(0.5)
            self._remove_finished_threads()

        t = threading.Thread(target=self._do_batch_work, args=(messages,))
        t.start()
        self.threads.append(t)

    def signal_handler(self, signal, frame):
        self.is_sigterm_received = True

    def stop_all_threads(self):
        for t in self.threads:
            t.join()

    def _remove_finished_threads(self):
        self.threads = [t for t in self.threads if t.is_alive()]
        self.service_logger.info(f"threads amount {len(self.threads)}")
