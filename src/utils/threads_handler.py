import threading
from src.utils.service_logger import ServiceLogger
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

    def _do_work_batch(self, messages):
        service_logger = ServiceLogger()
        request_handler_class = getattr(self.rh_module, self.rh_import_class)
        requests_handler = request_handler_class(service_logger)
        if hasattr(requests_handler, 'handle_batch'):
            requests_handler.handle_batch(self.producer, self.consumer, messages)
        else:
            for message in messages:
                requests_handler.handle_request(self.producer, self.consumer, message)
        if self.is_sigterm_received:
            service_logger.log_sigterm_received()

    def on_batch(self, messages):
        self._remove_finished_threads()
        t = threading.Thread(target=self._do_work_batch, args=(messages,))
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
