import threading
from src.utils.adapter_logger import AdapterLogger
from importlib import import_module


class ThreadsHandler:
    def __init__(self, producer, consumer, logger, request_handler_import_pkg, request_handler_import_class):
        self.threads = []
        self.adapter_logger = logger
        self.producer = producer
        self.consumer = consumer
        self.is_sigterm_received = False
        self.rh_module = import_module(request_handler_import_pkg)
        self.rh_import_class = request_handler_import_class

    def _do_work(self, message):
        adapter_logger = AdapterLogger()
        request_handler_class = getattr(self.rh_module, self.rh_import_class)
        requests_handler = request_handler_class(adapter_logger)
        requests_handler.handle_request(self.producer, self.consumer, message)
        if self.is_sigterm_received:
            adapter_logger.log_sigterm_received()

    def on_message(self, message):
        self._remove_finished_threads()
        t = threading.Thread(target=self._do_work, args=(message,))
        t.start()
        self.threads.append(t)

    def signal_handler(self, signal, frame):
        self.is_sigterm_received = True

    def stop_all_threads(self):
        for t in self.threads:
            t.join()

    def _remove_finished_threads(self):
        self.threads = [t for t in self.threads if t.is_alive()]
        self.adapter_logger.info(f"threads amount {len(self.threads)}")
