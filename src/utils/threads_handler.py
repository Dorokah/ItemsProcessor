import threading
from src.utils.adapter_logger import AlgorithmAdapterLogger
from importlib import import_module


class ThreadsHandler:
    def __init__(self, connection, logger, request_handler_import_pkg, request_handler_import_class):
        self.threads = []
        self.adapter_logger = logger
        self.connection = connection
        self.is_sigterm_received = False
        self.rh_module = import_module(request_handler_import_pkg)
        self.rh_import_class = request_handler_import_class

    def _do_work(self, ch, method, properties, body):
        algorithm_adapter_logger = AlgorithmAdapterLogger()
        request_handler_class = getattr(self.rh_module, self.rh_import_class)
        requests_handler = request_handler_class(algorithm_adapter_logger)
        requests_handler.handle_algo_request(self.connection, ch, method, properties, body)
        if self.is_sigterm_received:
            self._stop_consuming(ch)
            algorithm_adapter_logger.log_sigterm_received()

    def on_message(self, ch, method, properties, body):
        self._remove_finished_threads()
        t = threading.Thread(target=self._do_work, args=(ch, method, properties, body))
        t.start()
        self.threads.append(t)

    def signal_handler(self, signal, frame):
        self.is_sigterm_received = True

    def _stop_consuming(self, ch):
        ch.stop_consuming()
        for t in self.threads:
            t.join()

    def _remove_finished_threads(self):
        self.threads = [t for t in self.threads if t.is_alive()]
        self.adapter_logger.info(f"threads amount {len(self.threads)}")
