import sys
from types import ModuleType

# Mock tornado.stack_context for compatibility of opentracing with Tornado 6
import tornado
_mod = ModuleType('tornado.stack_context')
class _DummyStackContext:
    def __init__(self, *args, **kwargs): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass
_mod.StackContext = _DummyStackContext
_mod.ExceptionStackContext = _DummyStackContext
_mod.wrap = lambda f: f
sys.modules['tornado.stack_context'] = _mod
tornado.stack_context = _mod

import pika
import signal
from src.utils import config_provider
from src.utils.adapter_logger import AdapterLogger
from src.utils.threads_handler import ThreadsHandler
from src.utils.tracer import init_tracer


def declare_queues(ch, max_priorities):
    request_queue_names = config_provider.get_queues_to_declare_names()
    for queue_name in request_queue_names:
        ch.queue_declare(queue=queue_name,
                         durable=True,
                         arguments={"x-max-priority": max_priorities})
    return request_queue_names


if __name__ == '__main__':
    init_tracer()

    rabbit_prefetch = config_provider.get_rabbit_prefetch()
    consume_queue_name = config_provider.get_consume_queue_name()
    rabbit_max_priorities = config_provider.get_rabbit_max_priorities()
    rabbit_vhost = config_provider.get_rabbit_max_priorities()

    parameters = pika.ConnectionParameters(config_provider.get_rabbit_host(),
                                           config_provider.get_rabbit_port(),
                                           config_provider.get_rabbit_vhost(),
                                           pika.PlainCredentials(config_provider.get_rabbit_username(),
                                                                 config_provider.get_rabbit_password()))
    connection = pika.BlockingConnection(parameters)

    main_thread_logger = AdapterLogger()
    main_thread_logger.log_service_config()
    main_thread_logger.set_logstash_handler()

    import_and_init = config_provider.get_request_handler_class_name()
    rh_module_name = config_provider.get_request_handler_class_name()['moduleName']
    rh_class_name = config_provider.get_request_handler_class_name()['className']

    threads_handler = ThreadsHandler(connection,
                                     main_thread_logger,
                                     rh_module_name,
                                     rh_class_name)

    channel = connection.channel()
    algorithms_queue_names = declare_queues(channel, rabbit_max_priorities)
    channel.basic_qos(prefetch_count=rabbit_prefetch)
    channel.basic_consume(queue=consume_queue_name,
                          on_message_callback=threads_handler.on_message)

    signal.signal(signal.SIGTERM, threads_handler.signal_handler)
    channel.start_consuming()
