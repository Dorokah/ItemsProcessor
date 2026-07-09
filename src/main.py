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

import signal
import time
from confluent_kafka import Consumer, Producer
from src.utils import config_provider
from src.utils.service_logger import ServiceLogger
from src.utils.threads_handler import ThreadsHandler
from src.utils.tracer import init_tracer


if __name__ == '__main__':
    init_tracer()

    main_thread_logger = ServiceLogger()
    main_thread_logger.log_service_config()
    main_thread_logger.set_logstash_handler()

    # Kafka Setup
    bootstrap_servers = config_provider.get_kafka_bootstrap_servers()
    group_id = config_provider.get_kafka_group_id()
    consume_topic = config_provider.get_consume_queue_name()
    batch_size = config_provider.get_batch_size()
    batch_timeout_seconds = config_provider.get_batch_timeout_seconds()

    main_thread_logger.info(f"Initializing Kafka Consumer for topic: {consume_topic}")
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([consume_topic])

    main_thread_logger.info("Initializing Kafka Producer")
    producer_conf = {
        'bootstrap.servers': bootstrap_servers
    }
    producer = Producer(producer_conf)

    import_and_init = config_provider.get_request_handler_class_name()
    rh_module_name = import_and_init['moduleName']
    rh_class_name = import_and_init['className']

    threads_handler = ThreadsHandler(producer,
                                     consumer,
                                     main_thread_logger,
                                     rh_module_name,
                                     rh_class_name)

    signal.signal(signal.SIGTERM, threads_handler.signal_handler)

    main_thread_logger.info(
        f"Starting message consumption loop (batch_size={batch_size}, timeout={batch_timeout_seconds}s)"
    )
    try:
        while not threads_handler.is_sigterm_received:
            batch_deadline = time.time() + batch_timeout_seconds
            messages = consumer.consume(num_messages=batch_size, timeout=batch_timeout_seconds)
            messages = [msg for msg in messages if msg is not None]
            if not messages:
                continue

            while len(messages) < batch_size and time.time() < batch_deadline:
                remaining = batch_size - len(messages)
                remaining_timeout = max(batch_deadline - time.time(), 0.0)
                more_messages = consumer.consume(num_messages=remaining, timeout=remaining_timeout)
                messages.extend([msg for msg in more_messages if msg is not None])

            errored_messages = [msg for msg in messages if msg.error()]
            for msg in errored_messages:
                main_thread_logger.logger.error(f"Kafka error: {msg.error()}")
            valid_messages = [msg for msg in messages if not msg.error()]
            if not valid_messages:
                continue

            threads_handler.on_batch(valid_messages)
    finally:
        main_thread_logger.info("Closing Kafka consumer and flushing producer")
        consumer.close()
        producer.flush()
        # Join any active processing threads
        threads_handler.stop_all_threads()
