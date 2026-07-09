import signal
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

    batch_size = config_provider.get_batch_size()
    batch_timeout = config_provider.get_batch_timeout()

    main_thread_logger.info(f"Starting message consumption loop (batch_size={batch_size}, timeout={batch_timeout}s)")
    try:
        while not threads_handler.is_sigterm_received:
            msgs = consumer.consume(num_messages=batch_size, timeout=batch_timeout)
            if not msgs:
                continue

            valid_msgs = []
            for msg in msgs:
                if msg.error():
                    main_thread_logger.logger.error(f"Kafka error: {msg.error()}")
                    continue
                valid_msgs.append(msg)

            if valid_msgs:
                threads_handler.on_batch(valid_msgs)
    finally:
        main_thread_logger.info("Closing Kafka consumer and flushing producer")
        consumer.close()
        producer.flush()
        # Join any active processing threads
        threads_handler.stop_all_threads()
