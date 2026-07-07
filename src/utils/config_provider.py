import os


#  Mandatory configs:
def get_pipeline_name():
    return os.environ.get('PIPELINE_NAME', 'Algorithm')


def get_service_version():
    return os.environ.get('VERSION', 'Undefined')


def get_service_name():
    return os.environ.get('SERVICE_NAME', 'Undefined')


def get_next_service_name():
    return os.environ.get('NEXT_SERVICE_NAME', '')


def get_request_handler_class_name():
    parts = os.environ.get('REQUEST_HANDLER', '').split('.')
    return {"moduleName": f"{parts[0]}.{parts[1]}.{parts[2]}.{parts[3]}",
            "className": parts[3]}


#  Logstash configs:
def get_logstash_host():
    return os.environ.get('LOGSTASH_HOST', '')


def get_logstash_port():
    port = os.environ.get('LOGSTASH_PORT', '')
    return int(port) if port else 0


def get_logstash_enable():
    str_value = os.environ.get('LOGSTASH_ENABLE', 'false')
    if str_value == 'True' or str_value == "true":
        return True
    return False


def get_logs_db_name():
    str_value = os.environ.get('LOGS_DB_NAME', './QueueToLogstash.db')
    if str_value == 'None' or str_value == "none":
        return None
    return str_value


def get_elastic_index_name():
    return os.environ.get('ELASTIC_INDEX_NAME',
                          f"RabbitProcessor-{get_pipeline_name()}-{get_service_name()}")


def get_enable_results_logging():
    return return_true_by_str(os.environ.get('ENABLE_RESULTS_LOGGING', ""))


#  Kafka configs:
def get_kafka_bootstrap_servers():
    return os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


def get_kafka_group_id():
    return os.environ.get('KAFKA_GROUP_ID', 'processor-group')


#  HBase configs:
def get_hbase_host():
    return os.environ.get('HBASE_HOST', 'localhost')


def get_hbase_port():
    return int(os.environ.get('HBASE_PORT', '9090'))


def get_hbase_table_name():
    return os.environ.get('HBASE_TABLE_NAME', 'pokemon')


#  Webhook configs:
def get_webhook_url():
    return os.environ.get('WEBHOOK_URL', 'http://localhost:8080/webhook')


#  Kafka Topics (mapped to queue names for compatibility):
def get_results_queue_name():
    return os.environ.get('RESULTS_TOPIC', "results")


def get_consume_queue_name():
    return os.environ.get('CONSUME_TOPIC', "")


def get_publish_queue_name():
    return os.environ.get('PUBLISH_TOPIC', "")


def get_queues_to_declare_names():
    return [get_consume_queue_name(), get_publish_queue_name()]


def get_rabbit_prefetch():
    return int(os.environ.get('RABBIT_PREFETCH', '1'))


def get_next_requests_cpu_queue_name():
    return os.environ.get('RECOGNITION_CPU_QUEUE', '')


def get_next_requests_gpu_queue_name():
    return os.environ.get('RECOGNITION_GPU_QUEUE', '')


#  Image processing configs:
def get_image_service_url():
    return os.environ.get('IMAGE_SERVICE_URL', '')


def get_image_timeout():
    return os.environ.get('GET_IMAGE_TIMEOUT', 10)


def get_algorithm_service_post_timeout():
    return int(os.environ.get('ALGORITHM_SERVICE_TIMEOUT', '30'))


def get_image_minimum_width():
    return int(os.environ.get('run_image_min_width', '5'))


def get_image_minimum_height():
    return int(os.environ.get('run_image_min_height', '5'))


#  Jaeger support:
def get_tracing_enable():
    return return_true_by_str(os.environ.get('TRACING_ENABLE', ''))


def get_tracing_results_logs_enable():
    return return_true_by_str(os.environ.get('TRACING_RESULTS_LOGS_ENABLE', ''))


def get_is_first_service():
    return return_true_by_str(os.environ.get('FIRST_SERVICE', ''))


#  Common:
def return_true_by_str(str_value):
    if str_value == 'True' or str_value == "true":
        return True
    return False
