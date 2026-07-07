from logstash_async.constants import constants

from src.utils import config_provider

constants.FORMATTER_RECORD_FIELD_SKIP_LIST = [
    'type', 'program', 'process_name', 'port', 'interpreter',
    'host', 'func_name', 'pid', 'path', 'logstash_async_version',
    'logsource', 'logger_name', 'line', 'interpreter_version',
    'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
    'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module',
    'msecs', 'msg', 'name', 'pathname', 'process',
    'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName', 'thread_name']

import logging
from logstash_async.formatter import LogstashFormatter
from logstash_async.handler import AsynchronousLogstashHandler


class LogstashLogger:

    def __init__(self, logger, is_enabled, host, port, index_name):
        self.isEnabled = is_enabled
        self.host = host
        self.port = port
        self.service_name = index_name.lower()
        self.logger = logger
        self.logstash_async_logger = logging.getLogger('logstash_async_logger')
        self.logstash_async_logger.setLevel(logging.INFO)

    def log(self, message, log_dict=None, is_info=True):
        if not log_dict:
            log_dict = {}
        log_dict['project'] = self.service_name
        
        # Append traceId to the message text for Grafana derivedFields matching!
        trace_id = log_dict.get('traceId')
        if trace_id and trace_id != 'No_trace_ID':
            message = f"{message} | traceId={trace_id}"
            
        self.logger.info(f"Sending to elastic the log: {log_dict}")
        if self.isEnabled:
            if is_info:
                self.logstash_async_logger.info(message, extra=log_dict)
            else:
                self.logstash_async_logger.error(message, extra=log_dict)
        else:
            self.logger.info("logstash posting is not enabled")

    def set_logstash_handler(self):
        async_handler = AsynchronousLogstashHandler(self.host,
                                                    self.port,
                                                    database_path=config_provider.get_logs_db_name())
        logstash_formatter = LogstashFormatter(message_type='python-logstash', extra_prefix=None)
        async_handler.setFormatter(logstash_formatter)
        self.logstash_async_logger.addHandler(async_handler)
