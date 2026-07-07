import functools

import opentracing
from jaeger_client import Config
from opentracing import Format
from opentracing_instrumentation.client_hooks import install_all_patches

from src.utils import config_provider

tracing_enabled = config_provider.get_tracing_enable()


import os

def init_tracer():
    if tracing_enabled:
        agent_host = os.environ.get('JAEGER_AGENT_HOST', 'localhost')
        agent_port = int(os.environ.get('JAEGER_AGENT_PORT', '6831'))
        service_name = config_provider.get_elastic_index_name()
        print(f"Initializing Jaeger tracer for service '{service_name}' targeting {agent_host}:{agent_port}...", flush=True)
        config = Config(
            config={
                'sampler': {'type': 'const', 'param': 1},
                'local_agent': {
                    'reporting_host': agent_host,
                    'reporting_port': agent_port
                }
            },
            service_name=service_name,
            validate=True
        )
        install_all_patches()
        tracer = config.initialize_tracer()
        print(f"Tracer initialized successfully: {tracer}", flush=True)


def traced_consumer(func=None, name=None):
    if func is None:
        return functools.partial(traced_consumer, name=name)
    if name:
        operation_name = name
    else:
        operation_name = func.__name__

    @functools.wraps(func)
    def decorator(self, producer, consumer, message):
        if tracing_enabled:
            tracer = opentracing.global_tracer()
            print(f"Active global tracer in decorator: {tracer}", flush=True)
            references = None
            headers = message.headers()
            if headers:
                # Convert list of bytes tuples to string dictionary for opentracing
                headers_dict = {
                    k: (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                    for k, v in headers
                }
                context = tracer.extract(Format.TEXT_MAP, headers_dict)
                if context:
                    references = opentracing.follows_from(context)
            
            with tracer.start_active_span(operation_name, references=references):
                return func(self, producer, consumer, message)
        else:
            return func(self, producer, consumer, message)
    return decorator


def get_trace_id(tracer):
    span = tracer.active_span
    if hasattr(span, 'trace_id'):
        return format(span.trace_id, 'x')
    return "No_trace_ID"


def get_span_id(tracer):
    span = tracer.active_span
    if hasattr(span, 'span_id'):
        return format(span.span_id, 'x')
    return "No_span_ID"
