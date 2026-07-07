import functools

import opentracing
from jaeger_client import Config
from opentracing import Format
from opentracing_instrumentation.client_hooks import install_all_patches

from src.utils import config_provider

tracing_enabled = config_provider.get_tracing_enable()


def init_tracer():
    if tracing_enabled:
        config = Config(config={'sampler': {'type': 'const', 'param': 1}}
                        , service_name=config_provider.get_elastic_index_name(), validate=True)
        install_all_patches()
        config.initialize_tracer()


def traced_consumer(func=None, name=None):
    if func is None:
        return functools.partial(traced_consumer, name=name)
    if name:
        operation_name = name
    else:
        operation_name = func.__name__

    @functools.wraps(func)
    def decorator(self, connection, ch, method, properties, body):
        if tracing_enabled:
            tracer = opentracing.global_tracer()
            references = None
            context = tracer.extract(Format.TEXT_MAP, properties.headers)
            if properties and properties.headers:
                if context:
                    references = opentracing.follows_from(context)
            with tracer.start_active_span(operation_name, references=references):
                func(self, connection, ch, method, properties, body)
        else:
            func(self, connection, ch, method, properties, body)
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
