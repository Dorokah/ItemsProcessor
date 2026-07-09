import functools
from contextlib import contextmanager
import os
import sys
from types import ModuleType

import tornado

if 'tornado.stack_context' not in sys.modules:
    _mod = ModuleType('tornado.stack_context')

    class _DummyStackContext:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    _mod.StackContext = _DummyStackContext
    _mod.ExceptionStackContext = _DummyStackContext
    _mod.wrap = lambda f: f
    sys.modules['tornado.stack_context'] = _mod
    tornado.stack_context = _mod

import opentracing
from jaeger_client import Config
from opentracing import Format
from opentracing_instrumentation.client_hooks import install_all_patches, install_patches

from src.utils import config_provider

tracing_enabled = config_provider.get_tracing_enable()
ITEM_ID_HEADER = "pokemon.id"
HTTP_ITEM_ID_HEADER = "pokemon-id"
ITEM_ID_TAG = "pokemon.id"
ITEM_IDS_TAG = "pokemon.ids"
ITEM_IDS_COUNT_TAG = "pokemon.ids.count"
ITEM_IDS_SAMPLE_TAG = "pokemon.ids.sample"
JSON_TRACE_ID_HEADER = "json.trace_id"
JSON_TRACE_ID_TAG = "json.trace_id"
JSON_TRACE_IDS_COUNT_TAG = "json.trace_ids.count"
JSON_TRACE_IDS_SAMPLE_TAG = "json.trace_ids.sample"
SPLITTER_ID_HEADER = "splitter.id"
SPLITTER_ID_TAG = "splitter.id"
SPLITTER_IDS_TAG = "splitter.ids"
SPILLER_ID_TAG = "spillerid"
BATCH_TRACE_ID_TAG = "batch.trace_id"
BATCH_SPAN_ID_TAG = "batch.span_id"
MAX_BATCH_ID_SAMPLE = 20
MAX_BATCH_TRACE_REFERENCES = int(os.environ.get("MAX_BATCH_TRACE_REFERENCES", "100"))

ENRICHMENT_TRACE_SERVICES = {
    "pokemon-surname-generator",
    "surname-enrichment",
    "surname-api",
    "hbase-surname-updater",
}


def _clean_enrichment_span_attributes():
    return config_provider.get_service_name() in ENRICHMENT_TRACE_SERVICES


def init_tracer():
    if tracing_enabled:
        agent_host = os.environ.get('JAEGER_AGENT_HOST', 'localhost')
        agent_port = int(os.environ.get('JAEGER_AGENT_PORT', '6831'))
        service_name = config_provider.get_elastic_index_name()
        print(f"Initializing Jaeger tracer for service '{service_name}' targeting {agent_host}:{agent_port}...", flush=True)
        config = Config(
            config={
                'sampler': {'type': 'const', 'param': 1},
                'reporter_queue_size': int(os.environ.get('JAEGER_REPORTER_QUEUE_SIZE', '10000')),
                'reporter_batch_size': int(os.environ.get('JAEGER_REPORTER_BATCH_SIZE', '100')),
                'reporter_flush_interval': int(os.environ.get('JAEGER_REPORTER_FLUSH_INTERVAL', '1')),
                'local_agent': {
                    'reporting_host': agent_host,
                    'reporting_port': agent_port
                }
            },
            service_name=service_name,
            validate=True
        )
        if _clean_enrichment_span_attributes():
            install_patches([])
        else:
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
    def decorator(self, producer, consumer, message):
        if tracing_enabled:
            tracer = opentracing.global_tracer()
            references = []
            context = None
            batch_span = tracer.active_span
            headers = message.headers()
            if headers:
                # Convert list of bytes tuples to string dictionary for opentracing
                headers_dict = {
                    k: (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                    for k, v in headers
                }
                context = tracer.extract(Format.TEXT_MAP, headers_dict)
                if context:
                    references.append(opentracing.child_of(context))

            with tracer.start_active_span(
                operation_name,
                references=references or None,
                ignore_active_span=bool(references),
            ) as scope:
                _set_batch_link_tags(scope.span, batch_span)
                set_active_span_correlation_tags_from_headers(headers)
                if context:
                    set_active_span_correlation_tags_from_context(context)
                return func(self, producer, consumer, message)
        else:
            return func(self, producer, consumer, message)
    return decorator


def _headers_to_text_map(headers):
    if not headers:
        return {}
    if isinstance(headers, dict):
        return {
            k: (v.decode('utf-8') if isinstance(v, bytes) else str(v))
            for k, v in headers.items()
        }
    return {
        k: (v.decode('utf-8') if isinstance(v, bytes) else str(v))
        for k, v in headers
    }


def extract_message_context(message):
    if not tracing_enabled:
        return None

    headers_dict = _headers_to_text_map(message.headers())
    if not headers_dict:
        return None

    try:
        return opentracing.global_tracer().extract(Format.TEXT_MAP, headers_dict)
    except Exception:
        return None


def get_header_value(headers, key):
    headers_dict = _headers_to_text_map(headers)
    return headers_dict.get(key)


def get_message_item_id(message):
    return get_header_value(message.headers(), ITEM_ID_HEADER)


def get_message_splitter_id(message):
    return get_header_value(message.headers(), SPLITTER_ID_HEADER)


def get_message_json_trace_id(message):
    return get_header_value(message.headers(), JSON_TRACE_ID_HEADER)


def get_messages_item_ids(messages):
    item_ids = []
    for message in messages:
        item_id = get_message_item_id(message)
        if item_id and item_id not in item_ids:
            item_ids.append(item_id)
    return item_ids


def get_messages_json_trace_ids(messages):
    trace_ids = []
    for message in messages:
        trace_id = get_message_json_trace_id(message)
        if trace_id and trace_id not in trace_ids:
            trace_ids.append(trace_id)
    return trace_ids


def get_messages_splitter_ids(messages):
    splitter_ids = []
    for message in messages:
        splitter_id = get_message_splitter_id(message)
        if splitter_id and splitter_id not in splitter_ids:
            splitter_ids.append(splitter_id)
    return splitter_ids


def set_active_span_correlation_tags_from_headers(headers):
    item_id = get_header_value(headers, ITEM_ID_HEADER)
    if item_id:
        set_active_span_item_tags([item_id])
    json_trace_id = get_header_value(headers, JSON_TRACE_ID_HEADER)
    if json_trace_id:
        set_active_span_json_trace_tags([json_trace_id])
    splitter_id = get_header_value(headers, SPLITTER_ID_HEADER)
    if splitter_id:
        set_active_span_splitter_tags([splitter_id])


def set_active_span_correlation_tags_from_context(context):
    try:
        item_id = context.get_baggage_item(ITEM_ID_HEADER)
    except Exception:
        item_id = None

    if item_id:
        set_active_span_item_tags([item_id])

    try:
        json_trace_id = context.get_baggage_item(JSON_TRACE_ID_HEADER)
    except Exception:
        json_trace_id = None

    if json_trace_id:
        set_active_span_json_trace_tags([json_trace_id])

    try:
        splitter_id = context.get_baggage_item(SPLITTER_ID_HEADER)
    except Exception:
        splitter_id = None

    if splitter_id:
        set_active_span_splitter_tags([splitter_id])


def set_active_span_item_tags(item_ids):
    if not tracing_enabled:
        return

    span = opentracing.global_tracer().active_span
    if not span:
        return

    clean_ids = [str(item_id) for item_id in item_ids if item_id]
    if not clean_ids:
        return

    if len(clean_ids) == 1:
        span.set_tag(ITEM_ID_TAG, clean_ids[0])
        span.set_baggage_item(ITEM_ID_HEADER, clean_ids[0])
    else:
        span.set_tag(ITEM_IDS_TAG, ",".join(clean_ids))


def set_active_span_item_summary_tags(item_ids):
    if not tracing_enabled:
        return

    span = opentracing.global_tracer().active_span
    if not span:
        return

    clean_ids = [str(item_id) for item_id in item_ids if item_id]
    if not clean_ids:
        return

    if len(clean_ids) == 1:
        set_active_span_item_tags(clean_ids)
        return

    span.set_tag(ITEM_IDS_COUNT_TAG, len(clean_ids))
    span.set_tag(ITEM_IDS_SAMPLE_TAG, ",".join(clean_ids[:MAX_BATCH_ID_SAMPLE]))


def set_active_span_json_trace_tags(trace_ids):
    if not tracing_enabled:
        return

    span = opentracing.global_tracer().active_span
    if not span:
        return

    clean_ids = [str(trace_id) for trace_id in trace_ids if trace_id]
    if not clean_ids:
        return

    if len(clean_ids) == 1:
        span.set_tag(JSON_TRACE_ID_TAG, clean_ids[0])
        span.set_baggage_item(JSON_TRACE_ID_HEADER, clean_ids[0])
    else:
        span.set_tag(JSON_TRACE_IDS_COUNT_TAG, len(clean_ids))
        span.set_tag(JSON_TRACE_IDS_SAMPLE_TAG, ",".join(clean_ids[:MAX_BATCH_ID_SAMPLE]))


def set_active_span_splitter_tags(splitter_ids):
    if not tracing_enabled:
        return

    span = opentracing.global_tracer().active_span
    if not span:
        return

    clean_ids = [str(splitter_id) for splitter_id in splitter_ids if splitter_id]
    if not clean_ids:
        return

    span.set_tag(SPLITTER_IDS_TAG, ",".join(clean_ids))
    if len(clean_ids) == 1:
        span.set_tag(SPLITTER_ID_TAG, clean_ids[0])
        span.set_tag(SPILLER_ID_TAG, clean_ids[0])
        span.set_baggage_item(SPLITTER_ID_HEADER, clean_ids[0])


def _set_batch_link_tags(span, batch_span):
    if not span or not batch_span:
        return

    if hasattr(batch_span, "trace_id"):
        span.set_tag(BATCH_TRACE_ID_TAG, format(batch_span.trace_id, "x"))
    if hasattr(batch_span, "span_id"):
        span.set_tag(BATCH_SPAN_ID_TAG, format(batch_span.span_id, "x"))


def get_active_trace_id():
    span = opentracing.global_tracer().active_span
    if hasattr(span, "trace_id"):
        return format(span.trace_id, "x")
    return None


@contextmanager
def start_item_span(
    operation_name,
    item_id,
    splitter_id=None,
    json_trace_id=None,
    message=None,
    link_active_batch=True,
    new_trace=False,
):
    if not tracing_enabled:
        yield None
        return

    tracer = opentracing.global_tracer()
    batch_span = tracer.active_span if link_active_batch else None
    references = []

    if not new_trace and message is not None:
        message_context = extract_message_context(message)
        if message_context:
            references.append(opentracing.child_of(message_context))

    with tracer.start_active_span(
        operation_name,
        references=references or None,
        ignore_active_span=bool(references) or new_trace,
    ) as scope:
        _set_batch_link_tags(scope.span, batch_span)
        set_active_span_item_tags([item_id])
        if json_trace_id:
            set_active_span_json_trace_tags([json_trace_id])
        if splitter_id:
            set_active_span_splitter_tags([splitter_id])
        yield scope.span


@contextmanager
def start_batch_span(operation_name, messages):
    if not tracing_enabled:
        yield None
        return

    tracer = opentracing.global_tracer()
    references = []
    for message in messages:
        if len(references) >= MAX_BATCH_TRACE_REFERENCES:
            break
        context = extract_message_context(message)
        if context:
            references.append(opentracing.follows_from(context))

    with tracer.start_active_span(operation_name, references=references or None) as scope:
        span = scope.span
        if not _clean_enrichment_span_attributes():
            span.set_tag("messaging.batch.size", len(messages))
            span.set_tag("messaging.batch.linked_trace_count", len(references))
            topics = sorted({message.topic() for message in messages if hasattr(message, "topic")})
            if topics:
                span.set_tag("messaging.kafka.topics", ",".join(topics))
        item_ids = get_messages_item_ids(messages)
        if _clean_enrichment_span_attributes():
            if len(item_ids) == 1:
                set_active_span_item_tags(item_ids)
        else:
            set_active_span_item_summary_tags(item_ids)
            set_active_span_json_trace_tags(get_messages_json_trace_ids(messages))
        if not _clean_enrichment_span_attributes():
            set_active_span_splitter_tags(get_messages_splitter_ids(messages))
        yield span


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


def flush_tracer():
    if not tracing_enabled:
        return

    tracer = opentracing.global_tracer()
    if hasattr(tracer, "flush"):
        tracer.flush()


def inject_trace_headers(headers=None, item_id=None, splitter_id=None, json_trace_id=None):
    if not tracing_enabled:
        return headers
    
    # Convert existing headers list to dict if present
    headers_dict = {}
    if headers:
        if isinstance(headers, list):
            headers_dict = {
                k: (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                for k, v in headers
            }
        elif isinstance(headers, dict):
            headers_dict = headers.copy()

    if item_id:
        headers_dict[ITEM_ID_HEADER] = str(item_id)
    if json_trace_id:
        headers_dict[JSON_TRACE_ID_HEADER] = str(json_trace_id)
    if splitter_id:
        headers_dict[SPLITTER_ID_HEADER] = str(splitter_id)
            
    # Inject active tracing context
    tracer = opentracing.global_tracer()
    active_span = tracer.active_span
    if active_span:
        tracer.inject(active_span.context, Format.TEXT_MAP, headers_dict)
        
    # Convert dict back to list of tuples for confluent-kafka
    return [
        (k, (v.encode('utf-8') if isinstance(v, str) else v))
        for k, v in headers_dict.items()
    ]


def inject_text_map_headers(headers=None, item_id=None, splitter_id=None, json_trace_id=None):
    if not tracing_enabled:
        return headers or {}

    headers_dict = {}
    if headers:
        headers_dict = {
            k: (v.decode('utf-8') if isinstance(v, bytes) else str(v))
            for k, v in headers.items()
        }

    if item_id:
        headers_dict[ITEM_ID_HEADER] = str(item_id)
        headers_dict[HTTP_ITEM_ID_HEADER] = str(item_id)
    if json_trace_id:
        headers_dict[JSON_TRACE_ID_HEADER] = str(json_trace_id)
    if splitter_id:
        headers_dict[SPLITTER_ID_HEADER] = str(splitter_id)

    tracer = opentracing.global_tracer()
    active_span = tracer.active_span
    if active_span:
        tracer.inject(active_span.context, Format.TEXT_MAP, headers_dict)

    return headers_dict
