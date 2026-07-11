import functools
import os
import time
from contextlib import contextmanager

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import Link, NonRecordingSpan, SpanKind, Status, StatusCode

from src.utils import config_provider


tracing_enabled = config_provider.get_tracing_enable()
ITEM_ID_HEADER = "pokemon.id"
HTTP_ITEM_ID_HEADER = "pokemon-id"
ITEM_ID_TAG = "pokemon.id"
ITEM_IDS_TAG = "pokemon.ids"
ITEM_IDS_COUNT_TAG = "pokemon.ids.count"
ITEM_IDS_SAMPLE_TAG = "pokemon.ids.sample"
JSON_TRACE_ID_HEADER = "json.trace_id"
JSON_TRACE_ALIAS_HEADER = "jsonTrace"
JSON_TRACE_ID_TAG = "json.trace_id"
JSON_TRACE_ALIAS_TAG = "jsonTrace"
JSON_TRACE_IDS_COUNT_TAG = "json.trace_ids.count"
JSON_TRACE_IDS_SAMPLE_TAG = "json.trace_ids.sample"
SPLITTER_ID_HEADER = "splitter.id"
SPLITTER_ID_TAG = "splitter.id"
SPLITTER_IDS_TAG = "splitter.ids"
SPILLER_ID_TAG = "spillerid"
BATCH_TRACE_ID_TAG = "batch.trace_id"
BATCH_SPAN_ID_TAG = "batch.span_id"
SPLIT_TS_MS_HEADER = "split.ts.ms"
SPLIT_TS_MS_TAG = "split.ts.ms"
MAX_BATCH_ID_SAMPLE = 20
MAX_BATCH_SPAN_LINKS = int(os.environ.get("MAX_BATCH_SPAN_LINKS", "100"))

ENRICHMENT_TRACE_SERVICES = {
    "pokemon-surname-generator",
    "surname-enrichment",
    "surname-api",
    "hbase-surname-updater",
}

_tracer_provider_initialized = False


def _clean_enrichment_span_attributes():
    return config_provider.get_service_name() in ENRICHMENT_TRACE_SERVICES


def init_tracer():
    global _tracer_provider_initialized
    if not tracing_enabled or _tracer_provider_initialized:
        return

    service_name = config_provider.get_elastic_index_name()
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://tempo:4318/v1/traces")
    print(f"Initializing OpenTelemetry tracer for service '{service_name}' targeting {endpoint}...", flush=True)

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    exporter = OTLPSpanExporter(endpoint=endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _tracer_provider_initialized = True


def _tracer():
    return trace.get_tracer(config_provider.get_elastic_index_name())


def _headers_to_text_map(headers):
    if not headers:
        return {}
    if isinstance(headers, dict):
        return {
            k: (v.decode("utf-8") if isinstance(v, bytes) else str(v))
            for k, v in headers.items()
        }
    return {
        k: (v.decode("utf-8") if isinstance(v, bytes) else str(v))
        for k, v in headers
    }


def _text_map_to_kafka_headers(headers_dict):
    return [
        (k, (v.encode("utf-8") if isinstance(v, str) else v))
        for k, v in headers_dict.items()
    ]


def _span_context_to_context(span_context):
    if not span_context or not span_context.is_valid:
        return None
    return trace.set_span_in_context(NonRecordingSpan(span_context))


def _extract_context_from_headers(headers):
    if not tracing_enabled:
        return None
    headers_dict = _headers_to_text_map(headers)
    if not headers_dict:
        return None
    return propagate.extract(headers_dict)


def extract_message_context(message):
    return _extract_context_from_headers(message.headers())


def extract_text_map_context(headers):
    return _extract_context_from_headers(headers)


def get_header_value(headers, key):
    headers_dict = _headers_to_text_map(headers)
    return headers_dict.get(key)


def get_message_item_id(message):
    return get_header_value(message.headers(), ITEM_ID_HEADER)


def get_message_splitter_id(message):
    return get_header_value(message.headers(), SPLITTER_ID_HEADER)


def get_message_json_trace_id(message):
    return (
        get_header_value(message.headers(), JSON_TRACE_ID_HEADER)
        or get_header_value(message.headers(), JSON_TRACE_ALIAS_HEADER)
    )


def get_message_split_ts_ms(message):
    return get_header_value(message.headers(), SPLIT_TS_MS_HEADER)


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


def _format_trace_id(trace_id):
    return format(trace_id, "032x") if trace_id else None


def _format_span_id(span_id):
    return format(span_id, "016x") if span_id else None


def get_active_trace_id():
    span_context = trace.get_current_span().get_span_context()
    if span_context and span_context.is_valid:
        return _format_trace_id(span_context.trace_id)
    return None


def get_active_span_id():
    span_context = trace.get_current_span().get_span_context()
    if span_context and span_context.is_valid:
        return _format_span_id(span_context.span_id)
    return None


def get_trace_id(_tracer=None):
    return get_active_trace_id() or "No_trace_ID"


def get_span_id(_tracer=None):
    return get_active_span_id() or "No_span_ID"


def _set_attr(span, key, value):
    if span and span.is_recording() and value is not None:
        span.set_attribute(key, value)


def get_current_span():
    return trace.get_current_span()


def set_current_span_error(exception_message=None):
    span = trace.get_current_span()
    if not span or not span.is_recording():
        return
    span.set_status(Status(StatusCode.ERROR, str(exception_message or "")))
    span.set_attribute("error", True)


def add_current_span_event(name, attributes=None):
    span = trace.get_current_span()
    if span and span.is_recording():
        span.add_event(name, attributes or {})


def set_active_span_item_tags(item_ids):
    if not tracing_enabled:
        return

    span = trace.get_current_span()
    clean_ids = [str(item_id) for item_id in item_ids if item_id]
    if not clean_ids:
        return

    if len(clean_ids) == 1:
        _set_attr(span, ITEM_ID_TAG, clean_ids[0])
    else:
        _set_attr(span, ITEM_IDS_TAG, ",".join(clean_ids))


def set_active_span_item_summary_tags(item_ids):
    if not tracing_enabled:
        return

    span = trace.get_current_span()
    clean_ids = [str(item_id) for item_id in item_ids if item_id]
    if not clean_ids:
        return

    if len(clean_ids) == 1:
        set_active_span_item_tags(clean_ids)
        return

    _set_attr(span, ITEM_IDS_COUNT_TAG, len(clean_ids))
    _set_attr(span, ITEM_IDS_SAMPLE_TAG, ",".join(clean_ids[:MAX_BATCH_ID_SAMPLE]))


def set_active_span_json_trace_tags(trace_ids):
    if not tracing_enabled:
        return

    span = trace.get_current_span()
    clean_ids = [str(trace_id) for trace_id in trace_ids if trace_id]
    if not clean_ids:
        return

    if len(clean_ids) == 1:
        _set_attr(span, JSON_TRACE_ID_TAG, clean_ids[0])
        _set_attr(span, JSON_TRACE_ALIAS_TAG, clean_ids[0])
    else:
        _set_attr(span, JSON_TRACE_IDS_COUNT_TAG, len(clean_ids))
        _set_attr(span, JSON_TRACE_IDS_SAMPLE_TAG, ",".join(clean_ids[:MAX_BATCH_ID_SAMPLE]))


def set_active_span_splitter_tags(splitter_ids):
    if not tracing_enabled:
        return

    span = trace.get_current_span()
    clean_ids = [str(splitter_id) for splitter_id in splitter_ids if splitter_id]
    if not clean_ids:
        return

    _set_attr(span, SPLITTER_IDS_TAG, ",".join(clean_ids))
    if len(clean_ids) == 1:
        _set_attr(span, SPLITTER_ID_TAG, clean_ids[0])
        _set_attr(span, SPILLER_ID_TAG, clean_ids[0])


def set_active_span_correlation_tags_from_headers(headers):
    item_id = get_header_value(headers, ITEM_ID_HEADER)
    if item_id:
        set_active_span_item_tags([item_id])

    json_trace_id = get_header_value(headers, JSON_TRACE_ID_HEADER) or get_header_value(headers, JSON_TRACE_ALIAS_HEADER)
    if json_trace_id:
        set_active_span_json_trace_tags([json_trace_id])

    splitter_id = get_header_value(headers, SPLITTER_ID_HEADER)
    if splitter_id:
        set_active_span_splitter_tags([splitter_id])

    split_ts_ms = get_header_value(headers, SPLIT_TS_MS_HEADER)
    if split_ts_ms:
        _set_attr(trace.get_current_span(), SPLIT_TS_MS_TAG, split_ts_ms)


def _set_batch_link_tags(span, batch_span):
    if not span or not batch_span:
        return
    span_context = batch_span.get_span_context()
    if not span_context or not span_context.is_valid:
        return

    _set_attr(span, BATCH_TRACE_ID_TAG, _format_trace_id(span_context.trace_id))
    _set_attr(span, BATCH_SPAN_ID_TAG, _format_span_id(span_context.span_id))


def _span_link_from_context(context):
    if context is None:
        return None
    span_context = trace.get_current_span(context).get_span_context()
    if span_context and span_context.is_valid:
        return Link(span_context)
    return None


def _span_link_from_span(span):
    if not span:
        return None
    span_context = span.get_span_context()
    if span_context and span_context.is_valid:
        return Link(span_context)
    return None


def traced_consumer(func=None, name=None):
    if func is None:
        return functools.partial(traced_consumer, name=name)
    operation_name = name or func.__name__

    @functools.wraps(func)
    def decorator(self, producer, consumer, message):
        if not tracing_enabled:
            return func(self, producer, consumer, message)

        parent_context = extract_message_context(message)
        batch_span = trace.get_current_span()
        links = []
        batch_link = _span_link_from_span(batch_span)
        if batch_link:
            links.append(batch_link)

        with _tracer().start_as_current_span(
            operation_name,
            context=parent_context,
            links=links,
            kind=SpanKind.CONSUMER,
        ) as span:
            _set_batch_link_tags(span, batch_span)
            set_active_span_correlation_tags_from_headers(message.headers())
            return func(self, producer, consumer, message)

    return decorator


def traced_function(func=None, name=None):
    if func is None:
        return functools.partial(traced_function, name=name)
    operation_name = name or func.__name__

    @functools.wraps(func)
    def decorator(*args, **kwargs):
        if not tracing_enabled:
            return func(*args, **kwargs)
        with _tracer().start_as_current_span(operation_name):
            return func(*args, **kwargs)

    return decorator


@contextmanager
def start_span(operation_name, parent_context=None, links=None, new_trace=False, kind=SpanKind.INTERNAL):
    if not tracing_enabled:
        yield None
        return

    context = otel_context.Context() if new_trace else parent_context
    with _tracer().start_as_current_span(
        operation_name,
        context=context,
        links=links or [],
        kind=kind,
    ) as span:
        yield span


@contextmanager
def start_item_span(
    operation_name,
    item_id,
    splitter_id=None,
    json_trace_id=None,
    message=None,
    link_active_batch=True,
    new_trace=False,
    split_ts_ms=None,
):
    if not tracing_enabled:
        yield None
        return

    batch_span = trace.get_current_span() if link_active_batch else None
    batch_link = _span_link_from_span(batch_span) if link_active_batch else None
    links = [batch_link] if batch_link else []
    parent_context = None
    if not new_trace and message is not None:
        parent_context = extract_message_context(message)

    with start_span(
        operation_name,
        parent_context=parent_context,
        links=links,
        new_trace=new_trace,
    ) as span:
        _set_batch_link_tags(span, batch_span)
        set_active_span_item_tags([item_id])
        if json_trace_id:
            set_active_span_json_trace_tags([json_trace_id])
        if splitter_id:
            set_active_span_splitter_tags([splitter_id])
        if split_ts_ms:
            _set_attr(span, SPLIT_TS_MS_TAG, split_ts_ms)
        if message is not None:
            message_split_ts_ms = get_message_split_ts_ms(message)
            if message_split_ts_ms:
                _set_attr(span, SPLIT_TS_MS_TAG, message_split_ts_ms)
        yield span


@contextmanager
def start_batch_span(operation_name, messages):
    if not tracing_enabled:
        yield None
        return

    links = []
    dropped_link_count = 0
    for message in messages:
        context = extract_message_context(message)
        link = _span_link_from_context(context)
        if not link:
            continue
        if len(links) < MAX_BATCH_SPAN_LINKS:
            links.append(link)
        else:
            dropped_link_count += 1

    with _tracer().start_as_current_span(
        operation_name,
        context=otel_context.Context(),
        links=links,
        kind=SpanKind.CONSUMER,
    ) as span:
        if not _clean_enrichment_span_attributes():
            _set_attr(span, "messaging.batch.size", len(messages))
            _set_attr(span, "messaging.batch.linked_trace_count", len(links))
            _set_attr(span, "messaging.batch.dropped_link_count", dropped_link_count)
            _set_attr(span, "messaging.batch.max_links", MAX_BATCH_SPAN_LINKS)
            topics = sorted({message.topic() for message in messages if hasattr(message, "topic")})
            if topics:
                _set_attr(span, "messaging.kafka.topics", ",".join(topics))

        item_ids = get_messages_item_ids(messages)
        if _clean_enrichment_span_attributes():
            if len(item_ids) == 1:
                set_active_span_item_tags(item_ids)
        else:
            set_active_span_item_summary_tags(item_ids)
            set_active_span_json_trace_tags(get_messages_json_trace_ids(messages))
            set_active_span_splitter_tags(get_messages_splitter_ids(messages))
        yield span


def flush_tracer():
    if not tracing_enabled:
        return
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()


def inject_trace_headers(headers=None, item_id=None, splitter_id=None, json_trace_id=None, split_ts_ms=None):
    if not tracing_enabled:
        return headers

    headers_dict = _headers_to_text_map(headers)
    if item_id:
        headers_dict[ITEM_ID_HEADER] = str(item_id)
    if json_trace_id:
        headers_dict[JSON_TRACE_ID_HEADER] = str(json_trace_id)
        headers_dict[JSON_TRACE_ALIAS_HEADER] = str(json_trace_id)
    if splitter_id:
        headers_dict[SPLITTER_ID_HEADER] = str(splitter_id)
    if split_ts_ms:
        headers_dict[SPLIT_TS_MS_HEADER] = str(split_ts_ms)

    propagate.inject(headers_dict)
    return _text_map_to_kafka_headers(headers_dict)


def inject_text_map_headers(headers=None, item_id=None, splitter_id=None, json_trace_id=None, split_ts_ms=None):
    if not tracing_enabled:
        return headers or {}

    headers_dict = _headers_to_text_map(headers)
    if item_id:
        headers_dict[ITEM_ID_HEADER] = str(item_id)
        headers_dict[HTTP_ITEM_ID_HEADER] = str(item_id)
    if json_trace_id:
        headers_dict[JSON_TRACE_ID_HEADER] = str(json_trace_id)
        headers_dict[JSON_TRACE_ALIAS_HEADER] = str(json_trace_id)
    if splitter_id:
        headers_dict[SPLITTER_ID_HEADER] = str(splitter_id)
    if split_ts_ms:
        headers_dict[SPLIT_TS_MS_HEADER] = str(split_ts_ms)

    propagate.inject(headers_dict)
    return headers_dict


def now_ms():
    return int(time.time() * 1000)
