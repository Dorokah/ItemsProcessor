import functools
import os
from contextlib import contextmanager

from opentelemetry import context, propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Link, SpanKind

from src.utils import config_provider

tracing_enabled = config_provider.get_tracing_enable()
_instrumented = False


def init_tracer():
    if not tracing_enabled:
        return

    global _instrumented
    service_name = config_provider.get_service_name()
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": config_provider.get_service_version(),
            "deployment.environment": os.environ.get("DEPLOYMENT_ENVIRONMENT", "local"),
        }
    )

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)

    if not _instrumented:
        RequestsInstrumentor().instrument()
        _instrumented = True

    print(f"Initializing OpenTelemetry tracer for service '{service_name}' targeting {endpoint}...", flush=True)


def get_tracer():
    return trace.get_tracer(__name__)


def traced_consumer(func=None, name=None):
    if func is None:
        return functools.partial(traced_consumer, name=name)

    operation_name = name or func.__name__

    @functools.wraps(func)
    def decorator(self, producer, consumer, message):
        if tracing_enabled:
            with trace_message(message, operation_name):
                return func(self, producer, consumer, message)
        return func(self, producer, consumer, message)

    return decorator


def get_trace_id(_tracer=None):
    span_context = trace.get_current_span().get_span_context()
    if span_context.is_valid:
        return format(span_context.trace_id, "032x")
    return "No_trace_ID"


def get_span_id(_tracer=None):
    span_context = trace.get_current_span().get_span_context()
    if span_context.is_valid:
        return format(span_context.span_id, "016x")
    return "No_span_ID"


def inject_trace_headers(headers=None):
    if not tracing_enabled:
        return headers

    headers_dict = _headers_to_text_map(headers)
    propagate.inject(headers_dict)
    return [(k, v.encode("utf-8")) for k, v in headers_dict.items()]


def extract_trace_context(message):
    if not tracing_enabled:
        return context.Context()
    return propagate.extract(_headers_to_text_map(message.headers()))


def extract_span_link(message, attributes=None):
    extracted_context = extract_trace_context(message)
    span_context = trace.get_current_span(extracted_context).get_span_context()
    if span_context.is_valid:
        return Link(span_context, attributes=attributes or {})
    return None


def extract_span_links(messages, attributes=None):
    if not tracing_enabled:
        return []

    links = []
    for idx, message in enumerate(messages):
        link_attributes = dict(attributes or {})
        link_attributes["messaging.batch.message_index"] = idx
        link = extract_span_link(message, link_attributes)
        if link:
            links.append(link)
    return links


@contextmanager
def trace_message(message, operation_name="process_message", extra_links=None, attributes=None):
    if not tracing_enabled:
        yield None
        return

    parent_context = extract_trace_context(message)
    links = _compact_links(extra_links)
    tracer = get_tracer()
    span_context = trace.get_current_span(parent_context).get_span_context()
    span_options = {
        "kind": SpanKind.CONSUMER,
        "links": links,
        "attributes": attributes or {},
    }
    if span_context.is_valid:
        span_options["context"] = parent_context

    with tracer.start_as_current_span(operation_name, **span_options) as span:
        yield span


@contextmanager
def trace_span(operation_name, links=None, attributes=None, kind=SpanKind.INTERNAL):
    if not tracing_enabled:
        yield None
        return

    tracer = get_tracer()
    with tracer.start_as_current_span(
        operation_name,
        kind=kind,
        links=_compact_links(links),
        attributes=attributes or {},
    ) as span:
        yield span


def link_current_span(attributes=None):
    if not tracing_enabled:
        return None

    span_context = trace.get_current_span().get_span_context()
    if span_context.is_valid:
        return Link(span_context, attributes=attributes or {})
    return None


def _compact_links(links):
    return [link for link in links or [] if link]


def _headers_to_text_map(headers):
    if not headers:
        return {}

    if isinstance(headers, dict):
        items = headers.items()
    else:
        items = headers

    return {
        key: (value.decode("utf-8") if isinstance(value, bytes) else str(value))
        for key, value in items
        if value is not None
    }
