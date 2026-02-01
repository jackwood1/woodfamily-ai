from __future__ import annotations

import os
import sys
from typing import Optional, Type

try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
except Exception:  # pragma: no cover - optional dependency resolution
    trace = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    ConsoleSpanExporter = None
    OTLPSpanExporter = None


def init_observability(service_name: str = "woodfamily.ai") -> None:
    if "pytest" in sys.modules:
        return
    if trace is None or TracerProvider is None:
        return

    if isinstance(trace.get_tracer_provider(), TracerProvider):
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if otlp_endpoint and OTLPSpanExporter is not None and BatchSpanProcessor is not None:
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    elif BatchSpanProcessor is not None and ConsoleSpanExporter is not None:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
