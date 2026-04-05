import argparse
import json
import os

from concurrent import futures

import grpc
import kv_pb2
import kv_pb2_grpc

from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


parser = argparse.ArgumentParser()
parser.add_argument("--node-id", required=True, help="ID of this node")
parser.add_argument("--host", required=True, help="Host of this node")
parser.add_argument("--port", required=True, help="Port of this node")

args = parser.parse_args()

node_id = args.node_id
host = args.host
port = args.port

store = {}

with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

TOMBSTONE = CONFIG["tombstone"]

OTEL_EXPORTER_OTLP_ENDPOINT = "10.0.0.211:4317"
OTEL_INSECURE = True


def setup_tracing() -> trace.Tracer:
    resource = Resource.create(
        {
            "service.name": "quorum-server",
            "service.instance.id": node_id,
            "quorum.node.id": node_id,
            "quorum.role": "server",
        }
    )

    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint=OTEL_EXPORTER_OTLP_ENDPOINT,
        insecure=OTEL_INSECURE,
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer(__name__)


tracer = setup_tracing()


class MetadataGetter:
    def get(self, carrier, key):
        if carrier is None:
            return []
        return [v for k, v in carrier if k == key]

    def keys(self, carrier):
        if carrier is None:
            return []
        return [k for k, _ in carrier]


def version_of(entry):
    return (entry["timestamp"], entry["client_id"])


class KVServer(kv_pb2_grpc.KVStoreServicer):
    def Get(self, request, context):
        parent_ctx = extract(context.invocation_metadata(), getter=MetadataGetter())

        with tracer.start_as_current_span(
            "server.get",
            context=parent_ctx,
            kind=SpanKind.SERVER,
        ) as span:
            key = request.key

            span.set_attribute("db.operation", "get")
            span.set_attribute("kv.key", key)
            span.set_attribute("server.node_id", node_id)

            span.add_event("server_get_received")

            if key not in store:
                span.add_event("server_get_not_found")
                return kv_pb2.GetReply(found=False)

            entry = store[key]

            if entry["value"] == TOMBSTONE:
                span.add_event("server_get_tombstone")
                return kv_pb2.GetReply(found=False)

            span.add_event("server_get_found", {"timestamp": entry["timestamp"]})

            return kv_pb2.GetReply(
                found=True,
                value=entry["value"],
                timestamp=entry["timestamp"],
                client_id=entry["client_id"],
                request_id=entry["request_id"],
            )

    def Put(self, request, context):
        parent_ctx = extract(context.invocation_metadata(), getter=MetadataGetter())

        with tracer.start_as_current_span(
            "server.put",
            context=parent_ctx,
            kind=SpanKind.SERVER,
        ) as span:
            key = request.key

            span.set_attribute("db.operation", "put")
            span.set_attribute("kv.key", key)
            span.set_attribute("server.node_id", node_id)
            span.set_attribute("quorum.request_id", request.request_id)
            span.set_attribute("quorum.timestamp", request.timestamp)

            span.add_event("server_put_received")

            incoming = {
                "value": request.value,
                "timestamp": request.timestamp,
                "client_id": request.client_id,
                "request_id": request.request_id,
            }

            if key not in store:
                store[key] = incoming
                span.add_event("server_put_inserted")
            else:
                existing = store[key]

                if version_of(incoming) > version_of(existing):
                    store[key] = incoming
                    span.add_event("server_put_overwrote_existing")
                else:
                    span.add_event("server_put_ignored_older_version")

            return kv_pb2.PutReply(success=True)

    def Delete(self, request, context):
        parent_ctx = extract(context.invocation_metadata(), getter=MetadataGetter())

        with tracer.start_as_current_span(
            "server.delete",
            context=parent_ctx,
            kind=SpanKind.SERVER,
        ) as span:
            key = request.key

            span.set_attribute("db.operation", "delete")
            span.set_attribute("kv.key", key)
            span.set_attribute("server.node_id", node_id)

            span.add_event("server_delete_received")

            store[key] = {
                "value": TOMBSTONE,
                "timestamp": 0,
                "client_id": "",
                "request_id": "",
            }

            span.add_event("server_delete_tombstone_written")

            return kv_pb2.DeleteReply(success=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    kv_pb2_grpc.add_KVStoreServicer_to_server(KVServer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server {node_id} started on {host}:{port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()