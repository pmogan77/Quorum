import argparse
import json
import os
import time
import uuid

from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc
import kv_pb2
import kv_pb2_grpc

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


parser = argparse.ArgumentParser()
parser.add_argument("--node-id", required=True, help="ID of this node")
args = parser.parse_args()

node_id = args.node_id

with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

NODES = CONFIG["nodes"]
R = CONFIG["R"]
W = CONFIG["W"]
TOMBSTONE = CONFIG["tombstone"]

CLIENT_ID = str(uuid.uuid4())
TIMEOUT = 2

OTEL_EXPORTER_OTLP_ENDPOINT = "10.0.0.211:4317"
OTEL_INSECURE = True
ENABLE_TRACING = False


def setup_tracing() -> trace.Tracer:
    if not ENABLE_TRACING:
        return trace.get_tracer(__name__)
    resource = Resource.create(
        {
            "service.name": "quorum-coordinator",
            "service.instance.id": node_id,
            "quorum.node.id": node_id,
            "quorum.role": "coordinator",
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


class MetadataSetter:
    def set(self, carrier, key, value):
        carrier.append((key, value))


CHANNELS = {}
STUBS = {}

for name, node in NODES.items():
    addr = f"{node['host']}:{node['port']}"
    channel = grpc.insecure_channel(addr)
    grpc.channel_ready_future(channel).result(timeout=5)
    stub = kv_pb2_grpc.KVStoreStub(channel)
    CHANNELS[name] = channel
    STUBS[name] = stub

EXECUTOR = ThreadPoolExecutor(max_workers=len(NODES))


def quorum_put(key, value):
    request_id = str(uuid.uuid4())
    timestamp = time.time()
    successes = 0

    parent_ctx = otel_context.get_current()
    parent_span = trace.get_current_span()

    def call(node, ctx):
        token = otel_context.attach(ctx)
        try:
            with tracer.start_as_current_span("rpc.put_replica", kind=SpanKind.CLIENT) as span:
                span.set_attribute("rpc.system", "grpc")
                span.set_attribute("rpc.method", "Put")
                span.set_attribute("server.node.id", node)
                span.set_attribute("kv.key", key)
                span.set_attribute("quorum.request_id", request_id)
                span.set_attribute("quorum.timestamp", timestamp)

                metadata = []
                inject(metadata, setter=MetadataSetter())

                span.add_event("replica_put_send")

                try:
                    resp = STUBS[node].Put(
                        kv_pb2.PutRequest(
                            key=key,
                            value=value,
                            timestamp=timestamp,
                            client_id=CLIENT_ID,
                            request_id=request_id,
                        ),
                        timeout=TIMEOUT,
                        metadata=metadata,
                    )
                    span.set_attribute("rpc.success", resp.success)
                    span.add_event("replica_put_reply", {"success": resp.success})

                    if not resp.success:
                        span.set_status(Status(StatusCode.ERROR, "replica put returned false"))

                    return resp.success
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.add_event("replica_put_failed")
                    return False
        finally:
            otel_context.detach(token)

    futures = [EXECUTOR.submit(call, n, parent_ctx) for n in STUBS]

    for f in as_completed(futures):
        if f.result():
            successes += 1

        parent_span.add_event(
            "replica_put_result_processed",
            {
                "successes_so_far": successes,
                "required_w": W,
            },
        )

        if successes >= W:
            parent_span.add_event("write_quorum_reached", {"successes": successes})
            return True

    parent_span.add_event(
        "write_quorum_failed",
        {
            "successes": successes,
            "required_w": W,
        },
    )
    return False


def quorum_get(key):
    replies = 0
    responses = []

    parent_ctx = otel_context.get_current()
    parent_span = trace.get_current_span()

    def call(node, ctx):
        token = otel_context.attach(ctx)
        try:
            with tracer.start_as_current_span("rpc.get_replica", kind=SpanKind.CLIENT) as span:
                span.set_attribute("rpc.system", "grpc")
                span.set_attribute("rpc.method", "Get")
                span.set_attribute("server.node.id", node)
                span.set_attribute("kv.key", key)

                metadata = []
                inject(metadata, setter=MetadataSetter())

                span.add_event("replica_get_send")

                try:
                    resp = STUBS[node].Get(
                        kv_pb2.GetRequest(key=key),
                        timeout=TIMEOUT,
                        metadata=metadata,
                    )
                    span.set_attribute("rpc.found", resp.found)
                    span.add_event("replica_get_reply", {"found": resp.found})
                    return resp
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.add_event("replica_get_failed")
                    return None
        finally:
            otel_context.detach(token)

    futures = [EXECUTOR.submit(call, n, parent_ctx) for n in STUBS]

    for f in as_completed(futures):
        r = f.result()

        if r is not None:
            replies += 1
            if r.found:
                responses.append(r)

        parent_span.add_event(
            "replica_get_result_processed",
            {
                "replies_so_far": replies,
                "found_so_far": len(responses),
                "required_r": R,
            },
        )

        if replies >= R:
            break

    if replies < R:
        parent_span.add_event(
            "read_quorum_failed",
            {
                "replies": replies,
                "required_r": R,
            },
        )
        return "QUORUM_FAILED", None

    if len(responses) == 0:
        parent_span.add_event("read_not_found")
        return "NOT_FOUND", None

    latest = max(responses, key=lambda r: (r.timestamp, r.client_id))
    parent_span.add_event(
        "read_value_selected",
        {
            "timestamp": latest.timestamp,
        },
    )
    
    if latest.value == TOMBSTONE:
        parent_span.add_event("read_latest_is_tombstone")
        return "NOT_FOUND", None

    return "OK", latest.value


class AgentService(kv_pb2_grpc.AgentKVServicer):
    def Put(self, request, context):
        with tracer.start_as_current_span("coordinator.put", kind=SpanKind.SERVER) as span:
            span.set_attribute("db.operation", "put")
            span.set_attribute("kv.key", request.key)
            span.set_attribute("kv.value_length", len(request.value))
            span.set_attribute("quorum.w", W)
            span.set_attribute("quorum.cluster_size", len(NODES))
            span.set_attribute("coordinator.node_id", node_id)

            span.add_event("client_put_received")

            ok = quorum_put(request.key, request.value)

            span.set_attribute("quorum.success", ok)
            span.add_event("client_put_completed", {"success": ok})

            if not ok:
                span.set_status(Status(StatusCode.ERROR, "write quorum failed"))

            return kv_pb2.AgentPutReply(success=ok)

    def Delete(self, request, context):
        with tracer.start_as_current_span("coordinator.delete", kind=SpanKind.SERVER) as span:
            span.set_attribute("db.operation", "delete")
            span.set_attribute("kv.key", request.key)
            span.set_attribute("quorum.w", W)
            span.set_attribute("quorum.cluster_size", len(NODES))
            span.set_attribute("coordinator.node_id", node_id)

            span.add_event("client_delete_received")

            ok = quorum_put(request.key, TOMBSTONE)

            span.set_attribute("quorum.success", ok)
            span.add_event("client_delete_completed", {"success": ok})

            if not ok:
                span.set_status(Status(StatusCode.ERROR, "delete quorum failed"))

            return kv_pb2.AgentDeleteReply(success=ok)

    def Get(self, request, context):
        with tracer.start_as_current_span("coordinator.get", kind=SpanKind.SERVER) as span:
            span.set_attribute("db.operation", "get")
            span.set_attribute("kv.key", request.key)
            span.set_attribute("quorum.r", R)
            span.set_attribute("quorum.cluster_size", len(NODES))
            span.set_attribute("coordinator.node_id", node_id)

            span.add_event("client_get_received")

            status, value = quorum_get(request.key)

            span.set_attribute("quorum.result_status", status)
            span.add_event("client_get_completed", {"status": status})

            if status == "QUORUM_FAILED":
                span.set_status(Status(StatusCode.ERROR, "read quorum failed"))
                return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.QUORUM_FAILED)

            if status == "NOT_FOUND":
                return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.NOT_FOUND)

            return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.OK, value=value)


def serve():
    server = grpc.server(ThreadPoolExecutor(max_workers=32))
    kv_pb2_grpc.add_AgentKVServicer_to_server(AgentService(), server)
    server.add_insecure_port("[::]:6000")
    server.start()
    print(f"Coordinator {node_id} started on port 6000")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()