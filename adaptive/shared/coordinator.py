import argparse
import json
import time
import uuid

from concurrent.futures import ThreadPoolExecutor

import grpc
import kv_pb2
import kv_pb2_grpc

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from adaptive_quorum import AdaptiveQuorumManager


parser = argparse.ArgumentParser()
parser.add_argument("--node-id", required=True, help="ID of this coordinator node")
args = parser.parse_args()

node_id = args.node_id

with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

NODES = CONFIG["nodes"]
TOMBSTONE = CONFIG["tombstone"]

CLIENT_ID = str(uuid.uuid4())
TIMEOUT = 2
ENABLE_TRACING = True

COLLECTOR_CFG = CONFIG["observability"]["collector"]
OTEL_EXPORTER_OTLP_ENDPOINT = f"{COLLECTOR_CFG['host']}:{COLLECTOR_CFG['otlp_grpc_port']}"
OTEL_INSECURE = True

POLICY_CHANGE_LIKELIHOOD = CONFIG.get("adaptive_policy", {}).get("policy_change_likelihood", 1.0)


def setup_tracing() -> trace.Tracer:
    if not ENABLE_TRACING:
        return trace.get_tracer(__name__)

    resource = Resource.create(
        {
            "service.name": "adaptive-quorum-coordinator",
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

AQM = AdaptiveQuorumManager(
    config=CONFIG,
    stubs=STUBS,
    executor=EXECUTOR,
    client_id=CLIENT_ID,
    timeout=TIMEOUT,
    policy_change_likelihood=POLICY_CHANGE_LIKELIHOOD,
    tracer=tracer,
    node_id=node_id,
    enable_tracing=ENABLE_TRACING,
)


class AgentService(kv_pb2_grpc.AgentKVServicer):
    def Put(self, request, context):
        with tracer.start_as_current_span("coordinator.put", kind=SpanKind.SERVER) as span:
            # current_quorum = AQM.get_quorum(request.key)

            span.set_attribute("db.operation", "put")
            span.set_attribute("kv.key", request.key)
            span.set_attribute("kv.value_length", len(request.value))
            # span.set_attribute("quorum.r", current_quorum["R"])
            # span.set_attribute("quorum.w", current_quorum["W"])
            span.set_attribute("quorum.cluster_size", len(NODES))
            span.set_attribute("coordinator.node_id", node_id)
            span.set_attribute("adaptive.state", AQM.get_state(request.key))

            span.add_event("client_put_received")

            start = time.perf_counter()
            ok = AQM.quorum_put(request.key, request.value)
            duration_ms = (time.perf_counter() - start) * 1000.0

            span.set_attribute("quorum.success", ok)
            span.set_attribute("operation.duration_ms", duration_ms)
            span.add_event(
                "client_put_completed",
                {
                    "success": ok,
                    "duration_ms": duration_ms,
                },
            )

            AQM.async_record_write(request.key, trace.get_current_span().get_span_context())

            if not ok:
                span.set_status(Status(StatusCode.ERROR, "write quorum failed"))

            return kv_pb2.AgentPutReply(success=ok)

    def Delete(self, request, context):
        with tracer.start_as_current_span("coordinator.delete", kind=SpanKind.SERVER) as span:
            # current_quorum = AQM.get_quorum(request.key)

            span.set_attribute("db.operation", "delete")
            span.set_attribute("kv.key", request.key)
            # span.set_attribute("quorum.r", current_quorum["R"])
            # span.set_attribute("quorum.w", current_quorum["W"])
            span.set_attribute("quorum.cluster_size", len(NODES))
            span.set_attribute("coordinator.node_id", node_id)
            span.set_attribute("adaptive.state", AQM.get_state(request.key))

            span.add_event("client_delete_received")

            start = time.perf_counter()
            ok = AQM.quorum_put(request.key, TOMBSTONE)
            duration_ms = (time.perf_counter() - start) * 1000.0

            span.set_attribute("quorum.success", ok)
            span.set_attribute("operation.duration_ms", duration_ms)
            span.add_event(
                "client_delete_completed",
                {
                    "success": ok,
                    "duration_ms": duration_ms,
                },
            )

            AQM.async_record_write(request.key, trace.get_current_span().get_span_context())

            if not ok:
                span.set_status(Status(StatusCode.ERROR, "delete quorum failed"))

            return kv_pb2.AgentDeleteReply(success=ok)

    def Get(self, request, context):
        with tracer.start_as_current_span("coordinator.get", kind=SpanKind.SERVER) as span:
            # current_quorum = AQM.get_quorum(request.key)

            span.set_attribute("db.operation", "get")
            span.set_attribute("kv.key", request.key)
            # span.set_attribute("quorum.r", current_quorum["R"])
            # span.set_attribute("quorum.w", current_quorum["W"])
            span.set_attribute("quorum.cluster_size", len(NODES))
            span.set_attribute("coordinator.node_id", node_id)
            span.set_attribute("adaptive.state", AQM.get_state(request.key))

            span.add_event("client_get_received")

            start = time.perf_counter()
            status, value = AQM.quorum_get(request.key)
            duration_ms = (time.perf_counter() - start) * 1000.0

            span.set_attribute("quorum.result_status", status)
            span.set_attribute("operation.duration_ms", duration_ms)
            span.add_event(
                "client_get_completed",
                {
                    "status": status,
                    "duration_ms": duration_ms,
                },
            )

            AQM.async_record_read(request.key, trace.get_current_span().get_span_context())

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
    print(f"Adaptive coordinator {node_id} started on port 6000")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()