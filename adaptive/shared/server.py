import argparse
import json
import random
import threading
from concurrent import futures

import grpc
import kv_pb2
import kv_pb2_grpc

from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


parser = argparse.ArgumentParser()
parser.add_argument("--node-id", required=True, help="ID of this server node")
parser.add_argument("--host", required=True, help="Host of this server node")
parser.add_argument("--port", required=True, help="Port of this server node")
args = parser.parse_args()

node_id = args.node_id
host = args.host
port = args.port

with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

NODES = CONFIG["nodes"]
TOMBSTONE = CONFIG["tombstone"]

OTEL_EXPORTER_OTLP_ENDPOINT = "10.0.0.211:4317"
OTEL_INSECURE = True
ENABLE_TRACING = False

ANTI_ENTROPY_MIN_SECONDS = 300
ANTI_ENTROPY_MAX_SECONDS = 800
ANTI_ENTROPY_RPC_TIMEOUT = 5
NUM_SHARDS = 32

shutdown_event = threading.Event()

CHANNELS = {}
STUBS = {}


def setup_tracing() -> trace.Tracer:
    if not ENABLE_TRACING:
        return trace.get_tracer(__name__)
    resource = Resource.create(
        {
            "service.name": "adaptive-quorum-server",
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


class MetadataSetter:
    def set(self, carrier, key, value):
        carrier.append((key, value))


store_shards = [dict() for _ in range(NUM_SHARDS)]
shard_locks = [threading.Lock() for _ in range(NUM_SHARDS)]


def shard_index(key: str) -> int:
    return hash(key) % NUM_SHARDS


def version_of(entry):
    return (entry["timestamp"], entry["client_id"])


def build_channels():
    for name, node in NODES.items():
        if name == node_id:
            continue

        addr = f"{node['host']}:{node['port']}"
        channel = grpc.insecure_channel(addr)
        # grpc.channel_ready_future(channel).result(timeout=5)
        stub = kv_pb2_grpc.KVStoreStub(channel)
        CHANNELS[name] = channel
        STUBS[name] = stub


def choose_random_peer():
    peers = [name for name in NODES.keys() if name != node_id]
    if not peers:
        return None
    return random.choice(peers)


def make_entry(value, timestamp, client_id, request_id):
    return {
        "value": value,
        "timestamp": timestamp,
        "client_id": client_id,
        "request_id": request_id,
    }


def get_entry_for_key(key):
    idx = shard_index(key)
    with shard_locks[idx]:
        entry = store_shards[idx].get(key)
        if entry is None:
            return None
        return entry.copy()


def upsert_if_newer(key, incoming):
    idx = shard_index(key)
    with shard_locks[idx]:
        existing = store_shards[idx].get(key)

        if existing is None:
            store_shards[idx][key] = incoming
            return True, "inserted"

        if version_of(incoming) > version_of(existing):
            store_shards[idx][key] = incoming
            return True, "overwrote"

        return False, "ignored"


def take_exact_snapshot():
    """
    Exact snapshot:
    acquire all shard locks in a fixed global order,
    copy each shard,
    then release all locks.
    """
    for lock in shard_locks:
        lock.acquire()

    try:
        snapshot = {}
        for shard in store_shards:
            for key, entry in shard.items():
                snapshot[key] = entry.copy()
        return snapshot
    finally:
        for lock in reversed(shard_locks):
            lock.release()


def send_snapshot_to_peer(peer_name):
    stub = STUBS.get(peer_name)
    if stub is None:
        return

    with tracer.start_as_current_span(
        "anti_entropy.send_snapshot",
        kind=SpanKind.INTERNAL,
    ) as span:
        span.set_attribute("server.node_id", node_id)
        span.set_attribute("peer.node_id", peer_name)

        span.add_event("anti_entropy_snapshot_start")
        snapshot = take_exact_snapshot()
        span.set_attribute("anti_entropy.snapshot_keys", len(snapshot))
        span.add_event("anti_entropy_snapshot_complete")

        entries = []
        for key, entry in snapshot.items():
            entries.append(
                kv_pb2.SyncEntry(
                    key=key,
                    value=entry["value"],
                    timestamp=entry["timestamp"],
                    client_id=entry["client_id"],
                    request_id=entry["request_id"],
                )
            )

        req = kv_pb2.SyncRequest(
            from_node_id=node_id,
            entries=entries,
        )

        metadata = []
        inject(metadata, setter=MetadataSetter())

        try:
            span.add_event("anti_entropy_rpc_start")
            reply = stub.Sync(req, timeout=ANTI_ENTROPY_RPC_TIMEOUT, metadata=metadata)
            span.set_attribute("anti_entropy.peer_received_entries", reply.received_entries)
            span.set_attribute("anti_entropy.peer_applied_entries", reply.applied_entries)
            span.add_event("anti_entropy_rpc_success")
        except grpc.RpcError as e:
            span.record_exception(e)
            span.add_event(
                "anti_entropy_rpc_failed",
                {
                    "grpc.code": str(e.code()),
                    "grpc.details": e.details() or "",
                },
            )


def anti_entropy_loop():
    while not shutdown_event.is_set():
        sleep_seconds = random.randint(ANTI_ENTROPY_MIN_SECONDS, ANTI_ENTROPY_MAX_SECONDS)

        if shutdown_event.wait(sleep_seconds):
            break

        peer = choose_random_peer()
        if peer is None:
            continue

        send_snapshot_to_peer(peer)

def dump_all_entries():
    snapshot = take_exact_snapshot()
    return snapshot

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

            entry = get_entry_for_key(key)

            if entry is None:
                span.add_event("server_get_not_found")
                return kv_pb2.GetReply(found=False)

            if entry["value"] == TOMBSTONE:
                span.add_event(
                    "server_get_tombstone_found",
                    {
                        "timestamp": entry["timestamp"],
                    },
                )
            else:
                span.add_event(
                    "server_get_found",
                    {
                        "timestamp": entry["timestamp"],
                    },
                )

            span.add_event(
                "server_get_found",
                {
                    "timestamp": entry["timestamp"],
                },
            )

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

            incoming = make_entry(
                value=request.value,
                timestamp=request.timestamp,
                client_id=request.client_id,
                request_id=request.request_id,
            )

            applied, reason = upsert_if_newer(key, incoming)

            if applied and reason == "inserted":
                span.add_event("server_put_inserted")
            elif applied and reason == "overwrote":
                span.add_event("server_put_overwrote_existing")
            else:
                span.add_event("server_put_ignored_older_version")

            return kv_pb2.PutReply(success=True)

    def Sync(self, request, context):
        parent_ctx = extract(context.invocation_metadata(), getter=MetadataGetter())

        with tracer.start_as_current_span(
            "server.sync",
            context=parent_ctx,
            kind=SpanKind.SERVER,
        ) as span:
            span.set_attribute("server.node_id", node_id)
            span.set_attribute("peer.node_id", request.from_node_id)
            span.set_attribute("anti_entropy.received_entries", len(request.entries))
            span.add_event("server_sync_received")

            applied_entries = 0

            for item in request.entries:
                incoming = make_entry(
                    value=item.value,
                    timestamp=item.timestamp,
                    client_id=item.client_id,
                    request_id=item.request_id,
                )

                applied, _ = upsert_if_newer(item.key, incoming)
                if applied:
                    applied_entries += 1

            span.set_attribute("anti_entropy.applied_entries", applied_entries)
            span.add_event("server_sync_complete")

            return kv_pb2.SyncReply(
                success=True,
                received_entries=len(request.entries),
                applied_entries=applied_entries,
            )

    def DumpState(self, request, context):
        parent_ctx = extract(context.invocation_metadata(), getter=MetadataGetter())

        with tracer.start_as_current_span(
            "server.dump_state",
            context=parent_ctx,
            kind=SpanKind.SERVER,
        ) as span:
            span.set_attribute("server.node_id", node_id)

            snapshot = take_exact_snapshot()
            span.set_attribute("dump.entry_count", len(snapshot))

            entries = []
            for key, entry in snapshot.items():
                entries.append(
                    kv_pb2.DumpEntry(
                        key=key,
                        value=entry["value"],
                        timestamp=entry["timestamp"],
                        client_id=entry["client_id"],
                        request_id=entry["request_id"],
                    )
                )

            return kv_pb2.DumpStateReply(entries=entries)

def serve():
    build_channels()

    anti_thread = threading.Thread(
        target=anti_entropy_loop,
        name=f"anti-entropy-{node_id}",
        daemon=True,
    )
    anti_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    kv_pb2_grpc.add_KVStoreServicer_to_server(KVServer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    print(f"Adaptive server {node_id} started on {host}:{port}")

    try:
        server.wait_for_termination()
    finally:
        shutdown_event.set()
        for channel in CHANNELS.values():
            channel.close()


if __name__ == "__main__":
    serve()