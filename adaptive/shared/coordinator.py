import grpc
import json
import uuid

from concurrent.futures import ThreadPoolExecutor

import kv_pb2
import kv_pb2_grpc

from adaptive_quorum import AdaptiveQuorumManager


# -------------------------------
# Load cluster configuration
# -------------------------------

with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

NODES = CONFIG["nodes"]
TOMBSTONE = CONFIG["tombstone"]

CLIENT_ID = str(uuid.uuid4())
TIMEOUT = 2


# -------------------------------
# Build gRPC stubs
# -------------------------------

CHANNELS = {}
STUBS = {}

for name, node in NODES.items():

    addr = f"{node['host']}:{node['port']}"

    channel = grpc.insecure_channel(addr)
    stub = kv_pb2_grpc.KVStoreStub(channel)

    CHANNELS[name] = channel
    STUBS[name] = stub


EXECUTOR = ThreadPoolExecutor(max_workers=len(NODES))


# -------------------------------
# Adaptive quorum manager
# -------------------------------

aq = AdaptiveQuorumManager(
    CONFIG,
    STUBS,
    EXECUTOR,
    CLIENT_ID,
    TIMEOUT
)


# -------------------------------
# Agent service
# -------------------------------

class AgentService(kv_pb2_grpc.AgentKVServicer):

    def Put(self, request, context):

        ok = aq.quorum_put(request.key, request.value)

        aq.async_record_write(request.key)

        return kv_pb2.AgentPutReply(success=ok)

    def Delete(self, request, context):

        ok = aq.quorum_put(request.key, TOMBSTONE)

        aq.async_record_write(request.key)

        return kv_pb2.AgentDeleteReply(success=ok)

    def Get(self, request, context):

        status, value = aq.quorum_get(request.key)

        aq.async_record_read(request.key)

        if status == "QUORUM_FAILED":
            return kv_pb2.AgentGetReply(
                status=kv_pb2.AgentGetReply.QUORUM_FAILED
            )

        if status == "NOT_FOUND":
            return kv_pb2.AgentGetReply(
                status=kv_pb2.AgentGetReply.NOT_FOUND
            )

        return kv_pb2.AgentGetReply(
            status=kv_pb2.AgentGetReply.OK,
            value=value
        )


# -------------------------------
# Start coordinator server
# -------------------------------

def serve():

    server = grpc.server(
        ThreadPoolExecutor(max_workers=32)
    )

    kv_pb2_grpc.add_AgentKVServicer_to_server(
        AgentService(),
        server
    )

    server.add_insecure_port("[::]:6000")

    server.start()

    print("Coordinator started")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()