import grpc
import json
import uuid

from concurrent.futures import ThreadPoolExecutor

import kv_pb2
import kv_pb2_grpc

from adaptive_quorum import AdaptiveQuorumManager, AdaptiveQuorumManagerRatioUpdate, \
    AdaptiveQuorumManagerTimerExponentialUpdate


# Load cluster config
with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

NODES = CONFIG["nodes"]
TOMBSTONE = CONFIG["tombstone"]

CLIENT_ID = str(uuid.uuid4())
TIMEOUT = 2

# build gRPC channels and stubs for all storage nodes
CHANNELS = {}
STUBS = {}

for name, node in NODES.items():

    addr = f"{node['host']}:{node['port']}"

    channel = grpc.insecure_channel(addr)
    stub = kv_pb2_grpc.KVStoreStub(channel)

    CHANNELS[name] = channel
    STUBS[name] = stub


EXECUTOR = ThreadPoolExecutor(max_workers=len(NODES))

if CONFIG["adaptive_mode"] == "ratio":
    POLICY_CHANGE_LIKELIHOOD = 0.5
    # initialize adaptive quorum manager
    aq = AdaptiveQuorumManagerRatioUpdate(CONFIG, STUBS, EXECUTOR, CLIENT_ID, \
                               TIMEOUT, POLICY_CHANGE_LIKELIHOOD)
elif CONFIG["adaptive_mode"] == "timer_exponential":
    aq = AdaptiveQuorumManagerTimerExponentialUpdate(CONFIG, STUBS, EXECUTOR, CLIENT_ID, TIMEOUT)

# agent service implementation
class AgentService(kv_pb2_grpc.AgentKVServicer):

    def Put(self, request, context):

        quorum_put_dict = aq.quorum_put(request.key, request.value)

        aq.async_record_write(request.key, quorum_put_dict)

        return kv_pb2.AgentPutReply(success=quorum_put_dict["result"])

    def Delete(self, request, context):

        quorum_put_dict = aq.quorum_put(request.key, TOMBSTONE)

        aq.async_record_write(request.key, quorum_put_dict)

        return kv_pb2.AgentDeleteReply(success=quorum_put_dict["result"])

    def Get(self, request, context):

        quorum_get_dict = aq.quorum_get(request.key)

        aq.async_record_read(request.key, quorum_get_dict)

        if quorum_get_dict["result"] == "QUORUM_FAILED":
            return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.QUORUM_FAILED)

        if quorum_get_dict["result"] == "NOT_FOUND":
            return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.NOT_FOUND)

        return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.OK, value=quorum_get_dict["response"])


# start gRPC server to listen for client requests
def serve():

    server = grpc.server(ThreadPoolExecutor(max_workers=32))

    kv_pb2_grpc.add_AgentKVServicer_to_server(AgentService(), server)

    server.add_insecure_port("[::]:6000")

    server.start()

    print("Coordinator started")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
