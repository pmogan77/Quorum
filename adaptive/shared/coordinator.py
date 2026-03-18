import grpc
import json
import uuid
import time

from concurrent.futures import ThreadPoolExecutor

import kv_pb2
import kv_pb2_grpc

from adaptive_quorum import AdaptiveQuorumManager

from key_metrics import KeyMetrics


# Load cluster config
with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

NODES = CONFIG["nodes"]
TOMBSTONE = CONFIG["tombstone"]

CLIENT_ID = str(uuid.uuid4())
TIMEOUT = 2

POLICY_CHANGE_LIKELIHOOD = 0.5

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


# initialize adaptive quorum manager
aq = AdaptiveQuorumManager(CONFIG, STUBS, EXECUTOR, CLIENT_ID, TIMEOUT, POLICY_CHANGE_LIKELIHOOD)

# initialize metrics logger
aq_metrics = KeyMetrics(CONFIG)


# agent service implementation
class AgentService(kv_pb2_grpc.AgentKVServicer):

    def __init__(self):
        self.request_count = 0
        self.logging_freq = 100

    def Put(self, request, context):

        start_time = time.time()

        ok = aq.quorum_put(request.key, request.value)

        aq_metrics.async_record_write_latency(request.key, time.time() - start_time)

        aq.async_record_write(request.key)
        
        self.request_count += 1

        self.log_metrics(request.key)

        return kv_pb2.AgentPutReply(success=ok)

    def Delete(self, request, context):

        start_time = time.time()

        ok = aq.quorum_put(request.key, TOMBSTONE)

        aq_metrics.async_record_write_latency(request.key, time.time() - start_time)

        aq.async_record_write(request.key)
        
        self.request_count += 1

        self.log_metrics(request.key)

        return kv_pb2.AgentDeleteReply(success=ok)

    def Get(self, request, context):

        start_time = time.time()

        status, value = aq.quorum_get(request.key)
        
        aq_metrics.async_record_read_latency(request.key, time.time() - start_time)

        aq.async_record_read(request.key)
        
        self.request_count += 1

        self.log_metrics(request.key)

        if status == "QUORUM_FAILED":
            return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.QUORUM_FAILED)

        if status == "NOT_FOUND":
            return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.NOT_FOUND)

        return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.OK, value=value)

    def log_metrics(self, key):

        if self.request_count % self.logging_freq == 0:

            print(f"\n({self.request_count}) KEY = ", key)

            print("Policy = ", aq.get_state(key))

            metrics = aq_metrics.get_metrics(key)
            print(json.dumps(metrics, indent=4), "\n")


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
