import grpc
import json
import uuid
import time

from concurrent.futures import ThreadPoolExecutor

import kv_pb2
import kv_pb2_grpc

from adaptive_quorum import AdaptiveQuorumManager, AdaptiveQuorumManagerRatioUpdate, \
    AdaptiveQuorumManagerTimerExponentialUpdate

from key_metrics import KeyMetrics


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
# initialize metrics logger
aq_metrics = KeyMetrics(CONFIG)

# agent service implementation
class AgentService(kv_pb2_grpc.AgentKVServicer):

    def __init__(self):
        self.request_count = 0
        self.logging_freq = 100

    def Put(self, request, context):

        if request.key == "SIGNAL_START_RUN_PHASE":

            print("\nTRIGGER RECEIVED: preparing run phase\n")

            aq_metrics.reset_metrics()

            return kv_pb2.AgentPutReply(success=True)
        
        if request.key == "SIGNAL_CLEAR_KEYS":

            print("\nTRIGGER RECEIVED: clearing keys\n")

            aq_metrics.clear_keys()

            return kv_pb2.AgentPutReply(success=True)

        start_time = time.time()

        quorum_put_dict = aq.quorum_put(request.key, request.value)

        aq_metrics.async_record_write_latency(request.key, time.time() - start_time)

        aq.async_record_write(request.key, quorum_put_dict)
        
        self.request_count += 1

        self.log_metrics(request.key)

        return kv_pb2.AgentPutReply(success=quorum_put_dict["result"])

    def Delete(self, request, context):

        start_time = time.time()

        quorum_put_dict = aq.quorum_put(request.key, TOMBSTONE)

        aq_metrics.async_record_write_latency(request.key, time.time() - start_time)

        aq.async_record_write(request.key, quorum_put_dict)
        
        self.request_count += 1

        self.log_metrics(request.key)

        return kv_pb2.AgentDeleteReply(success=quorum_put_dict["result"])

    def Get(self, request, context):

        start_time = time.time()

        quorum_get_dict = aq.quorum_get(request.key)
        
        aq_metrics.async_record_read_latency(request.key, time.time() - start_time)

        aq.async_record_read(request.key, quorum_get_dict)
        
        self.request_count += 1

        self.log_metrics(request.key)

        if quorum_get_dict["result"] == "QUORUM_FAILED":
            return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.QUORUM_FAILED)

        if quorum_get_dict["result"] == "NOT_FOUND":
            return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.NOT_FOUND)

        return kv_pb2.AgentGetReply(status=kv_pb2.AgentGetReply.OK, value=quorum_get_dict["response"])

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
