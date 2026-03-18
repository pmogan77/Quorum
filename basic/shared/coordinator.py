import grpc
import json
import time
import uuid

from concurrent.futures import ThreadPoolExecutor, as_completed

import kv_pb2
import kv_pb2_grpc

from key_metrics import KeyMetrics


with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

NODES = CONFIG["nodes"]
R = CONFIG["R"]
W = CONFIG["W"]
TOMBSTONE = CONFIG["tombstone"]

CLIENT_ID = str(uuid.uuid4())

TIMEOUT = 2


CHANNELS = {}
STUBS = {}

for name, node in NODES.items():

    addr = f"{node['host']}:{node['port']}"

    channel = grpc.insecure_channel(addr)

    stub = kv_pb2_grpc.KVStoreStub(channel)

    CHANNELS[name] = channel
    STUBS[name] = stub


EXECUTOR = ThreadPoolExecutor(max_workers=len(NODES))

basic_metrics = KeyMetrics(CONFIG)


def quorum_put(key, value):

    request_id = str(uuid.uuid4())

    timestamp = time.time()

    successes = 0

    def call(node):

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
            )

            return resp.success

        except:

            return False

    futures = [EXECUTOR.submit(call, n) for n in STUBS]

    for f in as_completed(futures):

        if f.result():

            successes += 1

        if successes >= W:

            return True

    return False


def quorum_get(key):

    replies = 0

    responses = []

    def call(node):

        try:

            resp = STUBS[node].Get(kv_pb2.GetRequest(key=key), timeout=TIMEOUT)

            return resp

        except:

            return None

    futures = [EXECUTOR.submit(call, n) for n in STUBS]

    for f in as_completed(futures):

        r = f.result()

        if r is not None:

            replies += 1

            if r.found:

                responses.append(r)

        if replies >= R:

            break

    if replies < R:

        return "QUORUM_FAILED", None

    if len(responses) == 0:

        return "NOT_FOUND", None

    latest = max(responses, key=lambda r: (r.timestamp, r.client_id))

    return "OK", latest.value


class AgentService(kv_pb2_grpc.AgentKVServicer):

    def __init__(self):
        self.request_count = 0
        self.logging_freq = 100

    def Put(self, request, context):

        start_time = time.time()

        ok = quorum_put(request.key, request.value)

        basic_metrics.async_record_write_latency(request.key, time.time() - start_time)

        self.request_count += 1

        self.log_metrics(request.key)

        return kv_pb2.AgentPutReply(success=ok)

    def Delete(self, request, context):

        start_time = time.time()

        ok = quorum_put(request.key, TOMBSTONE)

        basic_metrics.async_record_write_latency(request.key, time.time() - start_time)

        self.request_count += 1

        self.log_metrics(request.key)

        return kv_pb2.AgentDeleteReply(success=ok)

    def Get(self, request, context):

        start_time = time.time()

        status, value = quorum_get(request.key)

        basic_metrics.async_record_read_latency(request.key, time.time() - start_time)

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

            metrics = basic_metrics.get_metrics(key)
            print(json.dumps(metrics, indent=4), "\n")


def serve():

    server = grpc.server(ThreadPoolExecutor(max_workers=32))

    kv_pb2_grpc.add_AgentKVServicer_to_server(AgentService(), server)

    server.add_insecure_port("[::]:6000")

    server.start()

    print("Coordinator started")

    server.wait_for_termination()


if __name__ == "__main__":

    serve()
