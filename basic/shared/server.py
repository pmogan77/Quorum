import grpc
import json
from concurrent import futures
import kv_pb2
import kv_pb2_grpc

store = {}

with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

TOMBSTONE = CONFIG["tombstone"]


def version_of(entry):
    return (entry["timestamp"], entry["client_id"])


class KVServer(kv_pb2_grpc.KVStoreServicer):

    def Get(self, request, context):

        key = request.key

        print(f"Received Get request for key: {key}")

        if key not in store:
            return kv_pb2.GetReply(found=False)

        entry = store[key]

        if entry["value"] == TOMBSTONE:
            return kv_pb2.GetReply(found=False)

        return kv_pb2.GetReply(
            found=True,
            value=entry["value"],
            timestamp=entry["timestamp"],
            client_id=entry["client_id"],
            request_id=entry["request_id"],
        )

    def Put(self, request, context):

        key = request.key

        print(f"Received Put request for key: {key} with value: {request.value}")

        incoming = {
            "value": request.value,
            "timestamp": request.timestamp,
            "client_id": request.client_id,
            "request_id": request.request_id,
        }

        if key not in store:
            store[key] = incoming
        else:
            existing = store[key]

            if version_of(incoming) > version_of(existing):
                store[key] = incoming

        return kv_pb2.PutReply(success=True)

    def Delete(self, request, context):

        key = request.key

        print(f"Received Delete request for key: {key}")

        store[key] = {
            "value": TOMBSTONE,
            "timestamp": 0,
            "client_id": "",
            "request_id": "",
        }

        return kv_pb2.DeleteReply(success=True)


def serve():

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

    kv_pb2_grpc.add_KVStoreServicer_to_server(KVServer(), server)

    server.add_insecure_port("[::]:5000")
    server.start()

    print("server started")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
