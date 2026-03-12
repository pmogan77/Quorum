import grpc
import json
import random
import sys

import kv_pb2
import kv_pb2_grpc


with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

COORDINATORS = CONFIG["coordinators"]


coord = random.choice(list(COORDINATORS.values()))

addr = f"{coord['host']}:{coord['port']}"

channel = grpc.insecure_channel(addr)

stub = kv_pb2_grpc.AgentKVStub(channel)


def put(key, value):

    resp = stub.Put(

        kv_pb2.AgentPutRequest(
            key=key,
            value=value
        )

    )

    print("WRITE SUCCESS" if resp.success else "WRITE FAILED")


def get(key):

    resp = stub.Get(

        kv_pb2.AgentGetRequest(key=key)

    )

    if resp.status == kv_pb2.AgentGetReply.OK:

        print("VALUE:", resp.value)

    elif resp.status == kv_pb2.AgentGetReply.NOT_FOUND:

        print("NOT FOUND")

    else:

        print("QUORUM FAILED")


def delete(key):

    resp = stub.Delete(

        kv_pb2.AgentDeleteRequest(key=key)

    )

    print("DELETE SUCCESS" if resp.success else "DELETE FAILED")


if __name__ == "__main__":

    cmd = sys.argv[1]

    key = sys.argv[2]

    if cmd == "put":

        put(key, sys.argv[3])

    elif cmd == "get":

        get(key)

    elif cmd == "delete":

        delete(key)