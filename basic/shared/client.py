import grpc
import json
import random
import sys

import kv_pb2
import kv_pb2_grpc


with open("/shared/cluster.json") as f:
    CONFIG = json.load(f)

COORDINATORS = CONFIG["coordinators"]
NODES = CONFIG["nodes"]


def parse_n(arg=None):
    if arg is None:
        return 1

    try:
        n = int(arg)
    except ValueError:
        raise ValueError(f"n must be an integer, got: {arg}")

    if n < 1:
        raise ValueError(f"n must be >= 1, got: {n}")

    return n


def get_random_coordinator_stub():
    coord = random.choice(list(COORDINATORS.values()))
    addr = f"{coord['host']}:{coord['port']}"
    channel = grpc.insecure_channel(addr)
    stub = kv_pb2_grpc.AgentKVStub(channel)
    return channel, stub, addr


def get_server_stub(node_id):
    if node_id not in NODES:
        raise ValueError(f"Unknown node_id: {node_id}")

    node = NODES[node_id]
    addr = f"{node['host']}:{node['port']}"
    channel = grpc.insecure_channel(addr)
    stub = kv_pb2_grpc.KVStoreStub(channel)
    return channel, stub, addr


def put_once(key, value, i=None, n=1):
    channel, stub, addr = get_random_coordinator_stub()
    try:
        resp = stub.Put(
            kv_pb2.AgentPutRequest(
                key=key,
                value=value,
            )
        )

        if n > 1:
            print(f"[{i}/{n}]")

        print(f"Coordinator: {addr}")
        print("WRITE SUCCESS" if resp.success else "WRITE FAILED")
    finally:
        channel.close()


def get_once(key, i=None, n=1):
    channel, stub, addr = get_random_coordinator_stub()
    try:
        resp = stub.Get(
            kv_pb2.AgentGetRequest(key=key)
        )

        if n > 1:
            print(f"[{i}/{n}]")

        print(f"Coordinator: {addr}")

        if resp.status == kv_pb2.AgentGetReply.OK:
            print("VALUE:", resp.value)
        elif resp.status == kv_pb2.AgentGetReply.NOT_FOUND:
            print("NOT FOUND")
        else:
            print("QUORUM FAILED")
    finally:
        channel.close()


def put(key, value, n=1):
    for i in range(1, n + 1):
        put_once(key, value, i=i, n=n)


def get(key, n=1):
    for i in range(1, n + 1):
        get_once(key, i=i, n=n)


def delete(key):
    channel, stub, addr = get_random_coordinator_stub()
    try:
        resp = stub.Delete(
            kv_pb2.AgentDeleteRequest(key=key)
        )
        print(f"Coordinator: {addr}")
        print("DELETE SUCCESS" if resp.success else "DELETE FAILED")
    finally:
        channel.close()


def dump_state(node_id):
    channel, stub, addr = get_server_stub(node_id)
    try:
        resp = stub.DumpState(kv_pb2.DumpStateRequest())

        data = {}
        for entry in resp.entries:
            data[entry.key] = {
                "value": entry.value,
                "timestamp": entry.timestamp,
                "client_id": entry.client_id,
                "request_id": entry.request_id,
            }

        print(f"Server node: {node_id}")
        print(f"Address: {addr}")
        print(json.dumps(data, indent=2, sort_keys=True))
    finally:
        channel.close()


def usage():
    print("Usage:")
    print("  python client.py put <key> <value> [n]")
    print("  python client.py get <key> [n]")
    print("  python client.py delete <key>")
    print("  python client.py dump <node_id>")
    print("")
    print("Examples:")
    print("  python client.py put x hello")
    print("  python client.py put x hello 10")
    print("  python client.py get x")
    print("  python client.py get x 10")
    print("  python client.py dump s1")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    cmd = sys.argv[1].lower()

    try:
        if cmd == "put":
            if len(sys.argv) not in (4, 5):
                usage()
                sys.exit(1)

            key = sys.argv[2]
            value = sys.argv[3]
            n = parse_n(sys.argv[4] if len(sys.argv) == 5 else None)

            put(key, value, n)

        elif cmd == "get":
            if len(sys.argv) not in (3, 4):
                usage()
                sys.exit(1)

            key = sys.argv[2]
            n = parse_n(sys.argv[3] if len(sys.argv) == 4 else None)

            get(key, n)

        elif cmd == "delete":
            if len(sys.argv) != 3:
                usage()
                sys.exit(1)

            delete(sys.argv[2])

        elif cmd == "dump":
            if len(sys.argv) != 3:
                usage()
                sys.exit(1)

            dump_state(sys.argv[2])

        else:
            usage()
            sys.exit(1)

    except grpc.RpcError as e:
        print(f"RPC FAILED: code={e.code()} details={e.details()}")
        sys.exit(1)
    except ValueError as e:
        print(str(e))
        sys.exit(1)