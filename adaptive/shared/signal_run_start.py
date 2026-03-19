import grpc
import kv_pb2
import kv_pb2_grpc
import json

def signal_run_start():
    with open("./cluster.json") as f:
        CONFIG = json.load(f)

    coordinators = CONFIG["coordinators"]
    host = coordinators["c1"]["host"]
    port = coordinators["c1"]["port"]

    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = kv_pb2_grpc.AgentKVStub(channel)
    
    print("\nSending signal to reset metrics...")
    try:
        stub.Put(kv_pb2.AgentPutRequest(key="SIGNAL_START_RUN_PHASE", value="BEGIN"))
    
    except Exception as e:
        print(f"Failed to send signal: {e}")
        return
    
    print("SIGNAL SENT. You can now start the YCSB RUN phase.")


if __name__ == "__main__":

    signal_run_start()