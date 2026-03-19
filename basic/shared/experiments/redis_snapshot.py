import redis
import json
import argparse

parser = argparse.ArgumentParser(description="Redis metrics snapshot tool")
parser.add_argument(
    "--output",
    "-o",
    default="snapshot.json",
    help="Output file path (default: snapshot.json)"
)
parser.add_argument(
    "--namespace",
    "-n",
    default="metrics",
    help="Namespace to search for matching keys"
)
args = parser.parse_args()

OUTPUT_FILE = f"data/{args.output}"


with open("../cluster.json") as f:
    CONFIG = json.load(f)

redis_cfg = CONFIG["redis"]
host = redis_cfg["host"]
port = redis_cfg["port"]

r = redis.Redis(host=host, port=port, decode_responses=True)

data = {}

cursor = 0

while True:
    cursor, keys = r.scan(cursor=cursor, match=f"*{args.namespace}:*")
    
    for k in keys:
        parts = k.split(":")
        if len(parts) <= 3:
            try:
                data[k] = r.hgetall(k)
            except Exception as e:
                print(f"\nERROR: {e}\n")
                pass
    
    if cursor == 0:
        break

with open(OUTPUT_FILE, "w") as f:
    json.dump(data, f, indent=2)

print(f"Snapshot saved: {len(data)} metric keys to {OUTPUT_FILE}")


# Files: 

# basic-single-a.json
# basic-single-b.json
# basic-single-c.json

# aq-single-a.json
# aq-single-b.json
# aq-single-c.json


# basic-consecutive-afterb.json
# basic-consecutive-afterc.json

# aq-consecutive-afterb.json
# aq-consecutive-afterc.json


# Namespaces: 
# metrics
# aq (only for consecutive workloads)