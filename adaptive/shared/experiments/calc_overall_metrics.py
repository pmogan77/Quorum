import redis
import json
import statistics

def get_redis_connection(config_path="../cluster.json"):
    try:
        with open(config_path) as f:
            config = json.load(f)
        return redis.Redis(
            host=config["redis"]["host"],
            port=config["redis"]["port"],
            decode_responses=True
        )
    except Exception as e:
        print(f"Error loading config: {e}")
        return None

def calculate_metrics():
    r = get_redis_connection()
    if not r: 
        print("\nERROR: not able to connect to Redis")
        return

    # Identify which policy is currently in the database
    # We look for the common prefixes
    has_aq = any(r.scan_iter(match="aq_metrics:*", count=1))
    has_basic = any(r.scan_iter(match="basic_metrics:*", count=1))

    if has_aq:
        prefix = "aq"
        policy_name = "ADAPTIVE (AQ)"
    elif has_basic:
        prefix = "basic"
        policy_name = "BASIC"
    else:
        print("No metrics found in Redis. Ensure your YCSB run completed and flushed to Redis.")
        return

    print(f"--- Analyzing Results for: {policy_name} ---")

    # Data structures for aggregation
    stats = {
        "read": {"sum_lat": 0.0, "count": 0, "p95_list": []},
        "write": {"sum_lat": 0.0, "count": 0, "p95_list": []}
    }

    pattern = f"{prefix}_metrics:*"
    
    for full_key in r.scan_iter(match=pattern):
        # Filter out the ZSET and LIST keys used for P95 calculations
        if "queue_latencies" in full_key or "set_latencies" in full_key:
            continue
            
        parts = full_key.split(":")
        if len(parts) < 3: continue
        
        op = parts[1] # 'read' or 'write'
        data = r.hgetall(full_key)
        
        if not data: continue

        # 1. Accumulate for Global Mean (Weighted by Op Count)
        total_lat = float(data.get("total_latency", 0))
        ops = int(data.get(f"{op}s", 0)) 
        
        if ops > 0:
            stats[op]["sum_lat"] += total_lat
            stats[op]["count"] += ops
            
            # 2. Collect P95s for Median calculation
            p95 = float(data.get("recent_p95_latency", 0))
            if p95 > 0:
                stats[op]["p95_list"].append(p95)

    # Display Table
    print("\n{:<25} | {:<20}".format("Metric", "Value (ms)"))
    print("-" * 48)

    for op in ["read", "write"]:
        s = stats[op]
        
        # Calculate Global Mean (Total Latency / Total Ops)
        global_mean = s["sum_lat"] / s["count"] if s["count"] > 0 else 0
        
        # Calculate Median of all P95s
        median_p95 = statistics.median(s["p95_list"]) if s["p95_list"] else 0
        
        print("{:<25} | {:<20.6f}".format(f"Global Mean {op.upper()}", global_mean))
        print("{:<25} | {:<20.6f}".format(f"Median P95 {op.upper()}", median_p95))
        print("{:<25} | {:<20}".format(f"Total {op.upper()} Ops", s["count"]))
        print("-" * 48)

if __name__ == "__main__":
    calculate_metrics()