import redis
import threading
import uuid
import math
from typing import Literal


class KeyMetrics:

    def __init__(self, config):
        redis_cfg = config["redis"]

        self.redis = redis.Redis(
            host=redis_cfg["host"],
            port=redis_cfg["port"],
            decode_responses=True
        )

        metrics_cfg = config["metrics"]
        self.window_length = metrics_cfg["window_length"]
        self.update_freq = metrics_cfg["update_freq"]

        self.temp_latencies = {
            "read": {},
            "write": {}
        }

        self.read_lock = threading.Lock()
        self.write_lock = threading.Lock()

    def base_key(self, key, operation: Literal["read", "write"]):
        return f"aq_metrics:{operation}:{key}"

    def queue_key(self, key, operation: Literal["read", "write"]):
        return f"aq_metrics:{operation}:queue_latencies:{key}"

    def sorted_set_key(self, key, operation: Literal["read", "write"]):
        return f"aq_metrics:{operation}:set_latencies:{key}"

    def redis_safe(self, fn, default=None):
        try:
            return fn()
        except Exception as e:
            print(f"\nREDIS ERROR: {e}\n")
            return default
    
    def aggregate_metrics(self, key, operation: Literal["read", "write"]):
        base_key = self.base_key(key, operation)
        queue_key = self.queue_key(key, operation)

        ops, total_latency, recent_latency = 0, 0, 0

        fields = self.redis_safe(
            lambda: self.redis.hgetall(base_key), 
            None
        )
        print("\nfields = ", fields)
        recent_ops = self.redis_safe(
            lambda: self.redis.llen(queue_key),
            0
        )
        print("\nrecent_ops = ", recent_ops)
        
        if fields:
            fields = { k: float(v) for k, v in fields.items() }
            ops = fields.get(f"{operation}s")
            total_latency = fields.get(f"total_latency")
            recent_latency = recent_ops * fields.get(f"recent_ave_latency")

        return ops, total_latency, recent_latency
    
    def calc_percentile(self, key, percentile, operation: Literal["read", "write"]):
        set_key = self.sorted_set_key(key, operation)

        n = self.redis_safe(
            lambda: self.redis.zcard(set_key),
            0
        )
        if n == 0:
            return 0
        
        rank = max(math.ceil(percentile * n) - 1, 0)
        latency = self.redis_safe(
            lambda: self.redis.zrange(set_key, rank, rank, withscores=True),
            None
        )
        return latency[0][1] if latency else 0

    def get_operation_metrics(self, key, operation: Literal["read", "write"]):
        metrics = self.redis_safe(
            lambda: self.redis.hgetall(self.base_key(key, operation)),
            None
        )

        if not(metrics):
            init_metrics = {
                f"{operation}s": 0,
                "acc": 0,
                "total_latency": 0,
                "ave_latency": 0,
                "recent_ave_latency": 0,
                "recent_p95_latency": 0,
            }

            self.redis_safe(
                lambda: self.redis.hset(
                    self.base_key(key, operation),
                    mapping = init_metrics
                )
            )

            return init_metrics
        
        return metrics
    
    def metrics_mapping(self, metrics, operation: Literal["read", "write"]):
        return {
            f"{operation}s": metrics.get(f"{operation}s"),
            f"{operation}_acc": metrics.get("acc"),
            f"total_{operation}_latency": metrics.get("total_latency"),
            f"ave_{operation}_latency": metrics.get("ave_latency"),
            f"recent_ave_{operation}_latency": metrics.get("recent_ave_latency"),
            f"recent_p95_{operation}_latency": metrics.get("recent_p95_latency")
        }

    def get_metrics(self, key):
        read_metrics = self.get_operation_metrics(key, "read")
        read_mapping = self.metrics_mapping(read_metrics, "read")
        write_metrics = self.get_operation_metrics(key, "write")
        write_mapping = self.metrics_mapping(write_metrics, "write")

        metrics = {
            "total_runtime": 0,
            "throughput": 0,
        }

        for k, v in read_mapping.items():
            metrics[k] = v
        
        for k, v in write_mapping.items():
            metrics[k] = v

        return metrics
        
    def redis_record_operation(self, key, latency, operation: Literal["read", "write"]):
        base_key = self.base_key(key, operation)
        queue_key = self.queue_key(key, operation)
        set_key = self.sorted_set_key(key, operation)

        self.get_operation_metrics(key, operation)  # Populates Redis data if missing
        ops, total_latency, recent_latency = self.aggregate_metrics(key, operation)
        print("\nops = ", ops)
        print("total_latency = ", total_latency)
        print("recent_latency = ", recent_latency, "\n")

        start_index = max(self.window_length - self.update_freq, 0)
        overflow = self.redis_safe(
            lambda: self.redis.lrange(queue_key, start_index, -1),
            []
        )
        num_recent = self.redis_safe(
            lambda: self.redis.llen(queue_key),
            0
        )
        num_recent = num_recent - len(overflow) + self.update_freq
        print("num_recent = ", num_recent)

        # Redis metric updates
        pipe = self.redis.pipeline()
        pipe.hincrby(base_key, f"{operation}s", self.update_freq)
        ops += self.update_freq

        serialized = [f"{item[0]}:{item[1]}" for item in self.temp_latencies[operation][key]]
        pipe.lpush(queue_key, *serialized)
        pipe.ltrim(queue_key, 0, self.window_length - 1)

        dict_latencies = { item[0]: item[1] for item in self.temp_latencies[operation][key] }
        pipe.zadd(set_key, dict_latencies)

        # Trim set latencies outside window
        if len(overflow) > 0:
            for item in overflow:
                uuid, latency = item.split(":")
                recent_latency -= float(latency)
                pipe.zrem(set_key, uuid)
        
        added_latency = sum([item[1] for item in self.temp_latencies[operation][key]])
        total_latency += added_latency
        recent_latency += added_latency

        pipe.hset(base_key, "total_latency", total_latency)
        pipe.hset(base_key, "ave_latency", total_latency / ops)
        pipe.hset(base_key, "recent_ave_latency", recent_latency / num_recent)

        result = pipe.execute()

        self.redis_safe(
            lambda: self.redis.hset(base_key, "recent_p95_latency", 
                                    self.calc_percentile(key, 0.95, operation))
        )

        return result
    
    def record_operation(self, key, operation: Literal["read", "write"], latency):
        lock = self.read_lock
        if operation == "write":
            lock = self.write_lock
        
        with lock:
            if self.temp_latencies[operation].get(key) is None:
                self.temp_latencies[operation][key] = []
            self.temp_latencies[operation][key].append((str(uuid.uuid4()), latency))
            
            if len(self.temp_latencies[operation][key]) >= self.update_freq:
                self.redis_record_operation(key, latency, operation)
                self.temp_latencies[operation][key] = []
    
    def async_record_read_latency(self, key, latency):
        def task():
            self.redis_safe(lambda: self.record_operation(key, "read", latency))

        thread = threading.Thread(target=task)
        thread.start()

    def async_record_write_latency(self, key, latency):
        def task():
            self.redis_safe(lambda: self.record_operation(key, "write", latency))

        thread = threading.Thread(target=task)
        thread.start()