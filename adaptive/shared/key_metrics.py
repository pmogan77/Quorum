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

        self.temp_reads = {}
        self.temp_writes = {}

        self.read_lock = threading.Lock()
        self.write_lock = threading.Lock()

    def base_key(self, key):
        return f"aq_metrics:{key}"

    def redis_safe(self, fn, default=None):
        try:
            return fn()
        except Exception as e:
            print(f"\nREDIS ERROR: {e}\n")
            return default
    
    def aggregate_read_metrics(self, key):
        base_key = self.base_key(key)
        queue_key = f"{base_key}:read_latency_queue"

        reads, total_read_latency, recent_read_latency = 0, 0, 0

        fields = self.redis_safe(
            lambda: self.redis.hgetall(base_key), 
            None
        )
        recent_reads = self.redis_safe(
            lambda: self.redis.llen(queue_key),
            0
        )
        if fields:
            fields = {k: float(v) for k, v in fields.items() }
            reads = fields.get("reads")
            total_read_latency = fields.get("total_read_latency")
            recent_read_latency = recent_reads * fields.get("recent_ave_read_latency")

        return reads, total_read_latency, recent_read_latency
    
    def aggregate_write_metrics(self, key):
        base_key = self.base_key(key)
        queue_key = f"{base_key}:write_latency_queue"

        writes, total_write_latency, recent_write_latency = 0, 0, 0

        fields = self.redis_safe(
            lambda: self.redis.hgetall(base_key), 
            None
        )
        recent_writes = self.redis_safe(
            lambda: self.redis.llen(queue_key),
            0
        )
        if fields:
            fields = {k: float(v) for k, v in fields.items() }
            writes = fields.get("writes")
            total_write_latency = fields.get("total_write_latency")
            recent_write_latency = recent_writes * fields.get("recent_ave_write_latency")

        return writes, total_write_latency, recent_write_latency
    
    def calc_percentile(self, key, percentile, operation: Literal["read", "write"]):
        base_key = self.base_key(key)
        set_key = f"{base_key}:{operation}_latency_set"

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

    def get_metrics(self, key):
        metrics = self.redis_safe(
            lambda: self.redis.hgetall(self.base_key(key)),
            None
        )

        # Currently: runtime, throughput, read/write acc not being used
        if not(metrics):
            init_metrics = {
                "reads": 0,
                "writes": 0,
                "total_runtime": 0,
                "throughput": 0,
                "read_acc": 0,
                "write_acc": 0,
                "total_read_latency": 0,
                "total_write_latency": 0,
                "ave_read_latency": 0,
                "ave_write_latency": 0,
                "recent_ave_read_latency": 0,
                "recent_ave_write_latency": 0,
                "recent_p95_read_latency": 0,
                "recent_p95_write_latency": 0
            }

            self.redis_safe(
                lambda: self.redis.hset(
                    self.base_key(key),
                    mapping = init_metrics
                )
            )

            return init_metrics
        
        return metrics
        
    def redis_record_read(self, key, latency):
        base_key = self.base_key(key)
        queue_key = f"{base_key}:read_latency_queue"
        set_key = f"{base_key}:read_latency_set"

        self.get_metrics(key)  # Populates Redis data if missing
        reads, total_read_latency, recent_read_latency = self.aggregate_read_metrics(key)

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

        # Redis metric updates
        pipe = self.redis.pipeline()
        pipe.hincrby(base_key, "reads", self.update_freq)
        reads += self.update_freq

        serialized = [f"{item[0]}:{item[1]}" for item in self.temp_reads[key]]
        pipe.lpush(queue_key, *serialized)
        pipe.ltrim(queue_key, 0, self.window_length - 1)

        dict_latencies = { item[0]: item[1] for item in self.temp_reads[key] }
        pipe.zadd(set_key, dict_latencies)

        # Trim set latencies outside window
        if len(overflow) > 0:
            for item in overflow:
                uuid, latency = item.split(":")
                recent_read_latency -= float(latency)
                pipe.zrem(set_key, uuid)
        
        added_latency = sum([item[1] for item in self.temp_reads[key]])
        total_read_latency += added_latency
        recent_read_latency += added_latency

        pipe.hset(base_key, "total_read_latency", total_read_latency)
        pipe.hset(base_key, "ave_read_latency", total_read_latency / reads)
        pipe.hset(base_key, "recent_ave_read_latency", recent_read_latency / num_recent)

        result = pipe.execute()

        self.redis_safe(
            lambda: self.redis.hset(base_key, "recent_p95_read_latency", 
                                    self.calc_percentile(key, 0.95, "read"))
        )

        return result

    def redis_record_write(self, key, latency):
        base_key = self.base_key(key)
        queue_key = f"{base_key}:write_latency_queue"
        set_key = f"{base_key}:write_latency_set"

        self.get_metrics(key)  # Populates Redis data if missing
        writes, total_write_latency, recent_write_latency = self.aggregate_write_metrics(key)

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

        # Redis metric updates
        pipe = self.redis.pipeline()
        pipe.hincrby(base_key, "writes", self.update_freq)
        writes += self.update_freq

        serialized = [f"{item[0]}:{item[1]}" for item in self.temp_writes[key]]
        pipe.lpush(queue_key, *serialized)
        pipe.ltrim(queue_key, 0, self.window_length - 1)

        dict_latencies = { item[0]: item[1] for item in self.temp_writes[key] }
        pipe.zadd(set_key, dict_latencies)

        # Trim set latencies outside window
        if len(overflow) > 0:
            for item in overflow:
                uuid, latency = item.split(":")
                recent_write_latency -= float(latency)
                pipe.zrem(set_key, uuid)
        
        added_latency = sum([item[1] for item in self.temp_writes[key]])
        total_write_latency += added_latency
        recent_write_latency += added_latency

        pipe.hset(base_key, "total_write_latency", total_write_latency)
        pipe.hset(base_key, "ave_write_latency", total_write_latency / writes)
        pipe.hset(base_key, "recent_ave_write_latency", recent_write_latency / num_recent)

        result = pipe.execute()

        self.redis_safe(
            lambda: self.redis.hset(base_key, "recent_p95_write_latency", 
                                    self.calc_percentile(key, 0.95, "write"))
        )

        return result
    
    def record_read(self, key, latency):
        with self.read_lock:
            if self.temp_reads.get(key) is None:
                self.temp_reads[key] = []
            self.temp_reads[key].append((str(uuid.uuid4()), latency))
            
            if len(self.temp_reads[key]) >= self.update_freq:
                self.redis_record_read(key, latency)
                self.temp_reads[key] = []

    def record_write(self, key, latency):
        with self.write_lock:
            if self.temp_writes.get(key) is None:
                self.temp_writes[key] = []
            self.temp_writes[key].append((str(uuid.uuid4()), latency))

            if len(self.temp_writes[key]) >= self.update_freq:
                self.redis_record_write(key, latency)
                self.temp_writes[key] = []
    
    def async_record_read_latency(self, key, latency):
        def task():
            self.redis_safe(lambda: self.record_read(key, latency))

        thread = threading.Thread(target=task)
        thread.start()

    def async_record_write_latency(self, key, latency):
        def task():
            self.redis_safe(lambda: self.record_write(key, latency))

        thread = threading.Thread(target=task)
        thread.start()