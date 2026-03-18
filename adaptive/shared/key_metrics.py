import redis
import threading
import uuid
import math


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

    def key_name(self, key):
        return f"aq_metrics:{key}"

    def redis_safe(self, fn, default=None):
        try:
            return fn()
        except Exception as e:
            print(f"\nREDIS ERROR: {e}\n")
            return default
    
    def aggregate_read_metrics(self, key):
        key_name = self.key_name(key)
        queue_key = f"{key_name}:read_latency_queue"

        reads, total_read_latency, recent_read_latency = 0, 0, 0

        fields = self.redis_safe(
            lambda: self.redis.hgetall(key_name), 
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
    
    def calc_percentile(self, key, percentile):
        key_name = self.key_name(key)
        set_key = f"{key_name}:read_latency_set"

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
            lambda: self.redis.hgetall(self.key_name(key)),
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
                    self.key_name(key),
                    mapping = init_metrics
                )
            )

            return init_metrics
        
        return metrics
        
    def redis_record_read(self, key, latency):
        key_name = self.key_name(key)
        queue_key = f"{key_name}:read_latency_queue"
        set_key = f"{key_name}:read_latency_set"

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
        pipe.hincrby(key_name, "reads", self.update_freq)
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

        pipe.hset(key_name, "total_read_latency", total_read_latency)
        pipe.hset(key_name, "ave_read_latency", total_read_latency / reads)
        pipe.hset(key_name, "recent_ave_read_latency", recent_read_latency / num_recent)

        result = pipe.execute()

        self.redis_safe(
            lambda: self.redis.hset(key_name, "recent_p95_read_latency", 
                                    self.calc_percentile(key, 0.95))
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
        # TODO: finish this when reads have been tested

        key_name = self.key_name(key)
        queue_key = f"{key_name}:write_latency_queue"
        set_key = f"{key_name}:write_latency_set"

        pipe = self.redis.pipeline()
        pipe.hincrby(key_name, "writes", 1)
        pipe.lpush(queue_key, latency)
        # TODO: add to sorted set
        # TODO: check for trim
            # Trim queue and sorted set
        pipe.execute()
    
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