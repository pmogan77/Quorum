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

        # TODO: get these from config
        self.window_length = 200
        self.update_freq = 10

        self.temp_read_latencies = {}
        self.temp_write_latencies = {}

    def key_name(self, key):
        return f"aq_metrics:{key}"

    def redis_safe(self, fn, default=None):
        try:
            return fn()
        except Exception:
            return default
    
    def calc_percentile(self, key, percentile):
        key_name = self.key_name(key)
        set_key = f"{key_name}:read_latency_set"
        n = self.redis.zcard(set_key)
        if n == 0:
            return 0
        rank = max(math.ceil(percentile * n) - 1, 0)
        latency = self.redis.zrange(key, rank, rank, withscores=True)
        return latency[0][1] if latency else 0

    def get_metrics(self, key):
        metrics = self.redis_safe(
            lambda: self.redis.hgetall(self.key_name(key)),
            None
        )
        metrics["p95_read_latency"] = self.calc_percentile(key, 0.95)

        # Currently: runtime, throughput, read/write acc not being used
        if metrics is None:
            init_metrics = {
                "reads": 0,
                "writes": 0,
                "total_runtime": 0,
                "throughput": 0,
                "total_read_acc": 0,
                "total_write_acc": 0,
                "total_ave_read_latency": 0,
                "total_ave_write_latency": 0,
                "recent_ave_read_latency": 0,
                "recent_ave_write_latency": 0,
            }

            self.redis_safe(
                lambda: self.redis.hset(
                    self.key_name(key),
                    mapping = init_metrics
                ),
                None
            )

            default_metrics = dict(init_metrics)
            default_metrics["p95_read_latency"] = 0
            return default_metrics

        return metrics

    def record_read_latency(self, key, latency):
        if self.temp_read_latencies.get(key) is None:
            self.temp_read_latencies[key] = []
        self.temp_read_latencies[key].append((str(uuid.uuid4()), latency))
        
        if len(self.temp_read_latencies[key]) < self.update_freq:
            return

        key_name = key_name(key)
        queue_key = f"{key_name}:read_latency_queue"
        set_key = f"{key_name}:read_latency_set"

        # Calculate updated metrics
        all_fields = self.redis.hgetall(key_name)
        metric_fields = {k.decode(): float(v) for k, v in all_fields.items() if k.decode() not in {queue_key, set_key}}
        reads = metric_fields.get("reads")
        total_read_latency = reads * metric_fields.get("total_ave_read_latency")
        recent_read_latency = reads * metric_fields.get("recent_ave_read_latency")

        overflow = self.redis.lrange(queue_key, self.window_length, -1)

        # Redis metric updates
        pipe = self.redis.pipeline()
        pipe.hincrby(key_name, "reads", self.update_freq)
        pipe.lpush(queue_key, *self.temp_read_latencies[key])

        dict_latencies = { item[0]: item[1] for item in self.temp_read_latencies[key]}
        pipe.zadd(set_key, dict_latencies)

        # Trim latencies outside window
        if len(overflow) > 0:
            for item in overflow:
                uuid = item[0]
                latency = item[1]
                total_read_latency -= latency
                recent_read_latency -= latency
                pipe.zrem(set_key, uuid)
            reads -= len(overflow)
        
        total_read_latency += sum(self.temp_read_latencies[key])
        recent_read_latency += sum(self.temp_read_latencies[key])
        total_ave_read_latency = total_read_latency / (reads + self.update_freq)
        recent_ave_read_latency = recent_read_latency / (reads + self.update_freq)

        pipe.hset(key_name, "total_ave_read_latency", total_ave_read_latency)
        pipe.hset(key_name, "recent_ave_read_latency", recent_ave_read_latency)

        pipe.execute()

        del self.temp_read_latencies[key]

    def record_write_latency(self, key, latency):
        # TODO: finish this when reads have been tested

        key_name = key_name(key)
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
            self.redis_safe(lambda: self.record_read_latency(key, latency), None)

        thread = threading.Thread(target=task)
        thread.start()

    def async_record_write_latency(self, key, latency):
        def task():
            self.redis_safe(lambda: self.record_write_latency(key, latency), None)

        thread = threading.Thread(target=task)
        thread.start()