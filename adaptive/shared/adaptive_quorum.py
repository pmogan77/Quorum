import redis
import threading


class AdaptiveQuorumManager:

    def __init__(self, config):

        redis_cfg = config["redis"]

        self.redis = redis.Redis(
            host=redis_cfg["host"],
            port=redis_cfg["port"],
            decode_responses=True
        )

        self.write_opt = config["quorum_policies"]["write_opt"]
        self.read_opt = config["quorum_policies"]["read_opt"]

        self.policy_cfg = config["adaptive_policy"]

    def meta_key(self, key):
        return f"aq:{key}"

    def lock_key(self, key):
        return f"aq_lock:{key}"

    def get_state(self, key):

        state = self.redis.hget(self.meta_key(key), "state")

        if state is None:

            self.redis.hset(self.meta_key(key), mapping={
                "state": "write_opt",
                "reads": 0,
                "writes": 0
            })

            return "write_opt"

        return state

    def get_quorum(self, key):

        state = self.get_state(key)

        if state == "write_opt":
            return self.write_opt

        if state == "read_opt":
            return self.read_opt

        # transitioning → read optimized intermediate state
        return {
            "R": self.write_opt["R"],
            "W": self.read_opt["W"]
        }

    def record_read(self, key):

        self.redis.hincrby(self.meta_key(key), "reads", 1)

    def record_write(self, key):

        self.redis.hincrby(self.meta_key(key), "writes", 1)

    def maybe_trigger_transition(self, key):

        meta = self.redis.hgetall(self.meta_key(key))

        reads = int(meta.get("reads", 0))
        writes = int(meta.get("writes", 0))

        if reads + writes < self.policy_cfg["min_operations"]:
            return

        ratio = reads / max(writes, 1)

        state = meta.get("state", "write_opt")

        if state == "write_opt" and ratio > self.policy_cfg["read_threshold"]:
            self.start_transition(key, "read_opt")

        if state == "read_opt" and ratio < self.policy_cfg["write_threshold"]:
            self.start_transition(key, "write_opt")

    def start_transition(self, key, target):

        if not self.redis.setnx(self.lock_key(key), 1):
            return

        if target == "write_opt":

            self.redis.hset(self.meta_key(key), mapping={
                "state": "write_opt",
                "target_policy": ""
            })

            self.redis.delete(self.lock_key(key))

        else:

            self.redis.hset(self.meta_key(key), mapping={
                "state": "transitioning",
                "target_policy": "read_opt"
            })

    def finalize_transition(self, key):

        meta = self.redis.hgetall(self.meta_key(key))

        if meta.get("state") != "transitioning":
            return

        self.redis.hset(self.meta_key(key), mapping={
            "state": "read_opt",
            "target_policy": ""
        })

        self.redis.delete(self.lock_key(key))

    def async_record_read(self, key):

        def task():
            self.record_read(key)
            self.maybe_trigger_transition(key)

        threading.Thread(target=task).start()

    def async_record_write(self, key):

        def task():
            self.record_write(key)
            self.finalize_transition(key)
            self.maybe_trigger_transition(key)

        threading.Thread(target=task).start()