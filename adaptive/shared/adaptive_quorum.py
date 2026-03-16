from random import random

import grpc
import kv_pb2
import kv_pb2_grpc
import redis
import threading
import time
import uuid

from concurrent.futures import as_completed


class AdaptiveQuorumManager:

    def __init__(self, config, stubs, executor, client_id, timeout, policy_change_likelihood):

        redis_cfg = config["redis"]

        self.redis = redis.Redis(
            host=redis_cfg["host"],
            port=redis_cfg["port"],
            decode_responses=True
        )

        self.write_opt = config["quorum_policies"]["write_opt"]
        self.read_opt = config["quorum_policies"]["read_opt"]
        self.policy_cfg = config["adaptive_policy"]

        self.stubs = stubs
        self.executor = executor
        self.client_id = client_id
        self.timeout = timeout
        self.policy_change_likelihood = policy_change_likelihood
        self.strict_policy = {
            "R": max(self.write_opt["R"], self.read_opt["R"]),
            "W": max(self.write_opt["W"], self.read_opt["W"])
        }

    def meta_key(self, key):
        return f"aq:{key}"

    def lock_key(self, key):
        return f"aq_lock:{key}"

    def redis_safe(self, fn, default=None):
        try:
            return fn()
        except Exception:
            return default

    def get_state(self, key):

        state = self.redis_safe(
            lambda: self.redis.hget(self.meta_key(key), "state"),
            None
        )

        if state is None:
            created = self.redis_safe(
                lambda: self.redis.hset(
                    self.meta_key(key),
                    mapping={
                        "state": "write_opt",
                        "reads": 0,
                        "writes": 0,
                        "target_policy": ""
                    }
                ),
                None
            )

            if created is None:
                return "strict_fallback"

            return "write_opt"

        return state

    def get_quorum(self, key):

        state = self.get_state(key)

        if state == "strict_fallback":
            return self.strict_policy

        if state == "write_opt":
            return self.write_opt

        if state == "read_opt":
            return self.read_opt

        # transitioning to read optimized
        # tighten writes first, then later relax reads
        return {
            "R": self.write_opt["R"],
            "W": self.read_opt["W"]
        }

    def record_read(self, key):

        self.redis_safe(
            lambda: self.redis.hincrby(self.meta_key(key), "reads", 1)
        )

    def record_write(self, key):

        self.redis_safe(
            lambda: self.redis.hincrby(self.meta_key(key), "writes", 1)
        )

    def maybe_trigger_transition(self, key):

        meta = self.redis_safe(
            lambda: self.redis.hgetall(self.meta_key(key)),
            None
        )

        if meta is None:
            return

        reads = int(meta.get("reads", 0))
        writes = int(meta.get("writes", 0))

        if reads + writes < self.policy_cfg["min_operations"]:
            return

        ratio = reads / max(writes, 1)
        state = meta.get("state", "write_opt")

        if state == "write_opt" and ratio > self.policy_cfg["read_threshold"]:
            self.start_transition(key, "read_opt")

        elif state == "read_opt" and ratio < self.policy_cfg["write_threshold"]:
            self.start_transition(key, "write_opt")

    def start_transition(self, key, target):

        locked = self.redis_safe(
            lambda: self.redis.setnx(self.lock_key(key), 1),
            False
        )

        if not locked:
            return

        if target == "write_opt":
            self.redis_safe(
                lambda: self.redis.hset(
                    self.meta_key(key),
                    mapping={
                        "state": "write_opt",
                        "target_policy": ""
                    }
                )
            )

            self.redis_safe(lambda: self.redis.delete(self.lock_key(key)))
            return

        # transition to read_opt
        self.redis_safe(
            lambda: self.redis.hset(
                self.meta_key(key),
                mapping={
                    "state": "transitioning",
                    "target_policy": "read_opt"
                }
            )
        )

        repair_thread = threading.Thread(
            target=self._repair_transition,
            args=(key,)
        )
        repair_thread.start()

    def _repair_transition(self, key):
        try:
            status, value = self.quorum_get(key)

            if status != "OK":
                return

            success = self.quorum_put(key, value)

            if success:
                self.finalize_transition(key)
        finally:
            self.redis_safe(lambda: self.redis.delete(self.lock_key(key)))

    def finalize_transition(self, key):

        meta = self.redis_safe(
            lambda: self.redis.hgetall(self.meta_key(key)),
            None
        )

        if meta is None:
            return

        if meta.get("state") != "transitioning":
            return

        self.redis_safe(
            lambda: self.redis.hset(
                self.meta_key(key),
                mapping={
                    "state": "read_opt",
                    "target_policy": ""
                }
            )
        )

        # self.redis_safe(lambda: self.redis.delete(self.lock_key(key)))

    def quorum_put(self, key, value):

        policy = self.get_quorum(key)
        W = policy["W"]

        request_id = str(uuid.uuid4())
        timestamp = time.time()

        successes = 0

        def call(node):

            try:
                resp = self.stubs[node].Put(
                    kv_pb2.PutRequest(
                        key=key,
                        value=value,
                        timestamp=timestamp,
                        client_id=self.client_id,
                        request_id=request_id
                    ),
                    timeout=self.timeout
                )
                return resp.success
            except Exception:
                return False

        futures = [self.executor.submit(call, n) for n in self.stubs]

        for future in as_completed(futures):

            if future.result():
                successes += 1

            if successes >= W:
                return True

        return False

    def quorum_get(self, key):

        policy = self.get_quorum(key)
        R = policy["R"]

        replies = 0
        responses = []

        def call(node):

            try:
                resp = self.stubs[node].Get(
                    kv_pb2.GetRequest(key=key),
                    timeout=self.timeout
                )
                return resp
            except Exception:
                return None

        futures = [self.executor.submit(call, n) for n in self.stubs]

        for future in as_completed(futures):

            response = future.result()

            if response is not None:
                replies += 1

                if response.found:
                    responses.append(response)

            if replies >= R:
                break

        if replies < R:
            return "QUORUM_FAILED", None

        if len(responses) == 0:
            return "NOT_FOUND", None

        latest = max(
            responses,
            key=lambda r: (r.timestamp, r.client_id)
        )

        return "OK", latest.value

    def async_record_read(self, key):

        def task():
            self.record_read(key)

            if random() < self.policy_change_likelihood:
                self.maybe_trigger_transition(key)

        thread = threading.Thread(target=task)
        thread.start()

    def async_record_write(self, key):

        def task():
            self.record_write(key)

            # opportunistic completion in case proactive repair dies
            self.finalize_transition(key)
            
            if random() < self.policy_change_likelihood:
                self.maybe_trigger_transition(key)

        thread = threading.Thread(target=task)
        thread.start()