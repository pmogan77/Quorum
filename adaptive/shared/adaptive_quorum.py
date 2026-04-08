from random import random

import threading
import time
import uuid

from concurrent.futures import as_completed

import kv_pb2
import redis

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind, Status, StatusCode


UNLOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


class MetadataSetter:
    def set(self, carrier, key, value):
        carrier.append((key, value))


class AdaptiveQuorumManager:
    def __init__(
        self,
        config,
        stubs,
        executor,
        client_id,
        timeout,
        policy_change_likelihood=1.0,
        tracer=None,
        node_id=None,
        enable_tracing=True,
    ):
        redis_cfg = config["redis"]

        self.redis = redis.Redis(
            host=redis_cfg["host"],
            port=redis_cfg["port"],
            decode_responses=True,
        )

        self.redis.ping()

        self.write_opt = config["quorum_policies"]["write_opt"]
        self.read_opt = config["quorum_policies"]["read_opt"]
        self.policy_cfg = config["adaptive_policy"]

        self.stubs = stubs
        self.executor = executor
        self.client_id = client_id
        self.timeout = timeout
        self.policy_change_likelihood = policy_change_likelihood

        self.tracer = tracer if tracer is not None else trace.get_tracer(__name__)
        self.node_id = node_id
        self.enable_tracing = enable_tracing

        self.lock_ttl_ms = self.policy_cfg.get("lock_ttl_ms", 10000)

        self.strict_policy = {
            "R": max(self.write_opt["R"], self.read_opt["R"]),
            "W": max(self.write_opt["W"], self.read_opt["W"]),
        }

    def _add_event(self, name, attributes=None):
        span = trace.get_current_span()
        try:
            span.add_event(name, attributes or {})
        except Exception:
            pass

    def meta_key(self, key):
        return f"aq:{key}"

    def lock_key(self, key):
        return f"aq_lock:{key}"

    def redis_safe(self, fn, default=None):
        try:
            return fn()
        except Exception as e:
            self._add_event(
                "redis_operation_failed",
                {"error": str(e)},
            )
            return default
        
    def traced_redis(self, op_name, fn, default=None, attributes=None):
        attrs = attributes or {}
        with self.tracer.start_as_current_span(
            f"redis.{op_name}",
            kind=SpanKind.CLIENT,
        ) as span:
            span.set_attribute("db.system", "redis")
            span.set_attribute("db.operation", op_name)
            for k, v in attrs.items():
                span.set_attribute(k, v)

            t0 = time.perf_counter()
            try:
                result = fn()
                span.set_attribute("redis.success", True)
                span.set_attribute(
                    "redis.duration_ms",
                    (time.perf_counter() - t0) * 1000.0,
                )

                if result is None:
                    span.set_attribute("redis.result_is_none", True)
                elif isinstance(result, dict):
                    span.set_attribute("redis.result_len", len(result))
                elif isinstance(result, (str, bytes, list, tuple)):
                    span.set_attribute("redis.result_len", len(result))

                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.set_attribute("redis.success", False)
                span.set_attribute(
                    "redis.duration_ms",
                    (time.perf_counter() - t0) * 1000.0,
                )
                return default

    def release_lock(self, key, token):
        if not token:
            return

        self.redis_safe(
            lambda: self.redis.eval(
                UNLOCK_SCRIPT,
                1,
                self.lock_key(key),
                token,
            ),
            0,
        )

    def get_state(self, key):
        meta_key = self.meta_key(key)

        state = self.traced_redis(
            "hget",
            lambda: self.redis.hget(meta_key, "state"),
            default=None,
            attributes={
                "db.redis.key": meta_key,
                "db.redis.field": "state",
                "kv.key": key,
            },
        )

        if state is None:
            created = self.traced_redis(
                "hset_init_state",
                lambda: self.redis.hset(
                    meta_key,
                    mapping={
                        "state": "write_opt",
                        "reads": 0,
                        "writes": 0,
                        "target_policy": "",
                    },
                ),
                default=None,
                attributes={
                    "db.redis.key": meta_key,
                    "kv.key": key,
                },
            )

            if created is None:
                return "strict_fallback"

            return "write_opt"

        return state

    def get_quorum(self, key):
        with self.tracer.start_as_current_span("adaptive.get_quorum") as span:
            span.set_attribute("kv.key", key)

            state = self.get_state(key)
            span.set_attribute("adaptive.state", state)

            if state == "strict_fallback":
                policy = self.strict_policy
            elif state == "write_opt":
                policy = self.write_opt
            elif state == "read_opt":
                policy = self.read_opt
            else:
                policy = {
                    "R": self.write_opt["R"],
                    "W": self.read_opt["W"],
                }

            span.set_attribute("quorum.r", policy["R"])
            span.set_attribute("quorum.w", policy["W"])
            return policy

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
            None,
        )

        if meta is None:
            return

        reads = int(meta.get("reads", 0))
        writes = int(meta.get("writes", 0))

        self._add_event(
            "adaptive_policy_counters_observed",
            {
                "reads": reads,
                "writes": writes,
            },
        )

        if reads + writes < self.policy_cfg["min_operations"]:
            self._add_event(
                "adaptive_transition_skipped_min_operations",
                {
                    "total_operations": reads + writes,
                    "min_operations": self.policy_cfg["min_operations"],
                },
            )
            return

        ratio = reads / max(writes, 1)
        state = meta.get("state", "write_opt")

        self._add_event(
            "adaptive_ratio_computed",
            {
                "ratio": ratio,
                "state": state,
            },
        )

        if state == "write_opt" and ratio > self.policy_cfg["read_threshold"]:
            self._add_event(
                "adaptive_transition_requested",
                {
                    "from_state": state,
                    "to_state": "read_opt",
                },
            )
            self.start_transition(key, "read_opt")

        elif state == "read_opt" and ratio < self.policy_cfg["write_threshold"]:
            self._add_event(
                "adaptive_transition_requested",
                {
                    "from_state": state,
                    "to_state": "write_opt",
                },
            )
            self.start_transition(key, "write_opt")

    def start_transition(self, key, target):
        self._add_event(
            "adaptive_transition_start_requested",
            {
                "key": key,
                "target": target,
            },
        )

        # lock_token = str(uuid.uuid4())

        # locked = self.redis_safe(
        #     lambda: self.redis.set(
        #         self.lock_key(key),
        #         lock_token,
        #         nx=True,
        #         px=self.lock_ttl_ms
        #     ),
        #     False
        # )

        # self._add_event(
        #     "adaptive_transition_lock_attempted",
        #     {
        #         "key": key,
        #         "target": target,
        #         "locked": bool(locked),
        #     },
        # )

        # if not locked:
        #     self._add_event(
        #         "adaptive_transition_lock_not_acquired",
        #         {
        #             "key": key,
        #             "target": target,
        #         },
        #     )
        #     return

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

            self._add_event(
                "adaptive_transition_completed_immediate",
                {
                    "key": key,
                    "state": "write_opt",
                },
            )

            # self.release_lock(key, lock_token)
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

        self._add_event(
            "adaptive_transition_marked_transitioning",
            {
                "key": key,
                "state": "transitioning",
                "target_policy": "read_opt",
            },
        )

        # repair_thread = threading.Thread(
        #     target=self._repair_transition,
        #     args=(key, lock_token)
        # )
        # repair_thread.start()

        # self._add_event(
        #     "adaptive_transition_repair_thread_started",
        #     {
        #         "key": key,
        #         "target_policy": "read_opt",
        #     },
        # )

    def _repair_transition(self, key, lock_token):
        self._add_event(
            "adaptive_repair_transition_started",
            {
                "key": key,
            },
        )

        try:
            status, value = self.quorum_get(key)

            self._add_event(
                "adaptive_repair_transition_read_completed",
                {
                    "key": key,
                    "status": status,
                },
            )

            if status != "OK":
                self._add_event(
                    "adaptive_repair_read_failed",
                    {
                        "key": key,
                        "status": status,
                    },
                )
                return

            success = self.quorum_put(key, value)

            self._add_event(
                "adaptive_repair_transition_write_completed",
                {
                    "key": key,
                    "success": success,
                },
            )

            if success:
                self.finalize_transition(key)
                self._add_event(
                    "adaptive_repair_transition_finalized",
                    {
                        "key": key,
                    },
                )
            else:
                self._add_event(
                    "adaptive_repair_transition_write_failed",
                    {
                        "key": key,
                    },
                )
        finally:
            self._add_event(
                "adaptive_repair_transition_finished",
                {
                    "key": key,
                },
            )
            # self.release_lock(key, lock_token)

    def finalize_transition(self, key):
        meta = self.redis_safe(
            lambda: self.redis.hgetall(self.meta_key(key)),
            None,
        )

        if meta is None:
            self._add_event(
                "adaptive_finalize_transition_missing_meta",
                {
                    "key": key,
                },
            )
            return

        if meta.get("state") != "transitioning":
            self._add_event(
                "adaptive_finalize_transition_skipped",
                {
                    "key": key,
                    "state": meta.get("state", ""),
                },
            )
            return

        self.redis_safe(
            lambda: self.redis.hset(
                self.meta_key(key),
                mapping={
                    "state": "read_opt",
                    "target_policy": "",
                },
            )
        )

        self._add_event(
            "adaptive_transition_finalized",
            {
                "key": key,
                "state": "read_opt",
            },
        )

    def quorum_put(self, key, value):
        policy = self.get_quorum(key)
        state = self.get_state(key)

        W = policy["W"]
        R = policy["R"]

        request_id = str(uuid.uuid4())
        timestamp = time.time()
        successes = 0

        parent_ctx = otel_context.get_current()
        parent_span = trace.get_current_span()

        parent_span.set_attribute("adaptive.state", state)
        parent_span.set_attribute("quorum.r", R)
        parent_span.set_attribute("quorum.w", W)

        self._add_event(
            "adaptive_quorum_selected",
            {
                "state": state,
                "R": R,
                "W": W,
            },
        )

        def call(node, ctx):
            token = otel_context.attach(ctx)
            try:
                with self.tracer.start_as_current_span("rpc.put_replica", kind=SpanKind.CLIENT) as span:
                    span.set_attribute("rpc.system", "grpc")
                    span.set_attribute("rpc.method", "Put")
                    span.set_attribute("server.node.id", node)
                    span.set_attribute("kv.key", key)
                    span.set_attribute("quorum.request_id", request_id)
                    span.set_attribute("quorum.timestamp", timestamp)
                    span.set_attribute("adaptive.state", state)
                    span.set_attribute("quorum.w", W)

                    metadata = []
                    inject(metadata, setter=MetadataSetter())

                    span.add_event("replica_put_send")

                    try:
                        resp = self.stubs[node].Put(
                            kv_pb2.PutRequest(
                                key=key,
                                value=value,
                                timestamp=timestamp,
                                client_id=self.client_id,
                                request_id=request_id,
                            ),
                            timeout=self.timeout,
                            metadata=metadata,
                        )

                        span.set_attribute("rpc.success", resp.success)
                        span.add_event("replica_put_reply", {"success": resp.success})

                        if not resp.success:
                            span.set_status(Status(StatusCode.ERROR, "replica put returned false"))

                        return resp.success
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        span.add_event("replica_put_failed")
                        return False
            finally:
                otel_context.detach(token)

        futures = [self.executor.submit(call, n, parent_ctx) for n in self.stubs]

        for future in as_completed(futures):
            if future.result():
                successes += 1

            self._add_event(
                "replica_put_result_processed",
                {
                    "successes_so_far": successes,
                    "required_w": W,
                },
            )

            if successes >= W:
                self._add_event(
                    "write_quorum_reached",
                    {
                        "successes": successes,
                        "required_w": W,
                    },
                )
                return True

        self._add_event(
            "write_quorum_failed",
            {
                "successes": successes,
                "required_w": W,
            },
        )
        return False

    def quorum_get(self, key):
        policy = self.get_quorum(key)
        state = self.get_state(key)

        R = policy["R"]
        W = policy["W"]

        replies = 0
        responses = []

        parent_ctx = otel_context.get_current()
        parent_span = trace.get_current_span()

        parent_span.set_attribute("adaptive.state", state)
        parent_span.set_attribute("quorum.r", R)
        parent_span.set_attribute("quorum.w", W)

        self._add_event(
            "adaptive_quorum_selected",
            {
                "state": state,
                "R": R,
                "W": W,
            },
        )

        def call(node, ctx):
            token = otel_context.attach(ctx)
            try:
                with self.tracer.start_as_current_span("rpc.get_replica", kind=SpanKind.CLIENT) as span:
                    span.set_attribute("rpc.system", "grpc")
                    span.set_attribute("rpc.method", "Get")
                    span.set_attribute("server.node.id", node)
                    span.set_attribute("kv.key", key)
                    span.set_attribute("adaptive.state", state)
                    span.set_attribute("quorum.r", R)

                    metadata = []
                    inject(metadata, setter=MetadataSetter())

                    span.add_event("replica_get_send")

                    try:
                        resp = self.stubs[node].Get(
                            kv_pb2.GetRequest(key=key),
                            timeout=self.timeout,
                            metadata=metadata,
                        )

                        span.set_attribute("rpc.found", resp.found)
                        span.add_event("replica_get_reply", {"found": resp.found})
                        return resp
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        span.add_event("replica_get_failed")
                        return None
            finally:
                otel_context.detach(token)

        futures = [self.executor.submit(call, n, parent_ctx) for n in self.stubs]

        for future in as_completed(futures):
            response = future.result()

            if response is not None:
                replies += 1
                if response.found:
                    responses.append(response)

            self._add_event(
                "replica_get_result_processed",
                {
                    "replies_so_far": replies,
                    "found_so_far": len(responses),
                    "required_r": R,
                },
            )

            if replies >= R:
                break

        if replies < R:
            self._add_event(
                "read_quorum_failed",
                {
                    "replies": replies,
                    "required_r": R,
                },
            )
            return "QUORUM_FAILED", None

        if len(responses) == 0:
            self._add_event("read_not_found")
            return "NOT_FOUND", None

        latest = max(
            responses,
            key=lambda r: (r.timestamp, r.client_id),
        )

        self._add_event(
            "read_value_selected",
            {
                "timestamp": latest.timestamp,
            },
        )

        return "OK", latest.value

    def async_record_read(self, key, parent_span_context=None):
        def task():
            token = None
            if parent_span_context is not None:
                parent_ctx = trace.set_span_in_context(
                    trace.NonRecordingSpan(parent_span_context)
                )
                token = otel_context.attach(parent_ctx)

            try:
                with self.tracer.start_as_current_span("adaptive.record_read") as span:
                    span.set_attribute("kv.key", key)
                    span.set_attribute("adaptive.node_id", self.node_id or "")
                    span.add_event("adaptive_record_read_started")

                    self.record_read(key)

                    if random() < self.policy_change_likelihood:
                        span.add_event("adaptive_maybe_trigger_transition")
                        self.maybe_trigger_transition(key)
                    else:
                        span.add_event("adaptive_transition_check_skipped_probability")
            finally:
                if token is not None:
                    otel_context.detach(token)

        thread = threading.Thread(target=task)
        thread.start()

    def async_record_write(self, key, parent_span_context=None):
        def task():
            token = None
            if parent_span_context is not None:
                parent_ctx = trace.set_span_in_context(
                    trace.NonRecordingSpan(parent_span_context)
                )
                token = otel_context.attach(parent_ctx)

            try:
                with self.tracer.start_as_current_span("adaptive.record_write") as span:
                    span.set_attribute("kv.key", key)
                    span.set_attribute("adaptive.node_id", self.node_id or "")
                    span.add_event("adaptive_record_write_started")

                    self.record_write(key)

                    self.finalize_transition(key)

                    if random() < self.policy_change_likelihood:
                        span.add_event("adaptive_maybe_trigger_transition")
                        self.maybe_trigger_transition(key)
                    else:
                        span.add_event("adaptive_transition_check_skipped_probability")
            finally:
                if token is not None:
                    otel_context.detach(token)

        thread = threading.Thread(target=task)
        thread.start()