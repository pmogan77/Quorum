from random import random

import threading
import time
import uuid

from concurrent.futures import as_completed

import kv_pb2
import redis
import socket

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

ENTER_READER_AND_GET_THRESHOLDS_SCRIPT = """
local readers = redis.call('HINCRBY', KEYS[1], 'active_readers', 1)
local state = redis.call('HGET', KEYS[1], 'state')
local target_policy = redis.call('HGET', KEYS[1], 'target_policy')
local transition_id = redis.call('HGET', KEYS[1], 'transition_id')

return {
    readers,
    state or '',
    target_policy or '',
    transition_id or ''
}
"""

EXIT_READER_SCRIPT = """
local readers = tonumber(redis.call('HGET', KEYS[1], 'active_readers') or '0')
if readers > 1 then
    return redis.call('HINCRBY', KEYS[1], 'active_readers', -1)
else
    redis.call('HDEL', KEYS[1], 'active_readers')
    return 0
end
"""

TRY_UPDATE_POLICY_IF_NO_READERS_SCRIPT = """
local readers = tonumber(redis.call('HGET', KEYS[1], 'active_readers') or '0')
if readers ~= 0 then
    return 0
end

local current_state = redis.call('HGET', KEYS[1], 'state') or 'write_opt'

-- Entering transitioning must only happen from write_opt.
if ARGV[1] == 'transitioning' and current_state ~= 'write_opt' then
    return -1
end

-- If entering transitioning, store transition_id.
if ARGV[1] == 'transitioning' then
    redis.call('HSET', KEYS[1],
        'state', ARGV[1],
        'target_policy', ARGV[2],
        'transition_id', ARGV[3]
    )
    return 1
end

-- If going directly to write_opt, clear transition metadata.
if ARGV[1] == 'write_opt' then
    redis.call('HSET', KEYS[1],
        'state', 'write_opt',
        'target_policy', '',
        'transition_id', ''
    )
    return 1
end

redis.call('HSET', KEYS[1],
    'state', ARGV[1],
    'target_policy', ARGV[2]
)
return 1
"""

FINALIZE_TRANSITION_IF_MATCH_SCRIPT = """
local readers = tonumber(redis.call('HGET', KEYS[1], 'active_readers') or '0')
if readers ~= 0 then
    return 0
end

local state = redis.call('HGET', KEYS[1], 'state') or ''
local target = redis.call('HGET', KEYS[1], 'target_policy') or ''
local tid = redis.call('HGET', KEYS[1], 'transition_id') or ''

if state == 'transitioning' and target == 'read_opt' and tid == ARGV[1] then
    redis.call('HSET', KEYS[1],
        'state', 'read_opt',
        'target_policy', '',
        'transition_id', ''
    )
    return 1
end

return -1
"""


class MetadataSetter:
    def set(self, carrier, key, value):
        carrier.append((key, value))


class SharedThreshold:
    def __init__(self, initial: int):
        self.value = initial
        self.enter_finished = False
        self.enter_done = False

        self.state = ""
        self.target_policy = ""
        self.transition_id = ""


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
        enable_tracing=False,
    ):
        redis_cfg = config["redis"]

        pool = redis.ConnectionPool(
            host=redis_cfg["host"],
            port=redis_cfg["port"],
            decode_responses=True,

            max_connections=8,

            socket_connect_timeout=0.5,
            socket_timeout=0.5,

            socket_keepalive=True,
            socket_keepalive_options={
                socket.TCP_KEEPIDLE: 30,
                socket.TCP_KEEPINTVL: 10,
                socket.TCP_KEEPCNT: 3,
            },

            health_check_interval=15,
        )

        self.redis = redis.Redis(connection_pool=pool)
        self.redis.ping()
        self.warm_redis_pool(self.redis)

        self.unlock_sha = self.redis.script_load(UNLOCK_SCRIPT)
        self.enter_reader_sha = self.redis.script_load(ENTER_READER_AND_GET_THRESHOLDS_SCRIPT)
        self.exit_reader_sha = self.redis.script_load(EXIT_READER_SCRIPT)
        self.try_update_policy_sha = self.redis.script_load(TRY_UPDATE_POLICY_IF_NO_READERS_SCRIPT)
        self.finalize_transition_if_match_sha = self.redis.script_load(
            FINALIZE_TRANSITION_IF_MATCH_SCRIPT
        )

        print("Loaded scripts:")
        print(f"unlock_sha: {self.unlock_sha}")
        print(f"enter_reader_sha: {self.enter_reader_sha}")
        print(f"exit_reader_sha: {self.exit_reader_sha}")
        print(f"try_update_policy_sha: {self.try_update_policy_sha}")
        print(f"finalize_transition_if_match_sha: {self.finalize_transition_if_match_sha}")

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
        self.transition_retry_sleep_s = self.policy_cfg.get("transition_retry_sleep_s", 0.001)

        self.strict_policy = {
            "R": max(self.write_opt["R"], self.read_opt["R"]),
            "W": max(self.write_opt["W"], self.read_opt["W"]),
        }

    def warm_redis_pool(self, r: redis.Redis, count: int = 4):
        conns = []
        try:
            for _ in range(count):
                conn = r.connection_pool.get_connection("_")
                conn.connect()
                conns.append(conn)
        finally:
            for conn in conns:
                try:
                    r.connection_pool.release(conn)
                except Exception:
                    pass

    def _add_event(self, name, attributes=None):
        if not self.enable_tracing:
            return
        span = trace.get_current_span()
        try:
            span.add_event(name, attributes or {})
        except Exception:
            pass

    def _set_attr(self, key, value):
        if not self.enable_tracing:
            return
        span = trace.get_current_span()
        try:
            span.set_attribute(key, value)
        except Exception:
            pass

    def _perf_ms(self, start_ns):
        return (time.perf_counter_ns() - start_ns) / 1e6

    def _redis_pool_stats(self):
        try:
            pool = self.redis.connection_pool
            created = getattr(pool, "_created_connections", None)
            available = None
            in_use = None

            if hasattr(pool, "_available_connections"):
                available = len(pool._available_connections)
            if hasattr(pool, "_in_use_connections"):
                in_use = len(pool._in_use_connections)

            stats = {
                "redis.pool.created": -1 if created is None else int(created),
                "redis.pool.available": -1 if available is None else int(available),
                "redis.pool.in_use": -1 if in_use is None else int(in_use),
            }

            kwargs = getattr(pool, "connection_kwargs", {}) or {}
            stats["redis.host"] = str(kwargs.get("host", ""))
            stats["redis.port"] = int(kwargs.get("port", -1))
            stats["redis.decode_responses"] = bool(kwargs.get("decode_responses", False))
            return stats
        except Exception as e:
            return {
                "redis.pool.created": -1,
                "redis.pool.available": -1,
                "redis.pool.in_use": -1,
                "redis.host": "",
                "redis.port": -1,
                "redis.decode_responses": False,
                "redis.pool.error": str(e),
            }

    def _evalsha_safe(self, sha_attr, script, numkeys, *args):
        sha = getattr(self, sha_attr)
        try:
            return self.redis.evalsha(sha, numkeys, *args)
        except redis.exceptions.ResponseError as e:
            if "NOSCRIPT" in str(e):
                sha = self.redis.script_load(script)
                setattr(self, sha_attr, sha)
                return self.redis.evalsha(sha, numkeys, *args)
            raise

    def meta_key(self, key):
        return f"aq:{key}"

    def lock_key(self, key):
        return f"aq_lock:{key}"

    def redis_safe(self, fn, default=None):
        try:
            return fn()
        except Exception as e:
            self._add_event("redis_operation_failed", {"error": str(e)})
            return default

    def release_lock(self, key, token):
        if not token:
            return

        self.redis_safe(
            lambda: self._evalsha_safe(
                "unlock_sha",
                UNLOCK_SCRIPT,
                1,
                self.lock_key(key),
                token,
            ),
            0,
        )

    def _policy_from_state(self, state):
        if state == "write_opt":
            return self.write_opt
        if state == "read_opt":
            return self.read_opt
        return self.strict_policy

    def resolve_policy(self, key):
        meta_key = self.meta_key(key)

        state, target_policy = self.redis_safe(
            lambda: self.redis.hmget(meta_key, ["state", "target_policy"]),
            None,
        ) or (None, None)

        if state is None:
            created = self.redis_safe(
                lambda: self.redis.hset(
                    meta_key,
                    mapping={
                        "state": "write_opt",
                        "reads": 0,
                        "writes": 0,
                        "target_policy": "",
                        "transition_id": "",
                    },
                ),
                None,
            )
            if created is None:
                state = "strict_fallback"
                target_policy = ""
            else:
                state = "write_opt"
                target_policy = ""

        policy = self._policy_from_state(state)
        return policy, state

    def get_state(self, key):
        _, state = self.resolve_policy(key)
        return state

    def get_quorum(self, key):
        policy, _ = self.resolve_policy(key)
        return policy

    def _enter_reader_and_update_threshold(self, key, shared, mode):
        start_ns = time.perf_counter_ns()

        try:
            result = self.redis_safe(
                lambda: self._evalsha_safe(
                    "enter_reader_sha",
                    ENTER_READER_AND_GET_THRESHOLDS_SCRIPT,
                    1,
                    self.meta_key(key),
                ),
                None,
            )

            redis_ms = self._perf_ms(start_ns)

            if result is None or len(result) < 2:
                self._add_event(
                    "adaptive_enter_reader_failed",
                    {
                        "kv.key": key,
                        "adaptive.mode": mode,
                        "adaptive.redis_ms": redis_ms,
                        "adaptive.fallback_threshold": int(shared.value),
                    },
                )
                return

            shared.enter_done = True

            readers = result[0]
            state = result[1] or "write_opt"
            target_policy = result[2] if len(result) > 2 else ""
            transition_id = result[3] if len(result) > 3 else ""

            shared.state = state
            shared.target_policy = target_policy
            shared.transition_id = transition_id

            policy = self._policy_from_state(state)

            old_threshold = int(shared.value)

            if mode == "put":
                actual_threshold = int(policy["W"])
                shared.value = min(shared.value, actual_threshold)
                threshold_type = "W"
            else:
                actual_threshold = int(policy["R"])
                shared.value = min(shared.value, actual_threshold)
                threshold_type = "R"

            self._add_event(
                "adaptive_quorum_threshold_resolved",
                {
                    "kv.key": key,
                    "adaptive.mode": mode,
                    "adaptive.state": state,
                    "adaptive.target_policy": target_policy,
                    "adaptive.transition_id": transition_id,
                    "adaptive.threshold_type": threshold_type,
                    "adaptive.strict_threshold": old_threshold,
                    "adaptive.resolved_threshold": actual_threshold,
                    "adaptive.applied_threshold": int(shared.value),
                    "adaptive.active_readers_after_enter": int(readers),
                    "adaptive.redis_ms": redis_ms,
                },
            )

        finally:
            shared.enter_finished = True

    def _exit_reader_async(self, key, shared):
        def task():
            while not shared.enter_finished:
                time.sleep(0.0001)

            if shared.enter_done:
                self.exit_reader(key)

        threading.Thread(target=task, daemon=True).start()

    def _finalize_transition_after_exit_async(self, key, shared):
        def task():
            while not shared.enter_finished:
                time.sleep(0.0001)

            if not shared.enter_done:
                return

            self.exit_reader(key)

            if (
                shared.state == "transitioning"
                and shared.target_policy == "read_opt"
                and shared.transition_id
            ):
                self.finalize_transition_if_match(key, shared.transition_id)

        threading.Thread(target=task, daemon=True).start()

    def exit_reader(self, key):
        self.redis_safe(
            lambda: self._evalsha_safe(
                "exit_reader_sha",
                EXIT_READER_SCRIPT,
                1,
                self.meta_key(key),
            ),
            0,
        )

    def try_update_policy_if_no_readers(self, key, state, target_policy="", transition_id=""):
        result = self.redis_safe(
            lambda: self._evalsha_safe(
                "try_update_policy_sha",
                TRY_UPDATE_POLICY_IF_NO_READERS_SCRIPT,
                1,
                self.meta_key(key),
                state,
                target_policy,
                transition_id,
            ),
            0,
        )
        return result

    def set_policy_when_quiet(self, key, state, target_policy="", transition_id=""):
        attempts = 0
        wait_start = time.perf_counter_ns()

        while True:
            attempts += 1

            result = self.try_update_policy_if_no_readers(
                key,
                state,
                target_policy,
                transition_id,
            )

            if result == 1:
                self._add_event(
                    "adaptive_set_policy_when_quiet_done",
                    {
                        "adaptive.transition_attempts": attempts,
                        "adaptive.transition_wait_ms": self._perf_ms(wait_start),
                        "adaptive.transition_state": state,
                        "adaptive.transition_target_policy": target_policy,
                        "adaptive.transition_id": transition_id,
                    },
                )
                return True

            if result == -1:
                self._add_event(
                    "adaptive_set_policy_when_quiet_stale",
                    {
                        "adaptive.transition_attempts": attempts,
                        "adaptive.transition_state": state,
                        "adaptive.transition_target_policy": target_policy,
                        "adaptive.transition_id": transition_id,
                    },
                )
                return False

            time.sleep(self.transition_retry_sleep_s)

    def record_read(self, key):
        start_ns = time.perf_counter_ns()
        result = self.redis_safe(
            lambda: self.redis.hincrby(self.meta_key(key), "reads", 1),
            None,
        )
        self._add_event(
            "adaptive_record_read_counter",
            {
                "adaptive.record_read.counter_ms": self._perf_ms(start_ns),
                "adaptive.reads_after": int(result) if result is not None else -1,
            },
        )
        return result

    def record_write(self, key):
        total_start_ns = time.perf_counter_ns()
        meta_key = self.meta_key(key)

        with self.tracer.start_as_current_span("adaptive.record_write.redis_hincrby.command") as span:
            span.set_attribute("kv.key", key)
            span.set_attribute("adaptive.node_id", self.node_id or "")
            span.set_attribute("redis.op", "HINCRBY")
            span.set_attribute("redis.key", meta_key)
            span.set_attribute("redis.hash_field", "writes")
            span.set_attribute("redis.increment", 1)

            pool_stats = self._redis_pool_stats()
            for attr_key, attr_value in pool_stats.items():
                if attr_key == "redis.pool.error":
                    continue
                span.set_attribute(attr_key, attr_value)
            if "redis.pool.error" in pool_stats:
                span.set_attribute("redis.pool.error", pool_stats["redis.pool.error"])

            cmd_start_ns = time.perf_counter_ns()
            try:
                result = self.redis.hincrby(meta_key, "writes", 1)
                cmd_ms = self._perf_ms(cmd_start_ns)

                span.set_attribute("adaptive.record_write.redis_hincrby.command_ms", cmd_ms)
                span.set_attribute("adaptive.writes_after", int(result))
                span.set_attribute("redis.command_ok", True)

                if cmd_ms > 5:
                    span.add_event(
                        "redis_hincrby_slow",
                        {
                            "latency_ms": cmd_ms,
                            "possible_causes": "network_or_redis_server_or_pool_contention",
                        },
                    )
            except Exception as e:
                cmd_ms = self._perf_ms(cmd_start_ns)
                span.set_attribute("adaptive.record_write.redis_hincrby.command_ms", cmd_ms)
                span.set_attribute("redis.command_ok", False)
                span.set_attribute("redis.error.type", type(e).__name__)
                span.set_attribute("redis.error.message", str(e))
                span.record_exception(e)
                raise

        total_ms = self._perf_ms(total_start_ns)
        self._add_event(
            "adaptive_record_write_counter",
            {
                "adaptive.record_write.counter_ms": total_ms,
                "adaptive.writes_after": int(result) if result is not None else -1,
                "adaptive.record_write.meta_key": meta_key,
            },
        )
        return result

    def maybe_trigger_transition_from_meta(self, key, state, reads, writes, parent_span_context=None):
        total_ops = reads + writes
        ratio = reads / max(writes, 1)

        self._set_attr("adaptive.state", state)
        self._set_attr("adaptive.reads", reads)
        self._set_attr("adaptive.writes", writes)
        self._set_attr("adaptive.total_operations", total_ops)
        self._set_attr("adaptive.read_write_ratio", ratio)
        self._set_attr("adaptive.min_operations", self.policy_cfg["min_operations"])
        self._set_attr("adaptive.read_threshold", self.policy_cfg["read_threshold"])
        self._set_attr("adaptive.write_threshold", self.policy_cfg["write_threshold"])

        if total_ops < self.policy_cfg["min_operations"]:
            self._set_attr("adaptive.transition_candidate", False)
            self._add_event(
                "adaptive_transition_skipped",
                {
                    "reason": "min_operations_not_met",
                    "total_ops": total_ops,
                },
            )
            return

        target = None
        if state == "write_opt" and ratio > self.policy_cfg["read_threshold"]:
            target = "read_opt"
        elif state == "read_opt" and ratio < self.policy_cfg["write_threshold"]:
            target = "write_opt"

        self._set_attr("adaptive.transition_candidate", target is not None)

        if target is None:
            self._add_event(
                "adaptive_transition_skipped",
                {
                    "reason": "threshold_not_crossed",
                    "state": state,
                    "ratio": ratio,
                },
            )
            return

        self._set_attr("adaptive.transition_target", target)

        self.async_start_transition(key, target, parent_span_context=parent_span_context)

    def maybe_trigger_transition(self, key, parent_span_context=None):
        with self.tracer.start_as_current_span("adaptive.transition_check") as span:
            span.set_attribute("kv.key", key)
            span.set_attribute("adaptive.node_id", self.node_id or "")

            meta_start = time.perf_counter_ns()
            with self.tracer.start_as_current_span("adaptive.transition_check.redis_hmget"):
                meta = self.redis_safe(
                    lambda: self.redis.hmget(self.meta_key(key), ["state", "reads", "writes"]),
                    None,
                )
            span.set_attribute("adaptive.transition_check.hmget_ms", self._perf_ms(meta_start))
            span.set_attribute("adaptive.transition_check.meta_found", meta is not None)

            if meta is None:
                return

            state, reads, writes = meta
            state = state or "write_opt"
            reads = int(reads or 0)
            writes = int(writes or 0)

            self.maybe_trigger_transition_from_meta(
                key,
                state,
                reads,
                writes,
                parent_span_context=parent_span_context,
            )

    def start_transition(self, key, target):
        if target == "write_opt":
            self.set_policy_when_quiet(key, "write_opt", "")
            return None

        transition_id = str(uuid.uuid4())

        ok = self.set_policy_when_quiet(
            key,
            "transitioning",
            "read_opt",
            transition_id,
        )

        if ok:
            return transition_id

        return None

    def async_start_transition(self, key, target, parent_span_context=None):
        def task():
            token = None
            if parent_span_context is not None:
                parent_ctx = trace.set_span_in_context(trace.NonRecordingSpan(parent_span_context))
                token = otel_context.attach(parent_ctx)

            start_ns = time.perf_counter_ns()
            try:
                with self.tracer.start_as_current_span("adaptive.transition") as span:
                    span.set_attribute("kv.key", key)
                    span.set_attribute("adaptive.node_id", self.node_id or "")
                    span.set_attribute("adaptive.transition_target", target)

                    transition_start = time.perf_counter_ns()
                    with self.tracer.start_as_current_span("adaptive.transition.apply"):
                        transition_id = self.start_transition(key, target)

                    if transition_id:
                        span.set_attribute("adaptive.transition_id", transition_id)

                    span.set_attribute("adaptive.transition.apply_ms", self._perf_ms(transition_start))
                    span.set_attribute("adaptive.transition.total_ms", self._perf_ms(start_ns))
            finally:
                if token is not None:
                    otel_context.detach(token)

        threading.Thread(target=task, daemon=True).start()

    def finalize_transition_if_match(self, key, transition_id):
        attempts = 0
        wait_start = time.perf_counter_ns()

        while True:
            attempts += 1

            result = self.redis_safe(
                lambda: self._evalsha_safe(
                    "finalize_transition_if_match_sha",
                    FINALIZE_TRANSITION_IF_MATCH_SCRIPT,
                    1,
                    self.meta_key(key),
                    transition_id,
                ),
                -1,
            )

            if result == 1:
                self._add_event(
                    "adaptive_finalize_transition_done",
                    {
                        "adaptive.transition_attempts": attempts,
                        "adaptive.transition_wait_ms": self._perf_ms(wait_start),
                        "adaptive.transition_id": transition_id,
                    },
                )
                return True

            if result == -1:
                self._add_event(
                    "adaptive_finalize_transition_stale",
                    {
                        "adaptive.transition_attempts": attempts,
                        "adaptive.transition_id": transition_id,
                    },
                )
                return False

            time.sleep(self.transition_retry_sleep_s)

    def quorum_put(self, key, value, policy=None, state=None):
        request_id = str(uuid.uuid4())
        timestamp = time.time()
        successes = 0

        parent_ctx = otel_context.get_current()
        parent_span = trace.get_current_span()

        # NOTE: Experiment mod
        shared_w = SharedThreshold(self.strict_policy["W"])

        threading.Thread(
            target=self._enter_reader_and_update_threshold,
            args=(key, shared_w, "put"),
            daemon=True,
        ).start()
        

        # FORCE WRITE OPT
        # shared_w = SharedThreshold(self.write_opt["W"])
        # shared_w.enter_finished = True
        # shared_w.enter_done = False
        # shared_w.state = "write_opt"

        # FORCE READ OPT
        shared_w = SharedThreshold(self.read_opt["W"])
        shared_w.enter_finished = True
        shared_w.enter_done = False
        shared_w.state = "read_opt"

        parent_span.set_attribute("quorum.r", self.strict_policy["R"])
        parent_span.set_attribute("quorum.w", shared_w.value)

        nodes = list(self.stubs.keys())

        def call(node, ctx):
            token = otel_context.attach(ctx)
            try:
                with self.tracer.start_as_current_span("rpc.put_replica", kind=SpanKind.CLIENT) as span:
                    metadata = []
                    inject(metadata, setter=MetadataSetter())

                    span.set_attribute("rpc.system", "grpc")
                    span.set_attribute("rpc.method", "Put")
                    span.set_attribute("server.node.id", node)
                    span.set_attribute("kv.key", key)
                    span.set_attribute("quorum.request_id", request_id)
                    span.set_attribute("quorum.timestamp", timestamp)

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
                        return resp.success
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        return False
            finally:
                otel_context.detach(token)

        futures = [self.executor.submit(call, n, parent_ctx) for n in nodes]

        write_ok = False

        try:
            for future in as_completed(futures):
                if future.result():
                    successes += 1

                parent_span.set_attribute("quorum.w", shared_w.value)

                if successes >= shared_w.value:
                    write_ok = True
                    break

        finally:
            if write_ok:
                self._finalize_transition_after_exit_async(key, shared_w)
            else:
                self._exit_reader_async(key, shared_w)

        return write_ok

    def quorum_get(self, key, policy=None, state=None):
        replies = 0
        responses = []

        parent_ctx = otel_context.get_current()
        parent_span = trace.get_current_span()

        # NOTE: Experiment mod
        shared_r = SharedThreshold(self.strict_policy["R"])

        threading.Thread(
            target=self._enter_reader_and_update_threshold,
            args=(key, shared_r, "get"),
            daemon=True,
        ).start()

        # FORCE WRITE OPT
        # shared_r = SharedThreshold(self.write_opt["R"])
        # shared_r.enter_finished = True
        # shared_r.enter_done = False
        # shared_r.state = "write_opt"

        # FORCE READ OPT
        # shared_r = SharedThreshold(self.read_opt["R"])
        # shared_r.enter_finished = True
        # shared_r.enter_done = False
        # shared_r.state = "read_opt"

        parent_span.set_attribute("quorum.r", shared_r.value)
        parent_span.set_attribute("quorum.w", self.strict_policy["W"])

        nodes = list(self.stubs.keys())

        def call(node, ctx):
            token = otel_context.attach(ctx)
            try:
                with self.tracer.start_as_current_span("rpc.get_replica", kind=SpanKind.CLIENT) as span:
                    metadata = []
                    inject(metadata, setter=MetadataSetter())

                    span.set_attribute("rpc.system", "grpc")
                    span.set_attribute("rpc.method", "Get")
                    span.set_attribute("server.node.id", node)
                    span.set_attribute("kv.key", key)

                    try:
                        resp = self.stubs[node].Get(
                            kv_pb2.GetRequest(key=key),
                            timeout=self.timeout,
                            metadata=metadata,
                        )
                        span.set_attribute("rpc.found", resp.found)
                        return resp
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        return None
            finally:
                otel_context.detach(token)

        futures = [self.executor.submit(call, n, parent_ctx) for n in nodes]

        try:
            for future in as_completed(futures):
                response = future.result()
                if response is not None:
                    replies += 1
                    if response.found:
                        responses.append(response)

                parent_span.set_attribute("quorum.r", shared_r.value)

                if replies >= shared_r.value:
                    break

            if replies < shared_r.value:
                return "QUORUM_FAILED", None

            if len(responses) == 0:
                return "NOT_FOUND", None

            latest = max(responses, key=lambda r: (r.timestamp, r.client_id))
            parent_span.add_event(
                "read_value_selected",
                {
                    "timestamp": latest.timestamp,
                },
            )
            return "OK", latest.value

        finally:
            self._exit_reader_async(key, shared_r)

    def post_read_update(self, key, parent_span_context=None):
        counter_start = time.perf_counter_ns()

        with self.tracer.start_as_current_span("adaptive.record_read.redis_hincrby"):
            self.record_read(key)

        self._set_attr("adaptive.record_read.hincrby_ms", self._perf_ms(counter_start))

        sampled = random() < self.policy_change_likelihood
        self._set_attr("adaptive.record_read.sampled_for_transition_check", sampled)

        if not sampled:
            return

        self.maybe_trigger_transition(key, parent_span_context=parent_span_context)

    def post_write_update(self, key, parent_span_context=None):
        total_start_ns = time.perf_counter_ns()

        with self.tracer.start_as_current_span("adaptive.record_write.redis_hincrby") as span:
            span.set_attribute("kv.key", key)
            span.set_attribute("adaptive.node_id", self.node_id or "")
            span.set_attribute("adaptive.meta_key", self.meta_key(key))

            counter_start_ns = time.perf_counter_ns()
            result = self.redis_safe(lambda: self.record_write(key), None)
            counter_ms = self._perf_ms(counter_start_ns)

            span.set_attribute("adaptive.record_write.hincrby_ms", counter_ms)
            span.set_attribute("adaptive.record_write.counter_success", result is not None)

            if result is not None:
                span.set_attribute("adaptive.writes_after", int(result))

            pool_stats = self._redis_pool_stats()
            for attr_key, attr_value in pool_stats.items():
                if attr_key == "redis.pool.error":
                    continue
                span.set_attribute(attr_key, attr_value)
            if "redis.pool.error" in pool_stats:
                span.set_attribute("redis.pool.error", pool_stats["redis.pool.error"])

        sampled = random() < self.policy_change_likelihood
        self._set_attr("adaptive.record_write.sampled_for_transition_check", sampled)

        if not sampled:
            self._set_attr("adaptive.record_write.total_ms", self._perf_ms(total_start_ns))
            return

        transition_check_start_ns = time.perf_counter_ns()
        self.maybe_trigger_transition(key, parent_span_context=parent_span_context)

        self._set_attr(
            "adaptive.record_write.transition_check_ms",
            self._perf_ms(transition_check_start_ns),
        )
        self._set_attr("adaptive.record_write.total_ms", self._perf_ms(total_start_ns))

    def async_record_read(self, key, parent_span_context=None):
        def task():
            token = None

            if parent_span_context is not None:
                parent_ctx = trace.set_span_in_context(trace.NonRecordingSpan(parent_span_context))
                token = otel_context.attach(parent_ctx)

            start_ns = time.perf_counter_ns()

            try:
                with self.tracer.start_as_current_span("adaptive.record_read") as span:
                    span.set_attribute("kv.key", key)
                    span.set_attribute("adaptive.node_id", self.node_id or "")
                    self.post_read_update(key, parent_span_context=span.get_span_context())
                    span.set_attribute("adaptive.record_read.total_ms", self._perf_ms(start_ns))
            finally:
                if token is not None:
                    otel_context.detach(token)

        threading.Thread(target=task, daemon=True).start()

    def async_record_write(self, key, parent_span_context=None):
        def task():
            token = None

            if parent_span_context is not None:
                parent_ctx = trace.set_span_in_context(trace.NonRecordingSpan(parent_span_context))
                token = otel_context.attach(parent_ctx)

            start_ns = time.perf_counter_ns()

            try:
                with self.tracer.start_as_current_span("adaptive.record_write") as span:
                    span.set_attribute("kv.key", key)
                    span.set_attribute("adaptive.node_id", self.node_id or "")
                    span.set_attribute("thread.name", threading.current_thread().name)
                    span.set_attribute("thread.ident", threading.get_ident())

                    self.post_write_update(key, parent_span_context=span.get_span_context())
                    span.set_attribute("adaptive.record_write.total_ms", self._perf_ms(start_ns))

            finally:
                if token is not None:
                    otel_context.detach(token)

        threading.Thread(target=task, daemon=True).start()