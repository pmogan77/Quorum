from random import random

import grpc
import kv_pb2
import kv_pb2_grpc
import redis
import threading
import time
import uuid
from abc import ABC, abstractmethod

from concurrent.futures import as_completed

class AdaptiveQuorumManager(ABC):

    def __init__(self, config, stubs, executor, client_id, timeout):

        redis_cfg = config["redis"]

        self.redis = redis.Redis(
            host=redis_cfg["host"],
            port=redis_cfg["port"],
            decode_responses=True
        )

        self.write_opt = config["quorum_policies"]["write_opt"]
        self.read_opt = config["quorum_policies"]["read_opt"]

        self.stubs = stubs
        self.executor = executor
        self.client_id = client_id
        self.timeout = timeout
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

    @abstractmethod
    def get_state(self, key):
        pass

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

    def reset_reads(self, key):
        self.redis_safe(
            lambda: self.redis.hset(self.meta_key(key), "reads", 0)
        )

    def reset_writes(self, key):
        self.redis_safe(
            lambda: self.redis.hset(self.meta_key(key), "writes", 0)
        )

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
            quorum_get_dict = self.quorum_get(key)

            if quorum_get_dict["result"] != "OK":
                return

            quorum_put_dict = self.quorum_put(key, quorum_get_dict["response"])

            if quorum_put_dict["result"]:
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

        start_time = time.time()
        futures = [self.executor.submit(call, n) for n in self.stubs]

        for future in as_completed(futures):

            if future.result():
                successes += 1

            if successes >= W:
                end_time = time.time()
                total_time = end_time - start_time
                quorum_put_return = {"result": True, "time": total_time}
                return quorum_put_return

        end_time = time.time()
        total_time = end_time - start_time
        quorum_put_return = {"result": False, "time": total_time}
        return quorum_put_return

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

        start_time = time.time()
        futures = [self.executor.submit(call, n) for n in self.stubs]
        quorum_get_return = {"result": "", "response": None, "time": 0}

        for future in as_completed(futures):

            response = future.result()

            if response is not None:
                replies += 1

                if response.found:
                    responses.append(response)

            if replies >= R:
                break

        end_time = time.time()
        total_time = end_time - start_time

        if replies < R:
            quorum_get_return = {"result": "QUORUM_FAILED", "response": None, "time": total_time}
            return quorum_get_return

        if len(responses) == 0:
            quorum_get_return = {"result": "NOT_FOUND", "response": None, "time": total_time}
            return quorum_get_return

        latest = max(
            responses,
            key=lambda r: (r.timestamp, r.client_id)
        )

        quorum_get_return = {"result": "OK", "response": latest.value, "time":total_time}
        return quorum_get_return
    
    @abstractmethod
    def evaluate_and_trigger_read_update(self, key):
        pass
        
    @abstractmethod
    def evaluate_and_trigger_write_update(self, key):
        pass

    @abstractmethod
    def async_record_read(self, key, metadata):
        pass

    @abstractmethod
    def async_record_write(self, key, metadata):
        pass

class AdaptiveQuorumManagerRatioUpdate(AdaptiveQuorumManager):
    def __init__(self, config, stubs, executor, client_id, timeout, policy_change_likelihood):
        self.policy_change_likelihood = policy_change_likelihood
        super().__init__(config, stubs, executor, client_id, timeout)

    def evaluate_and_trigger_read_update(self, key):
        if random() < self.policy_change_likelihood:
            self.maybe_trigger_transition(key)

    def evaluate_and_trigger_write_update(self, key):
        # opportunistic completion in case proactive repair dies
        self.finalize_transition(key)
        
        if random() < self.policy_change_likelihood:
            self.maybe_trigger_transition(key)

    def maybe_trigger_transition(self, key):
        meta = self.redis_safe(
            lambda: self.redis.hgetall(self.meta_key(key)),
            None
        )

        if meta is None:
            return

        reads = int(meta.get("reads", 0))
        writes = int(meta.get("writes", 0))

        if reads + writes < self.config["adaptive_policy_ratio_update"]["min_operations"]:
            return

        ratio = reads / max(writes, 1)
        state = meta.get("state", "write_opt")

        if state == "write_opt" and ratio > self.config["adaptive_policy_ratio_update"]["read_threshold"]:
            self.start_transition(key, "read_opt")

        elif state == "read_opt" and ratio < self.config["adaptive_policy_ratio_update"]["write_threshold"]:
            self.start_transition(key, "write_opt")

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
    
    def async_record_read(self, key, metadata):

        def task():
            self.record_read(key)
            self.evaluate_and_trigger_read_update(key)

        thread = threading.Thread(target=task)
        thread.start()

    def async_record_write(self, key, metadata):

        def task():
            self.record_write(key)
            self.evaluate_and_trigger_write_update(key)           

        thread = threading.Thread(target=task)
        thread.start()

class AdaptiveQuorumManagerTimerExponentialUpdate(AdaptiveQuorumManager):
    def __init__(self, config, stubs, executor, client_id, timeout):
        # Dictionary where key is redis key, value is a dicionary of 
        # {currWrites, currWriteTime, currReads, currReadTime, timer, lock}
        self.keyDict = {} 
        self.keyDictLock = threading.RLock()
        self.timerTime = config["adaptive_policy_timer_exponential_update"]["timeout"]
        self.alphaVal = config["adaptive_policy_timer_exponential_update"]["alpha"]
        self.changeThreshold = config["adaptive_policy_timer_exponential_update"]["changeThreshold"]
        self.consecutiveThreshold = config["adaptive_policy_timer_exponential_update"]["consecutiveChangeThresh"]
        super().__init__(config, stubs, executor, client_id, timeout)   

    def get_or_create_local_state(self, key):
        with self.keyDictLock:
            if(self.keyDict.get(key) is None):
                newObj = {
                    "currWrites": 0,
                    "currWriteTime": 0,
                    "currReads": 0,
                    "currReadTime": 0,
                    "timer": None,
                    "lock": threading.RLock(),
                    "users": 1
                }
                self.keyDict[key] = newObj
            else:
                self.keyDict[key]["users"] += 1
            return self.keyDict[key]
    
    def release_local_state(self, key):
        with self.keyDictLock:
            if(self.keyDict.get(key) is not None):
                self.keyDict[key]["users"] = max(0, self.keyDict[key]["users"] - 1)

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
                        "readLatency": 0,
                        "writeLatency": 0,
                        "target_policy": "",
                        "consecutive_zero_writes": 0,
                        "consecutive_zero_reads": 0
                    }
                ),
                None
            )
            if created is None:
                return "strict_fallback"
            return "write_opt"
        return state
    
    def async_record_read(self, key, metadata):
        def task():
            currLocalStore = self.get_or_create_local_state(key)
            with currLocalStore["lock"]:
                currLocalStore["currReads"] = currLocalStore["currReads"] + 1
                currLocalStore["currReadTime"] = currLocalStore["currReadTime"] + metadata["time"]
                if currLocalStore["timer"] == None:
                    currLocalStore["timer"] = threading.Timer(self.timerTime, lambda: self.resend_messages(key))
                    currLocalStore["timer"].start()
            self.release_local_state(key)

        thread = threading.Thread(target=task)
        thread.start()

    def async_record_write(self, key, metadata):

        def task():
            currLocalStore = self.get_or_create_local_state(key)
            with currLocalStore["lock"]:
                currLocalStore["currWrites"] = currLocalStore["currWrites"] + 1
                currLocalStore["currWriteTime"] = currLocalStore["currWriteTime"] + metadata["time"]
                if currLocalStore["timer"] == None:
                    currLocalStore["timer"] = threading.Timer(self.timerTime, lambda: self.resend_messages(key))
                    currLocalStore["timer"].start()
            self.release_local_state(key)

        thread = threading.Thread(target=task)
        thread.start()
        
    def resend_messages(self, key):
        def task():
            # Get local and saved state for the current key
            currLocalStore = self.get_or_create_local_state(key)
            currState = self.redis_safe(
                lambda: self.redis.hgetall(self.meta_key(key)),
                None
            )
            if currState == None:
                self.release_local_state(key)
                return
            with currLocalStore["lock"]:
                # Get the current local state for the key and then release the lock before redis updates.
                if currLocalStore["timer"] != None:
                    currLocalStore["timer"].cancel()
                currReads = currLocalStore["currReads"]
                currWrites = currLocalStore["currWrites"]
                currReadTime = currLocalStore["currReadTime"]
                currWriteTime = currLocalStore["currWriteTime"]
                currLocalStore["timer"] = None

                # If not enough total writes or reads, don't make any changes.
                numReads = int(currState.get("reads")) + currReads
                numWrites = int(currState.get("writes")) + currWrites
                if numReads < self.changeThreshold or numWrites < self.changeThreshold:
                    return
                
                # Reset the local state since updates have been seen 
                currLocalStore["currReads"] = 0
                currLocalStore["currWrites"] = 0
                currLocalStore["currReadTime"] = 0
                currLocalStore["currWriteTime"] = 0
            self.release_local_state(key)
            with self.keyDictLock:
                if self.keyDict.get(key) is not None and self.keyDict[key]["users"] == 0 and \
                    currLocalStore["currReads"] == 0 and currLocalStore["currWrites"] == 0 and \
                    currLocalStore["currReadTime"] == 0 and currLocalStore["currWriteTime"] == 0:
                        del self.keyDict[key]
            
            # Update the read latency and number of reads in redis with exponential moving average
            effectiveReadAlpha = 1 - pow(1 - self.alphaVal, currReads)
            averageReadTime = currReadTime/currReads if currReads > 0 else 0
            readLatency = float(currState.get("readLatency", 0))
            readLatency = effectiveReadAlpha * averageReadTime + (1 - effectiveReadAlpha) * readLatency
            self.redis_safe(
                lambda: self.redis.hset(self.meta_key(key), "readLatency", readLatency)
            )
            numReads = int(currState.get("reads")) + currReads
            self.redis_safe(
                lambda: self.redis.hset(self.meta_key(key), "reads", numReads)
            )

            # Update the write latency and number of writes in redis with exponential moving average
            effectiveWriteAlpha = 1 - pow(1 - self.alphaVal, currWrites)
            averageWriteTime = currWriteTime/currWrites if currWrites > 0 else 0
            writeLatency = float(currState.get("writeLatency", 0))
            writeLatency = effectiveWriteAlpha * averageWriteTime + (1 - effectiveWriteAlpha) * writeLatency
            self.redis_safe(
                lambda: self.redis.hset(self.meta_key(key), "writeLatency", writeLatency)
            )
            numWrites = int(currState.get("writes")) + currWrites
            self.redis_safe(
                lambda: self.redis.hset(self.meta_key(key), "writes", numWrites)
            )
            
            # Calculate total read and write time overall
            totalWriteTime = currWrites * writeLatency
            totalReadTime = currReads * readLatency
            currMode = currState.get("state")

            # If the current state is write-optimized and there are self.consecutiveThreshold periods where 
            # there is a either a read and no write or the read time / write time ratio exceeds 1.5, then
            # switch modes. It is the opposite if the current state is read-optimized. Otherwise, don't do anything.

            # Currently write mode and either no writes and read for current period, or more time to read than write
            if (currMode == "write_opt" and ((totalWriteTime == 0 and totalReadTime != 0) or \
                (totalReadTime != 0 and totalWriteTime != 0 and totalReadTime/totalWriteTime > 1.5))):
                # Reset consecutive zero read period, and update consecutive zero write period
                self.redis_safe(lambda: self.redis.hset(self.meta_key(key), \
                    "consecutive_zero_reads", 0))
                self.redis_safe(lambda: self.redis.hincrby(self.meta_key(key), \
                    "consecutive_zero_writes", 1))
                # If too many consecutive zero writes, change state 
                if int(currState.get("consecutive_zero_writes")) > self.consecutiveThreshold:
                    self.start_transition(key, "read_opt")
                    self.redis_safe(lambda: self.redis.hset(self.meta_key(key), 
                        "consecutive_zero_writes", 0))

            elif (currMode == "read_opt" and ((totalWriteTime != 0 and totalReadTime == 0) or \
                (totalReadTime != 0 and totalWriteTime != 0 and totalWriteTime/totalReadTime > 1.5))):
                # Reset consecutive zero write period and update consecutive zero read period
                self.redis_safe(lambda: self.redis.hset(self.meta_key(key), \
                    "consecutive_zero_writes", 0))
                self.redis_safe(lambda: self.redis.hincrby(self.meta_key(key), \
                    "consecutive_zero_reads", 1))
                # If too many consecutive zero reads, change state
                if int(currState.get("consecutive_zero_reads")) > self.consecutiveThreshold:
                    self.start_transition(key, "write_opt")
                    self.redis_safe(lambda: self.redis.hset(self.meta_key(key), \
                        "consecutive_zero_reads", 0))
        thread = threading.Thread(target=task)
        thread.start()

    def evaluate_and_trigger_read_update(self, key):
        return super().evaluate_and_trigger_read_update(key)
    
    def evaluate_and_trigger_write_update(self, key):
        return super().evaluate_and_trigger_write_update(key)

