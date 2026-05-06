-------------------------- MODULE AdaptiveQuorumPhase3 --------------------------

EXTENDS Naturals, FiniteSets

(***************************************************************************)
(*                                                                         *)
(* AdaptiveQuorumPhase3Lock                                                 *)
(*                                                                         *)
(* Phase 3 extends the Phase 2 model by adding a Redis write lock.           *)
(*                                                                         *)
(* Phase 2 modeled:                                                         *)
(*                                                                         *)
(*   - coordinators                                                         *)
(*   - delayed Redis/config responses                                       *)
(*   - conservative quorums before Redis responds                           *)
(*   - explicit checking that reads return the latest value                 *)
(*                                                                         *)
(* Phase 3 adds:                                                           *)
(*                                                                         *)
(*   - per-key Redis write locks                                            *)
(*   - only the lock holder may finish a write                              *)
(*   - policy switching is blocked while a key is write-locked              *)
(*   - stale writes can be aborted and the lock released                    *)
(*                                                                         *)
(* This is the first model where multiple coordinators can be tested more    *)
(* meaningfully.                                                            *)
(*                                                                         *)
(* Important remaining abstractions:                                        *)
(*                                                                         *)
(*   - no explicit network message queues                                   *)
(*   - no node failures                                                     *)
(*   - no below-quorum failed writes                                        *)
(*   - no real-time timeout model                                           *)
(*                                                                         *)
(* Below-quorum writes are still omitted because they are undefined behavior *)
(* in the intended design.                                                  *)
(*                                                                         *)
(***************************************************************************)


(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Keys,
        \* Set of keys in the store.

    Servers,
        \* Set of replica servers.

    Coordinators,
        \* Set of coordinators.
        \*
        \* Phase 3 supports multiple coordinators more safely because writes
        \* must acquire the Redis write lock before finishing.

    Values,
        \* Non-delete values.
        \* Tombstone is added separately.

    RH_R,
        \* Read quorum size under ReadHeavy.

    RH_W,
        \* Write quorum size under ReadHeavy.

    WH_R,
        \* Read quorum size under WriteHeavy.

    WH_W,
        \* Write quorum size under WriteHeavy.

    MaxTS,
        \* Maximum timestamp explored by TLC.

    MaxEpoch
        \* Maximum Redis config epoch explored by TLC.


(***************************************************************************)
(* BASIC SETS                                                              *)
(***************************************************************************)

N == Cardinality(Servers)

Timestamp == 0..MaxTS

Epoch == 0..MaxEpoch

Tombstone == "Tombstone"

AllValues == Values \cup {Tombstone}

Policies == {"ReadHeavy", "WriteHeavy", "Intermediate"}

NoKey == "NoKey"

NoValue == "NoValue"

NoPolicy == "NoPolicy"

NoCoord == "NoCoord"

OpKinds == {"Idle", "Read", "Write"}

MaybeKeys == Keys \cup {NoKey}

MaybeValues == AllValues \cup {NoValue}

MaybePolicies == Policies \cup {NoPolicy}

MaybeCoordinators == Coordinators \cup {NoCoord}


(***************************************************************************)
(* QUORUM DEFINITIONS                                                      *)
(***************************************************************************)

MaxReadQuorum ==
    IF RH_R >= WH_R THEN RH_R ELSE WH_R

MaxWriteQuorum ==
    IF RH_W >= WH_W THEN RH_W ELSE WH_W

ReadQuorumSize(policy) ==
    CASE policy = "ReadHeavy"    -> RH_R
       [] policy = "WriteHeavy"   -> WH_R
       [] policy = "Intermediate" -> MaxReadQuorum

WriteQuorumSize(policy) ==
    CASE policy = "ReadHeavy"    -> RH_W
       [] policy = "WriteHeavy"   -> WH_W
       [] policy = "Intermediate" -> MaxWriteQuorum

ReadQuorums(policy) ==
    {Q \in SUBSET Servers : Cardinality(Q) >= ReadQuorumSize(policy)}

    \* A read may receive more responses than the minimum required.

WriteQuorums(policy) ==
    {Q \in SUBSET Servers : Cardinality(Q) >= WriteQuorumSize(policy)}

    \* A write may receive more acknowledgments than the minimum required.


(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    redisConfig,
        \* Per-key policy and epoch stored in Redis.

    redisLock,
        \* redisLock[key] is either:
        \*
        \*   NoCoord
        \*       no coordinator holds the write lock for key
        \*
        \*   c \in Coordinators
        \*       coordinator c holds the write lock for key
        \*
        \* This is the main Phase 3 addition.

    replica,
        \* replica[server][key] stores a value/timestamp record.

    latestWrite,
        \* Abstract latest successful write for each key.

    lastWriteReplicas,
        \* Set of replicas known to store latestWrite[key].

    lastWritePolicy,
        \* Policy used by the latest successful write.

    coord,
        \* Per-coordinator operation state.

    readOk,
        \* TRUE iff every completed read so far returned the latest value.

    lastRead
        \* Diagnostic record for the most recent completed read.


vars ==
    <<redisConfig,
      redisLock,
      replica,
      latestWrite,
      lastWriteReplicas,
      lastWritePolicy,
      coord,
      readOk,
      lastRead>>


(***************************************************************************)
(* COORDINATOR RECORDS                                                     *)
(***************************************************************************)

IdleCoord ==
    [kind |-> "Idle",
     key |-> NoKey,
     value |-> NoValue,
     ts |-> 0,
     redisKnown |-> FALSE,
     observedPolicy |-> NoPolicy,
     observedEpoch |-> 0]

ReadCoord(key) ==
    [kind |-> "Read",
     key |-> key,
     value |-> NoValue,
     ts |-> 0,
     redisKnown |-> FALSE,
     observedPolicy |-> NoPolicy,
     observedEpoch |-> 0]

WriteCoord(key, value, timestamp) ==
    [kind |-> "Write",
     key |-> key,
     value |-> value,
     ts |-> timestamp,
     redisKnown |-> FALSE,
     observedPolicy |-> NoPolicy,
     observedEpoch |-> 0]

NoReadRecord ==
    [key |-> NoKey,
     value |-> NoValue,
     ts |-> 0,
     expectedValue |-> NoValue,
     expectedTs |-> 0]


(***************************************************************************)
(* REDIS RESPONSE HELPERS                                                  *)
(***************************************************************************)

CoordWithRedisReply(c) ==
    [coord[c] EXCEPT
        !.redisKnown = TRUE,
        !.observedPolicy = redisConfig[coord[c].key].policy,
        !.observedEpoch = redisConfig[coord[c].key].epoch]

EffectivePolicy(c) ==
    IF coord[c].redisKnown
    THEN coord[c].observedPolicy
    ELSE "Intermediate"

    \* Before Redis responds, the coordinator uses conservative Intermediate
    \* quorums. After Redis responds, it uses the returned policy.

RedisReplyFresh(c) ==
    \/ coord[c].redisKnown = FALSE
    \/ coord[c].observedEpoch # redisConfig[coord[c].key].epoch

HasFreshRedisInfo(c) ==
    /\ coord[c].redisKnown
    /\ coord[c].observedEpoch = redisConfig[coord[c].key].epoch

CanFinishUsingEffectivePolicy(c) ==
    \/ coord[c].redisKnown = FALSE
    \/ HasFreshRedisInfo(c)

    \* If Redis has not responded, conservative quorums are safe.
    \* If Redis has responded, the response must match the current epoch.


(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ redisConfig =
        [key \in Keys |-> [policy |-> "WriteHeavy", epoch |-> 0]]

    /\ redisLock =
        [key \in Keys |-> NoCoord]

        \* No write locks are held initially.

    /\ replica =
        [server \in Servers |->
            [key \in Keys |-> [value |-> Tombstone, ts |-> 0]]]

    /\ latestWrite =
        [key \in Keys |-> [value |-> Tombstone, ts |-> 0]]

    /\ lastWriteReplicas =
        [key \in Keys |-> Servers]

    /\ lastWritePolicy =
        [key \in Keys |-> "WriteHeavy"]

    /\ coord =
        [c \in Coordinators |-> IdleCoord]

    /\ readOk = TRUE

    /\ lastRead = NoReadRecord


(***************************************************************************)
(* BASIC HELPERS                                                           *)
(***************************************************************************)

WriteRecord(value, timestamp) ==
    [value |-> value, ts |-> timestamp]

NewTimestamp(key, timestamp) ==
    /\ timestamp \in Timestamp
    /\ timestamp > latestWrite[key].ts

ReplicaHasLatest(server, key) ==
    replica[server][key] = latestWrite[key]

LatestCoverageSafe(key) ==
    Cardinality(lastWriteReplicas[key])
        + ReadQuorumSize(redisConfig[key].policy)
        > N

    \* Current-policy quorum safety condition.

UpdateOneServer(serverState, key, value, timestamp) ==
    [serverState EXCEPT ![key] = WriteRecord(value, timestamp)]

UpdateReplicaSet(key, value, timestamp, writeSet) ==
    [server \in Servers |->
        IF server \in writeSet
        THEN UpdateOneServer(replica[server], key, value, timestamp)
        ELSE replica[server]]

NewRedisConfig(key, newPolicy) ==
    [redisConfig EXCEPT
        ![key] =
            [policy |-> newPolicy,
             epoch  |-> redisConfig[key].epoch + 1]]


(***************************************************************************)
(* READ RESULT HELPER                                                      *)
(***************************************************************************)

IsMaxReadRecord(rec, readSet, key) ==
    /\ rec \in {replica[server][key] : server \in readSet}
    /\ \A server \in readSet :
        rec.ts >= replica[server][key].ts

    \* The read algorithm returns a max-timestamp record from the read quorum.


(***************************************************************************)
(* START OPERATIONS                                                        *)
(***************************************************************************)

StartRead(c, key) ==
    /\ coord[c].kind = "Idle"

    /\ coord' =
        [coord EXCEPT ![c] = ReadCoord(key)]

    /\ UNCHANGED
        <<redisConfig,
          redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          readOk,
          lastRead>>


StartWrite(c, key, value, timestamp) ==
    /\ coord[c].kind = "Idle"

    /\ value \in AllValues

    /\ NewTimestamp(key, timestamp)

        \* The coordinator proposes a timestamp newer than the current latest
        \* write. With multiple coordinators, this may later become stale
        \* before the write finishes; AbortStaleWrite handles that case.

    /\ coord' =
        [coord EXCEPT ![c] = WriteCoord(key, value, timestamp)]

    /\ UNCHANGED
        <<redisConfig,
          redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          readOk,
          lastRead>>


(***************************************************************************)
(* ACQUIRE REDIS WRITE LOCK                                                *)
(***************************************************************************)

AcquireWriteLock(c) ==
    /\ coord[c].kind = "Write"

        \* Only write operations need the Redis write lock.

    /\ redisLock[coord[c].key] = NoCoord

        \* The lock for this key is currently free.

    /\ redisLock' =
        [redisLock EXCEPT ![coord[c].key] = c]

        \* Coordinator c now owns the write lock for its key.

    /\ UNCHANGED
        <<redisConfig,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          coord,
          readOk,
          lastRead>>


(***************************************************************************)
(* ABORT STALE WRITE                                                       *)
(***************************************************************************)

AbortStaleWrite(c) ==
    /\ coord[c].kind = "Write"

    /\ coord[c].ts <= latestWrite[coord[c].key].ts

        \* The write's timestamp is no longer newer than latestWrite.
        \*
        \* This can happen with multiple coordinators:
        \*
        \*   c1 starts write with ts = 1
        \*   c2 starts write with ts = 1
        \*   c1 finishes first
        \*   c2's timestamp is now stale
        \*
        \* Since writes must advance timestamps, c2 cannot finish and must
        \* abort/retry.

    /\ redisLock' =
        IF redisLock[coord[c].key] = c
        THEN [redisLock EXCEPT ![coord[c].key] = NoCoord]
        ELSE redisLock

        \* If this coordinator held the lock, release it.
        \* If it had not yet acquired the lock, nothing changes.

    /\ coord' =
        [coord EXCEPT ![c] = IdleCoord]

        \* The coordinator returns to idle. A real implementation would retry
        \* with a newer timestamp.

    /\ UNCHANGED
        <<redisConfig,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          readOk,
          lastRead>>


(***************************************************************************)
(* RECEIVE REDIS RESPONSE                                                  *)
(***************************************************************************)

ReceiveRedis(c) ==
    /\ coord[c].kind # "Idle"

    /\ RedisReplyFresh(c)

    /\ coord' =
        [coord EXCEPT ![c] = CoordWithRedisReply(c)]

    /\ UNCHANGED
        <<redisConfig,
          redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          readOk,
          lastRead>>


(***************************************************************************)
(* FINISH READ                                                             *)
(***************************************************************************)

FinishRead(c, readSet) ==
    /\ coord[c].kind = "Read"

    /\ CanFinishUsingEffectivePolicy(c)

    /\ readSet \in ReadQuorums(EffectivePolicy(c))

    /\ \E rec \in {replica[server][coord[c].key] : server \in readSet} :
        /\ IsMaxReadRecord(rec, readSet, coord[c].key)

        /\ lastRead' =
            [key |-> coord[c].key,
             value |-> rec.value,
             ts |-> rec.ts,
             expectedValue |-> latestWrite[coord[c].key].value,
             expectedTs |-> latestWrite[coord[c].key].ts]

        /\ readOk' =
            readOk /\ (rec = latestWrite[coord[c].key])

            \* If the read returns anything other than the current latest
            \* successful write, ReadReturnsLatest will fail.

    /\ coord' =
        [coord EXCEPT ![c] = IdleCoord]

    /\ UNCHANGED
        <<redisConfig,
          redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy>>


(***************************************************************************)
(* FINISH WRITE                                                            *)
(***************************************************************************)

FinishWrite(c, writeSet) ==
    /\ coord[c].kind = "Write"

    /\ redisLock[coord[c].key] = c

        \* Phase 3 lock rule:
        \*
        \* only the coordinator holding the Redis write lock for this key may
        \* complete the write.

    /\ CanFinishUsingEffectivePolicy(c)

    /\ NewTimestamp(coord[c].key, coord[c].ts)

        \* The timestamp must still be newer than latestWrite.
        \*
        \* This prevents a stale write from overwriting a newer successful
        \* write.

    /\ writeSet \in WriteQuorums(EffectivePolicy(c))

    /\ replica' =
        UpdateReplicaSet(coord[c].key,
                         coord[c].value,
                         coord[c].ts,
                         writeSet)

    /\ latestWrite' =
        [latestWrite EXCEPT
            ![coord[c].key] =
                WriteRecord(coord[c].value, coord[c].ts)]

    /\ lastWriteReplicas' =
        [lastWriteReplicas EXCEPT
            ![coord[c].key] = writeSet]

    /\ lastWritePolicy' =
        [lastWritePolicy EXCEPT
            ![coord[c].key] = EffectivePolicy(c)]

    /\ redisLock' =
        [redisLock EXCEPT ![coord[c].key] = NoCoord]

        \* Release the Redis write lock at the successful write's
        \* linearization point.

    /\ coord' =
        [coord EXCEPT ![c] = IdleCoord]

    /\ UNCHANGED
        <<redisConfig,
          readOk,
          lastRead>>


(***************************************************************************)
(* PROPAGATION                                                             *)
(***************************************************************************)

Propagate(src, dst, key) ==
    /\ src \in Servers
    /\ dst \in Servers
    /\ src # dst

    /\ replica[src][key].ts > replica[dst][key].ts

    /\ replica' =
        [replica EXCEPT
            ![dst][key] = replica[src][key]]

    /\ lastWriteReplicas' =
        IF replica[src][key] = latestWrite[key]
        THEN [lastWriteReplicas EXCEPT
                ![key] = lastWriteReplicas[key] \cup {dst}]
        ELSE lastWriteReplicas

    /\ UNCHANGED
        <<redisConfig,
          redisLock,
          latestWrite,
          lastWritePolicy,
          coord,
          readOk,
          lastRead>>


(***************************************************************************)
(* POLICY SWITCHING                                                        *)
(***************************************************************************)

SwitchReadHeavyToWriteHeavy(key) ==
    /\ redisConfig[key].policy = "ReadHeavy"

    /\ redisConfig[key].epoch < MaxEpoch

    /\ redisLock[key] = NoCoord

        \* Keep policy switching atomic with respect to writes for this key.
        \*
        \* A real Redis implementation would normally protect config changes
        \* and writes with consistent metadata/locking rules.

    /\ LatestCoverageSafe(key)

    /\ redisConfig' =
        NewRedisConfig(key, "WriteHeavy")

    /\ UNCHANGED
        <<redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          coord,
          readOk,
          lastRead>>


SwitchWriteHeavyToIntermediate(key) ==
    /\ redisConfig[key].policy = "WriteHeavy"

    /\ redisConfig[key].epoch < MaxEpoch

    /\ redisLock[key] = NoCoord

        \* Do not change policy while a write holds the key lock.

    /\ redisConfig' =
        NewRedisConfig(key, "Intermediate")

    /\ UNCHANGED
        <<redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          coord,
          readOk,
          lastRead>>


SwitchIntermediateToReadHeavy(key) ==
    /\ redisConfig[key].policy = "Intermediate"

    /\ redisConfig[key].epoch < MaxEpoch

    /\ redisLock[key] = NoCoord

        \* Do not complete the policy switch while a write holds the key lock.

    /\ Cardinality(lastWriteReplicas[key]) + RH_R > N

        \* Critical safety guard:
        \*
        \* before entering ReadHeavy, the latest write must be replicated
        \* widely enough to intersect ReadHeavy reads.

    /\ redisConfig' =
        NewRedisConfig(key, "ReadHeavy")

    /\ UNCHANGED
        <<redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          coord,
          readOk,
          lastRead>>


(***************************************************************************)
(* NEXT-STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E c \in Coordinators :
        \E key \in Keys :
            StartRead(c, key)

    \/ \E c \in Coordinators :
        \E key \in Keys :
        \E value \in AllValues :
        \E timestamp \in Timestamp :
            StartWrite(c, key, value, timestamp)

    \/ \E c \in Coordinators :
            AcquireWriteLock(c)

    \/ \E c \in Coordinators :
            AbortStaleWrite(c)

    \/ \E c \in Coordinators :
            ReceiveRedis(c)

    \/ \E c \in Coordinators :
        \E readSet \in ReadQuorums(EffectivePolicy(c)) :
            FinishRead(c, readSet)

    \/ \E c \in Coordinators :
        \E writeSet \in WriteQuorums(EffectivePolicy(c)) :
            FinishWrite(c, writeSet)

    \/ \E src \in Servers :
        \E dst \in Servers :
        \E key \in Keys :
            Propagate(src, dst, key)

    \/ \E key \in Keys :
            SwitchReadHeavyToWriteHeavy(key)

    \/ \E key \in Keys :
            SwitchWriteHeavyToIntermediate(key)

    \/ \E key \in Keys :
            SwitchIntermediateToReadHeavy(key)


Spec ==
    Init /\ [][Next]_vars


(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ redisConfig \in
        [Keys -> [policy : Policies, epoch : Epoch]]

    /\ redisLock \in
        [Keys -> MaybeCoordinators]

    /\ replica \in
        [Servers -> [Keys -> [value : AllValues, ts : Timestamp]]]

    /\ latestWrite \in
        [Keys -> [value : AllValues, ts : Timestamp]]

    /\ lastWriteReplicas \in
        [Keys -> SUBSET Servers]

    /\ lastWritePolicy \in
        [Keys -> Policies]

    /\ coord \in
        [Coordinators ->
            [kind : OpKinds,
             key : MaybeKeys,
             value : MaybeValues,
             ts : Timestamp,
             redisKnown : BOOLEAN,
             observedPolicy : MaybePolicies,
             observedEpoch : Epoch]]

    /\ readOk \in BOOLEAN

    /\ lastRead \in
        [key : MaybeKeys,
         value : MaybeValues,
         ts : Timestamp,
         expectedValue : MaybeValues,
         expectedTs : Timestamp]


(***************************************************************************)
(* CONSTANT ASSUMPTIONS                                                    *)
(***************************************************************************)

QuorumAssumptions ==
    /\ RH_R \in 1..N
    /\ RH_W \in 1..N
    /\ WH_R \in 1..N
    /\ WH_W \in 1..N

    /\ RH_R <= WH_R
        \* ReadHeavy has cheaper reads.

    /\ RH_W >= WH_W
        \* ReadHeavy has more expensive writes.

    /\ RH_R + RH_W > N
        \* ReadHeavy quorums intersect.

    /\ WH_R + WH_W > N
        \* WriteHeavy quorums intersect.

    /\ MaxReadQuorum + MaxWriteQuorum > N
        \* Intermediate quorums intersect.


(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

LastWriteReplicasCorrect ==
    \A key \in Keys :
        \A server \in lastWriteReplicas[key] :
            ReplicaHasLatest(server, key)

    \* The bookkeeping set lastWriteReplicas[key] is sound.


LatestWriteCoverage ==
    \A key \in Keys :
        LatestCoverageSafe(key)

    \* The latest successful write intersects every current-policy read
    \* quorum.


TimestampBoundedByLatest ==
    \A server \in Servers :
        \A key \in Keys :
            replica[server][key].ts <= latestWrite[key].ts

    \* Since below-quorum writes are not modeled, no replica should contain a
    \* timestamp newer than latestWrite[key].


DeleteIsJustWrite ==
    /\ Tombstone \in AllValues

    /\ \A key \in Keys :
        latestWrite[key].value \in AllValues

    /\ \A server \in Servers :
        \A key \in Keys :
            replica[server][key].value \in AllValues

    \* Tombstone is just another timestamped value.


CoordinatorIdleShape ==
    \A c \in Coordinators :
        coord[c].kind = "Idle" =>
            /\ coord[c].key = NoKey
            /\ coord[c].value = NoValue
            /\ coord[c].redisKnown = FALSE
            /\ coord[c].observedPolicy = NoPolicy

    \* Idle coordinators have the canonical idle fields.


CoordinatorActiveShape ==
    \A c \in Coordinators :
        coord[c].kind # "Idle" =>
            /\ coord[c].key \in Keys
            /\ coord[c].observedPolicy \in MaybePolicies

    \* Active coordinators must refer to real keys.


FreshRedisIfKnown ==
    \A c \in Coordinators :
        /\ coord[c].kind # "Idle"
        /\ coord[c].redisKnown
        =>
        coord[c].observedEpoch \in Epoch


ReadReturnsLatest ==
    readOk

    \* FinishRead sets readOk to FALSE if a completed read returns anything
    \* other than latestWrite[key] at the read's completion point.


LockOwnerIsWriter ==
    \A key \in Keys :
        redisLock[key] # NoCoord =>
            /\ redisLock[key] \in Coordinators
            /\ coord[redisLock[key]].kind = "Write"
            /\ coord[redisLock[key]].key = key

    \* If a key lock is held, it is held by an active writer for that key.


WriterHoldsAtMostOneLock ==
    \A c \in Coordinators :
        Cardinality({key \in Keys : redisLock[key] = c}) <= 1

    \* A coordinator can hold at most one key lock.
    \*
    \* This is not strictly necessary for a single-key operation model, but it
    \* catches accidental lock bookkeeping bugs.


IdleCoordinatorHoldsNoLock ==
    \A c \in Coordinators :
        coord[c].kind = "Idle" =>
            \A key \in Keys :
                redisLock[key] # c

    \* If a coordinator is idle, it must not still own any key lock.


OnlyLockHolderCanBeFinishingWriter ==
    \A c \in Coordinators :
        /\ coord[c].kind = "Write"
        /\ redisLock[coord[c].key] = NoCoord
        =>
        TRUE

    \* This is mostly documentation.
    \*
    \* The actual enforcement is in FinishWrite:
    \*
    \*     redisLock[coord[c].key] = c
    \*
    \* So a writer may exist without the lock, but it cannot finish until it
    \* acquires the lock.


=============================================================================