----------------------- MODULE AdaptiveQuorumPhase2 -----------------------

EXTENDS Naturals, FiniteSets

(***************************************************************************)
(*                                                                         *)
(* AdaptiveQuorumPhase2ReadCheck                                            *)
(*                                                                         *)
(* This model represents Phase 2 of the adaptive quorum protocol.            *)
(*                                                                         *)
(* Compared with Phase 1, this version adds:                                *)
(*                                                                         *)
(*   - coordinators                                                         *)
(*   - delayed Redis/config responses                                       *)
(*   - conservative quorum behavior before Redis responds                   *)
(*   - explicit checking that completed reads return the latest value       *)
(*                                                                         *)
(* The system has per-key quorum policies:                                  *)
(*                                                                         *)
(*   ReadHeavy:                                                             *)
(*      Reads use a smaller quorum.                                         *)
(*      Writes use a larger quorum.                                         *)
(*                                                                         *)
(*   WriteHeavy:                                                            *)
(*      Writes use a smaller quorum.                                        *)
(*      Reads use a larger quorum.                                          *)
(*                                                                         *)
(*   Intermediate:                                                          *)
(*      Uses the maximum read quorum and maximum write quorum.               *)
(*      This is used as a safe transitional mode.                            *)
(*                                                                         *)
(* A coordinator does not have to wait for Redis before continuing.          *)
(* If Redis has not responded yet, the coordinator uses Intermediate         *)
(* quorums, which are conservative.                                         *)
(*                                                                         *)
(* This model still does not include:                                       *)
(*                                                                         *)
(*   - explicit network messages                                            *)
(*   - real timeouts                                                        *)
(*   - node failures                                                        *)
(*   - failed below-quorum writes                                           *)
(*   - Redis write locks                                                    *)
(*                                                                         *)
(* Below-quorum writes are omitted because they are undefined behavior in    *)
(* the intended design.                                                     *)
(*                                                                         *)
(***************************************************************************)


(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Keys,
        \* Set of keys in the store.
        \* Example: {k1}

    Servers,
        \* Set of replica servers.
        \* Example: {s1, s2, s3}

    Coordinators,
        \* Set of coordinators.
        \*
        \* For this phase, start with one coordinator:
        \*   Coordinators = {c1}
        \*
        \* Multiple coordinators should be modeled after adding Redis locks.

    Values,
        \* Non-delete values.
        \* Tombstone is added separately and models deletes.

    RH_R,
        \* Read quorum size under ReadHeavy.

    RH_W,
        \* Write quorum size under ReadHeavy.

    WH_R,
        \* Read quorum size under WriteHeavy.

    WH_W,
        \* Write quorum size under WriteHeavy.

    MaxTS,
        \* Maximum timestamp TLC explores.

    MaxEpoch
        \* Maximum Redis config epoch TLC explores.


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

OpKinds == {"Idle", "Read", "Write"}

MaybeKeys == Keys \cup {NoKey}

MaybeValues == AllValues \cup {NoValue}

MaybePolicies == Policies \cup {NoPolicy}


(***************************************************************************)
(* QUORUM DEFINITIONS                                                      *)
(***************************************************************************)

MaxReadQuorum ==
    IF RH_R >= WH_R THEN RH_R ELSE WH_R

    \* Intermediate read quorum.


MaxWriteQuorum ==
    IF RH_W >= WH_W THEN RH_W ELSE WH_W

    \* Intermediate write quorum.


ReadQuorumSize(policy) ==
    CASE policy = "ReadHeavy"    -> RH_R
       [] policy = "WriteHeavy"   -> WH_R
       [] policy = "Intermediate" -> MaxReadQuorum

    \* Returns the read quorum size for a policy.


WriteQuorumSize(policy) ==
    CASE policy = "ReadHeavy"    -> RH_W
       [] policy = "WriteHeavy"   -> WH_W
       [] policy = "Intermediate" -> MaxWriteQuorum

    \* Returns the write quorum size for a policy.


ReadQuorums(policy) ==
    {Q \in SUBSET Servers : Cardinality(Q) >= ReadQuorumSize(policy)}

    \* Legal read response sets.
    \*
    \* Uses >= because the coordinator may receive more responses than the
    \* minimum quorum size.


WriteQuorums(policy) ==
    {Q \in SUBSET Servers : Cardinality(Q) >= WriteQuorumSize(policy)}

    \* Legal write acknowledgment sets.
    \*
    \* Uses >= because the coordinator may receive more acks than the minimum.


(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    redisConfig,
        \* redisConfig[key] = [policy |-> ..., epoch |-> ...]
        \*
        \* Redis stores the current policy and epoch for each key.

    replica,
        \* replica[server][key] = [value |-> ..., ts |-> ...]
        \*
        \* Each replica stores a timestamped value for every key.

    latestWrite,
        \* latestWrite[key] is the latest successful write in the abstract
        \* specification state.
        \*
        \* This is not an implementation variable; it is used to state
        \* correctness.

    lastWriteReplicas,
        \* lastWriteReplicas[key] is the set of replicas known to store
        \* latestWrite[key].

    lastWritePolicy,
        \* The policy used by the most recent successful write.

    coord,
        \* coord[c] stores coordinator c's current operation state.

    readOk,
        \* TRUE means every completed read so far returned the latest value.
        \*
        \* If a read ever returns a stale value, FinishRead sets this to FALSE.

    lastRead
        \* Debugging record for the most recent completed read.
        \*
        \* Useful when TLC shows a counterexample.


vars ==
    <<redisConfig,
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

    \* Canonical idle state.


ReadCoord(key) ==
    [kind |-> "Read",
     key |-> key,
     value |-> NoValue,
     ts |-> 0,
     redisKnown |-> FALSE,
     observedPolicy |-> NoPolicy,
     observedEpoch |-> 0]

    \* Coordinator has started a read but has not received Redis metadata yet.


WriteCoord(key, value, timestamp) ==
    [kind |-> "Write",
     key |-> key,
     value |-> value,
     ts |-> timestamp,
     redisKnown |-> FALSE,
     observedPolicy |-> NoPolicy,
     observedEpoch |-> 0]

    \* Coordinator has started a write but has not received Redis metadata yet.


NoReadRecord ==
    [key |-> NoKey,
     value |-> NoValue,
     ts |-> 0,
     expectedValue |-> NoValue,
     expectedTs |-> 0]

    \* Initial value for lastRead.
    \*
    \* After a read completes:
    \*
    \*   lastRead.value         = value returned by read
    \*   lastRead.ts            = timestamp returned by read
    \*   lastRead.expectedValue = latest value at read completion
    \*   lastRead.expectedTs    = latest timestamp at read completion


(***************************************************************************)
(* REDIS RESPONSE HELPERS                                                  *)
(***************************************************************************)

CoordWithRedisReply(c) ==
    [coord[c] EXCEPT
        !.redisKnown = TRUE,
        !.observedPolicy = redisConfig[coord[c].key].policy,
        !.observedEpoch = redisConfig[coord[c].key].epoch]

    \* Records Redis's current policy and epoch for coordinator c's key.


EffectivePolicy(c) ==
    IF coord[c].redisKnown
    THEN coord[c].observedPolicy
    ELSE "Intermediate"

    \* If Redis has responded, use the returned policy.
    \*
    \* If Redis has not responded, use conservative Intermediate quorums.


RedisReplyFresh(c) ==
    \/ coord[c].redisKnown = FALSE
    \/ coord[c].observedEpoch # redisConfig[coord[c].key].epoch

    \* Allows Redis to respond if:
    \*
    \*   - this is the first response, or
    \*   - the coordinator's previously observed epoch is stale.


HasFreshRedisInfo(c) ==
    /\ coord[c].redisKnown
    /\ coord[c].observedEpoch = redisConfig[coord[c].key].epoch

    \* TRUE when the coordinator's Redis metadata matches the current epoch.


CanFinishUsingEffectivePolicy(c) ==
    \/ coord[c].redisKnown = FALSE
    \/ HasFreshRedisInfo(c)

    \* A coordinator may finish in two cases:
    \*
    \*   1. Redis has not responded:
    \*        use Intermediate, which is conservative.
    \*
    \*   2. Redis has responded:
    \*        the response must be fresh.


(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ redisConfig =
        [key \in Keys |-> [policy |-> "WriteHeavy", epoch |-> 0]]

        \* Initially every key starts in WriteHeavy mode.

    /\ replica =
        [server \in Servers |->
            [key \in Keys |-> [value |-> Tombstone, ts |-> 0]]]

        \* Every server initially stores Tombstone at timestamp 0.

    /\ latestWrite =
        [key \in Keys |-> [value |-> Tombstone, ts |-> 0]]

        \* The abstract latest write is also Tombstone at timestamp 0.

    /\ lastWriteReplicas =
        [key \in Keys |-> Servers]

        \* Initially, every server has the latest value.

    /\ lastWritePolicy =
        [key \in Keys |-> "WriteHeavy"]

        \* Initial latest write is considered WriteHeavy.

    /\ coord =
        [c \in Coordinators |-> IdleCoord]

        \* All coordinators begin idle.

    /\ readOk = TRUE

        \* No read has failed yet.

    /\ lastRead = NoReadRecord

        \* No read has completed yet.


(***************************************************************************)
(* BASIC HELPERS                                                           *)
(***************************************************************************)

WriteRecord(value, timestamp) ==
    [value |-> value, ts |-> timestamp]

    \* Constructs a timestamped value record.


NewTimestamp(key, timestamp) ==
    /\ timestamp \in Timestamp
    /\ timestamp > latestWrite[key].ts

    \* A new write must use a timestamp greater than the latest successful
    \* write for that key.


ReplicaHasLatest(server, key) ==
    replica[server][key] = latestWrite[key]

    \* TRUE iff server stores the latest successful write for key.


LatestCoverageSafe(key) ==
    Cardinality(lastWriteReplicas[key])
        + ReadQuorumSize(redisConfig[key].policy)
        > N

    \* Core quorum-intersection condition.
    \*
    \* If latestWrite[key] is stored on W replicas and the current read quorum
    \* size is R, then W + R > N guarantees every read quorum intersects the
    \* latest-write replicas.


UpdateOneServer(serverState, key, value, timestamp) ==
    [serverState EXCEPT ![key] = WriteRecord(value, timestamp)]

    \* Returns a modified per-server key map.


UpdateReplicaSet(key, value, timestamp, writeSet) ==
    [server \in Servers |->
        IF server \in writeSet
        THEN UpdateOneServer(replica[server], key, value, timestamp)
        ELSE replica[server]]

    \* Writes the record to every server in writeSet.


NewRedisConfig(key, newPolicy) ==
    [redisConfig EXCEPT
        ![key] =
            [policy |-> newPolicy,
             epoch  |-> redisConfig[key].epoch + 1]]

    \* Changes the policy for key and increments its epoch.


(***************************************************************************)
(* READ RESULT HELPER                                                      *)
(***************************************************************************)

IsMaxReadRecord(rec, readSet, key) ==
    /\ rec \in {replica[server][key] : server \in readSet}
    /\ \A server \in readSet :
        rec.ts >= replica[server][key].ts

    \* The read algorithm returns a record with maximum timestamp among the
    \* responses from readSet.
    \*
    \* If several replicas have the same max timestamp, this allows any one
    \* of them. In this model, equal timestamps for the same key should refer
    \* to the same latest write because writes use increasing timestamps.


(***************************************************************************)
(* START OPERATIONS                                                        *)
(***************************************************************************)

StartRead(c, key) ==
    /\ coord[c].kind = "Idle"

        \* Coordinator c is available.

    /\ coord' =
        [coord EXCEPT ![c] = ReadCoord(key)]

        \* Start a read request.
        \*
        \* Redis has not responded yet.

    /\ UNCHANGED
        <<redisConfig,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          readOk,
          lastRead>>


StartWrite(c, key, value, timestamp) ==
    /\ coord[c].kind = "Idle"

        \* Coordinator c is available.

    /\ value \in AllValues

        \* Write may be a normal value or Tombstone.

    /\ NewTimestamp(key, timestamp)

        \* Timestamp must be newer than the latest successful write.

    /\ coord' =
        [coord EXCEPT ![c] = WriteCoord(key, value, timestamp)]

        \* Start a write request.
        \*
        \* Redis has not responded yet.

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

        \* Only active coordinators can receive Redis metadata.

    /\ RedisReplyFresh(c)

        \* Response is useful if first-time or if it refreshes a stale epoch.

    /\ coord' =
        [coord EXCEPT ![c] = CoordWithRedisReply(c)]

        \* Store the current Redis policy and epoch.

    /\ UNCHANGED
        <<redisConfig,
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

        \* Coordinator c is currently serving a read.

    /\ CanFinishUsingEffectivePolicy(c)

        \* Either Redis has not responded and we use Intermediate, or Redis
        \* has responded with a fresh epoch.

    /\ readSet \in ReadQuorums(EffectivePolicy(c))

        \* The coordinator has received enough read responses according to
        \* the effective policy.

    /\ \E rec \in {replica[server][coord[c].key] : server \in readSet} :
        /\ IsMaxReadRecord(rec, readSet, coord[c].key)

            \* rec is the value/timestamp pair returned by the read algorithm.

        /\ lastRead' =
            [key |-> coord[c].key,
             value |-> rec.value,
             ts |-> rec.ts,
             expectedValue |-> latestWrite[coord[c].key].value,
             expectedTs |-> latestWrite[coord[c].key].ts]

            \* Save diagnostic information about the completed read.

        /\ readOk' =
            readOk /\ (rec = latestWrite[coord[c].key])

            \* This is the explicit read correctness check.
            \*
            \* If the returned max-timestamp record differs from latestWrite,
            \* readOk becomes FALSE and the invariant ReadReturnsLatest fails.

    /\ coord' =
        [coord EXCEPT ![c] = IdleCoord]

        \* The coordinator is free again.

    /\ UNCHANGED
        <<redisConfig,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy>>


(***************************************************************************)
(* FINISH WRITE                                                            *)
(***************************************************************************)

FinishWrite(c, writeSet) ==
    /\ coord[c].kind = "Write"

        \* Coordinator c is currently serving a write.

    /\ CanFinishUsingEffectivePolicy(c)

        \* Either use conservative quorums before Redis returns, or use a
        \* fresh Redis response.

    /\ NewTimestamp(coord[c].key, coord[c].ts)

        \* The write timestamp must still be newer than latestWrite.
        \*
        \* This matters more once multiple coordinators are modeled.

    /\ writeSet \in WriteQuorums(EffectivePolicy(c))

        \* The coordinator has received enough write acknowledgments.

    /\ replica' =
        UpdateReplicaSet(coord[c].key,
                         coord[c].value,
                         coord[c].ts,
                         writeSet)

        \* Install the write on the acknowledged replicas.

    /\ latestWrite' =
        [latestWrite EXCEPT
            ![coord[c].key] =
                WriteRecord(coord[c].value, coord[c].ts)]

        \* The successful write becomes the abstract latest write.

    /\ lastWriteReplicas' =
        [lastWriteReplicas EXCEPT
            ![coord[c].key] = writeSet]

        \* The model records exactly which replicas acknowledged this write.
        \*
        \* Since WriteQuorums uses >=, this may be more than the minimum.

    /\ lastWritePolicy' =
        [lastWritePolicy EXCEPT
            ![coord[c].key] = EffectivePolicy(c)]

        \* Record the policy used to complete the write.

    /\ coord' =
        [coord EXCEPT ![c] = IdleCoord]

        \* The coordinator is free again.

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

        \* Copy from one server to a different server.

    /\ replica[src][key].ts > replica[dst][key].ts

        \* Only newer timestamps propagate forward.

    /\ replica' =
        [replica EXCEPT
            ![dst][key] = replica[src][key]]

        \* dst receives src's value for key.

    /\ lastWriteReplicas' =
        IF replica[src][key] = latestWrite[key]
        THEN [lastWriteReplicas EXCEPT
                ![key] = lastWriteReplicas[key] \cup {dst}]
        ELSE lastWriteReplicas

        \* If src had latestWrite[key], dst now has it too.
        \*
        \* Otherwise this propagation does not improve latest-write coverage.

    /\ UNCHANGED
        <<redisConfig,
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

    /\ LatestCoverageSafe(key)

        \* Only switch while latest-write coverage is safe.

    /\ redisConfig' =
        NewRedisConfig(key, "WriteHeavy")

    /\ UNCHANGED
        <<replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          coord,
          readOk,
          lastRead>>


SwitchWriteHeavyToIntermediate(key) ==
    /\ redisConfig[key].policy = "WriteHeavy"

    /\ redisConfig[key].epoch < MaxEpoch

    /\ redisConfig' =
        NewRedisConfig(key, "Intermediate")

        \* WriteHeavy does not switch directly to ReadHeavy.
        \*
        \* It must first enter Intermediate because WriteHeavy writes may have
        \* used a smaller write quorum.

    /\ UNCHANGED
        <<replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          coord,
          readOk,
          lastRead>>


SwitchIntermediateToReadHeavy(key) ==
    /\ redisConfig[key].policy = "Intermediate"

    /\ redisConfig[key].epoch < MaxEpoch

    /\ Cardinality(lastWriteReplicas[key]) + RH_R > N

        \* Critical safety guard.
        \*
        \* Before entering ReadHeavy, latestWrite[key] must be replicated
        \* widely enough to intersect future ReadHeavy read quorums.

    /\ redisConfig' =
        NewRedisConfig(key, "ReadHeavy")

    /\ UNCHANGED
        <<replica,
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

    \* Bookkeeping invariant:
    \*
    \* Every server listed in lastWriteReplicas[key] really stores
    \* latestWrite[key].


LatestWriteCoverage ==
    \A key \in Keys :
        LatestCoverageSafe(key)

    \* Quorum safety invariant:
    \*
    \* latestWrite[key] is stored on enough replicas to intersect any read
    \* quorum allowed by the current Redis policy.


TimestampBoundedByLatest ==
    \A server \in Servers :
        \A key \in Keys :
            replica[server][key].ts <= latestWrite[key].ts

    \* Since below-quorum writes are not modeled, no replica should contain a
    \* timestamp newer than the latest successful write.


DeleteIsJustWrite ==
    /\ Tombstone \in AllValues

    /\ \A key \in Keys :
        latestWrite[key].value \in AllValues

    /\ \A server \in Servers :
        \A key \in Keys :
            replica[server][key].value \in AllValues

    \* Deletes are represented as ordinary writes of Tombstone.


CoordinatorIdleShape ==
    \A c \in Coordinators :
        coord[c].kind = "Idle" =>
            /\ coord[c].key = NoKey
            /\ coord[c].value = NoValue
            /\ coord[c].redisKnown = FALSE
            /\ coord[c].observedPolicy = NoPolicy

    \* Idle coordinators should have the canonical idle fields.


CoordinatorActiveShape ==
    \A c \in Coordinators :
        coord[c].kind # "Idle" =>
            /\ coord[c].key \in Keys
            /\ coord[c].observedPolicy \in MaybePolicies

    \* Active coordinators must refer to a real key.


FreshRedisIfKnown ==
    \A c \in Coordinators :
        /\ coord[c].kind # "Idle"
        /\ coord[c].redisKnown
        =>
        coord[c].observedEpoch \in Epoch

    \* Sanity check for Redis metadata stored at a coordinator.


ReadReturnsLatest ==
    readOk

    \* Explicit read-correctness invariant.
    \*
    \* FinishRead sets readOk to FALSE if a completed read returns anything
    \* other than latestWrite[key] at the moment the read completes.
    \*
    \* If TLC violates this invariant, inspect lastRead in the error trace:
    \*
    \*   lastRead.value         returned value
    \*   lastRead.ts            returned timestamp
    \*   lastRead.expectedValue latest expected value
    \*   lastRead.expectedTs    latest expected timestamp


=============================================================================