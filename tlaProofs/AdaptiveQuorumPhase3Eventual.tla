---------------------- MODULE AdaptiveQuorumPhase3Eventual ----------------------

EXTENDS Naturals, FiniteSets

(***************************************************************************)
(*                                                                         *)
(* Phase 3 model with eventual consistency support.                         *)
(*                                                                         *)
(* This model includes:                                                     *)
(*   - adaptive read/write quorum policies                                  *)
(*   - coordinators                                                         *)
(*   - Redis policy delay                                                   *)
(*   - Redis write locks                                                    *)
(*   - read correctness checking                                            *)
(*   - anti-entropy propagation                                             *)
(*   - optional liveness check for eventual consistency                      *)
(*                                                                         *)
(* The default Spec is safety-only and runs faster.                          *)
(* LiveSpec adds fairness for propagation and is used only by the liveness   *)
(* config.                                                                  *)
(*                                                                         *)
(* Important addition in this version:                                      *)
(*                                                                         *)
(*   hasRealWrite                                                           *)
(*                                                                         *)
(* This prevents the liveness test from entering Draining immediately from   *)
(* the initial state, where all replicas already contain the initial         *)
(* Tombstone value. Draining is now allowed only after at least one real      *)
(* successful write has completed.                                          *)
(*                                                                         *)
(***************************************************************************)


(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Keys,
    Servers,
    Coordinators,
    Values,

    RH_R,
    RH_W,

    WH_R,
    WH_W,

    MaxTS,
    MaxEpoch,

    EnablePolicySwitch,
        \* TRUE  = include policy switching actions.
        \* FALSE = disable them. Useful for faster liveness runs.

    EnableReadsDuringDraining
        \* TRUE  = reads may still start after draining begins.
        \* FALSE = no new reads start in draining mode.
        \*
        \* Disabling reads during draining helps TLC focus on convergence.


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

Modes == {"Writing", "Draining"}

    \* Writing:
    \*   Normal operation. New writes may start.
    \*
    \* Draining:
    \*   New writes may not start. Propagation continues until every replica
    \*   converges to latestWrite.


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

    \* A coordinator may receive more read responses than the minimum.

WriteQuorums(policy) ==
    {Q \in SUBSET Servers : Cardinality(Q) >= WriteQuorumSize(policy)}

    \* A coordinator may receive more write acknowledgments than the minimum.
    \*
    \* This matters for your protocol because Redis may respond after more
    \* than the minimum number of replicas have already acknowledged.


(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    redisConfig,
        \* redisConfig[key] = [policy |-> ..., epoch |-> ...]

    redisLock,
        \* redisLock[key] = NoCoord if unlocked, otherwise the coordinator
        \* holding the write lock for that key.

    replica,
        \* replica[server][key] = [value |-> ..., ts |-> ...]

    latestWrite,
        \* Abstract latest successful write for each key.

    lastWriteReplicas,
        \* Replicas known to store latestWrite[key].

    lastWritePolicy,
        \* Policy used by the latest successful write.

    coord,
        \* Coordinator operation state.

    readOk,
        \* Becomes FALSE if any completed read returns stale data.

    lastRead,
        \* Diagnostic record for the most recent read.

    mode,
        \* "Writing" or "Draining".

    hasRealWrite
        \* FALSE initially.
        \*
        \* Set to TRUE by FinishWrite.
        \*
        \* EnterDraining requires hasRealWrite, so the eventual-consistency
        \* check cannot succeed vacuously from the initial default state.


vars ==
    <<redisConfig,
      redisLock,
      replica,
      latestWrite,
      lastWriteReplicas,
      lastWritePolicy,
      coord,
      readOk,
      lastRead,
      mode,
      hasRealWrite>>


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

    \* If Redis has not replied, the coordinator uses conservative
    \* Intermediate quorums.

RedisReplyFresh(c) ==
    \/ coord[c].redisKnown = FALSE
    \/ coord[c].observedEpoch # redisConfig[coord[c].key].epoch

HasFreshRedisInfo(c) ==
    /\ coord[c].redisKnown
    /\ coord[c].observedEpoch = redisConfig[coord[c].key].epoch

CanFinishUsingEffectivePolicy(c) ==
    \/ coord[c].redisKnown = FALSE
    \/ HasFreshRedisInfo(c)


(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ redisConfig =
        [key \in Keys |-> [policy |-> "WriteHeavy", epoch |-> 0]]

    /\ redisLock =
        [key \in Keys |-> NoCoord]

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

    /\ mode = "Writing"

    /\ hasRealWrite = FALSE


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

    \* Core quorum-safety condition.
    \*
    \* The latest successful write must be stored on enough replicas to
    \* intersect any read quorum allowed by the current Redis policy.

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

    \* A read returns one of the maximum-timestamp records from its responses.


(***************************************************************************)
(* START OPERATIONS                                                        *)
(***************************************************************************)

StartRead(c, key) ==
    /\ coord[c].kind = "Idle"

    /\ \/ mode = "Writing"
       \/ EnableReadsDuringDraining

        \* For liveness runs, set EnableReadsDuringDraining = FALSE so TLC
        \* does not explore endless read-only behavior after Draining begins.

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
          lastRead,
          mode,
          hasRealWrite>>


StartWrite(c, key, value, timestamp) ==
    /\ mode = "Writing"

        \* New writes are disabled in Draining mode.
        \*
        \* Eventual consistency requires writes to stop eventually.

    /\ coord[c].kind = "Idle"

    /\ value \in AllValues

    /\ NewTimestamp(key, timestamp)

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
          lastRead,
          mode,
          hasRealWrite>>


(***************************************************************************)
(* DRAINING MODE                                                           *)
(***************************************************************************)

EnterDraining ==
    /\ mode = "Writing"

    /\ hasRealWrite

        \* Prevents the model from entering Draining directly from the
        \* initial state, where all replicas already have Tombstone@0.
        \*
        \* This makes the liveness check exercise real post-write
        \* convergence.

    /\ mode' = "Draining"

    /\ UNCHANGED
        <<redisConfig,
          redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          coord,
          readOk,
          lastRead,
          hasRealWrite>>


(***************************************************************************)
(* WRITE LOCKING                                                           *)
(***************************************************************************)

AcquireWriteLock(c) ==
    /\ coord[c].kind = "Write"

    /\ redisLock[coord[c].key] = NoCoord

    /\ redisLock' =
        [redisLock EXCEPT ![coord[c].key] = c]

    /\ UNCHANGED
        <<redisConfig,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          coord,
          readOk,
          lastRead,
          mode,
          hasRealWrite>>


AbortStaleWrite(c) ==
    /\ coord[c].kind = "Write"

    /\ coord[c].ts <= latestWrite[coord[c].key].ts

        \* Another write has already advanced latestWrite past this
        \* coordinator's timestamp.

    /\ redisLock' =
        IF redisLock[coord[c].key] = c
        THEN [redisLock EXCEPT ![coord[c].key] = NoCoord]
        ELSE redisLock

    /\ coord' =
        [coord EXCEPT ![c] = IdleCoord]

    /\ UNCHANGED
        <<redisConfig,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          readOk,
          lastRead,
          mode,
          hasRealWrite>>


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
          lastRead,
          mode,
          hasRealWrite>>


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

            \* Explicit read-correctness check.
            \*
            \* If a completed read returns anything other than latestWrite,
            \* readOk becomes FALSE and ReadReturnsLatest fails.

    /\ coord' =
        [coord EXCEPT ![c] = IdleCoord]

    /\ UNCHANGED
        <<redisConfig,
          redisLock,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy,
          mode,
          hasRealWrite>>


(***************************************************************************)
(* FINISH WRITE                                                            *)
(***************************************************************************)

FinishWrite(c, writeSet) ==
    /\ coord[c].kind = "Write"

    /\ redisLock[coord[c].key] = c

        \* Only the Redis lock holder may complete the write.

    /\ CanFinishUsingEffectivePolicy(c)

    /\ NewTimestamp(coord[c].key, coord[c].ts)

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

    /\ coord' =
        [coord EXCEPT ![c] = IdleCoord]

    /\ hasRealWrite' = TRUE

        \* This is the key change.
        \*
        \* After the first real successful write, Draining may begin.

    /\ UNCHANGED
        <<redisConfig,
          readOk,
          lastRead,
          mode>>


(***************************************************************************)
(* PROPAGATION                                                             *)
(***************************************************************************)

Propagate(src, dst, key) ==
    /\ src \in Servers
    /\ dst \in Servers
    /\ src # dst

    /\ replica[src][key].ts > replica[dst][key].ts

        \* Timestamp monotonicity:
        \* older values cannot overwrite newer ones.

    /\ replica' =
        [replica EXCEPT
            ![dst][key] = replica[src][key]]

    /\ lastWriteReplicas' =
        IF replica[src][key] = latestWrite[key]
        THEN [lastWriteReplicas EXCEPT
                ![key] = lastWriteReplicas[key] \cup {dst}]
        ELSE lastWriteReplicas

        \* If src has latestWrite[key], dst now has it too.

    /\ UNCHANGED
        <<redisConfig,
          redisLock,
          latestWrite,
          lastWritePolicy,
          coord,
          readOk,
          lastRead,
          mode,
          hasRealWrite>>


(***************************************************************************)
(* POLICY SWITCHING                                                        *)
(***************************************************************************)

SwitchReadHeavyToWriteHeavy(key) ==
    /\ EnablePolicySwitch
    /\ redisConfig[key].policy = "ReadHeavy"
    /\ redisConfig[key].epoch < MaxEpoch
    /\ redisLock[key] = NoCoord
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
          lastRead,
          mode,
          hasRealWrite>>


SwitchWriteHeavyToIntermediate(key) ==
    /\ EnablePolicySwitch
    /\ redisConfig[key].policy = "WriteHeavy"
    /\ redisConfig[key].epoch < MaxEpoch
    /\ redisLock[key] = NoCoord

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
          lastRead,
          mode,
          hasRealWrite>>


SwitchIntermediateToReadHeavy(key) ==
    /\ EnablePolicySwitch
    /\ redisConfig[key].policy = "Intermediate"
    /\ redisConfig[key].epoch < MaxEpoch
    /\ redisLock[key] = NoCoord

    /\ Cardinality(lastWriteReplicas[key]) + RH_R > N

        \* Critical guard before entering small-read-quorum mode.

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
          lastRead,
          mode,
          hasRealWrite>>


(***************************************************************************)
(* NEXT                                                                    *)
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

    \/ EnterDraining

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


(***************************************************************************)
(* SPECS                                                                   *)
(***************************************************************************)

Spec ==
    Init /\ [][Next]_vars

    \* Safety-only spec.
    \*
    \* Use this for normal fast invariant checking.


FairPropagation ==
    \A src \in Servers :
        \A dst \in Servers :
            \A key \in Keys :
                src # dst => WF_vars(Propagate(src, dst, key))

    \* Weak fairness for propagation.
    \*
    \* If propagation from src to dst for key remains continuously enabled,
    \* TLC must eventually take it.
    \*
    \* This is needed only for eventual consistency liveness checking.


LiveSpec ==
    Init /\ [][Next]_vars /\ FairPropagation

    \* Liveness spec.
    \*
    \* Use only with the small liveness config.


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

    /\ mode \in Modes

    /\ hasRealWrite \in BOOLEAN


(***************************************************************************)
(* CONSTANT ASSUMPTIONS                                                    *)
(***************************************************************************)

QuorumAssumptions ==
    /\ RH_R \in 1..N
    /\ RH_W \in 1..N
    /\ WH_R \in 1..N
    /\ WH_W \in 1..N

    /\ RH_R <= WH_R
    /\ RH_W >= WH_W

    /\ RH_R + RH_W > N
    /\ WH_R + WH_W > N
    /\ MaxReadQuorum + MaxWriteQuorum > N

    /\ EnablePolicySwitch \in BOOLEAN
    /\ EnableReadsDuringDraining \in BOOLEAN

ASSUME QuorumAssumptions


(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

LastWriteReplicasCorrect ==
    \A key \in Keys :
        \A server \in lastWriteReplicas[key] :
            ReplicaHasLatest(server, key)

LatestWriteCoverage ==
    \A key \in Keys :
        LatestCoverageSafe(key)

TimestampBoundedByLatest ==
    \A server \in Servers :
        \A key \in Keys :
            replica[server][key].ts <= latestWrite[key].ts

DeleteIsJustWrite ==
    /\ Tombstone \in AllValues

    /\ \A key \in Keys :
        latestWrite[key].value \in AllValues

    /\ \A server \in Servers :
        \A key \in Keys :
            replica[server][key].value \in AllValues

CoordinatorIdleShape ==
    \A c \in Coordinators :
        coord[c].kind = "Idle" =>
            /\ coord[c].key = NoKey
            /\ coord[c].value = NoValue
            /\ coord[c].redisKnown = FALSE
            /\ coord[c].observedPolicy = NoPolicy

CoordinatorActiveShape ==
    \A c \in Coordinators :
        coord[c].kind # "Idle" =>
            /\ coord[c].key \in Keys
            /\ coord[c].observedPolicy \in MaybePolicies

FreshRedisIfKnown ==
    \A c \in Coordinators :
        /\ coord[c].kind # "Idle"
        /\ coord[c].redisKnown
        =>
        coord[c].observedEpoch \in Epoch

ReadReturnsLatest ==
    readOk

LockOwnerIsWriter ==
    \A key \in Keys :
        redisLock[key] # NoCoord =>
            /\ redisLock[key] \in Coordinators
            /\ coord[redisLock[key]].kind = "Write"
            /\ coord[redisLock[key]].key = key

WriterHoldsAtMostOneLock ==
    \A c \in Coordinators :
        Cardinality({key \in Keys : redisLock[key] = c}) <= 1

IdleCoordinatorHoldsNoLock ==
    \A c \in Coordinators :
        coord[c].kind = "Idle" =>
            \A key \in Keys :
                redisLock[key] # c


(***************************************************************************)
(* EVENTUAL CONSISTENCY PROPERTY                                           *)
(***************************************************************************)

AllReplicasHaveLatest ==
    \A key \in Keys :
        \A server \in Servers :
            replica[server][key] = latestWrite[key]

    \* Strong convergence condition:
    \*
    \* every replica stores the latest successful write for every key.


EventualConsistency ==
    [](mode = "Draining" => <>AllReplicasHaveLatest)

    \* Once the system enters Draining mode, replicas should eventually
    \* converge to latestWrite.
    \*
    \* Since EnterDraining requires hasRealWrite, this property is no longer
    \* tested only on the initial default Tombstone state.


=============================================================================