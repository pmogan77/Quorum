----------------------- MODULE AdaptiveQuorumPhase1 -----------------------

EXTENDS Naturals, FiniteSets

(***************************************************************************)
(*                                                                         *)
(* AdaptiveQuorumPhase1Fast                                                 *)
(*                                                                         *)
(* This is a small, TLC-friendly Phase 1 model of an adaptive quorum-based   *)
(* replicated key-value store.                                              *)
(*                                                                         *)
(* The system supports two main workload policies:                           *)
(*                                                                         *)
(*   1. ReadHeavy                                                           *)
(*      - Reads are cheap.                                                   *)
(*      - Writes are expensive.                                              *)
(*      - Example: R = 1, W = 3 for N = 3.                                   *)
(*                                                                         *)
(*   2. WriteHeavy                                                          *)
(*      - Writes are cheaper.                                                *)
(*      - Reads are more expensive.                                          *)
(*      - Example: R = 2, W = 2 for N = 3.                                   *)
(*                                                                         *)
(* There is also an Intermediate policy used during unsafe transitions from  *)
(* WriteHeavy to ReadHeavy.                                                  *)
(*                                                                         *)
(* Why is Intermediate needed?                                               *)
(*                                                                         *)
(* Under WriteHeavy, a write may have reached only a small write quorum.      *)
(* If we immediately switch to ReadHeavy, future reads may use a smaller      *)
(* read quorum and could miss the previous write.                            *)
(*                                                                         *)
(* Therefore:                                                               *)
(*                                                                         *)
(*   ReadHeavy  -> WriteHeavy       can happen directly.                     *)
(*   WriteHeavy -> ReadHeavy        must go through Intermediate.            *)
(*                                                                         *)
(* This model does not yet include:                                          *)
(*                                                                         *)
(*   - clients                                                              *)
(*   - coordinators                                                         *)
(*   - redis request/response messages                                      *)
(*   - redis locks                                                          *)
(*   - explicit timeouts                                                    *)
(*   - failed writes                                                        *)
(*   - node failures                                                        *)
(*                                                                         *)
(* Failed below-quorum writes are intentionally omitted because, per the      *)
(* design, they are undefined behavior. This model checks the normal safety   *)
(* envelope only.                                                           *)
(*                                                                         *)
(* Main safety idea:                                                        *)
(*                                                                         *)
(*   If the latest successful write is stored on W replicas, and every read  *)
(*   uses R replicas, then the read is guaranteed to see the latest write     *)
(*   when:                                                                  *)
(*                                                                         *)
(*       R + W > N                                                          *)
(*                                                                         *)
(* where N is the total number of replica servers.                            *)
(*                                                                         *)
(***************************************************************************)


(***************************************************************************)
(* CONSTANTS                                                               *)
(***************************************************************************)

CONSTANTS
    Keys,
        \* Set of keys in the key-value store.
        \*
        \* Example in the .cfg file:
        \*   Keys = {k1}

    Servers,
        \* Set of replica servers.
        \*
        \* Example:
        \*   Servers = {s1, s2, s3}

    Values,
        \* Set of ordinary non-delete values.
        \*
        \* Tombstone is added separately by the model.
        \*
        \* Example:
        \*   Values = {v1}

    RH_R,
        \* Read quorum size under ReadHeavy policy.

    RH_W,
        \* Write quorum size under ReadHeavy policy.

    WH_R,
        \* Read quorum size under WriteHeavy policy.

    WH_W,
        \* Write quorum size under WriteHeavy policy.

    MaxTS,
        \* Maximum timestamp explored by TLC.
        \*
        \* This bounds the state space. Without this, timestamp choices over
        \* Nat would make the model infinite.

    MaxEpoch
        \* Maximum redis configuration epoch explored by TLC.
        \*
        \* This prevents infinite policy switching.


(***************************************************************************)
(* BASIC DEFINITIONS                                                       *)
(***************************************************************************)

N == Cardinality(Servers)

    \* Total number of replica servers.


Timestamp == 0..MaxTS

    \* Finite timestamp domain for TLC.
    \*
    \* Timestamp 0 is the initial timestamp.
    \* Writes must choose a timestamp greater than the current latest write.


Epoch == 0..MaxEpoch

    \* Finite epoch domain for redisConfig.
    \*
    \* Every policy switch increments the epoch.


Tombstone == "Tombstone"

    \* Deletes are modeled as writes of Tombstone.
    \*
    \* This avoids needing a separate delete operation.


AllValues == Values \cup {Tombstone}

    \* Complete set of values that may appear in replicas or latestWrite.


Policies == {"ReadHeavy", "WriteHeavy", "Intermediate"}

    \* Possible per-key redis policy values.


(***************************************************************************)
(* QUORUM SIZE DEFINITIONS                                                 *)
(***************************************************************************)

MaxReadQuorum ==
    IF RH_R >= WH_R THEN RH_R ELSE WH_R

    \* Intermediate uses the larger read quorum of the two main policies.


MaxWriteQuorum ==
    IF RH_W >= WH_W THEN RH_W ELSE WH_W

    \* Intermediate uses the larger write quorum of the two main policies.


ReadQuorumSize(policy) ==
    CASE policy = "ReadHeavy"    -> RH_R
       [] policy = "WriteHeavy"   -> WH_R
       [] policy = "Intermediate" -> MaxReadQuorum

    \* Returns the read quorum size for a given policy.


WriteQuorumSize(policy) ==
    CASE policy = "ReadHeavy"    -> RH_W
       [] policy = "WriteHeavy"   -> WH_W
       [] policy = "Intermediate" -> MaxWriteQuorum

    \* Returns the write quorum size for a given policy.


ReadQuorums(policy) ==
    {Q \in SUBSET Servers : Cardinality(Q) = ReadQuorumSize(policy)}

    \* All valid read quorum sets for the given policy.
    \*
    \* We use exactly-sized quorums, not larger quorums.
    \*
    \* This reduces TLC state explosion.
    \* It is also enough for finding quorum-safety bugs because the smallest
    \* legal quorum is the dangerous case.


WriteQuorums(policy) ==
    {Q \in SUBSET Servers : Cardinality(Q) = WriteQuorumSize(policy)}

    \* All valid write quorum sets for the given policy.
    \*
    \* Again, this uses exactly-sized quorums to keep the state space small.


(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    redisConfig,
        \* Per-key configuration stored in the redis metadata node.
        \*
        \* redisConfig[key].policy is one of:
        \*   "ReadHeavy"
        \*   "WriteHeavy"
        \*   "Intermediate"
        \*
        \* redisConfig[key].epoch is the configuration version.

    replica,
        \* replica[server][key] is the value/timestamp record stored on a
        \* particular replica server.
        \*
        \* Example:
        \*
        \*   replica[s1][k1] = [value |-> v1, ts |-> 2]

    latestWrite,
        \* latestWrite[key] is the latest successful write known by the
        \* abstract model.
        \*
        \* This is not stored on a real node. It is a specification variable
        \* used to define what the system is supposed to return.

    lastWriteReplicas,
        \* lastWriteReplicas[key] is the set of servers that definitely store
        \* latestWrite[key].
        \*
        \* This is the central bookkeeping variable for the quorum proof.

    lastWritePolicy
        \* lastWritePolicy[key] records which policy was used when the latest
        \* successful write occurred.
        \*
        \* This is useful for debugging and explanation. It is not essential
        \* to the quorum proof.


vars ==
    <<redisConfig, replica, latestWrite, lastWriteReplicas, lastWritePolicy>>

    \* Tuple of all state variables.
    \*
    \* Used in Spec == Init /\ [][Next]_vars.


(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ redisConfig =
        [key \in Keys |-> [policy |-> "WriteHeavy", epoch |-> 0]]

        \* Initially every key is configured as WriteHeavy.
        \*
        \* The initial epoch is 0.

    /\ replica =
        [server \in Servers |->
            [key \in Keys |-> [value |-> Tombstone, ts |-> 0]]]

        \* Initially every replica has every key.
        \*
        \* Every key starts as Tombstone with timestamp 0.

    /\ latestWrite =
        [key \in Keys |-> [value |-> Tombstone, ts |-> 0]]

        \* The latest successful write is initially also Tombstone at ts 0.

    /\ lastWriteReplicas =
        [key \in Keys |-> Servers]

        \* Initially every server stores the latest write for every key.
        \*
        \* This means the initial value is fully replicated.

    /\ lastWritePolicy =
        [key \in Keys |-> "WriteHeavy"]

        \* The initial latest write is considered to have happened under the
        \* initial WriteHeavy policy.


(***************************************************************************)
(* SMALL HELPER OPERATORS                                                  *)
(***************************************************************************)

WriteRecord(value, timestamp) ==
    [value |-> value, ts |-> timestamp]

    \* Constructs a value/timestamp record.


NewTimestamp(key, timestamp) ==
    /\ timestamp \in Timestamp
    /\ timestamp > latestWrite[key].ts

    \* A successful write must use a timestamp greater than the latest
    \* successful write for that key.


ReplicaHasLatest(server, key) ==
    replica[server][key] = latestWrite[key]

    \* TRUE iff this server currently stores the abstract latest write for
    \* the given key.


LatestCoverageSafe(key) ==
    Cardinality(lastWriteReplicas[key])
        + ReadQuorumSize(redisConfig[key].policy)
        > N

    \* Core quorum intersection condition.
    \*
    \* If latestWrite[key] is stored on lastWriteReplicas[key], then every
    \* legal read quorum under the current policy must intersect that set.
    \*
    \* This is guaranteed by:
    \*
    \*   |lastWriteReplicas[key]| + ReadQuorumSize(policy) > N
    \*
    \* If this is false, then there exists a read quorum that can miss the
    \* latest successful write.


UpdateOneServer(serverState, key, value, timestamp) ==
    [serverState EXCEPT ![key] = WriteRecord(value, timestamp)]

    \* Returns a new per-server key-value map after updating one key.


UpdateReplicaSet(key, value, timestamp, writeSet) ==
    [server \in Servers |->
        IF server \in writeSet
        THEN UpdateOneServer(replica[server], key, value, timestamp)
        ELSE replica[server]]

    \* Returns a new replica function after writing [value, timestamp] to
    \* every server in writeSet.
    \*
    \* Servers not in writeSet are unchanged.


NewRedisConfig(key, newPolicy) ==
    [redisConfig EXCEPT
        ![key] =
            [policy |-> newPolicy,
             epoch  |-> redisConfig[key].epoch + 1]]

    \* Returns a new redisConfig after changing one key's policy.
    \*
    \* The epoch is incremented whenever the policy changes.
    \*
    \* The action using this helper must ensure the epoch does not exceed
    \* MaxEpoch.


(***************************************************************************)
(* SUCCESSFUL WRITE                                                        *)
(***************************************************************************)

SuccessfulWrite(key, value, timestamp, writeSet) ==
    /\ value \in AllValues

        \* The write may write either an ordinary value or Tombstone.

    /\ NewTimestamp(key, timestamp)

        \* Writes advance timestamps.

    /\ writeSet \in WriteQuorums(redisConfig[key].policy)

        \* The write must reach exactly the required write quorum for the
        \* current policy of this key.

    /\ replica' =
        UpdateReplicaSet(key, value, timestamp, writeSet)

        \* Install the new value/timestamp on the chosen write quorum.

    /\ latestWrite' =
        [latestWrite EXCEPT
            ![key] = WriteRecord(value, timestamp)]

        \* Since this is a successful write, the abstract latest write becomes
        \* this write.

    /\ lastWriteReplicas' =
        [lastWriteReplicas EXCEPT
            ![key] = writeSet]

        \* After the write, the model knows that the writeSet stores the latest
        \* write.
        \*
        \* Propagation may later add more servers to this set.

    /\ lastWritePolicy' =
        [lastWritePolicy EXCEPT
            ![key] = redisConfig[key].policy]

        \* Record the policy under which this write was performed.

    /\ UNCHANGED redisConfig

        \* A successful write does not directly change the redis policy.


(***************************************************************************)
(* SUCCESSFUL READ                                                         *)
(***************************************************************************)

SuccessfulRead(key, readSet) ==
    /\ readSet \in ReadQuorums(redisConfig[key].policy)

        \* A successful read chooses a valid read quorum under the current
        \* policy.
        \*
        \* This Phase 1 model does not store the returned read value as a
        \* variable. Instead, read correctness is represented by the invariant
        \* LatestWriteCoverage.
        \*
        \* If LatestWriteCoverage holds, then every valid read quorum must
        \* intersect the set of replicas storing latestWrite[key].

    /\ UNCHANGED
        <<redisConfig,
          replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy>>

        \* Reads do not mutate data or policy in this model.


(***************************************************************************)
(* PROPAGATION / ANTI-ENTROPY                                               *)
(***************************************************************************)

Propagate(src, dst, key) ==
    /\ src \in Servers
    /\ dst \in Servers
    /\ src # dst

        \* Propagation copies from one server to a different server.

    /\ replica[src][key].ts > replica[dst][key].ts

        \* The destination only accepts a newer timestamp.
        \*
        \* This prevents stale propagation from overwriting newer data.

    /\ replica' =
        [replica EXCEPT
            ![dst][key] = replica[src][key]]

        \* Copy the source's key record to the destination.

    /\ lastWriteReplicas' =
        IF replica[src][key] = latestWrite[key]
        THEN [lastWriteReplicas EXCEPT
                ![key] = lastWriteReplicas[key] \cup {dst}]
        ELSE lastWriteReplicas

        \* If the source had the latest successful write, then after
        \* propagation the destination also has the latest successful write.
        \*
        \* Therefore dst can be added to lastWriteReplicas[key].
        \*
        \* If the source did not have the latest successful write, this
        \* propagation does not improve latest-write coverage.

    /\ UNCHANGED
        <<redisConfig,
          latestWrite,
          lastWritePolicy>>

        \* Propagation does not change redis policy or the abstract latest
        \* successful write.


(***************************************************************************)
(* POLICY SWITCH: READHEAVY -> WRITEHEAVY                                  *)
(***************************************************************************)

SwitchReadHeavyToWriteHeavy(key) ==
    /\ redisConfig[key].policy = "ReadHeavy"

        \* This action applies only to keys currently in ReadHeavy mode.

    /\ redisConfig[key].epoch < MaxEpoch

        \* Prevent infinite policy switching in TLC.

    /\ LatestCoverageSafe(key)

        \* Only switch while the key is still inside the safety envelope.

    /\ redisConfig' =
        NewRedisConfig(key, "WriteHeavy")

        \* Change policy and increment epoch.

    /\ UNCHANGED
        <<replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy>>

        \* Policy switching does not directly change replica data.


(***************************************************************************)
(* POLICY SWITCH: WRITEHEAVY -> INTERMEDIATE                               *)
(***************************************************************************)

SwitchWriteHeavyToIntermediate(key) ==
    /\ redisConfig[key].policy = "WriteHeavy"

        \* This action starts the safe transition toward ReadHeavy.

    /\ redisConfig[key].epoch < MaxEpoch

        \* Prevent infinite switching.

    /\ redisConfig' =
        NewRedisConfig(key, "Intermediate")

        \* Move to Intermediate, not directly to ReadHeavy.

    /\ UNCHANGED
        <<replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy>>

        \* No data changes during the switch.


(***************************************************************************)
(* POLICY SWITCH: INTERMEDIATE -> READHEAVY                                *)
(***************************************************************************)

SwitchIntermediateToReadHeavy(key) ==
    /\ redisConfig[key].policy = "Intermediate"

        \* This action completes the transition to ReadHeavy.

    /\ redisConfig[key].epoch < MaxEpoch

        \* Prevent infinite switching.

    /\ Cardinality(lastWriteReplicas[key]) + RH_R > N

        \* Critical safety guard.
        \*
        \* Before entering ReadHeavy, the latest successful write must already
        \* be replicated widely enough to intersect future ReadHeavy reads.
        \*
        \* RH_R is the smaller read quorum used by ReadHeavy.
        \*
        \* If this guard were removed, TLC could find states where a ReadHeavy
        \* read misses the latest write.

    /\ redisConfig' =
        NewRedisConfig(key, "ReadHeavy")

        \* Change policy and increment epoch.

    /\ UNCHANGED
        <<replica,
          latestWrite,
          lastWriteReplicas,
          lastWritePolicy>>

        \* No data changes during the switch itself.


(***************************************************************************)
(* NEXT-STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E key \in Keys :
        \E value \in AllValues :
        \E timestamp \in Timestamp :
        \E writeSet \in WriteQuorums(redisConfig[key].policy) :
            SuccessfulWrite(key, value, timestamp, writeSet)

        \* A successful write may occur for any key, value, timestamp, and
        \* valid write quorum.

    \/ \E key \in Keys :
        \E readSet \in ReadQuorums(redisConfig[key].policy) :
            SuccessfulRead(key, readSet)

        \* A successful read may occur for any key and valid read quorum.

    \/ \E src \in Servers :
        \E dst \in Servers :
        \E key \in Keys :
            Propagate(src, dst, key)

        \* A replica may propagate a newer value to another replica.

    \/ \E key \in Keys :
            SwitchReadHeavyToWriteHeavy(key)

        \* Safe direct policy switch.

    \/ \E key \in Keys :
            SwitchWriteHeavyToIntermediate(key)

        \* First phase of unsafe-direction policy switch.

    \/ \E key \in Keys :
            SwitchIntermediateToReadHeavy(key)

        \* Second phase of unsafe-direction policy switch, guarded by
        \* latest-write coverage.


Spec ==
    Init /\ [][Next]_vars

    \* The behavior starts in Init and every step follows Next.
    \*
    \* [][Next]_vars allows stuttering steps.


(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeInvariant ==
    /\ redisConfig \in
        [Keys -> [policy : Policies, epoch : Epoch]]

        \* Every key has a valid policy and bounded epoch.

    /\ replica \in
        [Servers -> [Keys -> [value : AllValues, ts : Timestamp]]]

        \* Every server stores a valid value/timestamp record for every key.

    /\ latestWrite \in
        [Keys -> [value : AllValues, ts : Timestamp]]

        \* The abstract latest write is also a valid value/timestamp record.

    /\ lastWriteReplicas \in
        [Keys -> SUBSET Servers]

        \* For every key, lastWriteReplicas is a set of servers.

    /\ lastWritePolicy \in
        [Keys -> Policies]

        \* The remembered write policy is always a valid policy.


(***************************************************************************)
(* CONSTANT ASSUMPTIONS                                                    *)
(***************************************************************************)

QuorumAssumptions ==
    /\ RH_R \in 1..N
    /\ RH_W \in 1..N
    /\ WH_R \in 1..N
    /\ WH_W \in 1..N

        \* All quorum sizes must be valid nonzero server counts.

    /\ RH_R <= WH_R

        \* ReadHeavy has cheaper reads than WriteHeavy.

    /\ RH_W >= WH_W

        \* ReadHeavy has more expensive writes than WriteHeavy.

    /\ RH_R + RH_W > N

        \* ReadHeavy read and write quorums intersect.

    /\ WH_R + WH_W > N

        \* WriteHeavy read and write quorums intersect.

    /\ MaxReadQuorum + MaxWriteQuorum > N

        \* Intermediate read and write quorums also intersect.

ASSUME QuorumAssumptions

(***************************************************************************)
(* SAFETY INVARIANT: LAST-WRITE BOOKKEEPING                                *)
(***************************************************************************)

LastWriteReplicasCorrect ==
    \A key \in Keys :
        \A server \in lastWriteReplicas[key] :
            ReplicaHasLatest(server, key)

    \* Every server listed in lastWriteReplicas[key] really does store the
    \* abstract latest successful write for that key.
    \*
    \* This checks that our bookkeeping variable is not lying.


(***************************************************************************)
(* SAFETY INVARIANT: QUORUM COVERAGE                                       *)
(***************************************************************************)

LatestWriteCoverage ==
    \A key \in Keys :
        LatestCoverageSafe(key)

    \* The central safety invariant.
    \*
    \* It says the latest successful write is stored on enough replicas to
    \* intersect any read quorum allowed by the current policy.
    \*
    \* This is what protects reads from missing the latest successful write.


(***************************************************************************)
(* SAFETY INVARIANT: NO REPLICA HAS A FUTURE TIMESTAMP                     *)
(***************************************************************************)

TimestampBoundedByLatest ==
    \A server \in Servers :
        \A key \in Keys :
            replica[server][key].ts <= latestWrite[key].ts

    \* Since this model excludes below-quorum failed writes, no replica should
    \* contain a timestamp newer than latestWrite[key].
    \*
    \* In a later model that includes undefined failed writes, this invariant
    \* would need to be guarded or removed.


(***************************************************************************)
(* SAFETY INVARIANT: TOMBSTONE IS JUST A VALUE                             *)
(***************************************************************************)

DeleteIsJustWrite ==
    /\ Tombstone \in AllValues

    /\ \A key \in Keys :
        latestWrite[key].value \in AllValues

    /\ \A server \in Servers :
        \A key \in Keys :
            replica[server][key].value \in AllValues

    \* Deletes need no special rule.
    \*
    \* A delete is represented as:
    \*
    \*   [value |-> Tombstone, ts |-> timestamp]
    \*
    \* Therefore delete safety follows from normal timestamp/write safety.


=============================================================================