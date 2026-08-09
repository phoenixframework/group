------------------------- MODULE SnapshotAssembly -------------------------
EXTENDS Integers, FiniteSets, TLC

(*
Finite model of the exact-snapshot chunk assembly boundary. It deliberately
models two snapshots in one authority epoch plus a new-epoch snapshot so TLC
can explore loss, duplication, reordering, supersession, stale final chunks,
expiry, and receiver crashes independently of the larger anti-entropy model.
*)

Snapshots == {1, 2, 3}
Chunks == {1, 2}
Rows == {"a", "b", "c", "d"}

SnapshotEpoch(snapshot) ==
  CASE snapshot = 0 -> 0
    [] snapshot = 1 -> 1
    [] snapshot = 2 -> 1
    [] snapshot = 3 -> 2

SnapshotSeq(snapshot) ==
  CASE snapshot = 0 -> 0
    [] snapshot = 1 -> 1
    [] snapshot = 2 -> 2
    [] snapshot = 3 -> 1

SnapshotRows(snapshot) ==
  CASE snapshot = 1 -> {"a", "b"}
    [] snapshot = 2 -> {"c", "d"}
    [] snapshot = 3 -> {"a", "d"}

ChunkRows(snapshot, chunk) ==
  CASE snapshot = 1 /\ chunk = 1 -> {"a"}
    [] snapshot = 1 /\ chunk = 2 -> {"b"}
    [] snapshot = 2 /\ chunk = 1 -> {"c"}
    [] snapshot = 2 /\ chunk = 2 -> {"d"}
    [] snapshot = 3 /\ chunk = 1 -> {"a"}
    [] snapshot = 3 /\ chunk = 2 -> {"d"}

Message == [snapshot : Snapshots, chunk : Chunks]

VARIABLES authorityEpoch,
          cursor,
          visible,
          stagedSnapshot,
          stagedChunks,
          stagedRows,
          messages

vars ==
  <<authorityEpoch, cursor, visible, stagedSnapshot, stagedChunks,
    stagedRows, messages>>

Init ==
  /\ authorityEpoch = 1
  /\ cursor = 0
  /\ visible = {}
  /\ stagedSnapshot = 0
  /\ stagedChunks = {}
  /\ stagedRows = {}
  /\ messages = {}

Send(snapshot, chunk) ==
  /\ messages' = messages \union
       {[snapshot |-> snapshot, chunk |-> chunk]}
  /\ UNCHANGED <<authorityEpoch, cursor, visible, stagedSnapshot,
                 stagedChunks, stagedRows>>

Valid(message) ==
  /\ SnapshotEpoch(message.snapshot) = authorityEpoch
  /\ SnapshotSeq(message.snapshot) > cursor

StartsNewAssembly(message) ==
  /\ Valid(message)
  /\ \/ stagedSnapshot = 0
     \/ SnapshotEpoch(stagedSnapshot) # authorityEpoch
     \/ SnapshotSeq(message.snapshot) > SnapshotSeq(stagedSnapshot)

StartAssembly(message) ==
  /\ StartsNewAssembly(message)
  /\ stagedSnapshot' = message.snapshot
  /\ stagedChunks' = {message.chunk}
  /\ stagedRows' = ChunkRows(message.snapshot, message.chunk)
  /\ UNCHANGED <<authorityEpoch, cursor, visible, messages>>

ContinueAssembly(message) ==
  /\ Valid(message)
  /\ stagedSnapshot = message.snapshot
  /\ LET nextChunks == stagedChunks \union {message.chunk}
         nextRows == stagedRows \union
                       ChunkRows(message.snapshot, message.chunk)
     IN IF nextChunks = Chunks
        THEN /\ cursor' = SnapshotSeq(message.snapshot)
             /\ visible' = SnapshotRows(message.snapshot)
             /\ stagedSnapshot' = 0
             /\ stagedChunks' = {}
             /\ stagedRows' = {}
        ELSE /\ UNCHANGED <<cursor, visible, stagedSnapshot>>
             /\ stagedChunks' = nextChunks
             /\ stagedRows' = nextRows
  /\ UNCHANGED <<authorityEpoch, messages>>

IgnoreChunk(message) ==
  /\ ~StartsNewAssembly(message)
  /\ ~(/\ Valid(message)
        /\ stagedSnapshot = message.snapshot)
  /\ UNCHANGED vars

Deliver(message) ==
  /\ message \in messages
  /\ \/ StartAssembly(message)
     \/ ContinueAssembly(message)
     \/ IgnoreChunk(message)

Drop(message) ==
  /\ message \in messages
  /\ messages' = messages \ {message}
  /\ UNCHANGED <<authorityEpoch, cursor, visible, stagedSnapshot,
                 stagedChunks, stagedRows>>

InstallNewAuthority ==
  /\ authorityEpoch = 1
  /\ authorityEpoch' = 2
  /\ cursor' = 0
  /\ visible' = {}
  (* The implementation may retain invisible old staging until expiry. *)
  /\ UNCHANGED <<stagedSnapshot, stagedChunks, stagedRows, messages>>

ExpireStaging ==
  /\ stagedSnapshot # 0
  /\ stagedSnapshot' = 0
  /\ stagedChunks' = {}
  /\ stagedRows' = {}
  /\ UNCHANGED <<authorityEpoch, cursor, visible, messages>>

CrashReceiver ==
  /\ stagedSnapshot # 0
  /\ stagedSnapshot' = 0
  /\ stagedChunks' = {}
  /\ stagedRows' = {}
  /\ UNCHANGED <<authorityEpoch, cursor, visible, messages>>

Next ==
  \/ \E snapshot \in Snapshots, chunk \in Chunks : Send(snapshot, chunk)
  \/ \E message \in messages : Deliver(message)
  \/ \E message \in messages : Drop(message)
  \/ InstallNewAuthority
  \/ ExpireStaging
  \/ CrashReceiver

TypeOK ==
  /\ authorityEpoch \in {1, 2}
  /\ cursor \in 0..2
  /\ visible \subseteq Rows
  /\ stagedSnapshot \in {0} \union Snapshots
  /\ stagedChunks \subseteq Chunks
  /\ stagedRows \subseteq Rows
  /\ messages \subseteq Message

VisibleIsAnExactCommittedSnapshot ==
  \/ /\ authorityEpoch = 1
     /\ \/ /\ cursor = 0 /\ visible = {}
        \/ /\ cursor = 1 /\ visible = SnapshotRows(1)
        \/ /\ cursor = 2 /\ visible = SnapshotRows(2)
  \/ /\ authorityEpoch = 2
     /\ \/ /\ cursor = 0 /\ visible = {}
        \/ /\ cursor = 1 /\ visible = SnapshotRows(3)

StagingNeverLeaksIntoVisible ==
  stagedSnapshot # 0 /\ stagedChunks # Chunks =>
    VisibleIsAnExactCommittedSnapshot

StagingBelongsToOneSnapshot ==
  stagedSnapshot # 0 =>
    /\ stagedRows =
         UNION {ChunkRows(stagedSnapshot, chunk) : chunk \in stagedChunks}
    /\ stagedChunks # Chunks

Spec == Init /\ [][Next]_vars

=============================================================================
