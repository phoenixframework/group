------------------------- MODULE SnapshotAssembly -------------------------
EXTENDS Integers, FiniteSets, TLC

(*
Finite model of the exact-snapshot assembly boundary. Snapshot chunks are
provisional: a separately lossy, duplicable, and reorderable terminal commit
is required before a complete candidate can replace visible state. Two
snapshots share an authority epoch and one belongs to a replacement epoch so
TLC also explores supersession, expiry, receiver crashes, and stale messages.
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

Message ==
  [kind : {"chunk", "commit"}, snapshot : Snapshots, chunk : {0} \union Chunks]

VARIABLES authorityEpoch,
          cursor,
          visible,
          stagedSnapshot,
          stagedChunks,
          stagedRows,
          stagedCommitted,
          commitAllowed,
          messages

vars ==
  <<authorityEpoch, cursor, visible, stagedSnapshot, stagedChunks,
    stagedRows, stagedCommitted, commitAllowed, messages>>

Init ==
  /\ authorityEpoch = 1
  /\ cursor = 0
  /\ visible = {}
  /\ stagedSnapshot = 0
  /\ stagedChunks = {}
  /\ stagedRows = {}
  /\ stagedCommitted = FALSE
  /\ commitAllowed = Snapshots
  /\ messages = {}

SendChunk(snapshot, chunk) ==
  /\ messages' = messages \union
       {[kind |-> "chunk", snapshot |-> snapshot, chunk |-> chunk]}
  /\ UNCHANGED <<authorityEpoch, cursor, visible, stagedSnapshot,
                 stagedChunks, stagedRows, stagedCommitted, commitAllowed>>

SendCommit(snapshot) ==
  /\ snapshot \in commitAllowed
  /\ messages' = messages \union
       {[kind |-> "commit", snapshot |-> snapshot, chunk |-> 0]}
  /\ UNCHANGED <<authorityEpoch, cursor, visible, stagedSnapshot,
                 stagedChunks, stagedRows, stagedCommitted, commitAllowed>>

InvalidateBeforeCommit(snapshot) ==
  /\ snapshot \in commitAllowed
  /\ commitAllowed' = commitAllowed \ {snapshot}
  /\ UNCHANGED <<authorityEpoch, cursor, visible, stagedSnapshot,
                 stagedChunks, stagedRows, stagedCommitted, messages>>

Valid(snapshot) ==
  /\ SnapshotEpoch(snapshot) = authorityEpoch
  /\ SnapshotSeq(snapshot) > cursor

StartsNewAssembly(snapshot) ==
  /\ Valid(snapshot)
  /\ \/ stagedSnapshot = 0
     \/ SnapshotEpoch(stagedSnapshot) # authorityEpoch
     \/ SnapshotSeq(snapshot) > SnapshotSeq(stagedSnapshot)

StartChunk(message) ==
  /\ message.kind = "chunk"
  /\ StartsNewAssembly(message.snapshot)
  /\ stagedSnapshot' = message.snapshot
  /\ stagedChunks' = {message.chunk}
  /\ stagedRows' = ChunkRows(message.snapshot, message.chunk)
  /\ stagedCommitted' = FALSE
  /\ UNCHANGED <<authorityEpoch, cursor, visible, commitAllowed, messages>>

StartCommit(message) ==
  /\ message.kind = "commit"
  /\ StartsNewAssembly(message.snapshot)
  /\ stagedSnapshot' = message.snapshot
  /\ stagedChunks' = {}
  /\ stagedRows' = {}
  /\ stagedCommitted' = TRUE
  /\ UNCHANGED <<authorityEpoch, cursor, visible, commitAllowed, messages>>

ContinueChunk(message) ==
  /\ message.kind = "chunk"
  /\ Valid(message.snapshot)
  /\ stagedSnapshot = message.snapshot
  /\ LET nextChunks == stagedChunks \union {message.chunk}
         nextRows == stagedRows \union ChunkRows(message.snapshot, message.chunk)
     IN IF stagedCommitted /\ nextChunks = Chunks
        THEN /\ cursor' = SnapshotSeq(message.snapshot)
             /\ visible' = SnapshotRows(message.snapshot)
             /\ stagedSnapshot' = 0
             /\ stagedChunks' = {}
             /\ stagedRows' = {}
             /\ stagedCommitted' = FALSE
        ELSE /\ UNCHANGED <<cursor, visible, stagedSnapshot, stagedCommitted>>
             /\ stagedChunks' = nextChunks
             /\ stagedRows' = nextRows
  /\ UNCHANGED <<authorityEpoch, commitAllowed, messages>>

ContinueCommit(message) ==
  /\ message.kind = "commit"
  /\ Valid(message.snapshot)
  /\ stagedSnapshot = message.snapshot
  /\ IF stagedChunks = Chunks
     THEN /\ cursor' = SnapshotSeq(message.snapshot)
          /\ visible' = SnapshotRows(message.snapshot)
          /\ stagedSnapshot' = 0
          /\ stagedChunks' = {}
          /\ stagedRows' = {}
          /\ stagedCommitted' = FALSE
     ELSE /\ stagedCommitted' = TRUE
          /\ UNCHANGED <<cursor, visible, stagedSnapshot, stagedChunks, stagedRows>>
  /\ UNCHANGED <<authorityEpoch, commitAllowed, messages>>

Ignore(message) ==
  /\ ~StartsNewAssembly(message.snapshot)
  /\ ~(/\ Valid(message.snapshot)
        /\ stagedSnapshot = message.snapshot)
  /\ UNCHANGED vars

Deliver(message) ==
  /\ message \in messages
  /\ \/ StartChunk(message)
     \/ StartCommit(message)
     \/ ContinueChunk(message)
     \/ ContinueCommit(message)
     \/ Ignore(message)

Drop(message) ==
  /\ message \in messages
  /\ messages' = messages \ {message}
  /\ UNCHANGED <<authorityEpoch, cursor, visible, stagedSnapshot,
                 stagedChunks, stagedRows, stagedCommitted, commitAllowed>>

InstallNewAuthority ==
  /\ authorityEpoch = 1
  /\ authorityEpoch' = 2
  /\ cursor' = 0
  /\ visible' = {}
  (* Invisible old staging may remain until expiry, but can never commit. *)
  /\ UNCHANGED <<stagedSnapshot, stagedChunks, stagedRows, stagedCommitted,
                 commitAllowed, messages>>

DiscardStaging ==
  /\ stagedSnapshot # 0
  /\ stagedSnapshot' = 0
  /\ stagedChunks' = {}
  /\ stagedRows' = {}
  /\ stagedCommitted' = FALSE
  /\ UNCHANGED <<authorityEpoch, cursor, visible, commitAllowed, messages>>

Next ==
  \/ \E snapshot \in Snapshots, chunk \in Chunks : SendChunk(snapshot, chunk)
  \/ \E snapshot \in Snapshots : SendCommit(snapshot)
  \/ \E snapshot \in Snapshots : InvalidateBeforeCommit(snapshot)
  \/ \E message \in messages : Deliver(message)
  \/ \E message \in messages : Drop(message)
  \/ InstallNewAuthority
  \/ DiscardStaging

TypeOK ==
  /\ authorityEpoch \in {1, 2}
  /\ cursor \in 0..2
  /\ visible \subseteq Rows
  /\ stagedSnapshot \in {0} \union Snapshots
  /\ stagedChunks \subseteq Chunks
  /\ stagedRows \subseteq Rows
  /\ stagedCommitted \in BOOLEAN
  /\ commitAllowed \subseteq Snapshots
  /\ messages \subseteq Message

VisibleIsAnExactCommittedSnapshot ==
  \/ /\ authorityEpoch = 1
     /\ \/ /\ cursor = 0 /\ visible = {}
        \/ /\ cursor = 1 /\ visible = SnapshotRows(1)
        \/ /\ cursor = 2 /\ visible = SnapshotRows(2)
  \/ /\ authorityEpoch = 2
     /\ \/ /\ cursor = 0 /\ visible = {}
        \/ /\ cursor = 1 /\ visible = SnapshotRows(3)

StagingBelongsToOneSnapshot ==
  stagedSnapshot # 0 =>
    stagedRows = UNION {ChunkRows(stagedSnapshot, chunk) : chunk \in stagedChunks}

NoCommitMeansNoInstall ==
  stagedSnapshot # 0 /\ ~stagedCommitted =>
    SnapshotSeq(stagedSnapshot) > cursor

Spec == Init /\ [][Next]_vars

=============================================================================
