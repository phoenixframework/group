--------------------------- MODULE ReplicaAck ---------------------------
EXTENDS Integers, FiniteSets, TLC

(*
One origin stream and one receiver, following Group.Replica's actual ACK and
repair handlers. A record in net is one accepted transport frame; Drop models
:busy, :disconnected, or loss. A set retains frames for arbitrary duplicate
delivery, and independent frames can be delivered out of order.

The receiver's exact state is a single row toggled by each origin write. This
gives both creation and deletion (and detects zombie rows). Snapshot chunks and
their commit are abstracted as one *committed* snapshot here; the separate
SnapshotAssembly model checks the non-atomic transfer and install boundary.

The hello action models a sender-to-receiver replica_hello. One valid old hello
may already be in flight initially. When its probe has reached the sender, the
same action abstracts completion of the two-way peer_connect/hello exchange.
An old hello can restore the receiver's route without proving the sender saw a
new probe; this is an important recovery race.
*)

CONSTANTS MaxSeq, MaxMessages, MaxProbe, MaxPid, MaxEpoch

ASSUME /\ MaxSeq >= 2
       /\ MaxMessages >= 1
       /\ MaxProbe >= 1
       /\ MaxPid >= 1
       /\ MaxEpoch >= 1

NoToken == <<0, 0>>
Token(pid, probe) == <<pid, probe>>
ValueAt(n) == n % 2 = 1

Frame(kind, n, aux, tok, probe, pid, gen, epoch) ==
  [kind |-> kind, n |-> n, aux |-> aux, tok |-> tok,
   probe |-> probe, pid |-> pid, gen |-> gen, epoch |-> epoch]

VARIABLES s, net
vars == <<s, net>>

Init ==
  /\ s = [phase |-> "faulting",
          head |-> 0, floor |-> 1, cursor |-> 0, view |-> FALSE,
          connected |-> TRUE,
          probe |-> 0, probePending |-> FALSE,
          seenProbe |-> 0, seenPid |-> 1,
          receiverPid |-> 1, receiverGen |-> 1, receiverEpoch |-> 1,
          knownPid |-> 1, knownGen |-> 1, knownEpoch |-> 1,
          sendToken |-> Token(1, 0), receiveToken |-> NoToken,
          pending |-> FALSE,
          ackQueued |-> FALSE, ackCursor |-> 0, ackEpoch |-> 1,
          helloDue |-> FALSE, helloRetryReady |-> TRUE,
          snapshotBlocked |-> FALSE,
          lastSnapshotJustified |-> TRUE,
          lastHelloJustified |-> TRUE,
          ackEvidence |-> [head |-> 0, tok |-> NoToken,
                          pid |-> 0, gen |-> 0, epoch |-> 0]]
  /\ net \in {{},
              {Frame("hello", 0, 0, NoToken, 0, 1, 1, 1)}}

(* retain_pending_replica_head sets last_sent to nil on a newer head. *)
Write ==
  /\ s.phase = "faulting"
  /\ s.head < MaxSeq
  /\ s' = [s EXCEPT !.head = @ + 1,
                    !.pending = TRUE,
                    !.snapshotBlocked = FALSE]
  /\ UNCHANGED net

(* Data.prune_replica_oplog has a shard-wide cap. Writes to other streams may
   prune this stream's already-applied records without changing its head. *)
Prune ==
  /\ s.phase = "faulting"
  /\ s.floor < s.head + 1
  /\ s' = [s EXCEPT !.floor = @ + 1]
  /\ UNCHANGED net

(* expire_replica_peer deletes the cursor, rows, receive token, and queued ACKs,
   but the sender can still think its old ACK retired the head. *)
ReceiverExpire ==
  /\ s.phase = "faulting"
  /\ s.connected
  /\ s.probe < MaxProbe
  /\ s' = [s EXCEPT !.connected = FALSE,
                    !.cursor = 0, !.view = FALSE,
                    !.probe = @ + 1,
                    !.probePending = TRUE,
                    !.receiveToken = NoToken,
                    !.ackQueued = FALSE]
  /\ UNCHANGED net

ReceiverRestart ==
  /\ s.phase = "faulting"
  /\ s.receiverPid < MaxPid
  /\ s.probe < MaxProbe
  /\ s' = [s EXCEPT !.connected = FALSE,
                    !.cursor = 0, !.view = FALSE,
                    !.probe = @ + 1,
                    !.probePending = TRUE,
                    !.receiverPid = @ + 1,
                    !.receiverGen = @ + 1,
                    !.receiveToken = NoToken,
                    !.ackQueued = FALSE]
  /\ UNCHANGED net

(* A named-cluster leave/rejoin gives the receiver a new local epoch. *)
ReceiverRejoin ==
  /\ s.phase = "faulting"
  /\ s.receiverEpoch < MaxEpoch
  /\ s.probe < MaxProbe
  /\ s' = [s EXCEPT !.connected = FALSE,
                    !.cursor = 0, !.view = FALSE,
                    !.probe = @ + 1,
                    !.probePending = TRUE,
                    !.receiverEpoch = @ + 1,
                    !.receiveToken = NoToken,
                    !.ackQueued = FALSE]
  /\ UNCHANGED net

ProbeFrame ==
  Frame("probe", 0, 0, NoToken, s.probe,
        s.receiverPid, s.receiverGen, s.receiverEpoch)

ProbeAllowed == ~s.connected \/ s.probePending

SendProbe ==
  /\ ProbeAllowed
  /\ Cardinality(net) < MaxMessages
  /\ net' = net \union {ProbeFrame}
  /\ UNCHANGED s

FreshProbe(m) ==
  m.pid # s.seenPid \/ m.probe > s.seenProbe

(* peer_connect: a new probe from the *same* shard PID invalidates an ACKed
   stream by rotating replica_send_tokens and requeuing every current head.
   A duplicate never does. A new PID is requeued when its hello installs. *)
DeliverProbe(m) ==
  /\ m \in net
  /\ m.kind = "probe"
  /\ net' = (net \ {m}) \union
       (IF m.pid = s.seenPid /\ m.probe < s.seenProbe
        THEN {}
        ELSE {Frame("probe_ack", 0,
                    IF m.pid = s.knownPid /\ m.gen = s.knownGen /\
                       m.epoch = s.knownEpoch
                    THEN 1 ELSE 0, NoToken, m.probe,
                    0, 0, 0)})
  /\ IF m.pid = s.seenPid /\ m.probe < s.seenProbe
     THEN UNCHANGED s
     ELSE IF FreshProbe(m)
          THEN s' = [s EXCEPT !.seenPid = m.pid,
                              !.seenProbe = m.probe,
                              !.helloDue = TRUE,
                              !.sendToken =
                                IF m.pid = s.knownPid
                                THEN Token(m.pid, m.probe)
                                ELSE @,
                              !.pending =
                                IF m.pid = s.knownPid
                                THEN s.head > 0 ELSE @]
          ELSE s' = [s EXCEPT !.helloDue =
                               @ \/ (s.helloRetryReady /\
                                     ~(m.pid = s.knownPid /\
                                       m.gen = s.knownGen /\
                                       m.epoch = s.knownEpoch))]

(* peer_connect_ack echoes the probe epoch and says whether the sender route
   matched the receiver PID after any required reseed. A mismatched PID must
   complete its reverse hello before a later ready ACK can retire the probe. *)
DeliverProbeAck(m) ==
  /\ m \in net
  /\ m.kind = "probe_ack"
  /\ net' = net \ {m}
  /\ IF m.probe = s.probe /\ m.aux = 1 /\ s.connected
     THEN s' = [s EXCEPT !.probePending = FALSE]
     ELSE UNCHANGED s

(* The retry timer represents the implementation's 5-second discovery hello
   gate. A duplicate probe with an exact sender view needs only its ACK. *)
HelloRetryTimer ==
  /\ (~s.connected \/ s.probePending)
  /\ ~s.helloRetryReady
  /\ s' = [s EXCEPT !.helloRetryReady = TRUE]
  /\ UNCHANGED net

SendHello ==
  /\ s.helloDue
  /\ Cardinality(net) < MaxMessages
  /\ net' = net \union
       {Frame("hello", 0, 0, NoToken, s.seenProbe,
              s.seenPid, s.receiverGen, s.receiverEpoch)}
  /\ s' = [s EXCEPT !.helloDue = FALSE,
                    !.helloRetryReady = FALSE,
                    !.lastHelloJustified = s.helloDue]

(* A valid old hello can restore the receiver route even after its PID changes:
   the wire hello has no receiver PID fence. Only the current two-way handshake
   updates the sender's receiver PID/generation/epoch and requeues heads. *)
CurrentHandshake(m) ==
  m.pid = s.receiverPid /\ m.probe = s.probe /\
    s.seenPid = s.receiverPid /\ s.seenProbe = s.probe

DeliverHello(m) ==
  /\ m \in net
  /\ m.kind = "hello"
  /\ net' = net \ {m}
  /\ s' = [s EXCEPT !.connected = TRUE,
                         !.knownPid =
                           IF CurrentHandshake(m)
                           THEN s.receiverPid ELSE @,
                         !.knownGen =
                           IF CurrentHandshake(m)
                           THEN s.receiverGen ELSE @,
                         !.knownEpoch =
                           IF CurrentHandshake(m)
                           THEN s.receiverEpoch ELSE @,
                         !.pending =
                           IF CurrentHandshake(m) /\ s.head > 0 /\
                              (s.knownPid # s.receiverPid \/
                               s.knownGen # s.receiverGen \/
                               s.knownEpoch # s.receiverEpoch)
                           THEN TRUE ELSE @]

HeadFrame ==
  Frame("head", s.head, s.floor, s.sendToken, 0, 0, 0, 0)

(* retry_pending_replica_heads enumerates only unacknowledged streams. *)
SendHead ==
  /\ s.pending
  /\ s.head > 0
  /\ Cardinality(net) < MaxMessages
  /\ net' = net \union {HeadFrame}
  /\ UNCHANGED s

NeedFrame(next, advertised) ==
  Frame("need", next, advertised, NoToken, 0, 0, 0, 0)

DeliverHead(m) ==
  /\ m \in net
  /\ m.kind = "head"
  /\ net' = (net \ {m}) \union
       (IF s.connected /\ m.n > s.cursor
        THEN {NeedFrame(s.cursor + 1, m.n)} ELSE {})
  /\ IF s.connected
     THEN s' = [s EXCEPT !.receiveToken = m.tok,
                         !.ackQueued =
                           IF m.n <= s.cursor THEN TRUE ELSE @,
                         !.ackCursor =
                           IF m.n <= s.cursor THEN s.cursor ELSE @,
                         !.ackEpoch =
                           IF m.n <= s.cursor THEN s.receiverEpoch ELSE @]
     ELSE UNCHANGED s

(* send_replica_repairs requires a pending entry with the exact advertised
   head. A stale need cannot trigger delta or snapshot. The wire need has no
   receiver PID, generation, token, or epoch; do not invent those guards. *)
ValidNeed(m) ==
  s.pending /\ m.aux = s.head /\ m.n <= s.head

DeliverNeed(m) ==
  /\ m \in net
  /\ m.kind = "need"
  /\ net' = (net \ {m}) \union
       (IF ValidNeed(m) /\ m.n >= s.floor
        THEN {Frame("delta", m.n, s.head, NoToken, 0, 0, 0, 0)}
        ELSE IF ValidNeed(m) /\ ~s.snapshotBlocked
             THEN {Frame("snapshot", s.head, 0, NoToken, 0, 0, 0, 0)}
             ELSE {})
  /\ IF ValidNeed(m) /\ m.n < s.floor /\ ~s.snapshotBlocked
     THEN s' = [s EXCEPT !.snapshotBlocked = TRUE,
                         !.lastSnapshotJustified =
                           s.pending /\ m.aux = s.head /\
                           m.n <= s.head /\ m.n < s.floor]
     ELSE UNCHANGED s

(* The 5-second retry bound is a timer action, not an unbounded resend per
   repeated need. The pending-head condition is checked again at send time. *)
SnapshotRetryTimer ==
  /\ s.snapshotBlocked
  /\ s.pending
  /\ s' = [s EXCEPT !.snapshotBlocked = FALSE]
  /\ UNCHANGED net

DeliverDelta(m) ==
  /\ m \in net
  /\ m.kind = "delta"
  /\ net' = (net \ {m}) \union
       (IF s.connected /\ m.n > s.cursor + 1
        THEN {NeedFrame(s.cursor + 1, m.aux)} ELSE {})
  /\ IF s.connected /\ m.n = s.cursor + 1
     THEN s' = [s EXCEPT !.cursor = m.aux,
                         !.view = ValueAt(m.aux),
                         !.ackQueued = TRUE,
                         !.ackCursor = m.aux,
                         !.ackEpoch = s.receiverEpoch]
     ELSE IF s.connected /\ m.n <= s.cursor
          THEN s' = [s EXCEPT !.ackQueued = TRUE,
                              !.ackCursor = s.cursor,
                              !.ackEpoch = s.receiverEpoch]
          ELSE UNCHANGED s

(* Only a complete exact snapshot is represented here. *)
DeliverSnapshot(m) ==
  /\ m \in net
  /\ m.kind = "snapshot"
  /\ net' = net \ {m}
  /\ IF s.connected /\ m.n > s.cursor
     THEN s' = [s EXCEPT !.cursor = m.n,
                         !.view = ValueAt(m.n),
                         !.ackQueued = TRUE,
                         !.ackCursor = m.n,
                         !.ackEpoch = s.receiverEpoch]
     ELSE UNCHANGED s

(* flush_replica_acks reads the latest receive token when it emits a batch,
   rather than freezing the token when the ACK was first queued. *)
SendAck ==
  /\ s.ackQueued
  /\ Cardinality(net) < MaxMessages
  /\ net' = net \union
       {Frame("ack", s.ackCursor, 0, s.receiveToken, 0,
              s.receiverPid, s.receiverGen, s.ackEpoch)}
  /\ s' = [s EXCEPT !.ackQueued = FALSE]

(* handle_replica_message(:applied) and acknowledge_replica_cursor. *)
ValidAck(m) ==
  /\ s.pending
  /\ m.pid = s.knownPid
  /\ m.gen = s.knownGen
  /\ m.tok = s.sendToken
  /\ m.epoch = s.knownEpoch
  /\ m.n >= s.head

DeliverAck(m) ==
  /\ m \in net
  /\ m.kind = "ack"
  /\ net' = net \ {m}
  /\ IF ValidAck(m)
     THEN s' = [s EXCEPT !.pending = FALSE,
                         !.snapshotBlocked = FALSE,
                         !.ackEvidence =
                           [head |-> s.head, tok |-> m.tok,
                            pid |-> m.pid, gen |-> m.gen,
                            epoch |-> m.epoch]]
     ELSE UNCHANGED s

Drop(m) ==
  /\ s.phase = "faulting"
  /\ m \in net
  /\ net' = net \ {m}
  /\ UNCHANGED s

Heal ==
  /\ s.phase = "faulting"
  /\ s' = [s EXCEPT !.phase = "healed"]
  /\ UNCHANGED net

(* One successful anti-entropy round after the transport heals. The wire
   actions above still explore arbitrary loss/reordering for safety. This
   action abstracts a fair completion of the same probe, hello, head, repair,
   and ACK handlers; it is enabled only while their obligations exist. *)
Repair ==
  /\ s.phase = "healed"
  /\ UNCHANGED net
  /\ \/ /\ ProbeAllowed
        /\ s' = [s EXCEPT !.connected = TRUE,
                          !.probePending = FALSE,
                          !.seenPid = s.receiverPid,
                          !.seenProbe = s.probe,
                          !.knownPid = s.receiverPid,
                          !.knownGen = s.receiverGen,
                          !.knownEpoch = s.receiverEpoch,
                          !.sendToken =
                            IF s.receiverPid = s.knownPid
                            THEN Token(s.receiverPid, s.probe)
                            ELSE @,
                          !.pending = s.head > 0]
     \/ /\ ~ProbeAllowed
        /\ s.connected
        /\ s.pending
        /\ s.cursor < s.head
        /\ s' = [s EXCEPT !.cursor = s.head,
                          !.view = ValueAt(s.head),
                          !.receiveToken = s.sendToken,
                          !.ackQueued = TRUE,
                          !.ackCursor = s.head,
                          !.ackEpoch = s.receiverEpoch]
     \/ /\ ~ProbeAllowed
        /\ s.connected
        /\ s.pending
        /\ s.cursor >= s.head
        /\ s.knownPid = s.receiverPid
        /\ s.knownGen = s.receiverGen
        /\ s.knownEpoch = s.receiverEpoch
        /\ s' = [s EXCEPT !.pending = FALSE,
                          !.ackQueued = FALSE,
                          !.snapshotBlocked = FALSE,
                          !.ackEvidence =
                            [head |-> s.head, tok |-> s.sendToken,
                             pid |-> s.receiverPid, gen |-> s.receiverGen,
                             epoch |-> s.receiverEpoch]]

Next ==
  \/ Write
  \/ Prune
  \/ ReceiverExpire
  \/ ReceiverRestart
  \/ ReceiverRejoin
  \/ SendProbe
  \/ \E m \in net : DeliverProbe(m)
  \/ \E m \in net : DeliverProbeAck(m)
  \/ HelloRetryTimer
  \/ SendHello
  \/ \E m \in net : DeliverHello(m)
  \/ SendHead
  \/ \E m \in net : DeliverHead(m)
  \/ \E m \in net : DeliverNeed(m)
  \/ SnapshotRetryTimer
  \/ \E m \in net : DeliverDelta(m)
  \/ \E m \in net : DeliverSnapshot(m)
  \/ SendAck
  \/ \E m \in net : DeliverAck(m)
  \/ \E m \in net : Drop(m)
  \/ Heal
  \/ Repair

TypeOK ==
  /\ s.phase \in {"faulting", "healed"}
  /\ s.head \in 0..MaxSeq
  /\ s.floor \in 1..(s.head + 1)
  /\ s.cursor \in 0..MaxSeq
  /\ s.view \in BOOLEAN
  /\ s.connected \in BOOLEAN
  /\ s.probe \in 0..MaxProbe
  /\ s.probePending \in BOOLEAN
  /\ s.seenProbe \in 0..MaxProbe
  /\ s.seenPid \in 1..MaxPid
  /\ s.receiverPid \in 1..MaxPid
  /\ s.receiverGen \in 1..MaxPid
  /\ s.receiverEpoch \in 1..MaxEpoch
  /\ s.knownPid \in 1..MaxPid
  /\ s.knownGen \in 1..MaxPid
  /\ s.knownEpoch \in 1..MaxEpoch
  /\ s.pending \in BOOLEAN
  /\ s.ackQueued \in BOOLEAN
  /\ s.ackCursor \in 0..MaxSeq
  /\ s.ackEpoch \in 1..MaxEpoch
  /\ s.helloDue \in BOOLEAN
  /\ s.helloRetryReady \in BOOLEAN
  /\ s.snapshotBlocked \in BOOLEAN
  /\ Cardinality(net) <= MaxMessages

(* Every visible row is the exact prefix at the receiver's committed cursor.
   In particular, expiry and rejoin cannot leave a zombie row. *)
ExactCommittedPrefix ==
  /\ s.cursor <= s.head
  /\ s.view = ValueAt(s.cursor)

(* A quiet nonempty stream has evidence from the current sender token and
   installed receiver identity/epoch. An old ACK can never quiet a reseed. *)
QuietHasCurrentAck ==
  (~s.pending /\ s.head > 0) =>
    /\ s.ackEvidence.head = s.head
    /\ s.ackEvidence.tok = s.sendToken
    /\ s.ackEvidence.pid = s.knownPid
    /\ s.ackEvidence.gen = s.knownGen
    /\ s.ackEvidence.epoch = s.knownEpoch

NoUnjustifiedFullSend ==
  s.lastSnapshotJustified /\ s.lastHelloJustified

Converged ==
  /\ s.connected
  /\ s.cursor = s.head
  /\ s.view = ValueAt(s.head)
  /\ ~s.pending
  /\ ~s.probePending

HealedConvergence == s.phase = "healed" ~> Converged

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(Repair)

=============================================================================
