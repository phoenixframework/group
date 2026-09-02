------------------------- MODULE GroupAntiEntropy -------------------------
EXTENDS Integers, FiniteSets, TLC

(*
An abstract model of Group's per-origin anti-entropy stream.

The model intentionally does not duplicate the Elixir data structures. It
models the protocol contract: exact generation/epoch authority, sequenced
deltas, bounded retained prefixes, exact snapshot fallback, arbitrary finite
loss/reordering/duplication, and fair repair after the network heals.
*)

CONSTANTS Nodes, Origins, Keys, MaxSeq, OplogBound, MaxMessages

ASSUME /\ IsFiniteSet(Nodes)
       /\ Cardinality(Nodes) >= 2
       /\ Origins \subseteq Nodes
       /\ Cardinality(Origins) >= 1
       /\ IsFiniteSet(Keys)
       /\ Cardinality(Keys) >= 1
       /\ MaxSeq >= 1
       /\ OplogBound >= 1
       /\ MaxMessages >= 1

Seq == 1..MaxSeq
Generations == 0..2
Revisions == 0..4
Epochs == 0..4
BoolMap == [Keys -> BOOLEAN]
EmptyView == [key \in Keys |-> FALSE]
EmptyRecord == [key |-> CHOOSE key \in Keys : TRUE, value |-> FALSE]

HelloMessages ==
  [kind : {"hello"},
   from : Nodes,
   to : Nodes,
   wireGeneration : Generations,
   wireRevision : Revisions,
   wireEpoch : Epochs,
   wireActive : BOOLEAN]

DeltaMessages ==
  [kind : {"delta"},
   from : Nodes,
   to : Nodes,
   wireGeneration : Generations,
   wireRevision : Revisions,
   wireEpoch : Epochs,
   seq : Seq,
   key : Keys,
   value : BOOLEAN]

SnapshotMessages ==
  [kind : {"snapshot"},
   from : Nodes,
   to : Nodes,
   wireGeneration : Generations,
   wireRevision : Revisions,
   wireEpoch : Epochs,
   seq : 0..MaxSeq,
   state : BoolMap]

Message == HelloMessages \union DeltaMessages \union SnapshotMessages

VARIABLES phase,
          generation,
          revision,
          epoch,
          active,
          truth,
          head,
          floor,
          history,
          authorityGeneration,
          authorityRevision,
          authorityEpoch,
          authorityActive,
          cursor,
          replica,
          messages

vars ==
  <<phase, generation, revision, epoch, active, truth, head, floor, history,
    authorityGeneration, authorityRevision, authorityEpoch, authorityActive,
    cursor, replica, messages>>

RECURSIVE Replay(_, _)
Replay(records, n) ==
  IF n = 0
  THEN EmptyView
  ELSE [Replay(records, n - 1) EXCEPT
          ![records[n].key] = records[n].value]

CurrentAuthority(receiver, origin) ==
  /\ authorityGeneration[receiver][origin] = generation[origin]
  /\ authorityRevision[receiver][origin] = revision[origin]
  /\ authorityEpoch[receiver][origin] = epoch[origin]
  /\ authorityActive[receiver][origin] = active[origin]

PairConverged(receiver, origin) ==
  IF receiver = origin
  THEN TRUE
  ELSE
    /\ CurrentAuthority(receiver, origin)
    /\ IF active[origin]
          THEN /\ cursor[receiver][origin] = head[origin]
               /\ replica[receiver][origin] = truth[origin]
          ELSE /\ cursor[receiver][origin] = 0
               /\ replica[receiver][origin] = EmptyView

Converged ==
  \A receiver \in Nodes :
    \A origin \in Origins :
      PairConverged(receiver, origin)

Init ==
  /\ phase = "faulting"
  /\ generation = [node \in Nodes |-> 1]
  /\ revision = [node \in Nodes |-> 0]
  /\ epoch = [node \in Nodes |-> 0]
  /\ active = [node \in Nodes |-> FALSE]
  /\ truth = [node \in Nodes |-> EmptyView]
  /\ head = [node \in Nodes |-> 0]
  /\ floor = [node \in Nodes |-> 1]
  /\ history = [node \in Nodes |->
                  [seq \in Seq |-> EmptyRecord]]
  /\ authorityGeneration =
       [receiver \in Nodes |-> [origin \in Nodes |-> 0]]
  /\ authorityRevision =
       [receiver \in Nodes |-> [origin \in Nodes |-> 0]]
  /\ authorityEpoch =
       [receiver \in Nodes |-> [origin \in Nodes |-> 0]]
  /\ authorityActive =
       [receiver \in Nodes |-> [origin \in Nodes |-> FALSE]]
  /\ cursor =
       [receiver \in Nodes |-> [origin \in Nodes |-> 0]]
  /\ replica =
       [receiver \in Nodes |-> [origin \in Nodes |-> EmptyView]]
  /\ messages = {}

Open(origin) ==
  /\ phase = "faulting"
  /\ ~active[origin]
  /\ revision[origin] < 4
  /\ epoch[origin] < 4
  /\ active' = [active EXCEPT ![origin] = TRUE]
  /\ revision' = [revision EXCEPT ![origin] = @ + 1]
  /\ epoch' = [epoch EXCEPT ![origin] = @ + 1]
  /\ truth' = [truth EXCEPT ![origin] = EmptyView]
  /\ head' = [head EXCEPT ![origin] = 0]
  /\ floor' = [floor EXCEPT ![origin] = 1]
  /\ history' = [history EXCEPT
                   ![origin] = [seq \in Seq |-> EmptyRecord]]
  /\ UNCHANGED <<generation, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, cursor, replica, messages,
                 phase>>

Close(origin) ==
  /\ phase = "faulting"
  /\ active[origin]
  /\ revision[origin] < 4
  /\ active' = [active EXCEPT ![origin] = FALSE]
  /\ revision' = [revision EXCEPT ![origin] = @ + 1]
  /\ truth' = [truth EXCEPT ![origin] = EmptyView]
  /\ head' = [head EXCEPT ![origin] = 0]
  /\ floor' = [floor EXCEPT ![origin] = 1]
  /\ history' = [history EXCEPT
                   ![origin] = [seq \in Seq |-> EmptyRecord]]
  /\ UNCHANGED <<generation, epoch, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, cursor, replica, messages,
                 phase>>

Restart(origin) ==
  /\ phase = "faulting"
  /\ generation[origin] < 2
  /\ generation' = [generation EXCEPT ![origin] = @ + 1]
  /\ revision' = [revision EXCEPT ![origin] = 0]
  /\ epoch' = [epoch EXCEPT ![origin] = 0]
  /\ active' = [active EXCEPT ![origin] = FALSE]
  /\ truth' = [truth EXCEPT ![origin] = EmptyView]
  /\ head' = [head EXCEPT ![origin] = 0]
  /\ floor' = [floor EXCEPT ![origin] = 1]
  /\ history' = [history EXCEPT
                   ![origin] = [seq \in Seq |-> EmptyRecord]]
  /\ UNCHANGED <<authorityGeneration, authorityRevision, authorityEpoch,
                 authorityActive, cursor, replica, messages, phase>>

Mutate(origin, key, value) ==
  /\ phase = "faulting"
  /\ active[origin]
  /\ head[origin] < MaxSeq
  /\ LET next == head[origin] + 1
         nextFloor == IF next - floor[origin] + 1 > OplogBound
                      THEN floor[origin] + 1
                      ELSE floor[origin]
     IN /\ history' =
              [history EXCEPT
                ![origin][next] = [key |-> key, value |-> value]]
        /\ truth' = [truth EXCEPT ![origin][key] = value]
        /\ head' = [head EXCEPT ![origin] = next]
        /\ floor' = [floor EXCEPT ![origin] = nextFloor]
  /\ UNCHANGED <<phase, generation, revision, epoch, active,
                 authorityGeneration, authorityRevision, authorityEpoch,
                 authorityActive, cursor, replica, messages>>

SendHello(origin, receiver) ==
  /\ origin # receiver
  /\ Cardinality(messages) < MaxMessages
  /\ messages' =
       messages \union
         {[kind |-> "hello",
           from |-> origin,
           to |-> receiver,
           wireGeneration |-> generation[origin],
           wireRevision |-> revision[origin],
           wireEpoch |-> epoch[origin],
           wireActive |-> active[origin]]}
  /\ UNCHANGED <<phase, generation, revision, epoch, active, truth, head,
                 floor, history, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, cursor, replica>>

SendDelta(origin, receiver, seq) ==
  /\ origin # receiver
  /\ active[origin]
  /\ seq \in floor[origin]..head[origin]
  /\ Cardinality(messages) < MaxMessages
  /\ LET record == history[origin][seq]
     IN messages' =
          messages \union
            {[kind |-> "delta",
              from |-> origin,
              to |-> receiver,
              wireGeneration |-> generation[origin],
              wireRevision |-> revision[origin],
              wireEpoch |-> epoch[origin],
              seq |-> seq,
              key |-> record.key,
              value |-> record.value]}
  /\ UNCHANGED <<phase, generation, revision, epoch, active, truth, head,
                 floor, history, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, cursor, replica>>

SendSnapshot(origin, receiver) ==
  /\ origin # receiver
  /\ active[origin]
  /\ Cardinality(messages) < MaxMessages
  /\ messages' =
       messages \union
         {[kind |-> "snapshot",
           from |-> origin,
           to |-> receiver,
           wireGeneration |-> generation[origin],
           wireRevision |-> revision[origin],
           wireEpoch |-> epoch[origin],
           seq |-> head[origin],
           state |-> truth[origin]]}
  /\ UNCHANGED <<phase, generation, revision, epoch, active, truth, head,
                 floor, history, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, cursor, replica>>

FreshHello(message) ==
  \/ message.wireGeneration > authorityGeneration[message.to][message.from]
  \/ /\ message.wireGeneration =
        authorityGeneration[message.to][message.from]
     /\ message.wireRevision >= authorityRevision[message.to][message.from]

DeliverHello(message) ==
  /\ message.kind = "hello"
  /\ LET changed ==
           \/ message.wireGeneration #
                authorityGeneration[message.to][message.from]
           \/ message.wireRevision #
                authorityRevision[message.to][message.from]
           \/ message.wireEpoch #
                authorityEpoch[message.to][message.from]
           \/ message.wireActive #
                authorityActive[message.to][message.from]
         install == FreshHello(message)
     IN /\ authorityGeneration' =
              IF install
              THEN [authorityGeneration EXCEPT
                      ![message.to][message.from] = message.wireGeneration]
              ELSE authorityGeneration
        /\ authorityRevision' =
              IF install
              THEN [authorityRevision EXCEPT
                      ![message.to][message.from] = message.wireRevision]
              ELSE authorityRevision
        /\ authorityEpoch' =
              IF install
              THEN [authorityEpoch EXCEPT
                      ![message.to][message.from] = message.wireEpoch]
              ELSE authorityEpoch
        /\ authorityActive' =
              IF install
              THEN [authorityActive EXCEPT
                      ![message.to][message.from] = message.wireActive]
              ELSE authorityActive
        /\ cursor' =
              IF install /\ (changed \/ ~message.wireActive)
              THEN [cursor EXCEPT ![message.to][message.from] = 0]
              ELSE cursor
        /\ replica' =
              IF install /\ (changed \/ ~message.wireActive)
              THEN [replica EXCEPT
                      ![message.to][message.from] = EmptyView]
              ELSE replica
  /\ UNCHANGED <<phase, generation, revision, epoch, active, truth, head,
                 floor, history, messages>>

ValidData(message) ==
  /\ authorityGeneration[message.to][message.from] = message.wireGeneration
  /\ authorityRevision[message.to][message.from] = message.wireRevision
  /\ authorityEpoch[message.to][message.from] = message.wireEpoch
  /\ authorityActive[message.to][message.from]

DeliverDelta(message) ==
  /\ message.kind = "delta"
  /\ IF ValidData(message) /\
        message.seq = cursor[message.to][message.from] + 1
     THEN /\ cursor' =
               [cursor EXCEPT
                 ![message.to][message.from] = message.seq]
          /\ replica' =
               [replica EXCEPT
                 ![message.to][message.from][message.key] = message.value]
     ELSE /\ UNCHANGED cursor
          /\ UNCHANGED replica
  /\ UNCHANGED <<phase, generation, revision, epoch, active, truth, head,
                 floor, history, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, messages>>

DeliverSnapshot(message) ==
  /\ message.kind = "snapshot"
  /\ IF ValidData(message) /\
        message.seq >= cursor[message.to][message.from]
     THEN /\ cursor' =
               [cursor EXCEPT
                 ![message.to][message.from] = message.seq]
          /\ replica' =
               [replica EXCEPT
                 ![message.to][message.from] = message.state]
     ELSE /\ UNCHANGED cursor
          /\ UNCHANGED replica
  /\ UNCHANGED <<phase, generation, revision, epoch, active, truth, head,
                 floor, history, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, messages>>

Deliver(message) ==
  /\ message \in messages
  /\ \/ DeliverHello(message)
     \/ DeliverDelta(message)
     \/ DeliverSnapshot(message)

Drop(message) ==
  /\ phase = "faulting"
  /\ message \in messages
  /\ messages' = messages \ {message}
  /\ UNCHANGED <<phase, generation, revision, epoch, active, truth, head,
                 floor, history, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, cursor, replica>>

Heal ==
  /\ phase = "faulting"
  /\ phase' = "healed"
  /\ UNCHANGED <<generation, revision, epoch, active, truth, head, floor,
                 history, authorityGeneration, authorityRevision,
                 authorityEpoch, authorityActive, cursor, replica, messages>>

(*
After healing, Repair represents one fair successful hello/head/need response.
If the retained prefix no longer contains the next sequence, it performs an
exact snapshot replacement. Otherwise it applies exactly the next delta.
*)
Repair(receiver, origin) ==
  /\ phase = "healed"
  /\ receiver # origin
  /\ ~PairConverged(receiver, origin)
  /\ IF ~CurrentAuthority(receiver, origin)
     THEN /\ authorityGeneration' =
                [authorityGeneration EXCEPT
                  ![receiver][origin] = generation[origin]]
          /\ authorityRevision' =
                [authorityRevision EXCEPT
                  ![receiver][origin] = revision[origin]]
          /\ authorityEpoch' =
                [authorityEpoch EXCEPT
                  ![receiver][origin] = epoch[origin]]
          /\ authorityActive' =
                [authorityActive EXCEPT
                  ![receiver][origin] = active[origin]]
          /\ cursor' = [cursor EXCEPT ![receiver][origin] = 0]
          /\ replica' =
                [replica EXCEPT ![receiver][origin] = EmptyView]
     ELSE IF ~active[origin]
     THEN /\ UNCHANGED <<authorityGeneration, authorityRevision,
                         authorityEpoch, authorityActive>>
          /\ cursor' = [cursor EXCEPT ![receiver][origin] = 0]
          /\ replica' =
                [replica EXCEPT ![receiver][origin] = EmptyView]
     ELSE IF cursor[receiver][origin] + 1 < floor[origin]
     THEN /\ UNCHANGED <<authorityGeneration, authorityRevision,
                         authorityEpoch, authorityActive>>
          /\ cursor' =
                [cursor EXCEPT ![receiver][origin] = head[origin]]
          /\ replica' =
                [replica EXCEPT ![receiver][origin] = truth[origin]]
     ELSE LET next == cursor[receiver][origin] + 1
              record == history[origin][next]
          IN /\ UNCHANGED <<authorityGeneration, authorityRevision,
                            authorityEpoch, authorityActive>>
             /\ cursor' = [cursor EXCEPT ![receiver][origin] = next]
             /\ replica' =
                   [replica EXCEPT
                     ![receiver][origin][record.key] = record.value]
  /\ UNCHANGED <<phase, generation, revision, epoch, active, truth, head,
                 floor, history, messages>>

Next ==
  \/ \E origin \in Origins : Open(origin)
  \/ \E origin \in Origins : Close(origin)
  \/ \E origin \in Origins : Restart(origin)
  \/ \E origin \in Origins, key \in Keys, value \in BOOLEAN :
       Mutate(origin, key, value)
  \/ \E origin \in Origins, receiver \in Nodes :
       SendHello(origin, receiver)
  \/ \E origin \in Origins, receiver \in Nodes, seq \in Seq :
       SendDelta(origin, receiver, seq)
  \/ \E origin \in Origins, receiver \in Nodes :
       SendSnapshot(origin, receiver)
  \/ \E message \in messages : Deliver(message)
  \/ \E message \in messages : Drop(message)
  \/ Heal
  \/ \E receiver \in Nodes, origin \in Origins :
       Repair(receiver, origin)

TypeOK ==
  /\ phase \in {"faulting", "healed"}
  /\ generation \in [Nodes -> Generations]
  /\ revision \in [Nodes -> Revisions]
  /\ epoch \in [Nodes -> Epochs]
  /\ active \in [Nodes -> BOOLEAN]
  /\ truth \in [Nodes -> BoolMap]
  /\ head \in [Nodes -> 0..MaxSeq]
  /\ floor \in [Nodes -> 1..(MaxSeq + 1)]
  /\ history \in [Nodes -> [Seq -> [key : Keys, value : BOOLEAN]]]
  /\ authorityGeneration \in [Nodes -> [Nodes -> Generations]]
  /\ authorityRevision \in [Nodes -> [Nodes -> Revisions]]
  /\ authorityEpoch \in [Nodes -> [Nodes -> Epochs]]
  /\ authorityActive \in [Nodes -> [Nodes -> BOOLEAN]]
  /\ cursor \in [Nodes -> [Nodes -> 0..MaxSeq]]
  /\ replica \in [Nodes -> [Nodes -> BoolMap]]
  /\ messages \subseteq Message

BoundedJournal ==
  \A origin \in Origins :
    /\ floor[origin] <= head[origin] + 1
    /\ head[origin] - floor[origin] + 1 <= OplogBound

CurrentReplicaIsAStreamPrefix ==
  \A receiver \in Nodes :
    \A origin \in Origins :
      IF receiver # origin /\
         CurrentAuthority(receiver, origin) /\
         active[origin]
      THEN /\ cursor[receiver][origin] <= head[origin]
           /\ replica[receiver][origin] =
                Replay(history[origin], cursor[receiver][origin])
      ELSE TRUE

HealedConvergence ==
  phase = "healed" ~> Converged

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ \A receiver \in Nodes :
       \A origin \in Origins :
         WF_vars(Repair(receiver, origin))

=============================================================================
