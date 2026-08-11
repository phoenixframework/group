--------------------------- MODULE PeerEviction ---------------------------
EXTENDS Integers, FiniteSets, TLC

(*
Finite model of the peer lease and reincarnation boundary. An origin may
disappear permanently or restart at a larger generation while arbitrary old
hello and snapshot messages remain in the network. A receiver may expire its
lease at any point. Once the faulting prefix ends, fair repair must either
install the current generation exactly or erase the permanently absent peer.
*)

CONSTANTS Receivers, Keys, MaxGeneration, MaxMessages

ASSUME /\ IsFiniteSet(Receivers)
       /\ Cardinality(Receivers) >= 1
       /\ IsFiniteSet(Keys)
       /\ Cardinality(Keys) >= 1
       /\ MaxGeneration >= 2
       /\ MaxMessages >= 1

Generations == 1..MaxGeneration
Token == Generations \X Keys

HelloMessage ==
  [kind : {"hello"}, to : Receivers, wireGeneration : Generations,
   wireUp : BOOLEAN]

SnapshotMessage ==
  [kind : {"snapshot"}, to : Receivers, wireGeneration : Generations,
   rows : SUBSET Token]

Message == HelloMessage \union SnapshotMessage

VARIABLES phase, up, generation, truth, authorityGeneration, authorityUp, view, messages

vars ==
  <<phase, up, generation, truth, authorityGeneration, authorityUp, view,
    messages>>

Init ==
  /\ phase = "faulting"
  /\ up = TRUE
  /\ generation = 1
  /\ truth = {}
  /\ authorityGeneration = [receiver \in Receivers |-> 0]
  /\ authorityUp = [receiver \in Receivers |-> FALSE]
  /\ view = [receiver \in Receivers |-> {}]
  /\ messages = {}

Write(key, present) ==
  /\ phase = "faulting"
  /\ up
  /\ IF present
     THEN truth' = truth \union {<<generation, key>>}
     ELSE truth' = truth \ {<<generation, key>>}
  /\ UNCHANGED
       <<phase, up, generation, authorityGeneration, authorityUp, view, messages>>

Crash ==
  /\ phase = "faulting"
  /\ up
  /\ up' = FALSE
  /\ truth' = {}
  /\ UNCHANGED
       <<phase, generation, authorityGeneration, authorityUp, view, messages>>

Restart ==
  /\ phase = "faulting"
  /\ ~up
  /\ generation < MaxGeneration
  /\ up' = TRUE
  /\ generation' = generation + 1
  /\ truth' = {}
  /\ UNCHANGED <<phase, authorityGeneration, authorityUp, view, messages>>

SendHello(receiver) ==
  /\ phase = "faulting"
  /\ Cardinality(messages) < MaxMessages
  /\ messages' = messages \union
       {[kind |-> "hello", to |-> receiver,
         wireGeneration |-> generation, wireUp |-> up]}
  /\ UNCHANGED
       <<phase, up, generation, truth, authorityGeneration, authorityUp, view>>

SendSnapshot(receiver) ==
  /\ phase = "faulting"
  /\ up
  /\ Cardinality(messages) < MaxMessages
  /\ messages' = messages \union
       {[kind |-> "snapshot", to |-> receiver,
         wireGeneration |-> generation, rows |-> truth]}
  /\ UNCHANGED
       <<phase, up, generation, truth, authorityGeneration, authorityUp, view>>

DeliverHello(message) ==
  /\ message.kind = "hello"
  /\ IF message.wireGeneration >= authorityGeneration[message.to]
     THEN /\ authorityGeneration' =
                [authorityGeneration EXCEPT ![message.to] = message.wireGeneration]
          /\ authorityUp' = [authorityUp EXCEPT ![message.to] = message.wireUp]
          /\ view' =
                IF message.wireGeneration # authorityGeneration[message.to]
                   \/ ~message.wireUp
                THEN [view EXCEPT ![message.to] = {}]
                ELSE view
     ELSE /\ UNCHANGED authorityGeneration
          /\ UNCHANGED authorityUp
          /\ UNCHANGED view
  /\ UNCHANGED <<phase, up, generation, truth, messages>>

DeliverSnapshot(message) ==
  /\ message.kind = "snapshot"
  /\ IF message.wireGeneration = authorityGeneration[message.to]
        /\ authorityUp[message.to]
     THEN view' = [view EXCEPT ![message.to] = message.rows]
     ELSE UNCHANGED view
  /\ UNCHANGED
       <<phase, up, generation, truth, authorityGeneration, authorityUp, messages>>

Deliver(message) ==
  /\ phase = "faulting"
  /\ message \in messages
  /\ \/ DeliverHello(message)
     \/ DeliverSnapshot(message)

Drop(message) ==
  /\ phase = "faulting"
  /\ message \in messages
  /\ messages' = messages \ {message}
  /\ UNCHANGED
       <<phase, up, generation, truth, authorityGeneration, authorityUp, view>>

ExpireLease(receiver) ==
  /\ phase = "faulting"
  /\ authorityGeneration' = [authorityGeneration EXCEPT ![receiver] = 0]
  /\ authorityUp' = [authorityUp EXCEPT ![receiver] = FALSE]
  /\ view' = [view EXCEPT ![receiver] = {}]
  /\ UNCHANGED <<phase, up, generation, truth, messages>>

Heal ==
  /\ phase = "faulting"
  /\ phase' = "healed"
  /\ UNCHANGED
       <<up, generation, truth, authorityGeneration, authorityUp, view, messages>>

Repair(receiver) ==
  /\ phase = "healed"
  /\ IF up
     THEN /\ authorityGeneration' =
                [authorityGeneration EXCEPT ![receiver] = generation]
          /\ authorityUp' = [authorityUp EXCEPT ![receiver] = TRUE]
          /\ view' = [view EXCEPT ![receiver] = truth]
     ELSE /\ authorityGeneration' =
                [authorityGeneration EXCEPT ![receiver] = 0]
          /\ authorityUp' = [authorityUp EXCEPT ![receiver] = FALSE]
          /\ view' = [view EXCEPT ![receiver] = {}]
  /\ UNCHANGED <<phase, up, generation, truth, messages>>

Next ==
  \/ \E key \in Keys, present \in BOOLEAN : Write(key, present)
  \/ Crash
  \/ Restart
  \/ \E receiver \in Receivers : SendHello(receiver)
  \/ \E receiver \in Receivers : SendSnapshot(receiver)
  \/ \E message \in messages : Deliver(message)
  \/ \E message \in messages : Drop(message)
  \/ \E receiver \in Receivers : ExpireLease(receiver)
  \/ Heal
  \/ \E receiver \in Receivers : Repair(receiver)

TypeOK ==
  /\ phase \in {"faulting", "healed"}
  /\ up \in BOOLEAN
  /\ generation \in Generations
  /\ truth \subseteq Token
  /\ authorityGeneration \in [Receivers -> 0..MaxGeneration]
  /\ authorityUp \in [Receivers -> BOOLEAN]
  /\ view \in [Receivers -> SUBSET Token]
  /\ messages \subseteq Message

VisibleRowsMatchInstalledGeneration ==
  \A receiver \in Receivers :
    \A token \in view[receiver] : token[1] = authorityGeneration[receiver]

NoRowsWithoutAuthority ==
  \A receiver \in Receivers :
    (authorityGeneration[receiver] = 0 \/ ~authorityUp[receiver])
      => view[receiver] = {}

Converged ==
  \A receiver \in Receivers :
    IF up
    THEN /\ authorityGeneration[receiver] = generation
         /\ authorityUp[receiver]
         /\ view[receiver] = truth
    ELSE /\ authorityGeneration[receiver] = 0
         /\ ~authorityUp[receiver]
         /\ view[receiver] = {}

HealedConvergence == phase = "healed" ~> Converged

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ \A receiver \in Receivers : WF_vars(Repair(receiver))

=============================================================================
