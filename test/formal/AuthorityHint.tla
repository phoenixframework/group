---------------------------- MODULE AuthorityHint ----------------------------
EXTENDS Integers, TLC

(*
Finite model of the shared authority-hint fence. A heartbeat on any replica
lane may observe a newer generation/revision before shard zero receives its
exact hello. The hint must disable every old lane, reject delayed old exact
and lane-view installs, retain a bounded repair/lease obligation, and allow a
stale cleanup caller to finish without deleting a rediscovered peer's route.
*)

CONSTANTS MaxGeneration, MaxRevision

ASSUME /\ MaxGeneration >= 2
       /\ MaxRevision >= 1

Generations == 1..MaxGeneration
Revisions == 0..MaxRevision

VARIABLES exactGeneration,
          exactRevision,
          appliedRevision,
          hintedGeneration,
          hintedRevision,
          viewGeneration,
          viewExactRevision,
          viewObservedRevision,
          laneEnabled,
          repairPending,
          leasePending,
          routeGeneration,
          cleanupPending

vars ==
  <<exactGeneration, exactRevision, appliedRevision, hintedGeneration, hintedRevision,
    viewGeneration, viewExactRevision, viewObservedRevision, laneEnabled,
    repairPending, leasePending, routeGeneration, cleanupPending>>

Init ==
  /\ exactGeneration = 1
  /\ exactRevision = 0
  /\ appliedRevision = 0
  /\ hintedGeneration = 1
  /\ hintedRevision = 0
  /\ viewGeneration = 1
  /\ viewExactRevision = 0
  /\ viewObservedRevision = 0
  /\ laneEnabled = TRUE
  /\ repairPending = FALSE
  /\ leasePending = TRUE
  /\ routeGeneration = 1
  /\ cleanupPending = FALSE

Newer(generation, revision, currentGeneration, currentRevision) ==
  \/ generation > currentGeneration
  \/ /\ generation = currentGeneration
     /\ revision > currentRevision

NewerOrEqual(generation, revision, currentGeneration, currentRevision) ==
  \/ Newer(generation, revision, currentGeneration, currentRevision)
  \/ /\ generation = currentGeneration
     /\ revision = currentRevision

ObserveHint(generation, revision) ==
  /\ exactGeneration > 0
  /\ hintedGeneration > 0
  /\ generation \in Generations
  /\ revision \in Revisions
  /\ Newer(generation, revision, hintedGeneration, hintedRevision)
  /\ hintedGeneration' = generation
  /\ hintedRevision' = revision
  /\ laneEnabled' = FALSE
  /\ repairPending' = TRUE
  /\ leasePending' = TRUE
  /\ UNCHANGED
       <<exactGeneration, exactRevision, viewGeneration, viewExactRevision,
         appliedRevision, viewObservedRevision, routeGeneration, cleanupPending>>

InstallExact(generation, revision) ==
  /\ generation \in Generations
  /\ revision \in Revisions
  /\ NewerOrEqual(generation, revision, hintedGeneration, hintedRevision)
  /\ \/ exactGeneration = 0
     \/ NewerOrEqual(generation, revision, exactGeneration, exactRevision)
  /\ exactGeneration' = generation
  /\ exactRevision' = revision
  /\ appliedRevision' = revision
  /\ hintedGeneration' = generation
  /\ hintedRevision' = revision
  /\ routeGeneration' = generation
  /\ laneEnabled' = FALSE
  /\ repairPending' = TRUE
  /\ leasePending' = TRUE
  /\ UNCHANGED
       <<viewGeneration, viewExactRevision, viewObservedRevision,
         cleanupPending>>

(*
Shard zero may install a contiguous incremental authority change without
moving the last exact-snapshot revision. The expected revision is compared to
the shared applied revision and hint in the same serialized transition; a
heartbeat racing it forward therefore disables this action instead of allowing
partial epoch rows to become authoritative.
*)
InstallIncremental(expected, revision) ==
  /\ exactGeneration > 0
  /\ expected \in Revisions
  /\ revision \in Revisions
  /\ revision = expected + 1
  /\ appliedRevision = expected
  /\ hintedGeneration = exactGeneration
  /\ hintedRevision = expected
  /\ appliedRevision' = revision
  /\ hintedRevision' = revision
  /\ laneEnabled' = FALSE
  /\ repairPending' = TRUE
  /\ leasePending' = TRUE
  /\ UNCHANGED
       <<exactGeneration, exactRevision, hintedGeneration, viewGeneration,
         viewExactRevision, viewObservedRevision, routeGeneration,
         cleanupPending>>

InstallLaneView ==
  /\ exactGeneration > 0
  /\ exactGeneration = hintedGeneration
  /\ appliedRevision = hintedRevision
  /\ viewGeneration' = exactGeneration
  /\ viewExactRevision' = exactRevision
  /\ viewObservedRevision' = hintedRevision
  /\ laneEnabled' = TRUE
  /\ repairPending' = FALSE
  /\ leasePending' = TRUE
  /\ UNCHANGED
       <<exactGeneration, exactRevision, hintedGeneration, hintedRevision,
         appliedRevision, routeGeneration, cleanupPending>>

QueueCleanup ==
  /\ ~cleanupPending
  /\ cleanupPending' = TRUE
  /\ UNCHANGED
       <<exactGeneration, exactRevision, hintedGeneration, hintedRevision,
         appliedRevision, viewGeneration, viewExactRevision, viewObservedRevision, laneEnabled,
         repairPending, leasePending, routeGeneration>>

RunCleanup ==
  /\ cleanupPending
  /\ cleanupPending' = FALSE
  /\ routeGeneration' =
       IF exactGeneration = 0 /\ hintedGeneration = 0
       THEN 0
       ELSE routeGeneration
  /\ UNCHANGED
       <<exactGeneration, exactRevision, appliedRevision, hintedGeneration, hintedRevision,
         viewGeneration, viewExactRevision, viewObservedRevision, laneEnabled,
         repairPending, leasePending>>

RetireUnresolvedHint ==
  /\ repairPending
  /\ leasePending
  /\ exactGeneration' = 0
  /\ exactRevision' = 0
  /\ appliedRevision' = 0
  /\ hintedGeneration' = 0
  /\ hintedRevision' = 0
  /\ viewGeneration' = 0
  /\ viewExactRevision' = 0
  /\ viewObservedRevision' = 0
  /\ laneEnabled' = FALSE
  /\ repairPending' = FALSE
  /\ leasePending' = FALSE
  /\ routeGeneration' = 0
  /\ UNCHANGED cleanupPending

DropPeer ==
  /\ exactGeneration' = 0
  /\ exactRevision' = 0
  /\ appliedRevision' = 0
  /\ hintedGeneration' = 0
  /\ hintedRevision' = 0
  /\ viewGeneration' = 0
  /\ viewExactRevision' = 0
  /\ viewObservedRevision' = 0
  /\ laneEnabled' = FALSE
  /\ repairPending' = FALSE
  /\ leasePending' = FALSE
  /\ routeGeneration' = 0
  /\ UNCHANGED cleanupPending

Next ==
  \/ \E generation \in Generations, revision \in Revisions :
       ObserveHint(generation, revision)
  \/ \E generation \in Generations, revision \in Revisions :
       InstallExact(generation, revision)
  \/ \E expected \in Revisions, revision \in Revisions :
       InstallIncremental(expected, revision)
  \/ InstallLaneView
  \/ QueueCleanup
  \/ RunCleanup
  \/ RetireUnresolvedHint
  \/ DropPeer

TypeOK ==
  /\ exactGeneration \in 0..MaxGeneration
  /\ exactRevision \in Revisions
  /\ appliedRevision \in Revisions
  /\ hintedGeneration \in 0..MaxGeneration
  /\ hintedRevision \in Revisions
  /\ viewGeneration \in 0..MaxGeneration
  /\ viewExactRevision \in Revisions
  /\ viewObservedRevision \in Revisions
  /\ laneEnabled \in BOOLEAN
  /\ repairPending \in BOOLEAN
  /\ leasePending \in BOOLEAN
  /\ routeGeneration \in 0..MaxGeneration
  /\ cleanupPending \in BOOLEAN

EnabledLaneMatchesExactAuthority ==
  laneEnabled =>
    /\ exactGeneration > 0
    /\ viewGeneration = exactGeneration
    /\ viewExactRevision = exactRevision
    /\ viewObservedRevision = hintedRevision
    /\ hintedGeneration = exactGeneration
    /\ hintedRevision = appliedRevision

ExactAuthorityOwnsItsRoute ==
  exactGeneration > 0 => routeGeneration = exactGeneration

NoLaneWithoutExactAuthority ==
  exactGeneration = 0 => ~laneEnabled

HintRequiresPriorExactAuthority ==
  hintedGeneration > 0 => exactGeneration > 0

AppliedAuthorityRespectsItsFence ==
  /\ (exactGeneration > 0 => exactRevision <= appliedRevision)
  /\ (hintedGeneration = exactGeneration => appliedRevision <= hintedRevision)

UnresolvedHintIsBounded == repairPending ~> ~repairPending

DelayedCleanupCompletes == cleanupPending ~> ~cleanupPending

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(RetireUnresolvedHint)
  /\ WF_vars(RunCleanup)

=============================================================================
