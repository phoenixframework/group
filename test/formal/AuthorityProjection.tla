------------------------ MODULE AuthorityProjection ------------------------
EXTENDS TLC

(*
Finite model of the serialized boundary between named-cluster authority and
its routing/materialized state. The API caller may disappear after either
durable lifecycle mutation. Activation must already project every exact remote
authority; deactivation must leave cleanup enabled independently of the caller.
*)

VARIABLES localActive,
          selfRoute,
          remoteExact,
          remoteRoute,
          rows,
          cleanupPending,
          callerAlive

vars ==
  <<localActive, selfRoute, remoteExact, remoteRoute, rows, cleanupPending,
    callerAlive>>

Init ==
  /\ localActive = FALSE
  /\ selfRoute = FALSE
  /\ remoteExact = FALSE
  /\ remoteRoute = FALSE
  /\ rows = FALSE
  /\ cleanupPending = FALSE
  /\ callerAlive = TRUE

InstallRemote(exact) ==
  /\ remoteExact' = exact
  /\ remoteRoute' = localActive /\ exact
  /\ UNCHANGED
       <<localActive, selfRoute, rows, cleanupPending, callerAlive>>

Activate ==
  /\ callerAlive = TRUE
  /\ localActive = FALSE
  /\ cleanupPending = FALSE
  /\ localActive' = TRUE
  /\ selfRoute' = TRUE
  /\ remoteRoute' = remoteExact
  /\ UNCHANGED <<remoteExact, rows, cleanupPending, callerAlive>>

Write ==
  /\ callerAlive = TRUE
  /\ localActive = TRUE
  /\ rows' = TRUE
  /\ UNCHANGED
       <<localActive, selfRoute, remoteExact, remoteRoute, cleanupPending,
         callerAlive>>

Deactivate ==
  /\ callerAlive = TRUE
  /\ localActive = TRUE
  /\ localActive' = FALSE
  /\ selfRoute' = FALSE
  /\ cleanupPending' = TRUE
  /\ UNCHANGED <<remoteExact, remoteRoute, rows, callerAlive>>

Cleanup ==
  /\ cleanupPending = TRUE
  /\ cleanupPending' = FALSE
  /\ remoteRoute' = FALSE
  /\ rows' = FALSE
  /\ UNCHANGED <<localActive, selfRoute, remoteExact, callerAlive>>

CrashCaller ==
  /\ callerAlive = TRUE
  /\ callerAlive' = FALSE
  /\ UNCHANGED
       <<localActive, selfRoute, remoteExact, remoteRoute, rows,
         cleanupPending>>

NewCaller ==
  /\ callerAlive = FALSE
  /\ callerAlive' = TRUE
  /\ UNCHANGED
       <<localActive, selfRoute, remoteExact, remoteRoute, rows,
         cleanupPending>>

Next ==
  \/ \E exact \in BOOLEAN : InstallRemote(exact)
  \/ Activate
  \/ Write
  \/ Deactivate
  \/ Cleanup
  \/ CrashCaller
  \/ NewCaller

TypeOK ==
  /\ localActive \in BOOLEAN
  /\ selfRoute \in BOOLEAN
  /\ remoteExact \in BOOLEAN
  /\ remoteRoute \in BOOLEAN
  /\ rows \in BOOLEAN
  /\ cleanupPending \in BOOLEAN
  /\ callerAlive \in BOOLEAN

SelfProjectionIsExact == selfRoute = localActive

SettledRemoteProjectionIsExact ==
  cleanupPending = FALSE => remoteRoute = (localActive /\ remoteExact)

InactiveRowsAreBeingCleaned ==
  (localActive = FALSE /\ rows = TRUE) => cleanupPending = TRUE

CleanupSurvivesCaller == cleanupPending = TRUE ~> cleanupPending = FALSE

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(Cleanup)

=============================================================================
