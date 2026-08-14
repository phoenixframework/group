# Group anti-entropy formal model

`GroupAntiEntropy.tla` is an independent finite-state model of the replica
contract. It covers:

- generation and named-cluster epoch fencing;
- arbitrary finite frame loss, duplication, and reordering;
- contiguous sequence application;
- bounded oplog pruning;
- exact per-origin snapshot fallback; and
- fair convergence after healing.

`SnapshotAssembly.tla` separately models the non-atomic wire delivery of an
exact snapshot. It explores independent provisional-chunk and terminal-commit
loss, duplication, and reordering, source invalidation before commit emission,
newer-snapshot supersession, authority epoch changes, staging expiry, and
receiver crashes. Its invariants require visible data and the cursor to remain
at a previously committed exact state until every chunk and a valid terminal
commit for one snapshot are present; stale or mixed partial state can never
become visible.

`PeerEviction.tla` isolates the lifecycle boundary for a peer which never
returns and for a later process using the same node name with a fresh
generation. During its finite faulty prefix it retains and reorders stale
hello and snapshot messages while leases expire. Authority consists of both a
generation and an active bit, so an inactive hello fences even a same-epoch
snapshot. After healing, fair repair must either install only the current
generation or erase every row and authority reference for the absent peer.

`AuthorityProjection.tla` models concurrent exact remote installs, local named
cluster activation/deactivation, materialized rows, and a lifecycle caller that
may disappear after the durable mutation. Its safety invariants require local
activation and shared routing to project authority consistently; its liveness
property requires queued close cleanup to finish without the original caller.

`AuthorityHint.tla` models the cross-lane fence created when a heartbeat or lane
hello observes a newer authority before the exact hello arrives. It checks that
delayed old exact/view installs cannot re-enable a lane, unresolved hints retain
a bounded lease/repair obligation, contiguous incremental authority is installed
only from the currently hinted/applied revision, unknown post-retirement hints
cannot establish authority, and delayed retirement cleanup cannot erase a
rediscovered generation's route.

The default TLC configuration uses three nodes: one origin and two independent
receivers. The origin has one key, a two-record stream, a one-record oplog, and
the system retains one arbitrary network frame. This forces delta repair,
snapshot fallback, stale-frame fencing, and independent recovery at both
receivers. The retained frame may be redelivered for duplication, while
nondeterministic sequence selection and delivery model out-of-order arrival
without paying the state-space cost of every two-frame set.

The TLA+ protocol state is deliberately factored per origin: no transition for
one origin reads or writes another origin's stream. Checking multiple origins
in this model therefore forms a Cartesian product of the same state machine
rather than adding an interaction. Concurrent A/C authority, registry conflict
projection, and preservation of C-owned state while A recovers are instead
driven against three real BEAM nodes by `replica_model_property_test.exs`.

Run it with Java 17 or later and a current `tla2tools.jar`:

```bash
TLA_JAR=/path/to/tla2tools.jar test/formal/check.sh

TLA_JAR=/path/to/tla2tools.jar \
  TLA_SPEC="$PWD/test/formal/SnapshotAssembly.tla" \
  TLA_CONFIG="$PWD/test/formal/SnapshotAssembly.cfg" \
  test/formal/check.sh

# Run all default models
TLA_JAR=/path/to/tla2tools.jar test/formal/check_matrix.sh

# Also run the larger two-key, three-sequence anti-entropy state space
TLA_JAR=/path/to/tla2tools.jar TLA_EXTENDED=1 test/formal/check_matrix.sh
```

`TLC_WORKERS` controls worker concurrency and defaults to 4. `TLA_CONFIG` can
point at an alternate finite configuration.

TLC proves the listed invariants and liveness property for the configured
finite instance, not for arbitrary unbounded node and key sets. Larger models
should be run periodically by increasing `Nodes`, `Origins`, `Keys`, `MaxSeq`,
`OplogBound`, and `MaxMessages`. `check_matrix.sh` runs the protocol, snapshot
assembly, peer-eviction, authority-projection, and authority-hint models; set
`TLA_EXTENDED=1` for the larger anti-entropy configuration.

The checked three-node default explores 1,835,826 states, finds 490,236
distinct states to a depth of 30, and completes in roughly one minute on the
development machine used for the validation run.

The snapshot-assembly model generates 3,305,473 states, finds 167,936 distinct
states to a depth of 23, and completes in roughly five seconds on the current
development machine.

The peer-eviction model explores 1,527,116 states, finds 238,120 distinct
states to a depth of 26, and completes in roughly 20 seconds on the development
machine used for validation.

The authority-projection model explores 71 states, finds 24 distinct states to
a depth of 6, and completes in under a second. Its small state space is
intentional: it exhaustively crosses the two authority directions, lifecycle
caller loss/replacement, writes, and independently fair close cleanup.

The authority-hint model explores 2,989 states, finds 428 distinct states to a
depth of 9, and completes in roughly one second. It separates the last exact
revision from the complete applied revision and highest persisted hint.

The extended two-key, three-sequence model explores 127,557,634 states, finds
32,238,304 distinct states to a depth of 34, and completes in roughly two hours
on the development machine used for validation.
