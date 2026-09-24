# Group anti-entropy formal model

`GroupAntiEntropy.tla` is an independent finite-state model of the replica
contract. It covers:

- generation and named-cluster epoch fencing;
- arbitrary finite frame loss, duplication, and reordering;
- contiguous sequence application;
- bounded oplog pruning;
- exact per-origin snapshot fallback; and
- fair convergence after healing.

`ReplicaAck.tla` checks the ACK-driven stream protocol against the guards in
`Group.Replica`. Its state maps to `pending_replica_heads`,
`replica_send_tokens`, `replica_receive_tokens`, queued `:applied` cursors,
`remote_probe_epochs`, the receiver's cursor and materialized row, the peer
PID/generation, and the named-cluster epoch. It explores receiver lease expiry,
process restart, and rejoin after an epoch change with old heads, needs, ACKs,
and repairs still in flight. The exact source guards are represented:

- a new `peer_connect` probe for the same PID rotates the sender token and
  requeues heads; a duplicate probe preserves both and elicits no full hello
  when the sender already has the exact receiver view;
- `peer_connect_ack` echoes the probe epoch and certifies that the sender's
  route and exact authority matched the receiver PID, generation, and revision
  after any required reseed; an ACK for an uninstalled route or stale authority
  keeps the recovery probe pending;
- `:applied` clears a pending head only for the current PID, generation, token,
  epoch, stream target, and a cursor at least as high as the pending head;
- `:needs` can cause repair only while its advertised head is still pending;
- a retained prefix uses delta, while a pruned prefix uses a snapshot; a
  separate prune step reflects the shard-wide oplog cap, which can advance
  this stream's floor even when this stream has no new writes; a sent
  snapshot is held until its retry timer or a newer head; and
- no head is sent for a quiet stream, while a full hello needs discovery or
  authority work.

The model deliberately does **not** add a PID/token/epoch guard to `:needs`:
the Elixir wire message does not carry those fields. `:delta_batch` is modeled
as one contiguous run and `:snapshot_chunk` plus `:snapshot_commit` as one
committed exact state. `SnapshotAssembly.tla` checks the latter's partial wire
delivery and staging; `GroupAntiEntropy.tla` checks broader authority and
multi-receiver data convergence. The finite ACK model checks that a quiet head
has current ACK evidence, visible state is an exact committed prefix, stale
needs cannot justify a full send, and every healed run eventually converges.
The explicit wire actions check safety under loss and reordering. For liveness,
a weakly fair `Repair` action represents a successful probe/hello/head/repair/
ACK round after transport healing, using the same pending obligations. This
assumes a healthy transport eventually completes such rounds; it does not prove
independent fairness of every packet queue.
The default configuration checks liveness through a one-sided lease expiry
with one in-flight frame. `ReplicaAckReincarnation.cfg` and `ReplicaAckEpoch.cfg`
also check liveness when the receiver PID or named-cluster authority changes.
`ReplicaAckExtended.cfg` keeps two frames and crosses both changes for safety.
The extended instance omits the
temporal property to keep the much larger interleaving space practical.
The matrix also removes the ACK token fence, the stale-need pending guard,
the full-hello discovery guard, the route-independent recovery probe, and the
ready bit on a probe ACK, and the receiver authority revision in
isolated copies. TLC must reject each mutant using only its corresponding
invariant or convergence property; a parser failure or unrelated check cannot
count as detection.

`SnapshotAssembly.tla` separately models the non-atomic wire delivery of an
exact snapshot. It explores independent provisional-chunk and terminal-commit
loss, duplication, and reordering, source invalidation before commit emission,
newer-snapshot supersession, authority epoch changes, staging expiry, and
receiver crashes. Its invariants require visible data and the cursor to remain
at a previously committed exact state until every chunk and a valid terminal
commit for one snapshot are present; stale or mixed partial state can never
become visible.

The commit invariant retains the last installation's commit evidence separately
from disposable staging. The formal matrix also removes the terminal-commit guard
in an isolated copy and requires TLC to reject it specifically with
`NoCommitMeansNoInstall`. A surviving mutant, parser error, or runtime failure
fails this qualification.

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

# Run the ACK stream model alone (a converged terminal state is allowed)
TLA_JAR=/path/to/tla2tools.jar \
  TLA_SPEC="$PWD/test/formal/ReplicaAck.tla" \
  TLA_CONFIG="$PWD/test/formal/ReplicaAck.cfg" \
  test/formal/check.sh

TLA_JAR=/path/to/tla2tools.jar \
  TLA_SPEC="$PWD/test/formal/SnapshotAssembly.tla" \
  TLA_CONFIG="$PWD/test/formal/SnapshotAssembly.cfg" \
  test/formal/check.sh

# Run all default models and snapshot commit qualification (also requires Elixir)
TLA_JAR=/path/to/tla2tools.jar test/formal/check_matrix.sh

# Run only snapshot assembly and its negative commit qualification
TLA_JAR=/path/to/tla2tools.jar elixir test/formal/check_snapshot_commit.exs

# Also run the larger two-key, three-sequence anti-entropy state space
TLA_JAR=/path/to/tla2tools.jar TLA_EXTENDED=1 test/formal/check_matrix.sh
```

`TLC_WORKERS` controls worker concurrency and defaults to 4. `TLA_CONFIG` can
point at an alternate finite configuration. `TLA_METADIR` can move TLC's
working files outside the checkout. `ReplicaAck.cfg` allows the healthy
terminal state, where there is no work left to send.

TLC proves the listed invariants and liveness property for the configured
finite instance, not for arbitrary unbounded node and key sets. Larger models
should be run periodically by increasing `Nodes`, `Origins`, `Keys`, `MaxSeq`,
`OplogBound`, and `MaxMessages`. `check_matrix.sh` runs the protocol, ACK stream,
snapshot assembly, peer-eviction, authority-projection, and authority-hint models; set
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
