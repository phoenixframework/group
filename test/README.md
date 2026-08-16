# Group Test Infrastructure

## Running tests

```bash
mix test                           # every-PR ExUnit/property/chaos/checker gate
mix test.soak                      # nightly six-profile Jepsen campaign
mix test test/group_test.exs       # local only
mix test test/distributed_test.exs # distributed only
mix test test/replica_adversarial_test.exs # seeded transport chaos
mix test test/replica_model_property_test.exs # shrinkable model-based histories
test/jepsen/run.sh                 # one OS-partition/restart Jepsen model test
```

`mix test` preserves normal Mix test arguments while always running the pure
Jepsen lifecycle-checker qualification after ExUnit. It does not require
Docker. `mix test.soak` first runs that complete PR gate, kills every defined
protocol mutant, runs live positive/negative checker qualification, and then
runs the distribution/TCP/chaos × mixed/permanent Jepsen campaign. The soak
defaults to 20 five-minute fault histories per combination and is intended for
nightly and release qualification rather than individual edits.

## Test files

| File | What it tests |
|------|---------------|
| `group_test.exs` | Single-node: register/unregister, join/leave, members, monitor/demonitor, named clusters, concurrent operations |
| `distributed_test.exs` | Multi-node: replication, peer discovery, node disconnect cleanup, partition healing, conflict resolution, event ordering, rolling restarts, and adversarial replica-transport loss/busy/snapshot recovery |
| `anti_entropy_fault_regression_test.exs` | Three-node regressions for hidden-winner projection, receiver restart eviction, nodedown/lease lane retirement, authority gaps and cross-lane races, in-flight conflict fencing, crash-journal replay, cursorless/interrupted snapshot repair, malformed ingress, and sideband rediscovery |
| `replica_adversarial_test.exs` | Reproducible three-node mixed-operation state machines: drops, busy returns, duplication, reordering, bounded delay, oplog pruning, conflicts, owner death, and named-cluster epoch churn, followed by exact convergence/dead-owner/internal-index checks |
| `replica_model_property_test.exs` | StreamData-generated and shrunk owner histories against an independent lifecycle oracle and scheduler-controlled replica transport |
| `replica_snapshot_test.exs` | Pure single-pass byte-bounded streaming, suffix resume, receive staging, and event batching |
| `replica_snapshot_distributed_test.exs` | Real-node provisional-chunk/terminal-commit loss, reorder, duplicate, conflicting retransmission/manifest, concurrent-source invalidation, supersession, authority fencing, expiry, pooled staging, and shard-crash recovery |

## Model-based and formal checks

`replica_model_property_test.exs` runs real Group instances on three peer VMs.
The controlled transport queues each replica message so generated commands can
deliver, duplicate, drop, reorder, or strand it. After the bounded-fault
prefix, the test enables fair delivery and compares every tracked registry and
PG key against an independent application-level lifecycle oracle. It also
requires internal replica indexes to be consistent, every retained owner to be
alive, and registry conflict losers to be dead. Restart, pruning, and named
cluster histories retain independent C-owned state while A recovers, so repair
cannot pass merely by making one origin and one receiver agree. Model groups
use a deliberately tiny snapshot target so pruning recovery traverses the real
multi-chunk assembly path.

StreamData reports the ExUnit seed and shrinks a failure to its smallest command
history. Local defaults are intentionally quick. Increase the budgets without
changing the generator:

```bash
GROUP_MODEL_RUNS=1000 GROUP_MODEL_COMMANDS=100 \
  mix test test/replica_model_property_test.exs
```

The independent TLA+ model and TLC configuration live in `test/formal/`.
See [`formal/README.md`](formal/README.md) for its checked invariants, finite
model bounds, and run command.

The Docker-backed Jepsen harness lives in [`jepsen/`](jepsen/). It drives
three independent BEAM containers through concurrent, multi-entry owner
lifecycles, named-cluster epoch churn, directed/full partitions, transport
session resets, and VM restarts. The same workload runs over distribution,
a test-only real sideband TCP lane, and a lossy/duplicating/reordering
transport. After healing,
its independent oracle checks exact public views and the internal registry,
PG, claim, cluster, cursor, oplog, snapshot-staging, and retired-origin
invariants. Its permanent-retirement scenario proves eviction even when a peer
never returns. `test/jepsen/campaign.sh` runs the full profile/scenario matrix;
`test/jepsen/qualify.sh` mutation-tests the implementation and proves that the
live checker rejects injected faults. Chaos/mixed uses a larger repair window
and is invalid unless it observes a multi-record delta run; the other profiles
retain the one-record stress configuration.

## How distribution works

The test node starts as a named Erlang node in `test_helper.exs`:

```elixir
Node.start(:"test_12345@127.0.0.1", :longnames)
Node.set_cookie(:group_test)
```

Peer nodes are real BEAM VMs started via OTP's `:peer` module (not
`Node.spawn`). Each peer has its own schedulers, memory, and GC — they
communicate over Erlang distribution just like production nodes.

`:prevent_overlapping_partitions` is set to `false` on all nodes (test node
and peers). Without this, disconnecting two peers from each other would also
disconnect them from the test node, making partition tests impossible.

## Support modules

Files in `test/support/` are compiled to BEAM via `elixirc_paths(:test)` in
`mix.exs`. This is required because `:erpc.call` with anonymous functions
needs the defining module's beam file on the remote node. Since test files
aren't compiled to beam, all remote-callable code lives in support modules.

### `Group.TestCluster`

The main test helper. All distributed tests use this module.

#### Peer lifecycle

```elixir
# Start 3 peer nodes with Group app loaded
peers = TestCluster.start_peers(3)
# => [{pid1, :"peer42@hostname"}, {pid2, ...}, {pid3, ...}]

# Each peer gets:
#   - unique name
#   - same cookie as the test node
#   - all code paths from the test node (-pa flags)
#   - :elixir and :group applications started

# Stop all peers
TestCluster.stop_peers(peers)
```

#### Starting Group on peers

```elixir
TestCluster.start_group(node, name: :test, shards: 4)
```

Calls `Group.start_link` on the remote node, then `Process.unlink(pid)`.
The unlink is critical — `Supervisor.start_link` links to the calling process,
and when `:erpc.call` returns, that link would kill the supervisor.

#### Spawning registered/joined processes

`Group.register` and `Group.join` both use `self()` to determine which
process to register. You can't register a remote process — the process must
call Group on its own behalf. These helpers spawn a process on the remote
node, have it call Group, wait for confirmation, then keep it alive:

```elixir
# Spawn a process on node_a that registers "user/1" and sleeps forever
pid = TestCluster.spawn_register(node_a, :test, "user/1", %{role: :server})

# Same for join
pid = TestCluster.spawn_join(node_a, :test, "lobby", %{})

# Register + join in one process
pid = TestCluster.spawn_register_and_join(node_a, :test, "user/1", %{}, "lobby", %{})

# Register in a named cluster
pid = TestCluster.spawn_register_in_cluster(node_a, :test, "user/1", %{}, "game")

# Register with different keys for reg vs join (cross-shard testing)
pid = TestCluster.spawn_register_and_join_keys(node_a, :test, key1, %{}, key2, %{})

# Register then die (tests cleanup)
pid = TestCluster.spawn_register_then_kill(node_a, :test, "user/1", %{}, _delay = 100)

# Register → update meta → unregister (tests event ordering)
TestCluster.spawn_register_update_unregister(node_a, :test, "user/1", %{v: 1}, %{v: 2})
```

#### flush_shards option

`spawn_register` accepts `flush_shards: num_shards` which calls
`:sys.get_state` on the target shard's GenServer after registration. This
blocks until all pending messages (nodedown, replicate, etc.) are processed
on that shard — useful in partition tests where you need to guarantee ordering.

```elixir
TestCluster.spawn_register(node_a, :test, "key", %{}, flush_shards: 4)
```

#### Monitoring events remotely

Group events are delivered to the monitoring process's mailbox. To observe
events happening on a remote node from the test process, spawn a forwarder:

```elixir
TestCluster.spawn_monitor_forwarder(node_a, :test, "user/", self())
assert_receive {:monitor_ready, _forwarder_pid}, 5000

# Now any Group events matching "user/" on node_a arrive as:
assert_receive {:got_event, %Group.Event{type: :registered, key: "user/1"}}
```

The forwarder calls `Group.monitor`, then loops receiving `%Group.Event{}`
messages and sending them as `{:got_event, event}` to the test process.
Times out after 30 seconds.

#### Monitoring Erlang nodedown events

```elixir
TestCluster.monitor_nodes_on(node_a, self())
# When node_b disconnects from node_a:
assert_receive {:nodedown_on_remote, ^node_b}, 5000
```

#### Network partitions

```elixir
TestCluster.disconnect_nodes(node_a, node_b)
TestCluster.reconnect_nodes(node_a, node_b)
```

Partition tests use **3 nodes** and isolate one from the other two. Two-node
partitions don't work reliably because the test node bridges them — Erlang
distribution is fully meshed, so if the test node can reach both peers, they
can reach each other through it.

#### Polling for eventual consistency

```elixir
TestCluster.assert_eventually(fn ->
  TestCluster.rpc!(node_b, Group, :lookup, [:test, "user/1"]) != nil
end, timeout: 5000, interval: 50)
```

Retries the function until it returns `true` or the timeout expires.
Defaults: 2000ms timeout, 50ms interval.

#### Shard utilities

```elixir
# Find two keys guaranteed to land on different shards
{key1, key2} = TestCluster.keys_for_different_shards(4)
```

Useful for testing cross-shard scenarios like a process registered in one
shard and joined in another, then verifying cleanup hits both shards on death.

#### Generic RPC

```elixir
result = TestCluster.rpc!(node_a, Group, :lookup, [:test, "user/1"])
# Raises on {:badrpc, reason} instead of returning it
```

### `Group.TestConflictResolver`

A conflict resolution function for partition healing tests. When the same key
is registered on two different nodes during a partition, Group calls the
resolver on reconnection to pick a winner.

```elixir
# Configured via Group.start_link:
TestCluster.start_group(
  node,
  name: :test,
  resolve_registry_conflict: {Group.TestConflictResolver, :resolve, []}
)
```

The resolver ranks each claim by timestamp. Group chooses the maximum
`{rank, pid}`, so every peer reaches the same winner regardless of claim arrival
order.

### Replica transport fault injection

`Group.TestReplicaTransport` implements the production transport behaviour but
can return `:busy`, drop selected message types, duplicate or delay messages,
and capture messages for explicit stale-generation/epoch replay. Its
`{:chaos, opts}` mode is deterministic for a given message, which makes failures
reproducible.

`Group.ControlledReplicaTransport` is the model-test transport. It queues messages
at the test process without scheduling timers; `Group.ReplicaModelScheduler`
then owns the exact delivery schedule. These roles are separate so the existing
timing-oriented regressions retain their original mechanics while property
failures can be replayed and shrunk exactly.

The distributed anti-entropy tests cover dropped creates and deletes, cursor
gaps, globally pruned multi-stream oplogs, exact snapshot fallback, malformed
authority, stale message replay, lease expiry on a live VM, and multi-shard
generation recovery. They also restart a suspended data lane after deliberately
losing its cluster-close fence and require the lane to sweep the stale registry
and PG slices from shared authority. Authority topology tests suspend every
receiver shard and inspect the queued protocol: only shard 0 may receive/install
the full epoch snapshot, nonzero shards receive constant-size lane hellos, and
incremental controls arriving on any lane are serialized through shard 0.
Separate tests suspend a
backlogged authority shard while other replica lanes continue converging and
deliver data before authority to prove rejection does not advance the cursor
and the same message applies after authority repair. Concurrent snapshot tests
require every advertised revision to contain exactly that many unique named
epochs, and heartbeat tests prove observed revisions cannot advance the exact
authority marker. Crash-window tests interrupt journal, dual-index, receive
cursor, and named-cluster close updates, then require startup repair to remove
every invisible row and temporary close barrier. A three-node test-only TCP test
disconnects one origin's real socket, prunes its oplog, reconnects it, and
requires snapshot recovery without changing the third node's independent
registry or PG state.

Authority races also replace authority while conflict resolution is paused,
remove the local shard-zero owner, and deliver `nodedown` while a sibling lane
is suspended. The assertions require that stale remote winners cannot terminate
local owners, that retained conflict claims are reprojected after an authority
gap closes even when their stream cursor is already current, and that each lane
keeps its own restart breadcrumb until its rows and cursor are gone.
The same conflict-gap schedule is held open until lease expiry to prove a source
that never returns leaves no deferred projection key, claim, cursor, or visible
remote winner behind. A separate nodedown regression proves the immediate
retirement path cannot strand that deferred state after removing its lease.
Newer-generation and same-generation heartbeat schedules assert the persisted
hint fences every lane before exact repair, rejects delayed exact/incremental
authority, and reconstructs its retirement deadline after a lane crash. Once
the peer is fully retired, replayed heartbeats and lane hellos must create no
hint, lease, or outbound route. A separate compare-and-install regression races
a newer hint across an older incremental control and requires no epoch row or
lane view to become authoritative.
The inverse ordering is forced independently: a first-time lane hello is
processed while shard zero is suspended, then exact authority must trigger an
immediate shard-local re-probe without first admitting the speculative route.
Exact-authority regression also activates a local cluster alongside a remote
epoch install and requires the authority plus both cluster indexes to become
visible as one operation; the high-volume control test repeats the projection
check over three nodes before admitting replica writes.
Interrupted lifecycle tests suspend the notification shard, stop after the
durable activation/deactivation mutation, and require activation routing to be
complete immediately and close cleanup to finish after the shard resumes. This
proves neither path relies on the API caller remaining alive.

`replica_transport_outbox_test.exs` proves that a blocked sideband backend
cannot delay the Group-facing local push, messages expire behind that backend,
busy batches are not retried locally, batching preserves per-target order,
invalid deadlines fail at boot, and ingress drops rather than raising while a
destination shard is absent. Admission is bounded across the local mailbox,
pending batch, and backend send.
The real three-node TCP recovery test runs through the same outbox path.

`Group.TestCluster.assert_replica_consistent/1` checks the
public dual indexes plus exact deterministic registry projection, row/shard
placement, lane authority revisions, registry/PG origin authority,
oplog/order equivalence, and contiguous retained stream ranges. Seeded tests
additionally require every PID retained as authority to still be alive after
convergence.

The isolated mutation runner in `test/mutation/` disables individual protocol
guards and repair steps only in copied checkouts. See
[`mutation/README.md`](mutation/README.md) for the command and artifact format.

## Typical test patterns

### Basic replication test

```elixir
peers = TestCluster.start_peers(2)
[{_, node_a}, {_, node_b}] = peers

TestCluster.start_group(node_a, name: :test, shards: 4)
TestCluster.start_group(node_b, name: :test, shards: 4)

# Register on node_a
TestCluster.spawn_register(node_a, :test, "user/1", %{name: "alice"})

# Wait for replication to node_b
TestCluster.assert_eventually(fn ->
  TestCluster.rpc!(node_b, Group, :lookup, [:test, "user/1"]) != nil
end)

TestCluster.stop_peers(peers)
```

### Partition and heal test

```elixir
peers = TestCluster.start_peers(3)
[{_, a}, {_, b}, {_, c}] = peers

# Start Group on all 3 with conflict resolver
opts = [
  name: :test,
  shards: 4,
  resolve_registry_conflict: {Group.TestConflictResolver, :resolve, []}
]
Enum.each([a, b, c], &TestCluster.start_group(&1, opts))

# Monitor nodedown so we know when partition takes effect
TestCluster.monitor_nodes_on(a, self())

# Partition: isolate c from a and b
TestCluster.disconnect_nodes(c, a)
TestCluster.disconnect_nodes(c, b)
assert_receive {:nodedown_on_remote, ^c}, 5000

# Register same key on both sides of partition
TestCluster.spawn_register(a, :test, "conflict", %{side: :left})
TestCluster.spawn_register(c, :test, "conflict", %{side: :right})

# Heal partition
TestCluster.reconnect_nodes(c, a)
TestCluster.reconnect_nodes(c, b)

# Wait for conflict resolution — one side wins
TestCluster.assert_eventually(fn ->
  result_a = TestCluster.rpc!(a, Group, :lookup, [:test, "conflict"])
  result_c = TestCluster.rpc!(c, Group, :lookup, [:test, "conflict"])
  result_a != nil and result_a == result_c
end, timeout: 10_000)

TestCluster.stop_peers(peers)
```
