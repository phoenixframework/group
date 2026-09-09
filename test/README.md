# Group Test Infrastructure

## Running tests

```bash
mix test                           # all tests
mix test test/group_test.exs       # local only
mix test test/group_property_test.exs # local model properties
mix test test/replication_property_test.exs # receiver protocol properties
mix test test/distributed_test.exs # distributed only
```

## Test files

| File | What it tests |
|------|---------------|
| `group_test.exs` | Single-node: register/unregister, join/leave, members, monitor/demonitor, named clusters, concurrent operations |
| `group_property_test.exs` | StreamData local command histories checked against an independent map/set model |
| `replication_property_test.exs` | Generated receiver batch boundaries, buffered/snapshot cluster isolation, and local-write fairness |
| `distributed_test.exs` | Multi-node: replication, peer discovery, node disconnect cleanup, partition healing, conflict resolution, event ordering, rolling restarts |

## Local model properties

`group_property_test.exs` runs 100 generated histories of up to 60 commands,
with 1, 2, or 4 shards. Three symbolic actor IDs, five shared keys, small metadata
values, and two named clusters keep collisions, repeated operations, and updates
likely. Commands cover register/re-register, unregister (including another local
owner's registration), join/re-join, leave, owner death, connect/disconnect, and
exact/prefix/all monitor subscriptions. A killed actor is replaced with a fresh
process in the same symbolic slot so later commands remain executable.

After **every** command, the suite compares:

- Return values, including errors and rejected writes to disconnected clusters.
- All local entries against an independent map of registry owners and PG members.
- Lookup, exact/prefix members, local members, and registry/member counts in every
  cluster, including clusters the node has left.
- Monitor events, including metadata, previous metadata, reasons, duplicate
  delivery, and overlapping subscriptions.
- ETS dual-index consistency using the existing `TestCluster` checker.

Reads are assertions after every command rather than generated read commands.
Death cleanup explicitly waits for each shard to release the owner's monitor.
Mailbox barriers then settle event delivery to a dedicated observer. Events are
compared as multisets within a command because cleanup across shards has no
global ordering guarantee; successive commands are checked separately.

The Group supervisor, owners, observer, subscriptions, ETS tables, and model are
fresh for **each generated example and each shrink attempt**. Teardown runs in
`after`, and a fixed Group name avoids allocating atoms per generated example.
A hand-written history also exercises model branches that random short histories
can miss.

To reproduce a failure, use the seed printed by ExUnit:

```bash
mix test test/group_property_test.exs --seed 12345
```

StreamData reports the shrunk command sequence. Promote discovered failures to
focused regression tests. A seed reproduces generation, not BEAM scheduling.
The local model deliberately settles between commands. Receiver buffering and
fairness are covered separately below, without growing the command model into a
scheduler or transport simulator.
If the model grows into a substantial state-machine framework, evaluate
PropCheck/PropEr rather than implementing that framework here.

## Targeted protocol properties

`replication_property_test.exs` exercises both registry and PG receiver lanes:

- **Batch boundaries:** generate ordered writes, updates, and removals over hot
  keys, then partition each history into singleton, buffer-minus-one, buffer,
  buffer-plus-one, and whole-history messages. Each partition gets a fresh Group
  instance and is checked against a map-based oracle for contents and per-key
  event order, including metadata and removal reasons. Runs 100 examples per lane.
- **Cluster isolation:** prove writes are pending in receiver buffers before
  disconnecting. Neither flushing those buffers nor replaying batches/snapshots
  may repopulate the disconnected cluster or emit transient apply-then-purge
  events. Reconnect cycles must accept fresh snapshots while preserving entries
  in the default and another named cluster. Runs 50 examples per lane.
- **Fairness:** suspend one shard, enqueue a finite receiver backlog, and wait
  until a public local register/join request is also queued. On resume, the local
  event must appear at the first receiver flush, before the backlog finishes.
  The assertion uses the shard's event timeline rather than a wall-clock
  latency threshold or queue-length observation. Runs 50 examples per lane.

These are controlled receiver-protocol scenarios using real local owner PIDs;
they do not traverse Erlang distribution or exercise sender batching. Registry
keys retain one owner, leaving owner-conflict resolution to separate scenarios.
The distributed suite remains necessary for real peer lifecycle and transport
behavior. These properties do not claim reproducible network races or fairness
under an infinite stream of control messages.

`Group.PropertyFixture` owns the Group supervisor, owners, and a mailbox-preserving
observer per example/shrink attempt. It tears them down in `after`, stops
subscribers before Registry, and uses observer-addressed mailbox barriers for
event delivery. Fixed names avoid atom growth during shrinking.

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

The resolver uses "most recent wins" — keeps the registration with the higher
timestamp.

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
