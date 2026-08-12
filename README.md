# Group

Eventually Consistent distributed process registry, process groups,
lifecycle monitoring, and isolated subclusters for Elixir. No external dependencies.

## Features

- **Registry** — unique key-to-process mapping, cluster-wide. One process per
  key, enforced across all nodes.
- **Process groups** — many processes per key with join/leave. Discoverable via
  `members/2`.
- **Lifecycle monitoring** — pattern-based event subscriptions. Get notified
  when processes register, unregister, join, or leave anywhere in the cluster.
- **Named subclusters** — isolate registries and groups into named clusters
  where only connected nodes participate.
- **Sharded writes** — writes fan out across N GenServer shards to reduce
  contention. Reads go directly to ETS.
- **Nonblocking anti-entropy** — replica sends never wait on a remote socket;
  sequenced deltas, bounded oplogs, and exact snapshots repair dropped work.

## Installation

```elixir
def deps do
  [{:group, "~> 0.2.0"}]
end
```

## Quick Start

Start a Group instance under your supervision tree:

```elixir
children = [
  {Group, name: :my_app}
]
```

### Registry

```elixir
# Register the calling process under a unique key
:ok = Group.register(:my_app, "user/123", %{name: "Alice"})

# Look up by key — returns {pid, meta} or nil
{pid, %{name: "Alice"}} = Group.lookup(:my_app, "user/123")

# Unregister (also happens automatically on process death)
:ok = Group.unregister(:my_app, "user/123")
```

### Process Groups

```elixir
# Join a group (many processes can join the same key)
:ok = Group.join(:my_app, "chat/room/42", %{role: :member})

# List all members — returns [{pid, meta}, ...]
members = Group.members(:my_app, "chat/room/42")

# Read at most one arbitrary member without materializing the full group
[member] = Group.members(:my_app, "chat/room/42", limit: 1)

# Read only members whose owning process is on this node
local_members = Group.local_members(:my_app, "chat/room/42", limit: 10)

# Leave
:ok = Group.leave(:my_app, "chat/room/42")
```

`members/2` and `local_members/2` return joined processes for a key. Registered processes are not
included — use `lookup/2` for those.
Keys ending with `"/"` perform a prefix query across all shards:

```elixir
# All members in rooms under "chat/"
Group.members(:my_app, "chat/")
```

### Monitoring

Subscribe to lifecycle events matching a pattern:

```elixir
# Prefix match — all keys starting with "user/"
:ok = Group.monitor(:my_app, "user/")

# Exact match
:ok = Group.monitor(:my_app, "user/123")

# Everything
:ok = Group.monitor(:my_app, :all)
```

Events arrive as `{:group, events, info}` tuples in the monitoring process's mailbox:

```elixir
def handle_info({:group, events, _info}, state) do
  Enum.each(events, fn
    %Group.Event{type: :registered, key: key, pid: pid, meta: meta} ->
      # a process registered at `key`
      :ok
    %Group.Event{type: :unregistered, key: key, meta: meta, reason: reason} ->
      # a registered process died or unregistered
      :ok
    %Group.Event{type: :joined, key: key, pid: pid, meta: meta} ->
      # a process joined the group at `key`
      :ok
    %Group.Event{type: :left, key: key, pid: pid, meta: meta, reason: reason} ->
      # a process left or died
      :ok
  end)
  {:noreply, state}
end
```

Single operations (register, join) produce one event per tuple. Bulk operations
(nodedown, process death) batch all events from that operation into one tuple.

### Dispatch

Send a message to all members of a key:

```elixir
:ok = Group.dispatch(:my_app, "chat/room/42", {:new_message, "hello"})
:ok = Group.dispatch(:my_app, "chat/room/42", {:new_message, "hello"}, cluster: "servers_123")
```

Compared to `Phoenix.PubSub`, `dispatch` only broadcasts to nodes with at least
one subscription and can also be tailored to a given cluster.

### Named Clusters

Isolate groups and registries into named subclusters. Only nodes that have
called `connect/2` for a cluster participate in that cluster's replication.

```elixir
# Connect this node to a named cluster
:ok = Group.connect(:my_app, "game_servers_123")

# Or lease the connection while this node still has local interest in it
:ok = Group.connect(:my_app, "game_servers_123", ttl: 30_000)

# All operations accept a :cluster option
:ok = Group.join(:my_app, "room/1", %{}, cluster: "game_servers_123")
members = Group.members(:my_app, "room/1", cluster: "game_servers_123")
:ok = Group.monitor(:my_app, :all, cluster: "game_servers_123")
```

TTL leases are local policy only:

- `Group.connect(..., ttl: ms)` still does the normal ETS membership check first,
  so repeated connects while already connected stay a cheap noop and do not
  refresh the TTL.
- When a TTL expires, Group only disconnects that named cluster if the local
  node has no cluster-scoped monitors, no local registrations, and no local
  group memberships in that cluster.
- If local interest still exists, the next sweep extends the lease by one TTL
  interval and checks again later.

### Nodes

```elixir
# All Group peers (nodes that completed peer discovery), excluding self
Group.nodes(:my_app)

# All nodes in a named cluster
Group.nodes(:my_app, "game_servers_123")
```

### Runtime Log Level

Toggle verbose logging at runtime without restart:

```elixir
Group.log_level(:my_app, :verbose)  # turn on verbose
Group.log_level(:my_app, :info)     # back to normal
Group.log_level(:my_app, false)     # silence routine info/verbose logs
```

`Group.log_level/2` updates `:persistent_term`, so it should be used as an
occasional admin control, not from a hot path.

## Events

Events are delivered as `{:group, events, %{name: name}}` tuples containing
`%Group.Event{}` structs:

```elixir
%Group.Event{
  type: :registered | :unregistered | :joined | :left,
  supervisor: :my_app,
  cluster: nil | "cluster_name",
  key: "user/123",
  pid: #PID<0.150.0>,
  meta: %{},
  previous_meta: nil | %{},    # old meta on re-register/re-join
  reason: nil | term()          # exit reason on unregistered/left
}
```

| Event | Trigger |
|---|---|
| `:registered` | `register/4` — new or re-register (updates meta) |
| `:unregistered` | Process died or `unregister/3` called |
| `:joined` | `join/4` — new or re-join (updates meta) |
| `:left` | Process died or `leave/3` called |

Re-registering or re-joining an existing key updates the metadata in place and
delivers an event with `previous_meta` set to the old value.

## Consistency Model

All operations are **eventually consistent**:

- Writes (`register`, `join`, etc.) return immediately after updating local ETS.
- Changes replicate asynchronously over a configurable, nonblocking replica
  transport. Erlang distribution remains the membership/control plane.
- During network partitions, nodes may have divergent views.
- When connectivity returns, per-origin stream heads repair missing sequence
  ranges from a bounded oplog; a lag beyond the retained prefix falls back to
  an exact snapshot of that origin's shard/cluster slice.
- A dist-Erlang `nodedown` removes that node's view immediately. If the Erlang
  node remains connected but its Group instance stops responding, a bounded
  control-plane lease removes the same state and later discovery can rebuild it.
- Registry conflicts (same key registered on two nodes during a partition) can
  be resolved with a configurable `resolve_registry_conflict` callback. The
  callback selects a winner; each origin retires and terminates only its own
  losing process with `{:group_registry_conflict, key, winner_meta}`.

## Configuration

```elixir
{Group,
  name: :my_app,
  shards: 8,                                   # number of write shards (default)
  log: :info,                                  # :info | :verbose | false
  resolve_registry_conflict: {MyResolver, :resolve, []},  # partition conflict resolver
  extract_meta: {MyApp, :extract_meta, []},    # transform read/event metadata
  replicated_pg_receiver_buffer_size: 64,
  replicated_pg_receiver_flush_interval: 5,
  replicated_registry_receiver_buffer_size: 64,
  replicated_registry_receiver_flush_interval: 5,
  replicated_sender_buffer_size: 64,
  replicated_sender_flush_interval: 5,
  busy_dist_retry_attempts: 300,
  busy_dist_retry_interval: 1_000,
  replicated_pg_receiver_local_request_quota: 8,
  replica_transport: Group.Transport.DistErl,
  replicated_oplog_max_entries: 65_536,
  replicated_snapshot_chunk_target_bytes: 1_048_576,
  replicated_anti_entropy_interval: 1_000,
  replicated_peer_lease_timeout: 15_000
}
```

### Options

- **`name`** (required) — atom identifying this Group instance. Passed as the
  first argument to all API functions.
- **`shards`** — number of GenServer shards for write operations. Defaults to 8.
  Must match across all nodes.
- **`log`** — logging level. `:info` (default) logs peer discovery, node
  connects/disconnects, and cluster membership changes. `:verbose` additionally
  logs per-shard operations (register, join, leave, process deaths, replication).
  `false` disables routine info/verbose logs. Registry conflicts remain
  `Logger.error` events and busy distribution links remain `Logger.warning`
  events. The level can be changed at runtime with `Group.log_level/2`.
- **`resolve_registry_conflict`** — `{module, function, extra_args}` callback
  invoked as `apply(mod, fun, [name, key, {pid1, meta1, time1}, {pid2, meta2, time2} | extra_args])`.
  Called when partition healing or concurrent registration finds the same key
  registered on two nodes. Must return the winning pid (or neither pid to
  reject both). Group records an authoritative delete and terminates a losing
  owner only on that owner's local node. The callback runs synchronously inside
  the shard GenServer, so it must return quickly and never block.
- **`extract_meta`** — `{module, function, args}` or `fun(meta)` applied to
  metadata on reads and lifecycle events. Useful for stripping internal fields.
- **`replicated_pg_receiver_buffer_size`** — max buffered replicated PG
  join/leave ops per shard before the receiver flushes immediately. Defaults to 64.
- **`replicated_pg_receiver_flush_interval`** — max time in milliseconds a shard
  will buffer replicated PG join/leave ops before flushing. Defaults to 5.
- **`replicated_registry_receiver_buffer_size`** — max buffered replicated
  register/unregister operations per shard. Defaults to 64.
- **`replicated_registry_receiver_flush_interval`** — max registry receiver
  buffer age in milliseconds. Defaults to 5.
- **`replicated_sender_buffer_size`** — max buffered outbound operations per
  shard. Defaults to 64.
- **`replicated_sender_flush_interval`** — max outbound buffer age in
  milliseconds. Defaults to 5.
- **`busy_dist_retry_attempts`** — reconnect attempts after a non-suspending
  remote dispatch reports a busy dist link. Defaults to 300. Replica transport
  messages are simply dropped and repaired instead of forcing a disconnect.
- **`busy_dist_retry_interval`** — milliseconds between dispatch busy-link
  reconnect attempts. Defaults to 1,000.
- **`replicated_pg_receiver_local_request_quota`** — legacy-named quota for
  queued local shard requests drained per fairness turn while replica data or
  cluster controls are busy. Defaults to 8.
- **`replica_transport`** — a module implementing
  `Group.Transport`, or `{module, opts}`. The default
  `Group.Transport.DistErl` adapter uses `:erlang.send_nosuspend/3`; adapters
  must return promptly with `:ok`, `:busy`, or `:disconnected`. Dropped and busy
  messages are repaired by anti-entropy. Sideband implementations can use
  `Group.Transport.Outbox` to move bounded batching and socket work outside the
  Group shard.
- **`replicated_oplog_max_entries`** — maximum retained replica records per
  shard across all local streams. Defaults to 65,536. Pruning never waits for
  peer acknowledgements; a peer behind the retained floor receives an exact
  snapshot.
- **`replicated_snapshot_chunk_target_bytes`** — target maximum encoded size
  of each exact-snapshot message. Defaults to 1 MiB and applies above every
  transport, including dist Erlang. A single row larger than the target is
  sent alone. Receivers stage chunks in shard-owned private ETS and replace
  visible state only after the complete exact slice is present.
- **`replicated_anti_entropy_interval`** — interval in milliseconds for stream
  head advertisements and nonblocking control heartbeats. Defaults to 1,000.
- **`replicated_peer_lease_timeout`** — time without a dist-Erlang control
  heartbeat before state owned by that Group peer is purged. Defaults to 15,000
  and must exceed the anti-entropy interval. Probes continue after expiry so a
  Group restart on a still-connected VM recovers automatically.

## Architecture

```
Group.Supervisor (:"my_app_group_sup")
├── optional transport child  — sideband manager and per-shard outboxes
├── Group.Replica.Data        — owns ETS, journal, generations, and epochs
├── Group.PeerReconnect       — bounded recovery after busy remote dispatch
├── Group.Replica.Supervisor  — supervises N shard GenServers
│   ├── Group.Replica (shard 0)
│   ├── Group.Replica (shard 1)
│   └── ...
├── Registry                  — local monitor subscriptions (:"my_app_group_registry")
└── Group.ClusterLease        — local named-cluster TTL sweeper
```

### Sharding

Keys are routed to shards via `:erlang.phash2({cluster, key}, num_shards)`.
Including the cluster in the hash avoids false contention between the default
cluster and named clusters.

**Reads** (`lookup`, `members`) go directly to ETS — no GenServer hop. This is
the hot path and runs at millions of ops/sec.

**Writes** (`register`, `join`, etc.) go through the shard's GenServer, which
updates ETS and broadcasts replication messages. Multiple shards reduce write
contention for unrelated keys.

### ETS Tables

Each shard has materialized read indexes plus authority/recovery indexes:

| Table | Type | Key | Purpose |
|---|---|---|---|
| `reg_by_key` | `:set` | `{cluster, key}` | Registry lookup — O(1) |
| `reg_by_pid` | `:ordered_set` | `{pid, cluster, key}` | Reverse index for death cleanup |
| `reg_claim_by_key` | `:ordered_set` | `{cluster, key, origin, generation, epoch}` | One authoritative registry claim per origin |
| `reg_claim_by_pid` | `:ordered_set` | `{pid, cluster, key, origin, generation, epoch}` | Reverse claim index for owner death and repair |
| `pg_by_key` | `:ordered_set` | `{cluster, key, pid}` | Group membership lookup |
| `pg_by_pid` | `:ordered_set` | `{pid, cluster, key}` | Reverse index for death cleanup |
| `replica_stream_meta` | `:set` | `stream_id` | Local stream head, retained floor, and applied journal position |
| `replica_oplog` | `:ordered_set` | `{stream_id, sequence}` | Bounded sequenced mutation records |
| `replica_oplog_order` | `:ordered_set` | `append_id` | Shard-wide pruning order across streams |
| `replica_cursor` | `:set` | `stream_id` | Highest contiguous remote sequence applied |

Registry claim tables retain one authoritative claim per origin independently
of the visible winner. Stream metadata, oplog, append-order, and receive-cursor
tables support crash replay and gap repair. Keeping claims separate from the
single visible `reg_by_key` projection prevents a losing-but-still-live remote
claim from being forgotten before its owner emits an authoritative delete.

The node also has shared control/authority tables:

- `cluster_nodes` (`:bag`, cluster→nodes)
- `node_clusters` (`:bag`, node→clusters)
- `cluster_leases` (`:set`, cluster→`{ttl_ms, expires_at}`) for local
  `connect(..., ttl: ms)` policy
- `replication_meta` (`:set`) for the local generation, authority revisions,
  per-lane installed views, journal metadata, and one atomic append counter per
  shard
- `local_cluster_epochs` and `closed_local_cluster_epochs` (`:set`) for active
  and closing local named-cluster lifetimes
- `remote_cluster_epochs` (`:set`) for exact generation-fenced remote authority

`cluster_nodes` / `node_clusters` are the routing projection read by APIs and
replication fanout. Generation-fenced local/remote epoch tables are the
authority used to install that projection. `cluster_leases` is only local
policy metadata used by the sweeper.

`Group.Replica.Data` owns all tables and is supervised with `rest_for_one` so
tables survive shard crashes.

### Peer Discovery

When Group starts (or a new Erlang node connects), shards exchange
`peer_connect` / `peer_connect_ack` messages with their counterparts on other
nodes. This handshake:

1. Validates that shard counts match (raises on mismatch).
2. Exchanges cluster membership lists.
3. Shard 0 exchanges protocol version, origin generation, and one complete
   active named-cluster epoch snapshot per node. Matching data shards exchange
   only constant-size lane/transport descriptors tied to that authority
   revision.

Constant-size heartbeats renew the peer lease. If an origin generation or
cluster-epoch revision changes, the receiver requests a fresh authoritative
hello; if heartbeats stop, lease expiry purges that origin's complete local
view and discovery probes allow it to rejoin later.

Incremental cluster open/close controls are generation fenced, receiver
batched, and installed by shard 0 into one node-wide authority table. The
highest observed revision keeps heartbeats constant-size during a burst; after
the burst becomes quiet, one authoritative hello closes any gaps left by
dropped or reordered controls. Per-shard view rows record only constant-size
lane readiness; they do not copy the epoch map. Snapshot capture is serialized
with local epoch activation, so its revision and epoch rows are one coherent
point-in-time value. The highest observed incremental revision is tracked
separately and can never promote a partial view to exact authority. Discovery
hints never mutate membership on their own. Authority installation fans a
local fence to every lane, which sweeps only that lane's retained receive
streams. Shared authority may become visible before that fanout reaches a lane,
but the lane's constant-size view is not marked installed until its purge
finishes; data validation requires that marker. A heartbeat or lane hello can
confirm an installed view but cannot promote a pending one. Because PG rows
intentionally do not carry protocol epochs, a superseded origin/cluster slice
is cleared and its current cursor reset so the next head reconstructs it from
retained deltas or an exact snapshot.

Replica state itself does not travel on the control plane. Once the hello is
fenced, stream-head exchange on the replica transport catches the peer up.

### Replication

Every local mutation is first appended to a stream identified by
`{group, origin_node, origin_generation, shard, cluster, cluster_epoch}` and a
strictly increasing sequence number. It is then applied to the materialized
ETS view and batched into one delta message per target. Process-death registry
and PG removals can share one record and retain their one-event-batch behavior.

Receivers advance a cursor only across a contiguous sequence prefix. A gap
requests the missing suffix. Repeated head advertisements recover a dropped
tail even when no later write occurs. If the requested sequence is older than
the bounded oplog floor, the origin sends an exact snapshot of only its own
registry claims and PG memberships; absence from that snapshot is a delete.

There are no leaders, quorum acknowledgements, per-entry replicated tombstones,
or known-membership retention barriers. Oplog memory is bounded locally and
independently of slow peers. Deletes are normal ordered records while retained,
and exact snapshots close gaps after pruning. Exact snapshots are split into
transport-neutral byte-bounded messages; loss, duplication, or reordering leaves
the old visible slice and cursor untouched until all chunks arrive. Incomplete
staging expires after a peer-lease interval without progress and is destroyed
automatically with its owning shard. Named-cluster close uses only a temporary
local shard-completion barrier; the final shard removes it and all routing rows,
including after a caller timeout or shard restart. Reconnect waits for that
barrier so a prior close cannot erase newly accepted writes.

The sender flush timer is mainly a fallback for idle periods. The unified
outbound buffer also flushes immediately when it hits the configured size,
when a new enqueue finds the buffer already past its flush interval, and before
control or routing work such as cluster connect/disconnect or peer-protocol
handling.

Transport ordering is not required for correctness: each shard serializes
writes, each stream numbers them, and receivers reject gaps and duplicates.
Per-shard ordered delivery is still a useful fast path. Cross-stream order is
not a correctness dependency; cluster epochs reject data racing a disconnect
or reconnect, and generation fencing rejects data from a restarted origin.
An alternative sideband adapter passes incoming messages to
`Group.Transport.incoming/4` locally. Configure a custom adapter while
authority and membership remain on dist Erlang:

```elixir
replica_transport:
  {MyApp.GroupTransport,
   [outbox_batch_size: 64, outbox_batch_bytes: 1_048_576,
    outbox_flush_interval: 1, outbox_deadline: 100]}
```

The default `Group.Transport.DistErl` adapter sends directly to the remote shard and
does not pay for a local outbox. Sideband adapters can delegate `outgoing/5` to
`Group.Transport.Outbox.push/5` and supervise one outbox per shard
with `Group.Transport.Outbox.child_spec/1`. An outbox groups messages by
target and invokes the adapter's `send_batch/4` callback. Calls that expire or
return `:busy`/`:disconnected` are dropped without a local retry; the next
anti-entropy exchange repairs them.

A message-oriented backend fits this callback shape by obtaining a connection
once from `init_outbox/3`, then sending each `send_batch/4` result to a
registered incoming name on the target node. Queue pressure maps to `:busy` and
a missing session maps to `:disconnected`. The adapter passes its trusted peer
identity as the source node; Group verifies that stream origins and member pids
match that identity but does not authenticate the sideband connection itself.
Exact snapshots are already bounded by Group. A transport with a smaller
maximum frame may additionally segment an encoded batch, but it must completely
reassemble that batch before calling
`Group.Transport.incoming_batch/4`.

### Named Cluster TTL Leases

Named-cluster TTLs are a local way to reduce replication fanout to nodes that
no longer care about a cluster.

- `connect(..., ttl: ms)` writes a lease row only when the cluster is newly
  connected.
- A dedicated `Group.ClusterLease` process sweeps the local lease rows by
  nearest expiry.
- On expiry, the sweeper extends the lease if the local node still has
  cluster-scoped monitors, local registry entries, or local PG memberships in
  that cluster.
- Otherwise it runs the normal disconnect path, which removes the node from the
  named cluster and stops future replication for that cluster.

### Process Death Cleanup

Shards monitor only locally owned registered/joined processes. A node never
monitors or exits another node's member processes. On a local owner `DOWN`, the
shard:

1. Removes entries from both the primary and reverse-index ETS tables.
2. Appends authoritative unregister/leave mutations before deleting the rows,
   then sends one non-suspending sequenced delta batch per peer.
3. Fires `:unregistered` / `:left` events to local monitors.

### Peer Removal and Recovery

On `nodedown`, each shard purges all entries owned by the disconnected node
from its ETS tables, claims, cursors, and authority indexes and fires events for
each removed entry. If dist Erlang stays connected but a Group instance or its
control lane disappears, heartbeat lease expiry performs the same complete
purge. Discovery probes continue after expiry; a returning instance announces
a new or current generation and anti-entropy reconstructs its live state.

## Testing

```bash
mix test
mix test.soak   # nightly/release qualification
```

See [`test/README.md`](test/README.md) for the every-PR gate, shrinkable
StreamData lifecycle-model tests, bounded TLA+ models, and the nightly
three-node Jepsen transport/lifecycle campaign.

## Benchmarks

```bash
cd priv/bench

# Local (single-node)
./run_local.sh

# Distributed (3 separate BEAM VMs)
./run_distributed.sh
./run_distributed.sh --shards 4
```

See [`priv/bench/README.md`](priv/bench/README.md) for scenario descriptions.

## License

MIT
