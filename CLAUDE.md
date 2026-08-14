# Group — Maintainer Architecture Guide

## What Group Is

Group is an eventually consistent distributed process registry, process-group
service, lifecycle monitor, and named-subcluster layer. Erlang distribution is
the membership and authority control plane. Replica state moves over a
configurable, nonblocking data transport and converges through sequenced
anti-entropy streams.

## Project Structure

```
lib/
  group.ex                         — public API and configuration docs
  group/supervisor.ex              — rest_for_one instance supervisor
  group/cluster_lease.ex           — local named-cluster TTL policy
  group/peer_reconnect.ex          — bounded retry for busy dispatch links
  group/replica.ex                 — sharded writes, control, AE, projection
  group/replica/data.ex            — ETS owner, journal, authority, indexes
  group/replica/wire_protocol.ex   — wire version, stream identity, mutations
  group/replica/snapshot.ex        — byte-targeted snapshot chunks/staging
  group/transport.ex                — replica transport contract
  group/transport/dist_erl.ex       — default dist-Erlang adapter
  group/transport/outbox.ex         — optional lossy sideband outboxes
test/
  support/test_tcp_transport.ex     — test-only independent-socket adapter
  replica_model_property_test.exs  — shrinkable real-node lifecycle model
  replica_adversarial_test.exs     — seeded three-node transport chaos
  replica_snapshot_*               — chunk/assembly failure coverage
  formal/                          — TLC protocol, assembly, eviction models
  jepsen/                          — independent-VM lifecycle oracle/campaign
priv/bench/                        — local and distributed benchmark project
```

## Required Gates

```bash
mix test              # every-PR ExUnit/property/chaos/pure-checker gate
mix test.soak         # nightly/release six-profile Jepsen campaign

# Focused development
mix test test/group_test.exs
mix test test/distributed_test.exs
mix test test/replica_model_property_test.exs
```

`mix test` accepts normal Mix test paths/options and then runs the pure Jepsen
checker qualification. `mix test.soak` runs that gate first, then twenty
five-minute histories for distribution/TCP/chaos × mixed/permanent scenarios.
See `test/README.md`, `test/formal/README.md`, and `test/jepsen/README.md`.

## Supervision and Ownership

```
Group.Supervisor (rest_for_one)
├── optional sideband transport manager + per-shard outboxes
├── Group.Replica.Data
├── Group.PeerReconnect
├── Group.Replica.Supervisor
│   ├── Group.Replica shard 0
│   ├── Group.Replica shard 1
│   └── ...
├── Registry                    — local monitor subscriptions
└── Group.ClusterLease          — local named-cluster TTL sweeper
```

`Group.Replica.Data` owns every ETS table. A shard restart therefore preserves
the tables, repairs interrupted index/journal work, replays appended-but-not-
applied local mutations, and rebuilds monitors for locally owned processes.
Restart repair streams each primary materialized table once while rebuilding
its existing reverse index; it does not allocate a whole-table list or retain
additional per-origin indexes.
The optional transport precedes Data so losing its manager restarts the whole
instance and cannot leave stale transport sessions attached to retained state.

Reads use public ETS directly. Writes route through
`:erlang.phash2({cluster, key}, num_shards)` to a local shard request lane.
Shard counts must match across peers.

## ETS State

Per shard:

| Table | Key | Role |
|---|---|---|
| `reg_by_key` | `{cluster, key}` | visible registry winner |
| `reg_by_pid` | `{pid, cluster, key}` | visible reverse index |
| `reg_claim_by_key` | `{cluster, key, origin, generation, epoch}` | authoritative claims |
| `reg_claim_by_pid` | `{pid, cluster, key, origin, generation, epoch}` | claim reverse index |
| `pg_by_key` | `{cluster, key, pid}` | visible PG membership |
| `pg_by_pid` | `{pid, cluster, key}` | PG reverse index |
| `replica_stream_meta` | stream id | head, retained floor, journal position |
| `replica_oplog` | `{stream, sequence}` | retained mutation record |
| `replica_oplog_order` | append id | shard-wide pruning order |
| `replica_cursor` | stream id | highest contiguous sequence applied |

Shared tables hold cluster/node indexes, local TTL leases, local and remote
cluster epochs, closed-cluster barriers, origin generations, exact/observed
authority revisions, installed lane views, and journal metadata.

Per-shard tables omit `write_concurrency` because one shard serializes their
writes. The shared replication metadata table uses
`write_concurrency: :auto`: every shard atomically updates only its own
`{:append_counter, shard}` object. Cross-shard arrival order has no semantic
meaning; an append id exists only to bound that shard's oplog across streams.

## Authority and Stream Identity

Every mutation belongs to:

```
{group, origin_node, origin_generation, shard, cluster, cluster_epoch}
```

The origin appends a strictly increasing sequence before applying the
materialized change. Generation fences a restarted Group instance. A
named-cluster epoch fences close/reopen. The nil cluster uses the origin
generation as its epoch.

Shard 0 installs one exact node-wide authority snapshot:

- origin generation;
- complete active cluster→epoch set;
- exact authority revision; and
- opaque transport descriptor.

The exact authority and its shared-cluster forward/reverse indexes are one
serialized Data mutation. A concurrent local connect is ordered wholly before
or after that mutation, so exact authority cannot exist without the matching
replica route indefinitely.
Local activation projects its epoch, self route, and all already-exact remote
routes at that same boundary. Local deactivation removes admission and queues
old-epoch cleanup to every shard before replying; callers and timed-out aliases
are not lifecycle coordinators.

Other shards exchange constant-size lane hellos. Shared authority is not enough
to accept data: each shard records an installed lane view only after it has
purged streams outside that authority. Exact and incrementally observed
revisions are distinct from the complete applied revision. A persisted
`{generation, revision}` hint atomically fences every lane when any heartbeat
observes newer authority. Incremental controls compare-and-install their
expected revision against the applied/observed/hinted state in one Data turn,
so a raced heartbeat cannot publish partial authority. Hints refine only a
peer with prior exact authority; after retirement, delayed heartbeats or lane
hellos cannot recreate authority, a route, or a lease. Only the exact
dist-Erlang hello reintroduces the peer. If a lane hello precedes exact
authority during discovery, authority fanout immediately re-probes that lane;
the rejected hello is never retained as a route.

`peer_connect` and `peer_connect_ack` are discovery hints, not authority.
Unsequenced legacy state messages are ignored; all replica recovery uses
heads, contiguous deltas, or exact snapshots.

## Anti-Entropy

Replica data messages are:

- `heads`: stream, retained floor, and head;
- `delta_batch`: one or more contiguous stream runs;
- `need`: the receiver's next missing sequence; and
- `snapshot_chunk`: one byte-targeted part of an exact origin slice.

A receiver advances its cursor only through a contiguous prefix. Duplicates are
idempotent; gaps request the missing suffix. Periodic heads recover a dropped
tail even if no later write occurs. If the requested sequence is below the
bounded oplog floor, the origin sends an exact snapshot containing only its own
claims and memberships for that shard/cluster stream. Absence is deletion.

The oplog is bounded per shard and never waits for acknowledgements. There are
no leaders, quorums, replicated per-entry tombstones, known-member lists, or
retention barriers. A slow peer cannot pin memory.

Snapshots are transport-neutral and byte-targeted (1 MiB by default). A single
oversized row is one chunk. Multi-chunk receivers stage rows in shard-owned
private ETS and preserve the old visible slice/cursor until the complete,
authority-valid manifest is present. Loss, duplication, reordering,
supersession, stale authority, expiry, and shard crash must leave no partial
visible state. Staging expires after one peer-lease interval without progress.
At most one sender worker per shard captures rows off the control process and
sends only if the stream identity and fully-applied head remain unchanged after
the scan; otherwise anti-entropy retries. Sender capture and receiver staging
retain completed byte-bounded chunks in unnamed private ETS tables owned by
their worker/shard. Only one chunk is assembled or copied on a process heap at
a time. These tables are ephemeral: explicit completion/retry cleanup deletes
them, and owner death deletes them automatically. Monitor events produced by a
large exact install are staged in bounded private-ETS batches until the cursor
commits.

## Nonblocking Transport

All cross-node Group control sends use
`:erlang.send_nosuspend(..., [:noconnect])`. The default distribution replica
adapter sends directly the same way and adds no local hop. `:busy` and
`:disconnected` mean “drop this message”; periodic anti-entropy repairs it.

A sideband adapter may use one local `Group.Transport.Outbox` per shard.
Outboxes batch by peer, bound admitted messages, impose deadlines, and run
bounded socket work outside Group shards. Queue overflow, expiry, or socket
backpressure drops the batch.
The test suite's hidden TCP adapter exercises a genuinely independent socket
lane; it is validation infrastructure, not a supported production transport.

Transport ordering is not required for correctness. Per-shard ordered delivery
is a fast path; stream sequences reject duplicate/out-of-order data, and
generation/epoch/lane fences handle control/data reordering. A transport must
reassemble any transport segmentation before `incoming_batch/4`.

## Registry Projection and Process Ownership

Registry claims remain authoritative per origin even when hidden by another
origin's visible winner. The configured callback deterministically ranks each
claim independently; Group selects the maximum `{rank, pid}` so the result is
associative and commutative. Group owns lifecycle effects: each losing origin
appends its own authoritative unregister and exits only its local process with
`{:group_registry_conflict, key, winner_meta}`.

A node never monitors or exits another node's member processes. Local shards
monitor locally owned registration/PG pids. Local `DOWN` appends ordered
unregister/leave mutations before deletion and replication. Remote owner death
arrives through those records; `nodedown` or lease expiry is the fallback for
an origin that cannot emit them.

## Peer and Cluster Lifecycle

- Dist-Erlang `nodedown` immediately purges that node's visible rows, claims,
  cursors, authority, cluster routing, and sideband session on every shard.
- Constant-size control heartbeats cover the case where the Erlang node remains
  connected but its Group instance disappears. Peer-lease expiry performs the
  same complete purge.
- After a receiver shard restart, retained authority/view metadata reconstructs
  leases for origins whose ETS rows survived, so a permanently disappeared
  Group is still evicted without scanning registry or PG data.
- Each lane deletes its own persisted view only after purging its rows and
  cursors; shard 0 never erases a sibling's restart breadcrumb. If shard 0
  restarts with `observed_revision != exact_revision`, it also reconstructs the
  pending exact-hello repair obligation. Persisted hints reconstruct the same
  bounded lease if a lane crashes after fencing authority but before updating
  its in-memory deadline.
- Discovery probes continue after expiry. A returning current/new generation
  is fenced, installs authority per lane, and reconstructs through deltas or an
  exact snapshot. Delayed cleanup rechecks shared authority before deleting
  routes, and a lane hello is not reported `peer_up` until authority admits it.
- Incremental named-cluster open/close controls are generation fenced and
  batched, but shard 0 is their only node-wide authority writer; controls
  arriving on other lanes are forwarded locally. Revisions must be contiguous.
  A gap fences every lane and requests an exact hello instead of applying a
  partial authority set.
- Local cluster close uses a temporary all-shard completion barrier. The last
  shard removes routing/epoch rows; restart repair completes abandoned closes,
  and reconnect waits so an old close cannot erase new writes.

TTL leases are local policy only. On expiry, Group disconnects a named cluster
only when no local registrations, PG memberships, or cluster monitors remain.

## Batching and Fairness

Registry and PG mutations share one outbound sender buffer so local mailbox
order is retained. It flushes on size, age, idle timer, and before control or
routing barriers. Receivers apply contiguous runs in bulk and emit lifecycle
events in operation batches. After replicated work, each shard takes a bounded
FIFO local-request turn to prevent replica pressure from starving callers.

## Distributed Test Rules

- Use three peers for partition/recovery tests. Two nodes cannot cover an
  independent survivor while an origin and receiver disagree.
- Remote helpers must be compiled under `test/support/`; call them with MFA
  through `:erpc`.
- Unlink supervisors started inside RPC helpers.
- `flush_shards/2` is only a mailbox/barrier aid; convergence assertions must
  still wait for AE and inspect exact public/internal state.
- Always assert dead owners are absent, retained owners are alive, claims and
  projections agree, cursors are contiguous, no partial snapshot remains, and
  retired origins have no rows.

## Critical Invariants

1. A cursor never advances across a gap or before a full exact snapshot commits.
2. Exact snapshots replace one origin slice; they are never additive merges.
3. Authority requires generation, complete applied epoch revision, a matching
   persisted observation hint, and installed lane readiness. Observed
   heartbeats alone are not authority; the last exact snapshot revision remains
   separate while a contiguous incremental update is applied. Exact
   authority and its shared-cluster routing projection are installed atomically.
   Local activation is projected atomically; deactivation cleanup is durable
   before its initiating caller can disappear.
4. A stale generation, epoch, lane, shard, transitive pid, or stream whose
   origin differs from the transport-reported source is rejected before
   applying replica data. Group trusts the adapter's source identity;
   authenticating a sideband peer belongs to that transport.
5. Registry claims are retained per origin until that origin deletes them or is
   retired; the visible winner is reconstructible from remaining claims. A
   remote winner is serially revalidated against shared and lane authority
   immediately before any local losing owner is retired. Keys reconciled while
   a retained claim is fenced by an authority gap are reprojected when that
   lane installs the exact view; a current cursor must not strand an older
   visible winner.
6. Only an owner node monitors, retires, or exits its member processes.
7. Oplog pruning is local and bounded; lagging peers use exact snapshot repair.
8. `nodedown` and peer-lease expiry purge every public and internal reference
   to the retired origin. A lane retains its persisted restart breadcrumb until
   its own rows/cursors are gone. Remote shard death cannot leave state
   permanently; lease expiry or fenced rediscovery completes cleanup/recovery.
9. Snapshot staging is private, all-or-nothing, authority fenced, and expiring.
10. Local append/journal repair makes an appended mutation either replayable or
    durably applied after a shard crash.
11. Cross-node control and replica calls never block a Group shard.
12. Cluster close completion survives caller timeout and shard restart.

## Logging

`log: :info | :verbose | false` controls routine Group logs and can be changed
with `Group.log_level/2`. Registry conflicts remain errors and busy dispatch
links remain warnings regardless of the routine level.
