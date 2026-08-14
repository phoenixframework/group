# Group Benchmarks

Baseline performance numbers for Group's core operations — local ETS reads,
GenServer writes with shard scaling, and distributed replication across
separate BEAM VMs.

## Running

```bash
cd priv/bench
mix deps.get
```

### Local benchmarks

Single-node, no distribution required:

```bash
./run_local.sh
```

### Distributed benchmarks

Uses 3 separate BEAM VMs (coordinator + 2 replicas) as OS processes:

```bash
./run_distributed.sh
```

The script compiles once, starts both replicas in the background, then launches
the coordinator. Replicas are killed automatically on exit.

To isolate the 10,000-cluster lifecycle scenario:

```bash
./run_distributed.sh --shards 4 \
  --coordinator-expr 'GroupBench.Distributed.run_many_clusters_only(shards: 4)'
```

To measure the exact-snapshot fallback independently (one shard models one
busy lane of a much larger sharded deployment):

```bash
./run_distributed.sh --shards 1 \
  --coordinator-expr 'GroupBench.Distributed.run_snapshot_sync_only(shards: 1, entries: 50000)'
```

To exercise one million rows across the recommended 32-shard scale point,
including exact catch-up, hottest-shard restart repair, memory, and permanent
peer eviction:

```bash
./run_distributed.sh --shards 32 \
  --coordinator-expr 'GroupBench.Distributed.run_scale_recovery_only(shards: 32, entries: 1000000, mode: :registry)'

./run_distributed.sh --shards 32 \
  --coordinator-expr 'GroupBench.Distributed.run_scale_recovery_only(shards: 32, entries: 1000000, mode: :pg_hotspot)'
```

`:registry` spreads a million exact claims across all shards.
`:pg_hotspot` deliberately puts a million memberships on one key/shard, which
is the worst case for snapshot installation and restart rebuilding.

## Local Scenarios

All local benchmarks run for both the default (nil) cluster and a named cluster
(`"game"`) to verify there's no performance difference between the two paths.
Each spawned process cohort is stopped after its measurement and before the
next case; cohort teardown is outside the measured interval.

### 1. Lookup throughput

Pure ETS read — the hot path for Group. Registers 10K keys, then measures 100K
random `Group.lookup/3` calls.

Reports ops/sec and p50/p99/max latency.

### 2. Members throughput

ETS read that returns a list. Joins 100 processes to each of 100 groups (10K
memberships total), then measures 100K random `Group.members/3` calls.

Slower than lookup because each call copies a 100-element list out of ETS.

### 3. Register throughput (shard scaling)

Measures concurrent `Group.register/4` calls — each of 10K spawned processes
registers itself in parallel. Uses the library default of 8 shards for the
non-scaling scenarios and a fixed 1, 2, 4, 8, 16, 32, 64 shard sweep. The
fixed sweep keeps results comparable across machines and avoids treating BEAM
scheduler count as a shard-count recommendation.

### 4. Register/unregister cycle

Sequential register + unregister pairs from a single process. Measures the
GenServer round-trip cost of two writes back-to-back (10K cycles).

Reports per-cycle latency percentiles.

### 5. Join throughput (shard scaling)

Same shape as register throughput but with `Group.join/4`. 10K processes each
join a group concurrently, varying shard count.

### 6. Monitor event delivery

Calls `Group.monitor(:bench, :all)`, then registers 5K keys and measures the
time until all 5K `:registered` events are received by the monitoring process.

## Distributed Scenarios

Three separate BEAM VMs on 127.0.0.1 — each with its own schedulers, memory
allocator, and GC. The coordinator drives all operations via `:erpc.call` with
MFA (module/function/args) to the replica nodes.

### 1. Replication latency

The core distributed measurement. Registers a key on replica1, then spin-polls
`Group.lookup` on replica2 until it appears. Repeats 1,000 times.

Reports p50/p99/max latency covering the full path: GenServer call on replica1,
write-ahead append, nonblocking replica transport, receiver application, and
ETS projection on replica2.

### 2. Bulk sync (new peer catches up)

Measures how fast a new node catches up to an existing peer's state. Registers
N keys on replica1 (1K and 10K), then starts Group on replica2 and polls until
all N entries are visible.

Group advertises stream heads on peer discovery. A new peer requests the
missing range; if the bounded oplog no longer contains the prefix, Group sends
an exact per-origin snapshot. The measurement therefore covers the normal
catch-up decision as well as serialization and network transfer.

### 3. Concurrent cross-node writes

Both replicas register 5K keys simultaneously (10K total), then waits for full
convergence — both nodes see all 10K entries.

Measures total throughput including the time for all replication messages to
settle.

### 4. Named cluster replication latency

Same as scenario 1 (replication latency) but within a named cluster
(`"game"`). Both replicas call `Group.connect/2` before measuring.

Useful for verifying that the named cluster replication path has no overhead
compared to the default nil cluster.

### 5. Process death cleanup replication

The critical distributed cleanup path. Registers 1K and 5K processes on
replica1, kills them all, then measures how long until replica2 sees zero
entries. Exercises: local DOWN handler → authoritative sequenced unregister
records → nonblocking delta batch → remote ETS cleanup.

This scenario catches O(N²) message amplification bugs where remote nodes
redundantly monitor pids and re-broadcast cleanup messages.

### 6. Register/die churn throughput

Sustained churn: 10 waves of 500 register+kill cycles on replica1, measuring
total wall time including convergence on replica2. Simulates steady-state
deploy churn where processes are constantly starting and stopping.

### 7. Join/die cleanup replication

Same as scenario 5 but for process groups. Spawns 1K processes on replica1,
all joining the same group key, then kills them all. Measures cleanup
convergence on replica2 via the `replicate_leave` path.

All members hash to the same shard (single key), making this the worst case
for shard contention during bulk cleanup.

### 8. Many-cluster lifecycle

Connects 10K named clusters, registers one process in each, forces peer
re-discovery, then disconnects and verifies cleanup. This exposes control-plane
message amplification and epoch-fence costs.

The connect phase reports local `Group.connect/2` completion separately from
full remote control convergence. Full convergence checks the reverse cluster
index on both nodes for all 10K named clusters plus the default cluster; seeing
only the last submitted cluster is insufficient because controls may be
reordered or repaired asynchronously. On generation/epoch-aware builds the
barrier also requires every replica shard to hold the source's current control
revision and all 10K remote epoch rows. Registration starts only after this
barrier, so its result does not inherit unfinished connect work.

Registration reports local completion, the remote count at that handoff, and
the remaining data-convergence tail separately. Re-discovery similarly splits
restart, local reconnect, control convergence, and data convergence; disconnect
splits local completion from remote cleanup.

### 9. Busy application convergence

Runs registry and PG churn across 50 clusters and 40K initial pids. Reports
application throughput and then verifies the replicas agree exactly. A fast
wall-clock result is not considered successful unless convergence completes.

### 10. Local writes under replicated PG pressure

Floods one receiver shard with remote membership updates while measuring local
register and join calls at increasing concurrency. This checks that bounded
replica turns preserve local control-plane progress.

## Architecture

```
priv/bench/
├── mix.exs                          # depends on :group via path: "../../"
├── run_distributed.sh               # starts 3 VMs, cleans up on exit
├── README.md
├── lib/
│   ├── group_bench.ex               # CLI entry — dispatches local/distributed
│   ├── group_bench/
│   │   ├── local.ex                 # 6 local benchmarks
│   │   ├── distributed.ex           # coordinator: connects + drives 10 benchmarks
│   │   ├── replica.ex               # helpers called by coordinator via :erpc
│   │   └── helpers.ex               # timing, formatting, percentile math
```

The bench suite is a standalone Mix project that depends on `:group`. Protocol
consolidation is enabled for realistic production performance. Distributed
benchmarks use separate OS processes (not `:peer`) so each node gets its own
scheduler pool and memory allocator.
