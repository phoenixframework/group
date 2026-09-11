# Group Jepsen model test

This harness drives real Group instances in three independent BEAM VMs and
checks their quiescent state against a separate process-lifecycle oracle. It
tests failures outside the deterministic in-VM scheduler used by the
StreamData suite.

Each node has eight independent client drivers, so operations on different
owners can overlap on the same node. Owners can hold multiple registry and PG
entries in the root, `red`, and `blue` cluster epochs. During the bounded-fault
prefix Jepsen:

- registers, unregisters, joins, leaves, and kills real owner processes;
- closes and recreates named-cluster epochs while old traffic is stranded;
- creates a three-node registry conflict and requires every loser to die;
- partitions the full Erlang mesh, or only selected directed replica lanes;
- exercises isolation, all-way partition, and asymmetric one-way loss;
- resets transport sessions and kills/restarts complete BEAM nodes; and
- uses a 16-entry oplog and 1 KiB snapshot target so repair crosses pruning
  and multi-chunk exact-snapshot paths.

The replica lane is selectable without changing the workload or checker:

- `distribution` delegates to Group's production Erlang-distribution adapter;
- `tcp` uses Group's hidden test-only TCP adapter while Erlang distribution
  remains the control plane; healing a replica-port blackhole replaces each
  writer/socket so TCP retransmission backoff cannot outlive the bounded fault;
  and
- `chaos` is a local per-shard outbox which deterministically drops,
  duplicates, delays, and reorders replica messages.

After faults stop, every surviving node reconnects and the harness takes two
terminal snapshots. The independent checker requires:

- exact, identical public registry and PG views on every survivor;
- every live owner claim to be visible, and no dead owner token to remain;
- deterministic resolution of registry conflicts with no unexpected owner
  deaths;
- the expected peer set, including complete removal of a permanently retired
  node;
- consistent registry, PG, cluster, claim, cursor, oplog, and remote-authority
  indexes inside every shard;
- no staged partial snapshot and no retained data for a retired origin;
- coverage of delta batches, snapshot fallback, multi-chunk assembly, and
  registry conflict termination;
- two identical quiescent observations per node; and
- every acknowledged Group operation below the configured latency ceiling.

The oracle lives in owner processes outside Group's ETS and replica indexes.
Unexpected deaths and low-volume qualification evidence such as registry
conflict termination are also written to container-local logs which survive a
BEAM restart. A restart changes the boot component of new owner tokens, so a
stale generation, delayed delta, partial snapshot, or orphaned registry/PG row
cannot masquerade as a current owner. Every history explicitly restarts one
node after the deterministic conflict prelude, proving the checker does not
mistake restart-sensitive instrumentation for missing protocol coverage.

Registry-conflict exits are obligations, not trusted coverage counters. Before
calling Group, each owner journals its registration attempt independently of
Group's tables; it journals successful or rejected replies, unregisters, and
cluster-intent removal as well. Drivers retain the victim token, key, and
winner metadata from every conflict death. The checker reconstructs the
victim's registrations, requires the reported winner to rank strictly higher
by `{revision, token}`, and requires a matching historical winning claim in
the same cluster and key. Only validated deaths count toward coverage.

Registration calls interrupted by death or an indeterminate reply remain
possible claims: Group may have installed them before the owner could record
success. A definitive `:taken` reply excludes an attempt. Winning evidence is
retained after unregister, death, and BEAM restart because delayed replicas
can legitimately act on an older claim. Local journal order excludes winners
first attempted after a death; the oracle does not invent a global clock or
infer remote deletion delivery from wall time. This establishes independently
witnessed possible winners, not the exact instant a replica learned a claim.

The append-only conflict journal retains small operation records for the
bounded campaign, not production ETS rows. Its path defaults to
`/tmp/group-jepsen-conflict-evidence` inside each container and can be overridden
with the driver's `:conflict_evidence_path` option. Corrupt or unreadable
journals fail closed. At permanent retirement, the stopped-container collector
archives and decodes the journal alongside unexpected deaths. The checker
replays retired and surviving nodes' evidence together, without treating retired
owners as live. Conflict archives are bounded to 64 MiB with ten-second command
deadlines; missing or malformed archives fail the history.

Each new history resets the running recorder through the harness socket after
DB restart and before workload mutations. This clears disk and in-memory
evidence together and initializes an empty journal, so repeated histories
cannot inherit earlier conflict coverage.

Checker qualification also loads the real Elixir
Owner/Driver harness, injects valid and forged death reasons, exercises a
register interrupted before its reply, and checks the emitted EDN with the
Clojure lifecycle oracle.

## Requirements

- Docker with Compose v2
- Elixir/Mix with the repository dependencies installed (checker qualification)
- Java 21 or newer
- `curl`

The runner downloads Leiningen 2.12.0 into the ignored `.cache/` directory and
uses Jepsen 0.3.13. It installs nothing globally.

## Run

From the repository root:

```bash
test/jepsen/run.sh
```

The default is a 60-second mixed-lifecycle run over three nodes and six client
workers. Normal Jepsen options and Group-specific options can be supplied:

```bash
test/jepsen/run.sh test \
  --no-ssh \
  --nodes n1,n2,n3 \
  --concurrency 3n \
  --time-limit 300 \
  --test-count 10 \
  --key-count 32 \
  --owner-count 128 \
  --fault-interval 2 \
  --recovery-time 15 \
  --transport tcp \
  --scenario permanent \
  --max-operation-latency-ms 2000
```

`--transport` accepts `distribution`, `tcp`, or `chaos`. `--scenario mixed`
restarts every transiently killed node; `--scenario permanent` finally retires
`n1` and proves that `n2` and `n3` converge after purging all of its public and
internal state.

Run the sustained matrix over all transport and lifecycle combinations with:

```bash
test/jepsen/campaign.sh
```

Its defaults are 20 five-minute runs for each of six combinations. The
`GROUP_JEPSEN_CAMPAIGN_*` environment variables in the script control duration,
count, concurrency, keys, owners, and recovery time.

Run mutation qualification plus live positive- and negative-checker tests:

```bash
test/jepsen/qualify.sh
```

This runs every mutation defined by `test/mutation/run.exs`, then verifies that
a healthy live history is accepted and deliberately injected owner-death,
internal-index, stranded snapshot-cursor, registry claim/projection, and
unavailable-terminal-node corruption are rejected. Terminal reconnect and
snapshot retries are paced and bounded by the configured recovery time, so a
node that cannot recover yields a finite invalid history instead of an
unbounded qualification run.

Results and histories are written below `test/jepsen/store/`. Containers are
removed after a run. Set `GROUP_JEPSEN_KEEP_CONTAINERS=1` to retain them and
inspect logs with:

```bash
docker compose -f test/jepsen/docker-compose.yml logs
```

The pure checker qualification tests do not require Docker:

```bash
test/jepsen/checker.sh
```

At the repository root, `mix test` runs this pure checker after the complete
ExUnit, StreamData, and deterministic-chaos suite. `mix test.soak` runs that
same PR gate, the complete mutation/live-checker qualification, and then
`campaign.sh`. Chaos/mixed uses a sender/repair buffer of 32 and requires
evidence that one repaired delta run contained at least two records; all other
profiles keep the single-record stress setting.

## Scope

This checker verifies Group's eventual lifecycle contract, not
linearizability. While communication is unavailable, each side may serve its
local view and accept new owners. The requirement begins after the explicitly
bounded fault prefix: surviving peers must then always resolve to the exact
same lifecycle view, while a peer which never returns must be completely
evicted after its lease expires.

The formal models prove the protocol for finite state spaces; StreamData
generates and shrinks scheduler-controlled histories inside real Group nodes;
this harness tests OS sockets, independent VMs, VM death, and real transport
adapters. None of these layers alone is treated as a proof of the others.
