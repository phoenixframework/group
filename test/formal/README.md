# Group anti-entropy formal model

`GroupAntiEntropy.tla` is an independent finite-state model of the replica
contract. It covers:

- generation and named-cluster epoch fencing;
- arbitrary finite frame loss, duplication, and reordering;
- contiguous sequence application;
- bounded oplog pruning;
- exact per-origin snapshot fallback; and
- fair convergence after healing.

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
```

`TLC_WORKERS` controls worker concurrency and defaults to 4. `TLA_CONFIG` can
point at an alternate finite configuration.

TLC proves the listed invariants and liveness property for the configured
finite instance, not for arbitrary unbounded node and key sets. Larger models
should be run periodically by increasing `Nodes`, `Origins`, `Keys`, `MaxSeq`,
`OplogBound`, and `MaxMessages`.

The checked three-node default explores 1,835,826 states, finds 490,236
distinct states to a depth of 30, and completes in roughly 1 minute 40 seconds
on the development machine used for the validation run.
