# Replica mutation campaign

This campaign calibrates the anti-entropy tests against deliberate failures of
catastrophic protocol obligations. It covers generation and epoch fencing,
contiguous sequence application, exact registry and PG snapshots, below-floor
repair, process-down sequencing, conflict-loser retirement, authority fanout,
per-lane authority installation, periodic head advertisement, interrupted
journal/index repair, and named-cluster close completion. Snapshot calibration
also covers incomplete commit, conflicting retransmission rows, newer-snapshot
supersession, stale-authority fencing, and staging expiry.

The runner first verifies every unmodified regression target. It then copies
the current checkout once per mutant, changes only that copy, recompiles it,
and runs the designated real multi-node regression. A compiling mutant is
`killed` only when the regression fails. Any surviving or non-compiling mutant
makes the campaign fail.

```bash
mix run --no-start test/mutation/run.exs

# List or run selected mutations
mix run --no-start test/mutation/run.exs --list
mix run --no-start test/mutation/run.exs disable_below_floor_snapshot
```

Artifacts and complete logs are written below `tmp/mutation/`.
