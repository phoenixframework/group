# Replica mutation campaign

This campaign calibrates the anti-entropy tests against deliberate failures of
catastrophic protocol obligations. It covers generation and epoch fencing,
contiguous sequence application, exact registry and PG snapshots, below-floor
repair, process-down sequencing, conflict-loser retirement, authority fanout,
per-lane authority installation, periodic head advertisement, interrupted
journal/index repair, and named-cluster close completion. Snapshot calibration
also covers incomplete commit, conflicting retransmission rows, newer-snapshot
supersession, stale-authority fencing, and staging expiry.
Restart calibration covers per-lane eviction breadcrumbs and partially observed
authority, while wire calibration rejects wrong-shard rows and unsequenced
cluster lifecycle messages. The 64-mutant campaign also independently removes
the generation and epoch fences, races authority changes against local-owner
retirement, skips conflict reprojection after exact authority returns, bypasses
shard-zero authority serialization, separates exact authority from its shared
cluster projection, interrupts local cluster activation/deactivation before
shard notification, deletes terminal close/peer breadcrumbs before their route
cleanup, erases a suspended lane's nodedown breadcrumb, retains a dead peer's
authority-repair obligation, lets stale restart or duplicate-close cleanup
erase reactivated epochs, accepts the wrong epoch's close acknowledgement,
allows newer heartbeats or lane hellos to leave old lane views unfenced, retains
cursorless claims, admits incremental authority across a revision race or gap,
lets an unknown post-retirement hint recreate authority or an unleased lane
route, delays a pre-authority lane's rediscovery until the periodic probe, and
disables bounded snapshot-send/receive progress.

The runner first verifies every unmodified regression target. It then copies
the current checkout once per mutant, changes only that copy, recompiles it,
and runs the designated real multi-node regression. A compiling mutant is
`killed` only when the regression fails. Any surviving or non-compiling mutant
makes the campaign fail.
Before either listing or running mutants, it also requires every production
replacement to match exactly once and every test selector to point at the test
declaration itself. Source edits therefore fail loudly instead of turning a
stale mutation into a placebo run of an adjacent test.

```bash
mix run --no-start test/mutation/run.exs

# List or run selected mutations
mix run --no-start test/mutation/run.exs --list
mix run --no-start test/mutation/run.exs disable_below_floor_snapshot
```

Artifacts and complete logs are written below `tmp/mutation/`.
