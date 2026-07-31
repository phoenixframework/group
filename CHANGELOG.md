## Unreleased
- Replace replica state sends/snapshots with per-origin, generation- and
  cluster-epoch-fenced streams: sequenced deltas repair gaps from a bounded
  oplog and fall back to exact origin snapshots after pruning. Replica data now
  uses a pluggable nonblocking transport (dist Erlang by default via
  `send_nosuspend`), while dist Erlang remains the control plane. Nonblocking
  control heartbeats lease peer state, requesting a fresh authoritative hello
  on generation or epoch-revision changes, so a stopped Group on a connected
  VM cannot leave permanent registry or membership rows. Reconnects also sweep
  superseded per-shard receive cursors and reconstruct epochless PG rows, so
  reordered cluster controls cannot strand live rows from an older epoch. Full
  epoch authority is installed once by shard 0; matching data shards exchange
  constant-size lane hellos and retain shard-to-shard transport ordering.
  Authority capture is serialized with epoch activation, and exact versus
  incrementally observed revisions are tracked separately so a concurrent
  partial snapshot cannot be mistaken for complete authority.
- Registry authority is retained per origin separately from the visible
  winner. Conflict callbacks select the winner; Group now records and
  propagates an authoritative loser delete, and each owner node terminates only
  its own losing process. This also applies to custom conflict callbacks.
- Add `Group.monitor_generation/1` so long-lived registration owners can
  terminate and re-register when the local membership ETS generation is lost.
- **Breaking**: `Group.disconnect/3` now discards the complete local view of each departed
  cluster — remote entries included, and monitors receive `:unregistered`/`:left` events for
  them — instead of removing only locally owned rows. Reconnecting resyncs through the normal
  snapshot exchange. `connect`/`disconnect` also raise `ArgumentError` for non-binary cluster
  names instead of silently tolerating them.
- The registry conflict resolver now consistently includes the winner's metadata in
  the losing process's `{:group_registry_conflict, key, winner_meta}` exit reason.
- **Breaking**: `Group.dispatch/4` remote sends and process-DOWN replication are now
  non-suspending and never auto-connect. Busy dispatch drops still force a disconnect and
  bounded reconnect retry; replica frames are dropped and repaired by anti-entropy without
  disturbing the dist-Erlang control connection. Previously dispatch could block the caller
  and initiate new connections.
- Configured function-form `extract_meta` callbacks are now applied on reads and lifecycle
  events (previously they were silently ignored and full metadata was exposed), and invalid
  `:extract_meta` values raise `ArgumentError` at startup.
- `Group.lookup/3` no longer converts `ArgumentError` raised by metadata extraction callbacks
  into a `nil` miss; extractor errors now propagate to the caller.
- Invalid `:shards` values (zero, negative, non-integer) raise `ArgumentError` at startup
  instead of failing later during key routing.

## 0.2.1 (2026-07-17)
- Add bounded `Group.members/3` queries with `limit:` and local-owner process-group queries through `Group.local_members/3`

## 0.2.0 (2026-04-17)
- remove deprecate message handling

## 0.1.8 (2026-04-17)
- Use `send_nosuspend` for remote shard sends and add bounded reconnect retries after busy-link disconnects
  to avoid any single bad link from blocking a shard

## 0.1.7 (2026-04-17)
- Fix local shard request reply leaks by using reply aliases and draining any already-delivered timeout replies

## 0.1.6 (2026-04-16)
- Add bounded local PG turn-taking and bulk local PG ETS application
- Add sender-side replicated registry / PG batching by target node

## 0.1.5 (2026-04-14)
- Add receiver-side batching and fairness for replicated registry traffic

## 0.1.4 (2026-03-31)
- Add named-cluster `Group.connect(..., ttl: ms)` leases

## 0.1.3 (2026-03-31)
- Add configurable timeouts to the public register/unregister/join/leave/connect/disconnect APIs
- Add `Group.local_entries/1` for local tagged registry and process-group entries
- Buffer replicated PG join/leave receives with configurable receiver-side flush settings and bulk ETS application

## 0.1.2 (2026-03-30)
- Optimize pg ops

## 0.1.1 (2026-03-19)
- Optimize DOWN handling

## 0.1.0 (2026-02-12) 🚀
- Initial release!
