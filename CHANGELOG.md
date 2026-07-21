## Unreleased
- Add `Group.monitor_generation/1` so long-lived registration owners can
  terminate and re-register when the local membership ETS generation is lost.
- **Breaking**: `Group.disconnect/3` now discards the complete local view of each departed
  cluster — remote entries included, and monitors receive `:unregistered`/`:left` events for
  them — instead of removing only locally owned rows. Reconnecting resyncs through the normal
  snapshot exchange. `connect`/`disconnect` also raise `ArgumentError` for non-binary cluster
  names instead of silently tolerating them.
- The built-in registry conflict resolver now consistently includes the winner's metadata in
  the losing process's `{:group_registry_conflict, key, winner_meta}` exit reason. Custom
  `resolve_registry_conflict` callbacks remain responsible for any process exits they require.
- **Breaking**: `Group.dispatch/4` remote sends and process-DOWN replication are now
  non-suspending and never auto-connect — on a busy or disconnected distribution link the
  message is dropped, the link is force-disconnected, and bounded reconnect retries begin (the
  same policy replication lanes have used since 0.1.8). Previously dispatch could block the
  caller and initiate new connections.
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
