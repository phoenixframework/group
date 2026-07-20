## 0.2.1 (2026-07-17)
- Add bounded `Group.members/3` queries with `limit:` and local-owner process-group queries through `Group.local_members/3`

## 0.2.0 (2026-04-17)
- Add `Group.broadcast/4` for fan-out to every peer node in a cluster
- Change cross-node `dispatch/4` delivery to have the receiver perform a fresh local lookup for the key
- Route remote dispatch/broadcast through source and receiver shard causal barriers before dispatcher fan-out
- Remove deprecated message handling

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
