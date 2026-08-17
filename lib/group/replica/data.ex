defmodule Group.Replica.Data do
  @moduledoc false
  use GenServer

  alias Group.Replica.WireProtocol

  _archdoc = """
  GenServer that owns ETS tables for all shards.

  Survives Replica shard crashes via rest_for_one supervisor strategy.
  Provides a pure function API for all ETS operations.

  ## ETS Table Layout

  Each shard has materialized registry/PG indexes, authoritative registry-claim
  indexes, and replica stream/oplog/cursor tables. Shared tables hold cluster
  membership, local named-cluster TTL leases, generations, and cluster epochs.

  ### reg_by_key — `:set`, keyed by `{cluster, key}`

      {{cluster, key}, pid, meta, time, node}

  Primary registry lookup table. `:set` enforces one registration per key per cluster.
  `registry_lookup/4` does a direct `ets.lookup` on `{cluster, key}` — O(1) constant time.
  `registry_delete/4` does a direct `ets.delete` — O(1).

  ### reg_by_pid — `:ordered_set`, keyed by `{pid, cluster, key}`

      {{pid, cluster, key}, meta, time, node}

  Reverse index for process death cleanup. `:ordered_set` keyed by `{pid, ...}` so that
  `entries_by_pid` can select all entries for a pid as a contiguous range scan. Also used
  by `maybe_demonitor` to check if a pid has any remaining entries (select with limit 1).

  Re-registration (metadata update) overwrites the existing entry in place — no stale
  accumulation. Direct deletes use `ets.delete(table, {pid, cluster, key})` — O(log N).

  Mirrors the `pg_by_pid` design.

  ### pg_by_key — `:ordered_set`, keyed by `{cluster, key, pid}`

      {{cluster, key, pid}, meta, time, node}

  Primary process group table. `:ordered_set` is chosen so that `pg_members/4` can use
  `ets.select` with a match spec on `{cluster, key, :"$1"}` to efficiently find all pids
  for a given group. Because ordered_set sorts by key, entries for the same `{cluster, key}`
  are contiguous, so the select is a bounded range scan — O(members in group), not O(table).

  Direct lookups (`pg_lookup/5`) and deletes (`pg_delete/5`) are O(log N) on ordered_set.

  ### pg_by_pid — `:ordered_set`, keyed by `{pid, cluster, key}`

      {{pid, cluster, key}, meta, time, node}

  Reverse index for process death cleanup. `:ordered_set` keyed by `{pid, ...}` so that
  `entries_by_pid` can select all entries for a pid as a contiguous range scan. Also used
  by `maybe_demonitor` to check if a pid has any remaining entries (select with limit 1).

  ### reg_claim_by_key / reg_claim_by_pid — authoritative registry claims

      {{cluster, key, origin, generation, epoch}, pid, meta, time, sequence}
      {{pid, cluster, key, origin, generation, epoch}, meta, time, sequence}

  The visible `reg_by_key` row is only a deterministic projection. These tables retain one
  independently versioned claim per origin so a losing-but-live claim is not forgotten.
  Only its owner stream can delete it; conflict resolution then recomputes the visible winner.

  ### replica_stream_meta / replica_oplog / replica_oplog_order / replica_cursor

  Local streams are keyed by `{group, origin, generation, shard, cluster, epoch}`.
  `replica_stream_meta` records each stream's head, retained floor, and applied journal
  position. `replica_oplog` stores `{stream, sequence}` mutation records while
  `replica_oplog_order` gives them one shard-wide append order for bounded pruning.
  `replica_cursor` records only the highest contiguous sequence applied from each remote
  stream. During exact replacement it temporarily stores
  `{:snapshot_installing, snapshot_sequence}`; startup repair treats that marker as an
  interrupted transaction, purges the partial origin slice, and removes the cursor so the
  sender retransmits. Cursor absence is also the durable retirement marker: every peer,
  generation, epoch, and local-cluster purge clears cursors before deleting rows. A gap below
  the retained floor is repaired by exact per-origin snapshot replacement.

  ### cluster_nodes — `:bag`, keyed by cluster name

      {cluster, node}

  Forward index: cluster → nodes. One row per {cluster, node} pair — `:bag` deduplicates
  exact tuples on insert, so concurrent adds of the same node are idempotent with no
  read-modify-write race. `cluster_nodes/2` does a direct bucket lookup — O(nodes in cluster).

  ### node_clusters — `:bag`, keyed by node

      {node, cluster}

  Reverse index: node → clusters. Mirrors cluster_nodes for efficient node-centric lookups.
  `my_clusters/1` does a bucket lookup — O(clusters for this node) instead of a full table
  scan. `purge_cluster_node/2` uses this to find all clusters for a dead node, then does
  targeted deletes from both tables — O(clusters for node) instead of O(total entries).

  Both tables are shared across all shards. Used for the default cluster (nil) and named
  clusters. Peer-connect messages are discovery hints; shard 0's generation-fenced exact
  authority installs membership. Exact authority and both membership indexes are replaced
  in one Data GenServer turn, closing the local-connect/remote-install race without a scan
  outside the remote authority's cluster set. `nodedown`, shard death, or peer-lease expiry
  removes it. `Group.nodes/1` reads the nil cluster from cluster_nodes.

  ### cluster_leases — `:set`, keyed by cluster name

      {cluster, ttl_ms, expires_at}

  Local-only policy table for `Group.connect(..., ttl: ms)` named-cluster leases.
  This table does not affect replication membership directly — `cluster_nodes` /
  `node_clusters` remain the source of truth for who is connected. Instead, the
  `Group.ClusterLease` sweeper reads these rows and, when a lease expires, either:

  - extends `expires_at` by one TTL if the local node still has cluster-scoped
    monitors, local registry entries, or local PG memberships in that cluster
  - or calls the normal disconnect path and deletes the lease row

  Keeping leases separate avoids adding policy state to the hot cluster-membership
  lookups used by `Group.connect/3`, peer discovery, and replication fanout.

  ### replication_meta and epoch tables

  `replication_meta` holds the local origin generation, last exact, complete applied, and
  highest observed authority revisions, a persisted `{generation, revision}` authority hint,
  per-shard installed remote views, journal metadata, and one append counter per shard.
  `local_cluster_epochs` and `closed_local_cluster_epochs` fence local named-cluster
  lifetimes; `remote_cluster_epochs` is the node-wide authority installed by shard 0.
  The three revision roles are separate so a partial control burst cannot be promoted to
  authoritative membership. Contiguous incremental controls compare-and-install against the
  current generation, applied revision, observed revision, and hint in this GenServer turn;
  a concurrent heartbeat makes the whole update stale. A newer hint atomically fences every
  lane view, but cannot be created after exact authority has been retired; only a later exact
  hello can reintroduce the peer. Irreversible registry conflict retirement is
  revalidated through this GenServer, serializing the decision with node-wide generation,
  epoch, observed-revision, installed-lane, and local-cluster changes.
  Local cluster activation also projects self and already-authoritative remote routes in
  this serialized turn. Deactivation removes local admission and queues explicit old-epoch
  cleanup on every shard before replying, so a caller exit cannot strand rows or a close
  barrier; a shard that restarts before handling the message repairs from the marker.
  Final peer-route cleanup rechecks that both exact authority and its hint are absent, so a
  delayed retirement caller cannot erase a rediscovered generation's routes.

  ## Match Spec Patterns

  All match specs use `{:==, :"$N", value}` guards to filter on runtime values (e.g. node
  name). This is the correct ETS match spec syntax — `:const` is not valid. Literal values
  from Elixir variables (like `cluster` or `key`) are interpolated directly into the match
  pattern tuple positions and work as exact-match filters without needing a guard.

  ## Bulk Operations & Their Costs

  - `purge_node/3`: Full table scan via `ets.select` filtering by node, then individual
    deletes. O(table size) for the scan, but this only runs on nodedown, remote shard death,
    or peer-lease expiry — rare paths.

  - `registry_count`, `pg_count`, `pg_count_by_prefix`, `local_registry_count`,
    `local_pg_count`, `local_registry_present?`, `local_pg_present?`: Uses
    `ets.select_count`. Full scan but returns only a count/existence signal
    without materializing matching rows.

  - `entries_by_pid/3`: Range scan on the by_pid ordered_set tables. O(entries for that pid).

  ## Process Monitors

  Monitors live entirely in the Replica GenServer's `state.monitors` map (`pid => mref`).
  ETS stores pids but not monitor refs — mref is not needed in ETS.

  On Replica crash, the BEAM cleans up all monitors owned by the dead process. On restart,
  `rebuild_monitors/1` scans the surviving ETS tables for pids and calls `Process.monitor`
  fresh. This is the only reason ETS matters for monitors: without surviving pid entries,
  local processes that registered before the crash would be orphaned — nobody would monitor
  them, and their ETS entries would persist forever if they later died.

  Only locally owned member processes are monitored. Remote owner death arrives as a
  sequenced delete from that owner; nodedown or peer-lease expiry removes the complete
  remote origin if it cannot report the delete. Periodic anti-entropy then rebuilds a
  returning origin from retained deltas or an exact snapshot. No node monitors or exits
  another node's member processes.

  The `state.monitors` map also deduplicates: a pid registered under multiple keys in the
  same shard gets one monitor, not one per key.

  `maybe_demonitor/3` checks whether a pid still has any remaining entries across both
  tables before allowing demonitor. Short-circuits: checks reg_by_pid first (key lookup),
  falls back to pg_by_pid only if empty (select with limit 1 — existence check, not scan).

  ## Concurrency

  All tables are `:public` with `read_concurrency: true`. Reads happen directly from any
  process (the Replica GenServer, Group API callers, etc.). Writes are serialized through
  the Replica GenServer for each shard, ensuring consistent paired updates to both the
  by_key and by_pid tables. The Data GenServer owns the tables (for shard-crash survival
  via rest_for_one) and serializes cross-shard generation, epoch, and cluster-node changes.
  """

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)
    GenServer.start_link(__MODULE__, {name, num_shards}, name: data_name(name))
  end

  def data_name(name), do: :"#{name}_data"

  # =====================================================================
  # Replica generations, epochs, journal, and cursors
  # =====================================================================

  def generation(name) do
    :ets.lookup_element(replication_meta_table(name), :generation, 2)
  end

  def local_cluster_epoch_revision(name) do
    :ets.lookup_element(replication_meta_table(name), :cluster_epoch_revision, 2)
  end

  def local_cluster_epoch(name, nil), do: generation(name)

  def local_cluster_epoch(name, cluster) do
    case :ets.lookup(local_cluster_epochs_table(name), cluster) do
      [{^cluster, epoch}] -> epoch
      [] -> nil
    end
  end

  def local_cluster_epochs(name) do
    [{nil, generation(name)} | :ets.tab2list(local_cluster_epochs_table(name))]
  end

  def local_replica_authority(name) do
    GenServer.call(data_name(name), :local_replica_authority, :infinity)
  end

  def closed_local_cluster_epoch(name, cluster) do
    case :ets.lookup(closed_local_cluster_epochs_table(name), cluster) do
      [{^cluster, epoch, _pending_shards}] -> epoch
      [] -> nil
    end
  end

  def closed_local_clusters(name) do
    closed_local_cluster_epochs_table(name)
    |> :ets.tab2list()
    |> Enum.map(&elem(&1, 0))
  end

  def closed_local_cluster_epochs(name) do
    closed_local_cluster_epochs_table(name)
    |> :ets.tab2list()
    |> Enum.map(fn {cluster, epoch, _pending_shards} -> {cluster, epoch} end)
  end

  def closed_local_cluster_pending?(name, cluster, epoch, shard) do
    case :ets.lookup(closed_local_cluster_epochs_table(name), cluster) do
      [{^cluster, ^epoch, pending_shards}] -> MapSet.member?(pending_shards, shard)
      _ -> false
    end
  end

  def await_closed_local_clusters(name, clusters, timeout)
      when is_list(clusters) and is_integer(timeout) and timeout >= 0 do
    started_at = System.monotonic_time(:millisecond)
    await_closed_local_clusters(name, clusters, timeout, started_at)
  end

  def remote_generation(name, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_generation, remote_node}) do
      [{{:remote_generation, ^remote_node}, generation}] -> generation
      [] -> nil
    end
  end

  def remote_cluster_epoch(name, remote_node, nil), do: remote_generation(name, remote_node)

  def remote_cluster_epoch(name, remote_node, cluster) do
    case :ets.lookup(remote_cluster_epochs_table(name), {remote_node, cluster}) do
      [{{^remote_node, ^cluster}, epoch}] -> epoch
      [] -> nil
    end
  end

  def remote_cluster_epoch_revision(name, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_epoch_revision, remote_node}) do
      [{{:remote_epoch_revision, ^remote_node}, revision}] -> revision
      [] -> nil
    end
  end

  def remote_cluster_epoch_exact_revision(name, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_epoch_exact, remote_node}) do
      [{{:remote_epoch_exact, ^remote_node}, revision}] -> revision
      [] -> nil
    end
  end

  def remote_cluster_epoch_observed_revision(name, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_epoch_observed, remote_node}) do
      [{{:remote_epoch_observed, ^remote_node}, revision}] -> revision
      [] -> nil
    end
  end

  def remote_replica_authority_hint(name, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_authority_hint, remote_node}) do
      [{{:remote_authority_hint, ^remote_node}, generation, revision}] ->
        {generation, revision}

      [] ->
        nil
    end
  end

  @doc false
  def remote_authority_install_count(name, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_authority_installs, remote_node}) do
      [{{:remote_authority_installs, ^remote_node}, count}] -> count
      [] -> 0
    end
  end

  def remote_view_generation(name, shard, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_view_info, shard, remote_node}) do
      [{{:remote_view_info, ^shard, ^remote_node}, generation, _revision, _observed}] ->
        generation

      [] ->
        nil
    end
  end

  def remote_view_cluster_epoch_revision(name, shard, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_view_info, shard, remote_node}) do
      [{{:remote_view_info, ^shard, ^remote_node}, _generation, revision, _observed}] ->
        revision

      [] ->
        nil
    end
  end

  def remote_view_observed_revision(name, shard, remote_node) do
    case :ets.lookup(replication_meta_table(name), {:remote_view_info, shard, remote_node}) do
      [{{:remote_view_info, ^shard, ^remote_node}, _generation, _revision, observed}] ->
        observed

      [] ->
        nil
    end
  end

  def put_remote_replica_info(name, shard, remote_node, generation, epoch_revision, epochs) do
    GenServer.call(
      data_name(name),
      {:put_remote_replica_info, shard, remote_node, generation, epoch_revision, epochs},
      :infinity
    )
  end

  def put_remote_view_info(name, shard, remote_node, generation, authoritative, observed) do
    GenServer.call(
      data_name(name),
      {:put_remote_view_info, shard, remote_node, generation, authoritative, observed},
      :infinity
    )
  end

  def put_remote_cluster_epochs(
        name,
        shard,
        remote_node,
        generation,
        expected_revision,
        revision,
        epochs
      ) do
    GenServer.call(
      data_name(name),
      {:put_remote_cluster_epochs, shard, remote_node, generation, expected_revision, revision,
       epochs},
      :infinity
    )
  end

  def observe_remote_cluster_epoch_revision(name, remote_node, revision) do
    GenServer.call(
      data_name(name),
      {:observe_remote_cluster_epoch_revision, remote_node, revision},
      :infinity
    )
  end

  def observe_remote_replica_hint(name, remote_node, generation, revision) do
    GenServer.call(
      data_name(name),
      {:observe_remote_replica_hint, remote_node, generation, revision},
      :infinity
    )
  end

  def remote_registry_claim_authoritative?(
        name,
        shard,
        remote_node,
        generation,
        cluster,
        epoch
      ) do
    GenServer.call(
      data_name(name),
      {:remote_registry_claim_authoritative, shard, remote_node, generation, cluster, epoch},
      :infinity
    )
  end

  def close_remote_cluster_epochs(
        name,
        shard,
        remote_node,
        generation,
        expected_revision,
        revision,
        epochs
      ) do
    GenServer.call(
      data_name(name),
      {:close_remote_cluster_epochs, shard, remote_node, generation, expected_revision, revision,
       epochs},
      :infinity
    )
  end

  def delete_remote_replica_info(name, shard, remote_node) do
    GenServer.call(
      data_name(name),
      {:delete_remote_replica_info, shard, remote_node},
      :infinity
    )
  end

  def expire_remote_replica_lane(name, shard, remote_node) do
    GenServer.call(
      data_name(name),
      {:expire_remote_replica_lane, shard, remote_node},
      :infinity
    )
  end

  def activate_local_clusters(name, clusters) do
    GenServer.call(data_name(name), {:activate_local_clusters, clusters}, :infinity)
  end

  def activate_local_clusters_durable(name, clusters) do
    GenServer.call(data_name(name), {:activate_local_clusters_durable, clusters}, :infinity)
  end

  def deactivate_local_clusters(name, clusters) do
    GenServer.call(data_name(name), {:deactivate_local_clusters, clusters}, :infinity)
  end

  def deactivate_local_clusters_durable(name, clusters) do
    GenServer.call(data_name(name), {:deactivate_local_clusters_durable, clusters}, :infinity)
  end

  def mark_closed_cluster_shard(name, cluster_epochs, shard) do
    GenServer.call(
      data_name(name),
      {:mark_closed_cluster_shard, cluster_epochs, shard},
      :infinity
    )
  end

  def local_stream_id(name, shard, cluster) do
    case local_cluster_epoch(name, cluster) do
      nil ->
        nil

      epoch ->
        Group.Replica.WireProtocol.stream_id(
          name,
          node(),
          generation(name),
          shard,
          cluster,
          epoch
        )
    end
  end

  def append_replica_record(name, shard, stream_id, mutations) when is_list(mutations) do
    stream_table = replica_stream_meta_table(name, shard)

    head =
      :ets.update_counter(
        stream_table,
        stream_id,
        {2, 1},
        {stream_id, 0, 1, 0}
      )

    append_id =
      :ets.update_counter(
        replication_meta_table(name),
        {:append_counter, shard},
        {2, 1},
        {{:append_counter, shard}, 0}
      )

    :ets.insert(replica_oplog_table(name, shard), {{stream_id, head}, append_id, mutations})
    :ets.insert(replica_oplog_order_table(name, shard), {append_id, stream_id, head})
    {head, mutations}
  end

  def mark_local_replica_applied(name, shard, stream_id, seq) do
    :ets.update_element(replica_stream_meta_table(name, shard), stream_id, {4, seq})
    :ok
  end

  def local_replica_unapplied(name, shard) do
    replica_stream_meta_table(name, shard)
    |> :ets.tab2list()
    |> Enum.flat_map(fn {stream_id, head, _floor, applied} ->
      if applied < head do
        replica_records(name, shard, stream_id, applied + 1, head - applied)
        |> Enum.map(fn {seq, mutations} -> {stream_id, seq, mutations} end)
      else
        []
      end
    end)
  end

  @doc false
  def repair_local_replica_journal(name, shard) do
    stream_table = replica_stream_meta_table(name, shard)
    oplog_table = replica_oplog_table(name, shard)
    order_table = replica_oplog_order_table(name, shard)

    stream_table
    |> :ets.tab2list()
    |> Enum.each(fn {stream_id, head, floor, applied} ->
      if current_local_stream?(name, shard, stream_id) do
        present =
          oplog_table
          |> :ets.select([
            {{{stream_id, :"$1"}, :_, :_}, [], [:"$1"]}
          ])
          |> MapSet.new()

        repaired_floor =
          floor
          |> missing_applied_sequences(applied, present)
          |> case do
            [] -> floor
            missing -> Enum.max(missing) + 1
          end

        repaired_head = contiguous_unapplied_head(applied, head, present)

        if repaired_head < head do
          :ets.select_delete(oplog_table, [
            {{{stream_id, :"$1"}, :_, :_}, [{:>, :"$1", repaired_head}], [true]}
          ])
        end

        repaired_floor = min(repaired_floor, repaired_head + 1)
        repaired_applied = min(applied, repaired_head)

        :ets.insert(
          stream_table,
          {stream_id, repaired_head, repaired_floor, repaired_applied}
        )
      else
        drop_local_stream(
          name,
          shard,
          WireProtocol.stream_cluster(stream_id),
          WireProtocol.stream_epoch(stream_id)
        )
      end
    end)

    :ets.delete_all_objects(order_table)

    oplog_table
    |> :ets.tab2list()
    |> Enum.each(fn {{stream_id, seq}, append_id, _mutations} ->
      :ets.insert(order_table, {append_id, stream_id, seq})
    end)

    :ok
  end

  @doc false
  def repair_shard_indexes(name, shard) do
    repair_interrupted_snapshot_installs(name, shard)
    repair_primary_replica_rows(name, shard)
    :ok
  end

  def replica_stream_heads(name, shard) do
    :ets.tab2list(replica_stream_meta_table(name, shard))
    |> Enum.map(fn {stream_id, head, floor, _applied} -> {stream_id, floor, head} end)
  end

  defp missing_applied_sequences(floor, applied, _present) when floor > applied, do: []

  defp missing_applied_sequences(floor, applied, present) do
    Enum.reject(floor..applied, &MapSet.member?(present, &1))
  end

  defp contiguous_unapplied_head(applied, head, _present) when applied >= head, do: head

  defp contiguous_unapplied_head(applied, head, present) do
    Enum.reduce_while((applied + 1)..head, applied, fn seq, _last ->
      if MapSet.member?(present, seq), do: {:cont, seq}, else: {:halt, seq - 1}
    end)
  end

  defp current_local_stream?(name, shard, stream_id) do
    cluster = WireProtocol.stream_cluster(stream_id)

    WireProtocol.stream_name(stream_id) == name and
      WireProtocol.stream_origin(stream_id) == node() and
      WireProtocol.stream_generation(stream_id) == generation(name) and
      WireProtocol.stream_shard(stream_id) == shard and
      WireProtocol.stream_epoch(stream_id) == local_cluster_epoch(name, cluster)
  end

  defp repair_interrupted_snapshot_installs(name, shard) do
    streams =
      :ets.select(replica_cursor_table(name, shard), [
        {{:"$1", {:snapshot_installing, :_}}, [], [:"$1"]}
      ])

    {streams, malformed} =
      Enum.split_with(streams, fn stream_id ->
        WireProtocol.valid_stream_id?(stream_id) and
          WireProtocol.stream_name(stream_id) == name and
          WireProtocol.stream_shard(stream_id) == shard and
          WireProtocol.stream_origin(stream_id) != node()
      end)

    Enum.each(malformed, &:ets.delete(replica_cursor_table(name, shard), &1))

    if streams != [] do
      _affected_keys = purge_registry_claims_for_streams(name, shard, streams)

      streams
      |> Enum.group_by(&WireProtocol.stream_origin/1, &WireProtocol.stream_cluster/1)
      |> Enum.each(fn {origin, clusters} ->
        delete_pg_for_origin_clusters(name, shard, Enum.uniq(clusters), origin)
      end)

      Enum.each(streams, &:ets.delete(replica_cursor_table(name, shard), &1))
    end

    :ok
  end

  defp await_closed_local_clusters(name, clusters, timeout, started_at) do
    pending? =
      Enum.any?(clusters, fn cluster ->
        not is_nil(closed_local_cluster_epoch(name, cluster))
      end)

    elapsed = System.monotonic_time(:millisecond) - started_at

    cond do
      not pending? ->
        max(timeout - elapsed, 0)

      elapsed >= timeout ->
        exit(
          {:timeout,
           {GenServer, :call,
            [data_name(name), {:await_closed_local_clusters, clusters}, timeout]}}
        )

      true ->
        receive do
        after
          min(10, timeout - elapsed) ->
            await_closed_local_clusters(name, clusters, timeout, started_at)
        end
    end
  end

  # A shard can crash between writes to its materialized rows and receive
  # cursor, or while retiring an epoch across multiple ETS tables. Recover from
  # the primary tables themselves: stale claims carry their complete stream
  # authority, while a remote PG row is retained only when the current stream
  # has a cursor (including the sequence-zero admission marker). This pass also
  # replaces the old multi-million-element cluster list with one fixed-table
  # traversal and O(number of inactive clusters) accumulator memory. Reverse
  # indexes are rebuilt during the same primary-table pass, avoiding both a
  # second traversal and a complete `tab2list/1` heap copy per index.
  defp repair_primary_replica_rows(name, shard) do
    reg_reverse = reg_by_pid_table(name, shard)
    claim_reverse = reg_claim_by_pid_table(name, shard)
    pg_reverse = pg_by_pid_table(name, shard)
    :ets.delete_all_objects(reg_reverse)
    :ets.delete_all_objects(claim_reverse)
    :ets.delete_all_objects(pg_reverse)

    inactive_clusters = MapSet.new()

    inactive_clusters =
      repair_ets_table(
        reg_by_key_table(name, shard),
        inactive_clusters,
        fn {{cluster, key}, pid, meta, time, entry_node}, inactive ->
          if active_local_cluster?(name, cluster) do
            :ets.insert(reg_reverse, {{pid, cluster, key}, meta, time, entry_node})
            inactive
          else
            :ets.delete(reg_by_key_table(name, shard), {cluster, key})
            remember_inactive_cluster(name, inactive, cluster)
          end
        end
      )

    inactive_clusters =
      repair_ets_table(
        reg_claim_by_key_table(name, shard),
        inactive_clusters,
        fn {{cluster, key, origin, claim_generation, epoch}, pid, meta, time, seq}, inactive ->
          if active_local_cluster?(name, cluster) and
               valid_claim_authority?(
                 name,
                 shard,
                 cluster,
                 origin,
                 claim_generation,
                 epoch
               ) do
            :ets.insert(
              claim_reverse,
              {{pid, cluster, key, origin, claim_generation, epoch}, meta, time, seq}
            )

            inactive
          else
            :ets.delete(
              reg_claim_by_key_table(name, shard),
              {cluster, key, origin, claim_generation, epoch}
            )

            remember_inactive_cluster(name, inactive, cluster)
          end
        end
      )

    inactive_clusters =
      repair_ets_table(
        pg_by_key_table(name, shard),
        inactive_clusters,
        fn {{cluster, key, pid}, meta, time, entry_node}, inactive ->
          valid? =
            active_local_cluster?(name, cluster) and node(pid) == entry_node and
              (entry_node == node() or
                 valid_remote_pg_authority?(name, shard, cluster, entry_node))

          if valid? do
            :ets.insert(pg_reverse, {{pid, cluster, key}, meta, time, entry_node})
            inactive
          else
            :ets.delete(pg_by_key_table(name, shard), {cluster, key, pid})
            remember_inactive_cluster(name, inactive, cluster)
          end
        end
      )

    inactive_clusters =
      repair_ets_table(
        replica_cursor_table(name, shard),
        inactive_clusters,
        fn {stream_id, _cursor}, inactive ->
          cluster =
            if WireProtocol.valid_stream_id?(stream_id),
              do: WireProtocol.stream_cluster(stream_id)

          if valid_remote_cursor_authority?(name, shard, stream_id) do
            inactive
          else
            :ets.delete(replica_cursor_table(name, shard), stream_id)
            remember_inactive_cluster(name, inactive, cluster)
          end
        end
      )

    inactive_clusters = MapSet.to_list(inactive_clusters)
    if inactive_clusters != [], do: remove_clusters(name, inactive_clusters)
    :ok
  end

  defp repair_ets_table(table, acc, fun) do
    :ets.safe_fixtable(table, true)

    try do
      repair_ets_table(table, :ets.first(table), acc, fun)
    after
      :ets.safe_fixtable(table, false)
    end
  end

  defp repair_ets_table(_table, :"$end_of_table", acc, _fun), do: acc

  defp repair_ets_table(table, key, acc, fun) do
    next_key = :ets.next(table, key)

    acc =
      case :ets.lookup(table, key) do
        [object] -> fun.(object, acc)
        [] -> acc
      end

    repair_ets_table(table, next_key, acc, fun)
  end

  defp active_local_cluster?(_name, nil), do: true
  defp active_local_cluster?(name, cluster), do: not is_nil(local_cluster_epoch(name, cluster))

  defp remember_inactive_cluster(_name, inactive, nil), do: inactive

  defp remember_inactive_cluster(name, inactive, cluster) do
    if active_local_cluster?(name, cluster), do: inactive, else: MapSet.put(inactive, cluster)
  end

  defp valid_claim_authority?(name, _shard, cluster, origin, claim_generation, epoch)
       when origin == node() do
    claim_generation == generation(name) and epoch == local_cluster_epoch(name, cluster)
  end

  defp valid_claim_authority?(name, shard, cluster, origin, claim_generation, epoch) do
    if claim_generation == remote_generation(name, origin) and
         epoch == remote_cluster_epoch(name, origin, cluster) do
      stream_id =
        WireProtocol.stream_id(name, origin, claim_generation, shard, cluster, epoch)

      :ets.member(replica_cursor_table(name, shard), stream_id)
    else
      false
    end
  end

  defp valid_remote_pg_authority?(name, shard, cluster, origin) do
    remote_generation = remote_generation(name, origin)
    remote_epoch = remote_cluster_epoch(name, origin, cluster)

    if WireProtocol.valid_generation?(remote_generation) and not is_nil(remote_epoch) do
      stream_id =
        WireProtocol.stream_id(name, origin, remote_generation, shard, cluster, remote_epoch)

      :ets.member(replica_cursor_table(name, shard), stream_id)
    else
      false
    end
  end

  defp valid_remote_cursor_authority?(name, shard, stream_id) do
    WireProtocol.valid_stream_id?(stream_id) and
      WireProtocol.stream_name(stream_id) == name and
      WireProtocol.stream_shard(stream_id) == shard and
      WireProtocol.stream_origin(stream_id) != node() and
      active_local_cluster?(name, WireProtocol.stream_cluster(stream_id)) and
      WireProtocol.stream_generation(stream_id) ==
        remote_generation(name, WireProtocol.stream_origin(stream_id)) and
      WireProtocol.stream_epoch(stream_id) ==
        remote_cluster_epoch(
          name,
          WireProtocol.stream_origin(stream_id),
          WireProtocol.stream_cluster(stream_id)
        )
  end

  def replica_stream_head(name, shard, stream_id) do
    case :ets.lookup(replica_stream_meta_table(name, shard), stream_id) do
      [{^stream_id, head, floor, applied}] -> {floor, head, applied}
      [] -> {1, 0, 0}
    end
  end

  def replica_records(_name, _shard, _stream_id, _from_seq, limit) when limit <= 0, do: []

  def replica_records(name, shard, stream_id, from_seq, limit) do
    table = replica_oplog_table(name, shard)

    table
    |> :ets.select(
      [
        {{{stream_id, :"$1"}, :_, :"$2"}, [{:>=, :"$1", from_seq}], [{{:"$1", :"$2"}}]}
      ],
      limit
    )
    |> case do
      :"$end_of_table" -> []
      {records, _continuation} -> records
    end
  end

  def prune_replica_oplog(name, shard, max_entries) do
    order_table = replica_oplog_order_table(name, shard)
    do_prune_replica_oplog(name, shard, order_table, :ets.info(order_table, :size) - max_entries)
  end

  defp do_prune_replica_oplog(_name, _shard, _order_table, remaining) when remaining <= 0,
    do: :ok

  defp do_prune_replica_oplog(name, shard, order_table, remaining) do
    case :ets.first(order_table) do
      :"$end_of_table" ->
        :ok

      append_id ->
        [{^append_id, stream_id, seq}] = :ets.lookup(order_table, append_id)
        {_floor, _head, applied} = replica_stream_head(name, shard, stream_id)

        if seq <= applied do
          :ets.delete(order_table, append_id)
          :ets.delete(replica_oplog_table(name, shard), {stream_id, seq})

          case :ets.lookup(replica_stream_meta_table(name, shard), stream_id) do
            [{^stream_id, head, floor, local_applied}] ->
              :ets.insert(
                replica_stream_meta_table(name, shard),
                {stream_id, head, max(floor, seq + 1), local_applied}
              )

            [] ->
              :ok
          end

          do_prune_replica_oplog(name, shard, order_table, remaining - 1)
        else
          :ok
        end
    end
  end

  def replica_cursor(name, shard, stream_id) do
    case :ets.lookup(replica_cursor_table(name, shard), stream_id) do
      [{^stream_id, seq}] when is_integer(seq) -> seq
      [{^stream_id, {:snapshot_installing, _snapshot_seq}}] -> 0
      [] -> 0
    end
  end

  def replica_cursor_streams_for_origin_cluster(name, shard, origin_node, cluster) do
    :ets.select(replica_cursor_table(name, shard), [
      {{{name, origin_node, :"$1", shard, cluster, :"$2"}, :_}, [],
       [{{name, origin_node, :"$1", shard, cluster, :"$2"}}]}
    ])
  end

  def replica_cursor_streams_for_origin(name, shard, origin_node) do
    :ets.select(replica_cursor_table(name, shard), [
      {{{name, origin_node, :"$1", shard, :"$2", :"$3"}, :_}, [],
       [{{name, origin_node, :"$1", shard, :"$2", :"$3"}}]}
    ])
  end

  def put_replica_cursor(name, shard, stream_id, seq) do
    :ets.insert(replica_cursor_table(name, shard), {stream_id, seq})
    :ok
  end

  def ensure_replica_cursor(name, shard, stream_id) do
    :ets.insert_new(replica_cursor_table(name, shard), {stream_id, 0})
    :ok
  end

  def begin_replica_snapshot_install(name, shard, stream_id, snapshot_seq) do
    :ets.insert(
      replica_cursor_table(name, shard),
      {stream_id, {:snapshot_installing, snapshot_seq}}
    )

    :ok
  end

  def delete_replica_cursors_for_origin(name, shard, origin_node) do
    :ets.select_delete(replica_cursor_table(name, shard), [
      {{{name, origin_node, :_, shard, :_, :_}, :_}, [], [true]}
    ])

    :ok
  end

  def delete_replica_cursors_for_clusters(_name, _shard, []), do: :ok

  def delete_replica_cursors_for_clusters(name, shard, clusters) do
    match_specs =
      Enum.map(clusters, fn cluster ->
        {{{name, :_, :_, shard, cluster, :_}, :_}, [], [true]}
      end)

    :ets.select_delete(replica_cursor_table(name, shard), match_specs)
    :ok
  end

  def delete_replica_cursor(name, shard, stream_id) do
    :ets.delete(replica_cursor_table(name, shard), stream_id)
    :ok
  end

  @doc false
  def retained_replica_origins(name, shard) do
    # Accepted replica data is always fenced by this persisted lane view. Every
    # retirement path destroys data and cursors before deleting the view, so a
    # shard crash cannot leave valid data without this restart index. Scanning
    # it is O(known peers * shards), never O(registry + PG cardinality).
    match_specs = [
      {{{:remote_view_info, shard, :"$1"}, :_, :_, :_}, [], [:"$1"]},
      # A hint is the durable fence left before the observing lane records its
      # in-memory lease deadline. Every restarting lane must recognize it so a
      # crash in that window cannot strand the peer forever.
      {{{:remote_authority_hint, :"$1"}, :_, :_}, [], [:"$1"]}
    ]

    match_specs =
      if shard == 0 do
        [{{{:remote_generation, :"$1"}, :_}, [], [:"$1"]} | match_specs]
      else
        match_specs
      end

    replication_meta_table(name)
    |> :ets.select(match_specs)
    |> Enum.reject(&(&1 == node()))
    |> Enum.uniq()
  end

  def drop_local_stream(name, shard, cluster, epoch) do
    stream_id =
      Group.Replica.WireProtocol.stream_id(name, node(), generation(name), shard, cluster, epoch)

    append_rows =
      :ets.select(replica_oplog_table(name, shard), [
        {{{stream_id, :"$1"}, :"$2", :_}, [], [{{:"$1", :"$2"}}]}
      ])

    Enum.each(append_rows, fn {seq, append_id} ->
      :ets.delete(replica_oplog_table(name, shard), {stream_id, seq})
      :ets.delete(replica_oplog_order_table(name, shard), append_id)
    end)

    :ets.delete(replica_stream_meta_table(name, shard), stream_id)

    :ok
  end

  # =====================================================================
  # Registry operations
  # =====================================================================

  def registry_insert(name, shard, cluster, key, pid, meta, time, node) do
    table = reg_by_key_table(name, shard)
    :ets.insert(table, {{cluster, key}, pid, meta, time, node})
    table_pid = reg_by_pid_table(name, shard)
    :ets.insert(table_pid, {{pid, cluster, key}, meta, time, node})
    :ok
  end

  def registry_insert_many(_name, _shard, []), do: :ok

  def registry_insert_many(name, shard, entries) do
    table = reg_by_key_table(name, shard)
    table_pid = reg_by_pid_table(name, shard)

    :ets.insert(
      table,
      Enum.map(entries, fn {cluster, key, pid, meta, time, node} ->
        {{cluster, key}, pid, meta, time, node}
      end)
    )

    :ets.insert(
      table_pid,
      Enum.map(entries, fn {cluster, key, pid, meta, time, node} ->
        {{pid, cluster, key}, meta, time, node}
      end)
    )

    :ok
  end

  @doc false
  def registry_insert_new_many(_name, _shard, []), do: true

  def registry_insert_new_many(name, shard, entries) do
    table = reg_by_key_table(name, shard)
    table_pid = reg_by_pid_table(name, shard)

    objects =
      Enum.map(entries, fn {cluster, key, pid, meta, time, node} ->
        {{cluster, key}, pid, meta, time, node}
      end)

    if :ets.insert_new(table, objects) do
      :ets.insert(
        table_pid,
        Enum.map(entries, fn {cluster, key, pid, meta, time, node} ->
          {{pid, cluster, key}, meta, time, node}
        end)
      )

      true
    else
      false
    end
  end

  def registry_delete(name, shard, cluster, key, pid) do
    table = reg_by_key_table(name, shard)
    :ets.delete(table, {cluster, key})
    table_pid = reg_by_pid_table(name, shard)
    :ets.delete(table_pid, {pid, cluster, key})
    :ok
  end

  def registry_delete_many(_name, _shard, []), do: :ok

  def registry_delete_many(name, shard, entries) do
    entries = Enum.uniq(entries)
    table = reg_by_key_table(name, shard)
    table_pid = reg_by_pid_table(name, shard)

    :ets.select_delete(
      table,
      Enum.map(entries, fn {cluster, key, _pid} ->
        {{{cluster, key}, :_, :_, :_, :_}, [], [true]}
      end)
    )

    :ets.select_delete(
      table_pid,
      Enum.map(entries, fn {cluster, key, pid} ->
        {{{pid, cluster, key}, :_, :_, :_}, [], [true]}
      end)
    )

    :ok
  end

  def registry_lookup(name, shard, cluster, key) do
    registry_lookup_in_table(reg_by_key_table(name, shard), cluster, key)
  end

  @doc false
  def registry_lookup_in_table(table, cluster, key) do
    case :ets.lookup(table, {cluster, key}) do
      [{{^cluster, ^key}, pid, meta, time, node}] ->
        {pid, meta, time, node}

      [] ->
        nil
    end
  end

  def registry_lookup_by_pid(name, shard, pid) do
    table = reg_by_pid_table(name, shard)

    :ets.select(table, [
      {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
    ])
  end

  def registry_lookup_by_prefix(name, shard, cluster, prefix) do
    table = reg_by_key_table(name, shard)
    prefix_end = next_binary_prefix(prefix)

    :ets.select(table, [
      {{{cluster, :"$1"}, :"$2", :"$3", :_, :_},
       [{:andalso, {:>=, :"$1", prefix}, {:<, :"$1", prefix_end}}], [{{:"$2", :"$3"}}]}
    ])
  end

  # =====================================================================
  # Authoritative registry claims
  # =====================================================================

  def put_registry_claim(name, shard, stream_id, seq, key, pid, meta, time) do
    cluster = Group.Replica.WireProtocol.stream_cluster(stream_id)
    origin_node = Group.Replica.WireProtocol.stream_origin(stream_id)
    generation = Group.Replica.WireProtocol.stream_generation(stream_id)
    epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)
    claim_key = {cluster, key, origin_node, generation, epoch}
    by_key = reg_claim_by_key_table(name, shard)

    case :ets.lookup(by_key, claim_key) do
      [{^claim_key, _old_pid, _old_meta, _old_time, old_seq}] when old_seq >= seq ->
        :ok

      [{^claim_key, old_pid, _old_meta, _old_time, _old_seq}] ->
        :ets.delete(
          reg_claim_by_pid_table(name, shard),
          {old_pid, cluster, key, origin_node, generation, epoch}
        )

        insert_registry_claim(name, shard, claim_key, seq, pid, meta, time)

      [] ->
        insert_registry_claim(name, shard, claim_key, seq, pid, meta, time)
    end
  end

  defp insert_registry_claim(name, shard, claim_key, seq, pid, meta, time) do
    {cluster, key, origin_node, generation, epoch} = claim_key
    :ets.insert(reg_claim_by_key_table(name, shard), {claim_key, pid, meta, time, seq})

    :ets.insert(
      reg_claim_by_pid_table(name, shard),
      {{pid, cluster, key, origin_node, generation, epoch}, meta, time, seq}
    )

    :ok
  end

  @doc false
  def insert_exact_registry_claims_many(_name, _shard, _stream_id, _seq, []), do: :ok

  def insert_exact_registry_claims_many(name, shard, stream_id, seq, claims)
      when is_list(claims) do
    cluster = Group.Replica.WireProtocol.stream_cluster(stream_id)
    origin_node = Group.Replica.WireProtocol.stream_origin(stream_id)
    generation = Group.Replica.WireProtocol.stream_generation(stream_id)
    epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)

    :ets.insert(
      reg_claim_by_key_table(name, shard),
      Enum.map(claims, fn {key, pid, meta, time} ->
        {{cluster, key, origin_node, generation, epoch}, pid, meta, time, seq}
      end)
    )

    :ets.insert(
      reg_claim_by_pid_table(name, shard),
      Enum.map(claims, fn {key, pid, meta, time} ->
        {{pid, cluster, key, origin_node, generation, epoch}, meta, time, seq}
      end)
    )

    :ok
  end

  def delete_registry_claim(name, shard, stream_id, seq, key, pid) do
    cluster = Group.Replica.WireProtocol.stream_cluster(stream_id)
    origin_node = Group.Replica.WireProtocol.stream_origin(stream_id)
    generation = Group.Replica.WireProtocol.stream_generation(stream_id)
    epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)
    claim_key = {cluster, key, origin_node, generation, epoch}

    case :ets.lookup(reg_claim_by_key_table(name, shard), claim_key) do
      [{^claim_key, ^pid, _meta, _time, old_seq}] when old_seq <= seq ->
        delete_registry_claim_indexes(
          name,
          shard,
          cluster,
          key,
          origin_node,
          generation,
          epoch,
          pid
        )

        :ok

      _ ->
        :ok
    end
  end

  def registry_claims(name, shard, cluster, key) do
    :ets.select(reg_claim_by_key_table(name, shard), [
      {{{cluster, key, :"$1", :"$2", :"$3"}, :"$4", :"$5", :"$6", :"$7"}, [],
       [{{:"$4", :"$5", :"$6", :"$1", :"$2", :"$3", :"$7"}}]}
    ])
  end

  @doc false
  def registry_claim_uncontended_in_table?(
        table,
        {cluster, key, _origin_node, _generation, _epoch} = claim_key
      ) do
    not same_registry_claim_key?(:ets.prev(table, claim_key), cluster, key) and
      not same_registry_claim_key?(:ets.next(table, claim_key), cluster, key)
  end

  defp same_registry_claim_key?({cluster, key, _origin, _generation, _epoch}, cluster, key),
    do: true

  defp same_registry_claim_key?(_claim_key, _cluster, _key), do: false

  def registry_claims_for_stream(name, shard, stream_id) do
    fold_registry_claims_for_stream(name, shard, stream_id, [], fn row, rows -> [row | rows] end)
    |> Enum.reverse()
  end

  def fold_registry_claims_for_stream(name, shard, stream_id, acc, fun)
      when is_function(fun, 2) do
    {table, match_spec} = registry_claim_stream_selection(name, shard, stream_id)

    fold_select_batches_fixed(table, match_spec, acc, fn rows, inner ->
      Enum.reduce(rows, inner, fun)
    end)
  end

  def reduce_registry_claim_batches_for_stream(name, shard, stream_id, acc, fun)
      when is_function(fun, 2) do
    {table, match_spec} = registry_claim_stream_selection(name, shard, stream_id)
    fold_select_batches(table, match_spec, acc, fun)
  end

  defp registry_claim_stream_selection(name, shard, stream_id) do
    cluster = Group.Replica.WireProtocol.stream_cluster(stream_id)
    origin_node = Group.Replica.WireProtocol.stream_origin(stream_id)
    generation = Group.Replica.WireProtocol.stream_generation(stream_id)
    epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)

    {
      reg_claim_by_key_table(name, shard),
      {{{cluster, :"$1", origin_node, generation, epoch}, :"$2", :"$3", :"$4", :_}, [],
       [{{:"$1", :"$2", :"$3", :"$4"}}]}
    }
  end

  def replace_registry_claims_for_stream(name, shard, stream_id, snapshot_seq, claims) do
    cluster = Group.Replica.WireProtocol.stream_cluster(stream_id)
    origin_node = Group.Replica.WireProtocol.stream_origin(stream_id)
    generation = Group.Replica.WireProtocol.stream_generation(stream_id)
    epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)
    existing = registry_claims_for_stream(name, shard, stream_id)

    Enum.each(existing, fn {key, pid, _meta, _time} ->
      delete_registry_claim_indexes(
        name,
        shard,
        cluster,
        key,
        origin_node,
        generation,
        epoch,
        pid
      )
    end)

    Enum.each(claims, fn {key, pid, meta, time} ->
      put_registry_claim(name, shard, stream_id, snapshot_seq, key, pid, meta, time)
    end)

    Enum.uniq(Enum.map(existing, &elem(&1, 0)) ++ Enum.map(claims, &elem(&1, 0)))
  end

  def replace_registry_claims_for_stream_from_staging(
        name,
        shard,
        stream_id,
        snapshot_seq,
        staging_table,
        chunk_count,
        acc,
        fun
      )
      when is_function(fun, 2) do
    replace_registry_claims_for_stream_from_staging(
      name,
      shard,
      stream_id,
      snapshot_seq,
      staging_table,
      chunk_count,
      acc,
      fun,
      fn claims, inner ->
        Enum.reduce(claims, inner, fn {key, _pid, _meta, _time}, batch_inner ->
          fun.(key, batch_inner)
        end)
      end
    )
  end

  def replace_registry_claims_for_stream_from_staging(
        name,
        shard,
        stream_id,
        snapshot_seq,
        staging_table,
        _chunk_count,
        acc,
        removed_fun,
        installed_batch_fun
      )
      when is_function(removed_fun, 2) and is_function(installed_batch_fun, 2) do
    cluster = Group.Replica.WireProtocol.stream_cluster(stream_id)
    origin_node = Group.Replica.WireProtocol.stream_origin(stream_id)
    generation = Group.Replica.WireProtocol.stream_generation(stream_id)
    epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)

    acc =
      fold_registry_claims_for_stream(name, shard, stream_id, acc, fn
        {key, pid, _meta, _time}, inner ->
          delete_registry_claim_indexes(
            name,
            shard,
            cluster,
            key,
            origin_node,
            generation,
            epoch,
            pid
          )

          if Group.Replica.Snapshot.member_registry?(staging_table, key) do
            inner
          else
            removed_fun.(key, inner)
          end
      end)

    Group.Replica.Snapshot.reduce_registry_batches(
      staging_table,
      acc,
      fn claims, inner ->
        :ok = insert_exact_registry_claims_many(name, shard, stream_id, snapshot_seq, claims)
        installed_batch_fun.(claims, inner)
      end
    )
  end

  def purge_registry_claims_for_origin(name, shard, origin_node) do
    claims =
      :ets.select(reg_claim_by_key_table(name, shard), [
        {{{:"$1", :"$2", origin_node, :"$3", :"$4"}, :"$5", :"$6", :"$7", :_}, [],
         [{{:"$1", :"$2", :"$5", :"$6", :"$7", :"$3", :"$4"}}]}
      ])

    Enum.each(claims, fn {cluster, key, pid, _meta, _time, generation, epoch} ->
      delete_registry_claim_indexes(
        name,
        shard,
        cluster,
        key,
        origin_node,
        generation,
        epoch,
        pid
      )
    end)

    Enum.uniq(Enum.map(claims, fn {cluster, key, _, _, _, _, _} -> {cluster, key} end))
  end

  def purge_registry_claims_for_streams(_name, _shard, []), do: []

  def purge_registry_claims_for_streams(name, shard, stream_ids) do
    # Select only matching claims into memory. Epoch churn is rare, but the
    # complete claim table may contain millions of unrelated rows.
    match_specs =
      Enum.map(stream_ids, fn stream_id ->
        cluster = Group.Replica.WireProtocol.stream_cluster(stream_id)
        origin = Group.Replica.WireProtocol.stream_origin(stream_id)
        generation = Group.Replica.WireProtocol.stream_generation(stream_id)
        epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)

        {{{cluster, :"$1", origin, generation, epoch}, :"$2", :"$3", :"$4", :"$5"}, [], [:"$_"]}
      end)

    claims = :ets.select(reg_claim_by_key_table(name, shard), match_specs)

    Enum.each(claims, fn {{cluster, key, origin, generation, epoch}, pid, _meta, _time, _seq} ->
      delete_registry_claim_indexes(
        name,
        shard,
        cluster,
        key,
        origin,
        generation,
        epoch,
        pid
      )
    end)

    claims
    |> Enum.map(fn {{cluster, key, _origin, _generation, _epoch}, _pid, _meta, _time, _seq} ->
      {cluster, key}
    end)
    |> Enum.uniq()
  end

  def purge_registry_claims_for_cluster(name, shard, cluster, origin_node \\ :all)

  def purge_registry_claims_for_cluster(name, shard, cluster, :all) do
    claims =
      :ets.select(reg_claim_by_key_table(name, shard), [
        {{{cluster, :"$1", :"$2", :"$3", :"$4"}, :"$5", :"$6", :"$7", :_}, [],
         [{{:"$1", :"$5", :"$6", :"$7", :"$2", :"$3", :"$4"}}]}
      ])

    delete_registry_claim_rows(name, shard, cluster, claims)
  end

  def purge_registry_claims_for_cluster(name, shard, cluster, origin_node) do
    claims =
      :ets.select(reg_claim_by_key_table(name, shard), [
        {{{cluster, :"$1", origin_node, :"$2", :"$3"}, :"$4", :"$5", :"$6", :_}, [],
         [{{:"$1", :"$4", :"$5", :"$6", origin_node, :"$2", :"$3"}}]}
      ])

    delete_registry_claim_rows(name, shard, cluster, claims)
  end

  defp delete_registry_claim_rows(name, shard, cluster, claims) do
    Enum.each(claims, fn {key, pid, _meta, _time, claim_origin, generation, epoch} ->
      delete_registry_claim_indexes(
        name,
        shard,
        cluster,
        key,
        claim_origin,
        generation,
        epoch,
        pid
      )
    end)

    Enum.uniq(Enum.map(claims, &elem(&1, 0)))
  end

  defp delete_registry_claim_indexes(
         name,
         shard,
         cluster,
         key,
         origin,
         generation,
         epoch,
         pid
       ) do
    :ets.delete(reg_claim_by_key_table(name, shard), {cluster, key, origin, generation, epoch})

    :ets.delete(
      reg_claim_by_pid_table(name, shard),
      {pid, cluster, key, origin, generation, epoch}
    )

    :ok
  end

  def local_registry_claims_by_pids(name, shard, pids) do
    local_node = node()

    Enum.flat_map(Enum.uniq(pids), fn pid ->
      :ets.select(reg_claim_by_pid_table(name, shard), [
        {{{pid, :"$1", :"$2", local_node, :"$3", :"$4"}, :"$5", :"$6", :_}, [],
         [{{pid, :"$1", :"$2", :"$5", :"$3", :"$4"}}]}
      ])
    end)
  end

  # =====================================================================
  # Process group operations
  # =====================================================================

  def pg_insert(name, shard, cluster, key, pid, meta, time, node) do
    table = pg_by_key_table(name, shard)
    :ets.insert(table, {{cluster, key, pid}, meta, time, node})
    table_pid = pg_by_pid_table(name, shard)
    :ets.insert(table_pid, {{pid, cluster, key}, meta, time, node})
    :ok
  end

  def pg_insert_many(_name, _shard, []), do: :ok

  def pg_insert_many(name, shard, entries) do
    table = pg_by_key_table(name, shard)
    table_pid = pg_by_pid_table(name, shard)

    :ets.insert(
      table,
      Enum.map(entries, fn {cluster, key, pid, meta, time, node} ->
        {{cluster, key, pid}, meta, time, node}
      end)
    )

    :ets.insert(
      table_pid,
      Enum.map(entries, fn {cluster, key, pid, meta, time, node} ->
        {{pid, cluster, key}, meta, time, node}
      end)
    )

    :ok
  end

  def pg_delete(name, shard, cluster, key, pid) do
    table = pg_by_key_table(name, shard)
    :ets.delete(table, {cluster, key, pid})
    table_pid = pg_by_pid_table(name, shard)
    :ets.delete(table_pid, {pid, cluster, key})
    :ok
  end

  def pg_delete_many(_name, _shard, []), do: :ok

  def pg_delete_many(name, shard, entries) do
    entries = Enum.uniq(entries)
    table = pg_by_key_table(name, shard)
    table_pid = pg_by_pid_table(name, shard)

    :ets.select_delete(
      table,
      Enum.map(entries, fn {cluster, key, pid} ->
        {{{cluster, key, pid}, :_, :_, :_}, [], [true]}
      end)
    )

    :ets.select_delete(
      table_pid,
      Enum.map(entries, fn {cluster, key, pid} ->
        {{{pid, cluster, key}, :_, :_, :_}, [], [true]}
      end)
    )

    :ok
  end

  def pg_lookup(name, shard, cluster, key, pid) do
    table = pg_by_key_table(name, shard)

    case :ets.lookup(table, {cluster, key, pid}) do
      [{{^cluster, ^key, ^pid}, meta, time, node}] ->
        {meta, time, node}

      [] ->
        nil
    end
  end

  def pg_members(name, shard, cluster, key, limit \\ :infinity) do
    table = pg_by_key_table(name, shard)
    # Use match spec to find all entries with the given {cluster, key, _pid} prefix
    match_spec = [
      {{{cluster, key, :"$1"}, :"$2", :_, :_}, [], [{{:"$1", :"$2"}}]}
    ]

    select(table, match_spec, limit)
  end

  def pg_members_with_node(name, shard, cluster, key) do
    table = pg_by_key_table(name, shard)

    match_spec = [
      {{{cluster, key, :"$1"}, :"$2", :_, :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}
    ]

    :ets.select(table, match_spec)
  end

  def pg_members_by_prefix(name, shard, cluster, prefix, limit \\ :infinity) do
    table = pg_by_key_table(name, shard)
    prefix_end = next_binary_prefix(prefix)

    select(
      table,
      [
        {{{cluster, :"$1", :"$2"}, :"$3", :_, :_},
         [{:andalso, {:>=, :"$1", prefix}, {:<, :"$1", prefix_end}}], [{{:"$2", :"$3"}}]}
      ],
      limit
    )
  end

  def pg_members_local(name, shard, cluster, key) do
    local_node = node()
    table = pg_by_key_table(name, shard)

    match_spec = [
      {{{cluster, key, :"$1"}, :_, :_, :"$2"}, [{:==, :"$2", local_node}], [:"$1"]}
    ]

    :ets.select(table, match_spec)
  end

  def pg_members_local_with_meta(name, shard, cluster, key, limit \\ :infinity) do
    local_node = node()
    table = pg_by_key_table(name, shard)

    match_spec = [
      {{{cluster, key, :"$1"}, :"$2", :_, :"$3"}, [{:==, :"$3", local_node}], [{{:"$1", :"$2"}}]}
    ]

    select(table, match_spec, limit)
  end

  def pg_members_local_by_prefix(name, shard, cluster, prefix, limit \\ :infinity) do
    local_node = node()
    table = pg_by_key_table(name, shard)
    prefix_end = next_binary_prefix(prefix)

    select(
      table,
      [
        {{{cluster, :"$1", :"$2"}, :"$3", :_, :"$4"},
         [
           {:==, :"$4", local_node},
           {:andalso, {:>=, :"$1", prefix}, {:<, :"$1", prefix_end}}
         ], [{{:"$2", :"$3"}}]}
      ],
      limit
    )
  end

  @doc """
  Delete all entries for a pid from this shard. Used on process DOWN.
  Returns lean `{cluster, key, meta}` tuples for dispatch.
  """
  def delete_all_for_pid(name, shard, pid) do
    case delete_all_for_pids(name, shard, [pid]) do
      {[{^pid, cluster, key, meta}], pg_entries} ->
        {[{cluster, key, meta}], Enum.map(pg_entries, fn {^pid, c, k, m} -> {c, k, m} end)}

      {reg_entries, pg_entries} ->
        {
          Enum.map(reg_entries, fn {^pid, cluster, key, meta} -> {cluster, key, meta} end),
          Enum.map(pg_entries, fn {^pid, cluster, key, meta} -> {cluster, key, meta} end)
        }
    end
  end

  @doc """
  Delete all entries for the given pids from this shard. Used to batch local
  process DOWN cleanup. Returns lean `{pid, cluster, key, meta}` tuples for
  dispatch/event building.
  """
  def entries_for_pids(_name, _shard, []), do: {[], []}

  def entries_for_pids(name, shard, pids) do
    pids = Enum.uniq(pids)

    {
      select_entries_for_pids(reg_by_pid_table(name, shard), pids),
      select_entries_for_pids(pg_by_pid_table(name, shard), pids)
    }
  end

  def delete_all_for_pids(_name, _shard, []), do: {[], []}

  def delete_all_for_pids(name, shard, pids) do
    pids = Enum.uniq(pids)
    reg_table = reg_by_key_table(name, shard)
    reg_pid_table = reg_by_pid_table(name, shard)

    reg_entries = select_entries_for_pids(reg_pid_table, pids)

    for {_pid, cluster, key, _meta} <- reg_entries do
      :ets.delete(reg_table, {cluster, key})
    end

    select_delete_pids(reg_pid_table, pids)

    pg_table = pg_by_key_table(name, shard)
    pg_pid_table = pg_by_pid_table(name, shard)

    pg_entries = select_entries_for_pids(pg_pid_table, pids)

    for {pid, cluster, key, _meta} <- pg_entries do
      :ets.delete(pg_table, {cluster, key, pid})
    end

    select_delete_pids(pg_pid_table, pids)

    {reg_entries, pg_entries}
  end

  def registry_delete_matching_many(_name, _shard, []), do: []

  def registry_delete_matching_many(name, shard, entries) do
    reg_table = reg_by_key_table(name, shard)
    reg_pid_table = reg_by_pid_table(name, shard)

    Enum.reduce(entries, [], fn {pid, cluster, key, _meta, _reason} = entry, acc ->
      case :ets.lookup(reg_table, {cluster, key}) do
        [{{^cluster, ^key}, ^pid, _current_meta, _time, _node}] ->
          :ets.delete(reg_table, {cluster, key})
          :ets.delete(reg_pid_table, {pid, cluster, key})
          [entry | acc]

        _ ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  def pg_delete_matching_many(_name, _shard, []), do: []

  def pg_delete_matching_many(name, shard, entries) do
    pg_table = pg_by_key_table(name, shard)
    pg_pid_table = pg_by_pid_table(name, shard)

    Enum.reduce(entries, [], fn {pid, cluster, key, _meta, _reason} = entry, acc ->
      case :ets.lookup(pg_table, {cluster, key, pid}) do
        [{{^cluster, ^key, ^pid}, _current_meta, _time, _node}] ->
          :ets.delete(pg_table, {cluster, key, pid})
          :ets.delete(pg_pid_table, {pid, cluster, key})
          [entry | acc]

        _ ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  # =====================================================================
  # Monitor helpers (per-shard, no cross-shard coordination)
  # =====================================================================

  def maybe_demonitor(name, shard, pid) do
    # Check remaining entries for this pid across both tables in this shard
    table_reg = reg_by_pid_table(name, shard)

    has_reg =
      case :ets.select(table_reg, [{{{pid, :_, :_}, :_, :_, :_}, [], [true]}], 1) do
        {[true], _} -> true
        :"$end_of_table" -> false
      end

    has_pg =
      if has_reg do
        true
      else
        table_pg = pg_by_pid_table(name, shard)

        case :ets.select(table_pg, [{{{pid, :_, :_}, :_, :_, :_}, [], [true]}], 1) do
          {[true], _} -> true
          :"$end_of_table" -> false
        end
      end

    if has_reg or has_pg, do: :still_monitored, else: :ok
  end

  # =====================================================================
  # Bulk operations
  # =====================================================================

  def entries_by_pid(name, shard, pid) do
    reg_table = reg_by_pid_table(name, shard)

    reg_entries =
      :ets.select(reg_table, [
        {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.map(fn {cluster, key, meta, time, node} ->
        {:registry, cluster, key, pid, meta, time, node}
      end)

    pg_table = pg_by_pid_table(name, shard)

    pg_entries =
      :ets.select(pg_table, [
        {{{pid, :"$1", :"$2"}, :"$3", :"$4", :"$5"}, [], [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.map(fn {cluster, key, meta, time, node} ->
        {:pg, cluster, key, pid, meta, time, node}
      end)

    reg_entries ++ pg_entries
  end

  def local_entries(name, shard) do
    local_node = node()
    reg_table = reg_by_key_table(name, shard)

    reg_entries =
      :ets.select(reg_table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :_, :"$5"}, [{:==, :"$5", local_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {cluster, key, pid, meta} ->
        {:registry, cluster, key, pid, meta}
      end)

    pg_table = pg_by_key_table(name, shard)

    pg_entries =
      :ets.select(pg_table, [
        {{{:"$1", :"$2", :"$3"}, :"$4", :_, :"$5"}, [{:==, :"$5", local_node}],
         [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])
      |> Enum.map(fn {cluster, key, pid, meta} ->
        {:pg, cluster, key, pid, meta}
      end)

    reg_entries ++ pg_entries
  end

  defp select_entries_for_pids(table, pids) do
    :ets.select(
      table,
      Enum.map(pids, fn pid ->
        {{{pid, :"$1", :"$2"}, :"$3", :_, :_}, [], [{{pid, :"$1", :"$2", :"$3"}}]}
      end)
    )
  end

  defp select_delete_pids(table, pids) do
    :ets.select_delete(
      table,
      Enum.map(pids, fn pid ->
        {{{pid, :_, :_}, :_, :_, :_}, [], [true]}
      end)
    )
  end

  def pg_entries_for_origin(name, shard, cluster, origin_node) do
    fold_pg_entries_for_origin(name, shard, cluster, origin_node, [], fn row, rows ->
      [row | rows]
    end)
    |> Enum.reverse()
  end

  def fold_pg_entries_for_origin(name, shard, cluster, origin_node, acc, fun)
      when is_function(fun, 2) do
    {table, match_spec} = pg_origin_selection(name, shard, cluster, origin_node)

    fold_select_batches_fixed(table, match_spec, acc, fn rows, inner ->
      Enum.reduce(rows, inner, fun)
    end)
  end

  def reduce_pg_entry_batches_for_origin(name, shard, cluster, origin_node, acc, fun)
      when is_function(fun, 2) do
    {table, match_spec} = pg_origin_selection(name, shard, cluster, origin_node)
    fold_select_batches(table, match_spec, acc, fun)
  end

  defp pg_origin_selection(name, shard, cluster, origin_node) do
    {
      pg_by_key_table(name, shard),
      {{{cluster, :"$1", :"$2"}, :"$3", :"$4", origin_node}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
    }
  end

  def delete_pg_for_origin_cluster(name, shard, cluster, origin_node) do
    entries = pg_entries_for_origin(name, shard, cluster, origin_node)

    Enum.each(entries, fn {key, pid, _meta, _time} ->
      pg_delete(name, shard, cluster, key, pid)
    end)

    Enum.map(entries, fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)
  end

  def delete_pg_for_origin_clusters(_name, _shard, [], _origin_node), do: []

  def delete_pg_for_origin_clusters(name, shard, clusters, origin_node) do
    cluster_set = MapSet.new(clusters)

    entries =
      :ets.select(pg_by_key_table(name, shard), [
        {{{:"$1", :"$2", :"$3"}, :"$4", :"$5", origin_node}, [],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.filter(fn {cluster, _key, _pid, _meta, _time} ->
        MapSet.member?(cluster_set, cluster)
      end)

    Enum.each(entries, fn {cluster, key, pid, _meta, _time} ->
      pg_delete(name, shard, cluster, key, pid)
    end)

    entries
  end

  def delete_registry_keys(name, shard, cluster, keys) do
    Enum.flat_map(keys, fn key ->
      case registry_lookup(name, shard, cluster, key) do
        {pid, meta, time, _entry_node} ->
          registry_delete(name, shard, cluster, key, pid)
          [{cluster, key, pid, meta, time}]

        nil ->
          []
      end
    end)
  end

  def delete_pg_cluster(name, shard, cluster) do
    entries =
      :ets.select(pg_by_key_table(name, shard), [
        {{{cluster, :"$1", :"$2"}, :"$3", :"$4", :_}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ])

    Enum.each(entries, fn {key, pid, _meta, _time} ->
      pg_delete(name, shard, cluster, key, pid)
    end)

    Enum.map(entries, fn {key, pid, meta, time} -> {cluster, key, pid, meta, time} end)
  end

  def purge_node(name, shard, dead_node) do
    reg_table = reg_by_key_table(name, shard)
    reg_pid_table = reg_by_pid_table(name, shard)

    purged_reg =
      :ets.select(reg_table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"}, [{:==, :"$6", dead_node}],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    for {cluster, key, pid, _meta, _time} <- purged_reg do
      :ets.delete(reg_table, {cluster, key})
      :ets.delete(reg_pid_table, {pid, cluster, key})
    end

    pg_table = pg_by_key_table(name, shard)
    pg_pid_table = pg_by_pid_table(name, shard)

    purged_pg =
      :ets.select(pg_table, [
        {{{:"$1", :"$2", :"$3"}, :"$4", :"$5", :"$6"}, [{:==, :"$6", dead_node}],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])

    for {cluster, key, pid, _meta, _time} <- purged_pg do
      :ets.delete(pg_table, {cluster, key, pid})
      :ets.delete(pg_pid_table, {pid, cluster, key})
    end

    {purged_reg, purged_pg}
  end

  # =====================================================================
  # Counting
  # =====================================================================

  def registry_count(name, num_shards, cluster) do
    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = reg_by_key_table(name, shard)

      count =
        :ets.select_count(table, [
          {{{cluster, :_}, :_, :_, :_, :_}, [], [true]}
        ])

      acc + count
    end)
  end

  def pg_count(name, shard, cluster, key) do
    table = pg_by_key_table(name, shard)

    :ets.select_count(table, [
      {{{cluster, key, :_}, :_, :_, :_}, [], [true]}
    ])
  end

  def pg_count_by_prefix(name, num_shards, cluster, prefix) do
    prefix_end = next_binary_prefix(prefix)

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = pg_by_key_table(name, shard)

      count =
        :ets.select_count(table, [
          {{{cluster, :"$1", :_}, :_, :_, :_},
           [{:andalso, {:>=, :"$1", prefix}, {:<, :"$1", prefix_end}}], [true]}
        ])

      acc + count
    end)
  end

  def local_registry_count(name, num_shards, cluster) do
    local_node = node()

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = reg_by_key_table(name, shard)

      count =
        :ets.select_count(table, [
          {{{cluster, :_}, :_, :_, :_, :"$1"}, [{:==, :"$1", local_node}], [true]}
        ])

      acc + count
    end)
  end

  def local_registry_present?(name, num_shards, cluster) do
    local_node = node()

    Enum.any?(0..(num_shards - 1), fn shard ->
      table = reg_by_key_table(name, shard)

      select_exists?(table, [
        {{{cluster, :_}, :_, :_, :_, :"$1"}, [{:==, :"$1", local_node}], [true]}
      ])
    end)
  end

  def local_pg_count(name, shard, cluster, key) do
    local_node = node()
    table = pg_by_key_table(name, shard)

    :ets.select_count(table, [
      {{{cluster, key, :_}, :_, :_, :"$1"}, [{:==, :"$1", local_node}], [true]}
    ])
  end

  def local_pg_present?(name, num_shards, cluster) do
    local_node = node()

    Enum.any?(0..(num_shards - 1), fn shard ->
      table = pg_by_key_table(name, shard)

      select_exists?(table, [
        {{{cluster, :_, :_}, :_, :_, :"$1"}, [{:==, :"$1", local_node}], [true]}
      ])
    end)
  end

  def local_pg_count_by_prefix(name, num_shards, cluster, prefix) do
    local_node = node()
    prefix_end = next_binary_prefix(prefix)

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = pg_by_key_table(name, shard)

      count =
        :ets.select_count(table, [
          {{{cluster, :"$1", :_}, :_, :_, :"$2"},
           [
             {:==, :"$2", local_node},
             {:andalso, {:>=, :"$1", prefix}, {:<, :"$1", prefix_end}}
           ], [true]}
        ])

      acc + count
    end)
  end

  defp select_exists?(table, match_spec) do
    case :ets.select(table, match_spec, 1) do
      {[_match], _continuation} -> true
      :"$end_of_table" -> false
    end
  end

  # =====================================================================
  # Cluster membership (dual-index: cluster_nodes + node_clusters)
  # =====================================================================

  def cluster_nodes(name, cluster) do
    table = cluster_nodes_table(name)
    :ets.lookup(table, cluster) |> Enum.map(&elem(&1, 1))
  end

  def add_cluster_node(name, clusters, node) when is_list(clusters) do
    GenServer.call(data_name(name), {:add_cluster_node, clusters, node}, :infinity)
  end

  def remove_cluster_node(name, clusters, node) when is_list(clusters) do
    GenServer.call(data_name(name), {:remove_cluster_node, clusters, node}, :infinity)
  end

  def remove_clusters(name, clusters) when is_list(clusters) do
    GenServer.call(data_name(name), {:remove_clusters, clusters}, :infinity)
  end

  def all_clusters(name) do
    table = cluster_nodes_table(name)
    :ets.select(table, [{{:"$1", :_}, [], [:"$1"]}]) |> Enum.uniq()
  end

  def my_clusters(name) do
    table = node_clusters_table(name)
    :ets.lookup(table, node()) |> Enum.map(&elem(&1, 1))
  end

  def clusters_for_node(name, target_node) do
    :ets.lookup(node_clusters_table(name), target_node) |> Enum.map(&elem(&1, 1))
  end

  def purge_cluster_node(name, dead_node) do
    GenServer.call(data_name(name), {:purge_cluster_node, dead_node}, :infinity)
  end

  # =====================================================================
  # Local cluster TTL leases
  # =====================================================================

  def put_cluster_lease(name, cluster, ttl_ms, expires_at) do
    :ets.insert(cluster_leases_table(name), {cluster, ttl_ms, expires_at})
    :ok
  end

  def delete_cluster_lease(name, cluster) do
    :ets.delete(cluster_leases_table(name), cluster)
    :ok
  end

  def cluster_lease(name, cluster) do
    case :ets.lookup(cluster_leases_table(name), cluster) do
      [{^cluster, ttl_ms, expires_at}] -> {ttl_ms, expires_at}
      [] -> nil
    end
  end

  def cluster_leases(name) do
    :ets.tab2list(cluster_leases_table(name))
  end

  # =====================================================================
  # Helpers
  # =====================================================================

  defp next_binary_prefix(prefix) do
    size = byte_size(prefix) - 1
    <<head::binary-size(^size), last_byte>> = prefix
    <<head::binary, last_byte + 1>>
  end

  # =====================================================================
  # Table names
  # =====================================================================

  def reg_by_key_table(name, shard), do: :"#{name}_s#{shard}_reg_by_key"
  def reg_by_pid_table(name, shard), do: :"#{name}_s#{shard}_reg_by_pid"
  def reg_claim_by_key_table(name, shard), do: :"#{name}_s#{shard}_reg_claim_by_key"
  def reg_claim_by_pid_table(name, shard), do: :"#{name}_s#{shard}_reg_claim_by_pid"
  def pg_by_key_table(name, shard), do: :"#{name}_s#{shard}_pg_by_key"
  def pg_by_pid_table(name, shard), do: :"#{name}_s#{shard}_pg_by_pid"
  def cluster_nodes_table(name), do: :"#{name}_cluster_nodes"
  def node_clusters_table(name), do: :"#{name}_node_clusters"
  def cluster_leases_table(name), do: :"#{name}_cluster_leases"
  def replication_meta_table(name), do: :"#{name}_replication_meta"
  def local_cluster_epochs_table(name), do: :"#{name}_local_cluster_epochs"
  def closed_local_cluster_epochs_table(name), do: :"#{name}_closed_local_cluster_epochs"
  def remote_cluster_epochs_table(name), do: :"#{name}_remote_cluster_epochs"
  def replica_stream_meta_table(name, shard), do: :"#{name}_s#{shard}_replica_stream_meta"
  def replica_oplog_table(name, shard), do: :"#{name}_s#{shard}_replica_oplog"
  def replica_oplog_order_table(name, shard), do: :"#{name}_s#{shard}_replica_oplog_order"
  def replica_cursor_table(name, shard), do: :"#{name}_s#{shard}_replica_cursor"

  # =====================================================================
  # GenServer callbacks
  # =====================================================================

  defp replace_remote_cluster_projection(name, remote_node, remote_epochs) do
    local_clusters =
      local_cluster_epochs_table(name)
      |> :ets.select([{{:"$1", :_}, [], [:"$1"]}])
      |> MapSet.new()
      |> MapSet.put(nil)

    shared_clusters =
      remote_epochs
      |> Map.keys()
      |> Enum.filter(&MapSet.member?(local_clusters, &1))
      |> MapSet.new()

    previous_clusters = MapSet.new(clusters_for_node(name, remote_node))

    previous_clusters
    |> MapSet.difference(shared_clusters)
    |> Enum.each(fn cluster ->
      :ets.delete_object(cluster_nodes_table(name), {cluster, remote_node})
      :ets.delete_object(node_clusters_table(name), {remote_node, cluster})
    end)

    rows = MapSet.to_list(shared_clusters)
    :ets.insert(cluster_nodes_table(name), Enum.map(rows, &{&1, remote_node}))
    :ets.insert(node_clusters_table(name), Enum.map(rows, &{remote_node, &1}))
    :ok
  end

  defp insert_cluster_nodes(name, clusters, target_node) do
    :ets.insert(cluster_nodes_table(name), Enum.map(clusters, &{&1, target_node}))
    :ets.insert(node_clusters_table(name), Enum.map(clusters, &{target_node, &1}))
    :ok
  end

  defp delete_cluster_nodes(name, clusters, target_node) do
    Enum.each(clusters, fn cluster ->
      :ets.delete_object(cluster_nodes_table(name), {cluster, target_node})
      :ets.delete_object(node_clusters_table(name), {target_node, cluster})
    end)

    :ok
  end

  defp delete_cluster_routes(name, clusters) do
    Enum.each(clusters, fn cluster ->
      nodes = cluster_nodes(name, cluster)
      :ets.delete(cluster_nodes_table(name), cluster)

      Enum.each(nodes, fn cluster_node ->
        :ets.delete_object(node_clusters_table(name), {cluster_node, cluster})
      end)
    end)

    :ok
  end

  defp delete_peer_routes(name, remote_node) do
    # Scan the forward index directly so this also repairs a one-sided row left
    # by an interrupted or older dual-index mutation.
    :ets.select_delete(cluster_nodes_table(name), [
      {{:_, remote_node}, [], [true]}
    ])

    :ets.delete(node_clusters_table(name), remote_node)
    :ok
  end

  defp project_activated_local_clusters(name, clusters) do
    :ok = insert_cluster_nodes(name, clusters, node())

    name
    |> cluster_nodes(nil)
    |> Enum.reject(&(&1 == node()))
    |> Enum.each(fn remote_node ->
      shared =
        Enum.filter(clusters, fn cluster ->
          not is_nil(remote_cluster_epoch(name, remote_node, cluster))
        end)

      :ok = insert_cluster_nodes(name, shared, remote_node)
    end)

    :ok
  end

  defp cast_cluster_lifecycle(name, shards, request) do
    Enum.each(shards, fn shard ->
      :ok = Group.Replica.local_cast(Group.Replica.shard_name(name, shard), request)
    end)
  end

  defp activate_local_clusters(state, clusters, durable?) do
    if clusters != [] do
      :ets.update_counter(
        replication_meta_table(state.name),
        :cluster_epoch_revision,
        {2, 1},
        {:cluster_epoch_revision, 0}
      )
    end

    epochs =
      Enum.map(clusters, fn cluster ->
        epoch =
          case :ets.lookup(local_cluster_epochs_table(state.name), cluster) do
            [{^cluster, existing}] -> existing
            [] -> make_ref()
          end

        :ets.insert(local_cluster_epochs_table(state.name), {cluster, epoch})
        :ets.delete(closed_local_cluster_epochs_table(state.name), cluster)
        {cluster, epoch}
      end)

    if durable?, do: project_activated_local_clusters(state.name, clusters)

    {epochs, state}
  end

  defp deactivate_local_clusters(state, clusters, durable?) do
    if clusters != [] do
      :ets.update_counter(
        replication_meta_table(state.name),
        :cluster_epoch_revision,
        {2, 1},
        {:cluster_epoch_revision, 0}
      )
    end

    epochs =
      Enum.map(clusters, fn cluster ->
        epoch = local_cluster_epoch(state.name, cluster)
        :ets.delete(local_cluster_epochs_table(state.name), cluster)

        if epoch do
          pending_shards = MapSet.new(0..(state.num_shards - 1))

          :ets.insert(
            closed_local_cluster_epochs_table(state.name),
            {cluster, epoch, pending_shards}
          )
        end

        {cluster, epoch}
      end)

    if durable? do
      :ok = delete_cluster_nodes(state.name, clusters, node())

      cast_cluster_lifecycle(
        state.name,
        0..(state.num_shards - 1),
        {:cluster_disconnect, clusters, epochs}
      )
    end

    {epochs, state}
  end

  @impl true
  def handle_call({:add_cluster_node, clusters, node}, _from, state) do
    :ok = insert_cluster_nodes(state.name, clusters, node)
    {:reply, :ok, state}
  end

  def handle_call({:remove_cluster_node, clusters, node}, _from, state) do
    :ok = delete_cluster_nodes(state.name, clusters, node)
    {:reply, :ok, state}
  end

  def handle_call({:remove_clusters, clusters}, _from, state) do
    # Startup repair discovers inactive clusters outside the Data process. A
    # reconnect may install a new epoch before this serialized cleanup runs;
    # recheck authority here so stale repair work cannot erase the new routes.
    inactive_clusters =
      Enum.filter(clusters, &is_nil(local_cluster_epoch(state.name, &1)))

    :ok = delete_cluster_routes(state.name, inactive_clusters)

    {:reply, :ok, state}
  end

  def handle_call({:purge_cluster_node, dead_node}, _from, state) do
    # Nodedown/lease callers may resume after a newer exact authority has
    # already reinstalled this peer. Recheck the serialized authority fence so
    # stale cleanup cannot erase routes belonging to the new incarnation.
    if is_nil(remote_generation(state.name, dead_node)) and
         is_nil(remote_replica_authority_hint(state.name, dead_node)) do
      :ok = delete_peer_routes(state.name, dead_node)
    end

    {:reply, :ok, state}
  end

  def handle_call({:activate_local_clusters, clusters}, _from, state) do
    {epochs, state} = activate_local_clusters(state, clusters, false)
    {:reply, epochs, state}
  end

  def handle_call({:activate_local_clusters_durable, clusters}, _from, state) do
    {epochs, state} = activate_local_clusters(state, clusters, true)
    {:reply, epochs, state}
  end

  def handle_call(:local_replica_authority, _from, state) do
    generation = generation(state.name)
    revision = local_cluster_epoch_revision(state.name)
    epochs = [{nil, generation} | :ets.tab2list(local_cluster_epochs_table(state.name))]
    {:reply, {generation, revision, epochs}, state}
  end

  def handle_call({:deactivate_local_clusters, clusters}, _from, state) do
    {epochs, state} = deactivate_local_clusters(state, clusters, false)
    {:reply, epochs, state}
  end

  def handle_call({:deactivate_local_clusters_durable, clusters}, _from, state) do
    {epochs, state} = deactivate_local_clusters(state, clusters, true)
    {:reply, epochs, state}
  end

  def handle_call({:mark_closed_cluster_shard, cluster_epochs, shard}, _from, state) do
    completed =
      Enum.reduce(cluster_epochs, [], fn {cluster, request_epoch}, acc ->
        case :ets.lookup(closed_local_cluster_epochs_table(state.name), cluster) do
          [{^cluster, ^request_epoch, pending_shards}] ->
            pending_shards = MapSet.delete(pending_shards, shard)

            if MapSet.size(pending_shards) == 0 do
              # The close marker is the durable recovery obligation. Remove
              # every route before deleting it so a caller and the final shard
              # can both disappear immediately after this acknowledgement
              # without leaving an unowned cluster membership behind.
              :ok = delete_cluster_routes(state.name, [cluster])
              :ets.delete(closed_local_cluster_epochs_table(state.name), cluster)
              [cluster | acc]
            else
              :ets.insert(
                closed_local_cluster_epochs_table(state.name),
                {cluster, request_epoch, pending_shards}
              )

              acc
            end

          _stale_or_completed ->
            acc
        end
      end)

    {:reply, Enum.reverse(completed), state}
  end

  def handle_call(
        {:put_remote_replica_info, shard, remote_node, generation, epoch_revision, epochs},
        _from,
        state
      ) do
    # The epoch snapshot is node-wide authority, not shard-local replica data.
    # Only shard 0 sends it, and Data serializes the one exact replacement for
    # every local replica lane. Keeping the argument in the API makes the
    # control-owner invariant explicit and catches accidental reintroduction of
    # one full copy per shard.
    0 = shard
    seen_generation = remote_generation(state.name, remote_node)

    stale? =
      stale_remote_authority_install?(
        state.name,
        remote_node,
        generation,
        epoch_revision
      )

    if stale? do
      {:reply, :stale, state}
    else
      current_epochs = Map.new(epochs)

      stale_epochs =
        if seen_generation == generation do
          for {{^remote_node, cluster}, epoch} <-
                :ets.match_object(
                  remote_cluster_epochs_table(state.name),
                  {{remote_node, :_}, :_}
                ),
              not is_nil(cluster),
              Map.get(current_epochs, cluster) != epoch,
              do: {cluster, epoch}
        else
          []
        end

      # A hello is a complete epoch snapshot. The replica handler fences older
      # revisions before this call, so replace the shared view rather than merely
      # adding rows; otherwise a dropped close control could leave a cluster epoch
      # permanently valid after the heartbeat-driven repair.
      :ets.select_delete(remote_cluster_epochs_table(state.name), [
        {{{remote_node, :_}, :_}, [], [true]}
      ])

      :ets.insert(
        replication_meta_table(state.name),
        {{:remote_generation, remote_node}, generation}
      )

      :ets.insert(
        replication_meta_table(state.name),
        {{:remote_epoch_revision, remote_node}, epoch_revision}
      )

      :ets.insert(
        replication_meta_table(state.name),
        {{:remote_epoch_exact, remote_node}, epoch_revision}
      )

      :ets.insert(
        replication_meta_table(state.name),
        {{:remote_epoch_observed, remote_node}, epoch_revision}
      )

      put_remote_authority_hint(state.name, remote_node, generation, epoch_revision)

      replace_remote_cluster_projection(state.name, remote_node, current_epochs)

      :ets.update_counter(
        replication_meta_table(state.name),
        {:remote_authority_installs, remote_node},
        {2, 1},
        {{:remote_authority_installs, remote_node}, 0}
      )

      rows =
        for {cluster, epoch} <- epochs,
            not is_nil(cluster),
            do: {{remote_node, cluster}, epoch}

      :ets.insert(remote_cluster_epochs_table(state.name), rows)

      {:reply, {seen_generation, stale_epochs}, state}
    end
  end

  def handle_call(
        {:put_remote_view_info, shard, remote_node, generation, authoritative, observed},
        _from,
        state
      ) do
    if remote_generation(state.name, remote_node) == generation and
         remote_cluster_epoch_exact_revision(state.name, remote_node) == authoritative and
         remote_cluster_epoch_revision(state.name, remote_node) == observed and
         remote_cluster_epoch_observed_revision(state.name, remote_node) == observed and
         remote_replica_authority_hint(state.name, remote_node) == {generation, observed} do
      :ets.insert(
        replication_meta_table(state.name),
        {{:remote_view_info, shard, remote_node}, generation, authoritative, observed}
      )

      {:reply, :ok, state}
    else
      {:reply, :stale, state}
    end
  end

  def handle_call(
        {:put_remote_cluster_epochs, shard, remote_node, generation, expected_revision, revision,
         epochs},
        _from,
        state
      ) do
    0 = shard

    if incremental_authority_installable?(
         state.name,
         remote_node,
         generation,
         expected_revision,
         revision
       ) do
      observe_remote_cluster_revision(state.name, remote_node, revision, state.num_shards)

      stale_epochs =
        Enum.flat_map(epochs, fn {cluster, epoch} ->
          case remote_cluster_epoch(state.name, remote_node, cluster) do
            old_epoch when not is_nil(old_epoch) and old_epoch != epoch -> [{cluster, old_epoch}]
            _ -> []
          end
        end)

      rows =
        for {cluster, epoch} <- epochs,
            not is_nil(cluster),
            do: {{remote_node, cluster}, epoch}

      :ets.insert(remote_cluster_epochs_table(state.name), rows)

      :ets.insert(
        replication_meta_table(state.name),
        {{:remote_epoch_revision, remote_node}, revision}
      )

      {:reply, {:ok, stale_epochs}, state}
    else
      {:reply, :stale, state}
    end
  end

  def handle_call(
        {:remote_registry_claim_authoritative, shard, remote_node, generation, cluster, epoch},
        _from,
        state
      ) do
    exact = remote_cluster_epoch_exact_revision(state.name, remote_node)
    observed = remote_cluster_epoch_observed_revision(state.name, remote_node)

    current_view? =
      case :ets.lookup(
             replication_meta_table(state.name),
             {:remote_view_info, shard, remote_node}
           ) do
        [{{:remote_view_info, ^shard, ^remote_node}, ^generation, ^exact, ^observed}] -> true
        _ -> false
      end

    authoritative? =
      current_view? and remote_generation(state.name, remote_node) == generation and
        remote_cluster_epoch_revision(state.name, remote_node) == observed and
        remote_replica_authority_hint(state.name, remote_node) == {generation, observed} and
        remote_cluster_epoch(state.name, remote_node, cluster) == epoch and
        (is_nil(cluster) or active_local_cluster?(state.name, cluster))

    {:reply, authoritative?, state}
  end

  def handle_call(
        {:observe_remote_cluster_epoch_revision, remote_node, revision},
        _from,
        state
      ) do
    :ok = observe_remote_cluster_revision(state.name, remote_node, revision, state.num_shards)
    {:reply, :ok, state}
  end

  def handle_call(
        {:observe_remote_replica_hint, remote_node, generation, revision},
        _from,
        state
      ) do
    known_generation = remote_generation(state.name, remote_node)

    {hint_generation, hint_revision} =
      remote_replica_authority_hint(state.name, remote_node) ||
        {known_generation, remote_cluster_epoch_observed_revision(state.name, remote_node)}

    fenced? =
      cond do
        hint_generation == generation and
            (is_nil(hint_revision) or revision > hint_revision) ->
          put_remote_authority_hint(state.name, remote_node, generation, revision)

          if known_generation == generation do
            :ok =
              observe_remote_cluster_revision(
                state.name,
                remote_node,
                revision,
                state.num_shards
              )
          end

          true

        not is_nil(hint_generation) and
            WireProtocol.generation_newer?(generation, hint_generation) ->
          # This one shared row is the cross-lane admission fence. Replica lanes
          # include it in replica_view_current?/2, so no lane can admit the prior
          # generation after this insert even while the per-lane breadcrumbs
          # below are being updated. Existing materialized projections may remain
          # visible until exact-authority repair or bounded lease retirement.
          put_remote_authority_hint(state.name, remote_node, generation, revision)

          # Preserve each lane's old generation as the later exact install's
          # purge key, but make its authority revisions impossible to match.
          # This fences every shard in the same Data turn even when only one
          # sideband lane observes the restarted origin first.
          state.name
          |> replication_meta_table()
          |> :ets.match_object({{:remote_view_info, :_, remote_node}, :_, :_, :_})
          |> Enum.each(fn {key, lane_generation, _exact, _observed} ->
            :ets.insert(
              replication_meta_table(state.name),
              {key, lane_generation, nil, nil}
            )
          end)

          true

        true ->
          false
      end

    {:reply, fenced?, state}
  end

  def handle_call(
        {:close_remote_cluster_epochs, shard, remote_node, generation, expected_revision,
         revision, epochs},
        _from,
        state
      ) do
    0 = shard

    if incremental_authority_installable?(
         state.name,
         remote_node,
         generation,
         expected_revision,
         revision
       ) do
      observe_remote_cluster_revision(state.name, remote_node, revision, state.num_shards)

      closed =
        Enum.filter(epochs, fn {cluster, epoch} ->
          remote_cluster_epoch(state.name, remote_node, cluster) == epoch
        end)

      Enum.each(epochs, fn {cluster, epoch} ->
        case :ets.lookup(remote_cluster_epochs_table(state.name), {remote_node, cluster}) do
          [{{^remote_node, ^cluster}, ^epoch}] ->
            :ets.delete(remote_cluster_epochs_table(state.name), {remote_node, cluster})

          _ ->
            :ok
        end
      end)

      :ets.insert(
        replication_meta_table(state.name),
        {{:remote_epoch_revision, remote_node}, revision}
      )

      {:reply, {:ok, closed}, state}
    else
      {:reply, :stale, state}
    end
  end

  def handle_call({:delete_remote_replica_info, shard, remote_node}, _from, state) do
    :ets.delete(
      replication_meta_table(state.name),
      {:remote_view_info, shard, remote_node}
    )

    if shard == 0 do
      delete_remote_authority(state.name, remote_node)
    end

    {:reply, :ok, state}
  end

  def handle_call({:expire_remote_replica_lane, shard, remote_node}, _from, state) do
    :ets.delete(
      replication_meta_table(state.name),
      {:remote_view_info, shard, remote_node}
    )

    remaining_lanes =
      :ets.select_count(replication_meta_table(state.name), [
        {{{:remote_view_info, :_, remote_node}, :_, :_, :_}, [], [true]}
      ])

    result =
      if remaining_lanes == 0 do
        delete_remote_authority(state.name, remote_node)
        :node_retired
      else
        :lane_retired
      end

    {:reply, result, state}
  end

  @impl true
  def init({name, num_shards}) do
    # ETS performance options:
    # - read_concurrency: splits table into read-optimized segments (less lock contention)
    # - decentralized_counters: reduces contention on table size counter (OTP 23+)
    # Per-shard tables omit write_concurrency because each shard GenServer serializes
    # their writes. The shared replication metadata table is different: every shard
    # atomically advances its own {:append_counter, shard} object there, so it needs
    # concurrent writes without weakening ETS's single-object atomicity.
    set_opts = [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      decentralized_counters: true
    ]

    shared_meta_opts = Keyword.put(set_opts, :write_concurrency, :auto)

    ordered_set_opts = [
      :ordered_set,
      :public,
      :named_table,
      read_concurrency: true,
      decentralized_counters: true
    ]

    bag_opts = [
      :bag,
      :public,
      :named_table,
      read_concurrency: true,
      decentralized_counters: true
    ]

    for shard <- 0..(num_shards - 1) do
      :ets.new(reg_by_key_table(name, shard), set_opts)
      :ets.new(reg_by_pid_table(name, shard), ordered_set_opts)
      :ets.new(reg_claim_by_key_table(name, shard), ordered_set_opts)
      :ets.new(reg_claim_by_pid_table(name, shard), ordered_set_opts)
      :ets.new(pg_by_key_table(name, shard), ordered_set_opts)
      :ets.new(pg_by_pid_table(name, shard), ordered_set_opts)
      :ets.new(replica_stream_meta_table(name, shard), set_opts)
      :ets.new(replica_oplog_table(name, shard), ordered_set_opts)
      :ets.new(replica_oplog_order_table(name, shard), ordered_set_opts)
      :ets.new(replica_cursor_table(name, shard), set_opts)
    end

    :ets.new(cluster_nodes_table(name), bag_opts)
    :ets.new(node_clusters_table(name), bag_opts)
    :ets.new(cluster_leases_table(name), set_opts)
    :ets.new(replication_meta_table(name), shared_meta_opts)
    :ets.new(local_cluster_epochs_table(name), set_opts)
    :ets.new(closed_local_cluster_epochs_table(name), set_opts)
    :ets.new(remote_cluster_epochs_table(name), set_opts)
    :ets.insert(replication_meta_table(name), {:generation, WireProtocol.new_generation()})
    :ets.insert(replication_meta_table(name), {:cluster_epoch_revision, 0})

    {:ok, %{name: name, num_shards: num_shards}}
  end

  defp observe_remote_cluster_revision(name, remote_node, revision, _num_shards) do
    key = {:remote_epoch_observed, remote_node}

    case :ets.lookup(replication_meta_table(name), key) do
      [{^key, current}] when current >= revision -> :ok
      _ -> :ets.insert(replication_meta_table(name), {key, revision})
    end

    case remote_generation(name, remote_node) do
      generation when not is_nil(generation) ->
        put_remote_authority_hint(name, remote_node, generation, revision)

      nil ->
        :ok
    end

    :ok
  end

  defp put_remote_authority_hint(name, remote_node, generation, revision) do
    key = {:remote_authority_hint, remote_node}

    install? =
      case remote_replica_authority_hint(name, remote_node) do
        nil ->
          true

        {^generation, current_revision} ->
          revision > current_revision

        {current_generation, _current_revision} ->
          WireProtocol.generation_newer?(generation, current_generation)
      end

    if install? do
      :ets.insert(replication_meta_table(name), {key, generation, revision})
    end

    :ok
  end

  defp stale_remote_authority_install?(name, remote_node, generation, revision) do
    known_generation = remote_generation(name, remote_node)
    authoritative_revision = remote_cluster_epoch_revision(name, remote_node)
    observed_revision = remote_cluster_epoch_observed_revision(name, remote_node)

    hinted_stale? =
      case remote_replica_authority_hint(name, remote_node) do
        nil ->
          false

        {^generation, hinted_revision} ->
          revision < hinted_revision

        {hinted_generation, _hinted_revision} ->
          not WireProtocol.generation_newer?(generation, hinted_generation)
      end

    known_stale? =
      not is_nil(known_generation) and known_generation != generation and
        not WireProtocol.generation_newer?(generation, known_generation)

    revision_stale? =
      known_generation == generation and
        Enum.any?([authoritative_revision, observed_revision], fn
          current when is_integer(current) -> revision < current
          _ -> false
        end)

    hinted_stale? or known_stale? or revision_stale?
  end

  defp incremental_authority_installable?(
         name,
         remote_node,
         generation,
         expected_revision,
         revision
       ) do
    is_integer(expected_revision) and is_integer(revision) and revision > expected_revision and
      remote_generation(name, remote_node) == generation and
      remote_cluster_epoch_revision(name, remote_node) == expected_revision and
      remote_cluster_epoch_observed_revision(name, remote_node) == expected_revision and
      remote_replica_authority_hint(name, remote_node) == {generation, expected_revision}
  end

  defp delete_remote_authority(name, remote_node) do
    # Fence every public-ETS reader first. The remaining mutations execute in
    # this same Data callback, so a caller cannot observe completion between
    # them; if Data itself dies, it takes every owned table with it. Removing
    # the generation before routing therefore closes the read interleaving
    # where a sibling lane could still accept data during retirement.
    :ets.delete(replication_meta_table(name), {:remote_generation, remote_node})
    :ok = delete_peer_routes(name, remote_node)
    :ets.delete(replication_meta_table(name), {:remote_epoch_revision, remote_node})
    :ets.delete(replication_meta_table(name), {:remote_epoch_exact, remote_node})
    :ets.delete(replication_meta_table(name), {:remote_epoch_observed, remote_node})
    :ets.delete(replication_meta_table(name), {:remote_authority_hint, remote_node})
    :ets.delete(replication_meta_table(name), {:remote_authority_installs, remote_node})

    :ets.select_delete(remote_cluster_epochs_table(name), [
      {{{remote_node, :_}, :_}, [], [true]}
    ])

    :ok
  end

  defp fold_select_batches(table, match_spec, acc, fun) do
    case :ets.select(table, [match_spec], 4_096) do
      :"$end_of_table" -> acc
      {matches, continuation} -> fold_select_batches(continuation, fun.(matches, acc), fun)
    end
  end

  defp fold_select_batches_fixed(table, match_spec, acc, fun) do
    # Some consumers delete the selected origin slice while folding it. Keep
    # the traversal fixed so deleting the current batch cannot move unseen
    # ordered-set keys past the continuation and leave a permanent stale row.
    :ets.safe_fixtable(table, true)

    try do
      fold_select_batches(table, match_spec, acc, fun)
    after
      :ets.safe_fixtable(table, false)
    end
  end

  defp fold_select_batches(continuation, acc, fun) do
    case :ets.select(continuation) do
      :"$end_of_table" -> acc
      {matches, next} -> fold_select_batches(next, fun.(matches, acc), fun)
    end
  end

  defp select(table, match_spec, :infinity), do: :ets.select(table, match_spec)
  defp select(_table, _match_spec, 0), do: []

  defp select(table, match_spec, limit) when is_integer(limit) and limit > 0 do
    case :ets.select(table, match_spec, limit) do
      :"$end_of_table" -> []
      {matches, _continuation} -> matches
    end
  end
end
