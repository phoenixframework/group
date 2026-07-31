defmodule Group.Replica.Data do
  @moduledoc false
  use GenServer

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
  clusters. The nil cluster is maintained by the peer_connect protocol — nodes are added on
  peer discovery and removed on nodedown/shard death. `Group.nodes/1` reads nil cluster
  from cluster_nodes.

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

  ## Match Spec Patterns

  All match specs use `{:==, :"$N", value}` guards to filter on runtime values (e.g. node
  name). This is the correct ETS match spec syntax — `:const` is not valid. Literal values
  from Elixir variables (like `cluster` or `key`) are interpolated directly into the match
  pattern tuple positions and work as exact-match filters without needing a guard.

  ## Bulk Operations & Their Costs

  - `purge_node/3`: Full table scan via `ets.select` filtering by node, then individual
    deletes. O(table size) for the scan, but this only runs on nodedown — rare path.

  - `local_data_by_cluster/3`: Full table scan filtering by `node() == local_node`,
    grouped by cluster. Only runs during discovery/sync protocol.

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

  Remote data doesn't need this protection — the discovery protocol re-syncs everything
  from remote nodes on restart. Only local process entries need the ETS scan.

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
      [{^cluster, epoch}] -> epoch
      [] -> nil
    end
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

  def put_remote_cluster_epochs(name, shard, remote_node, revision, epochs) do
    GenServer.call(
      data_name(name),
      {:put_remote_cluster_epochs, shard, remote_node, revision, epochs},
      :infinity
    )
  end

  def close_remote_cluster_epochs(name, shard, remote_node, revision, epochs) do
    GenServer.call(
      data_name(name),
      {:close_remote_cluster_epochs, shard, remote_node, revision, epochs},
      :infinity
    )
  end

  def forget_remote_cluster_epochs(name, shard, remote_node, epochs) do
    GenServer.call(
      data_name(name),
      {:forget_remote_cluster_epochs, shard, remote_node, epochs},
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

  def activate_local_clusters(name, clusters) do
    GenServer.call(data_name(name), {:activate_local_clusters, clusters}, :infinity)
  end

  def deactivate_local_clusters(name, clusters) do
    GenServer.call(data_name(name), {:deactivate_local_clusters, clusters}, :infinity)
  end

  def local_stream_id(name, shard, cluster) do
    case local_cluster_epoch(name, cluster) do
      nil ->
        nil

      epoch ->
        Group.Replica.Protocol.stream_id(
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

  def replica_stream_heads(name, shard) do
    :ets.tab2list(replica_stream_meta_table(name, shard))
    |> Enum.map(fn {stream_id, head, floor, _applied} -> {stream_id, floor, head} end)
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
      [{^stream_id, seq}] -> seq
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

  def drop_local_stream(name, shard, cluster, epoch) do
    stream_id =
      Group.Replica.Protocol.stream_id(name, node(), generation(name), shard, cluster, epoch)

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
    table = reg_by_key_table(name, shard)

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
    cluster = Group.Replica.Protocol.stream_cluster(stream_id)
    origin_node = Group.Replica.Protocol.stream_origin(stream_id)
    generation = Group.Replica.Protocol.stream_generation(stream_id)
    epoch = Group.Replica.Protocol.stream_epoch(stream_id)
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

  def delete_registry_claim(name, shard, stream_id, seq, key, pid) do
    cluster = Group.Replica.Protocol.stream_cluster(stream_id)
    origin_node = Group.Replica.Protocol.stream_origin(stream_id)
    generation = Group.Replica.Protocol.stream_generation(stream_id)
    epoch = Group.Replica.Protocol.stream_epoch(stream_id)
    claim_key = {cluster, key, origin_node, generation, epoch}

    case :ets.lookup(reg_claim_by_key_table(name, shard), claim_key) do
      [{^claim_key, ^pid, _meta, _time, old_seq}] when old_seq <= seq ->
        :ets.delete(reg_claim_by_key_table(name, shard), claim_key)

        :ets.delete(
          reg_claim_by_pid_table(name, shard),
          {pid, cluster, key, origin_node, generation, epoch}
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

  def registry_claims_for_stream(name, shard, stream_id) do
    cluster = Group.Replica.Protocol.stream_cluster(stream_id)
    origin_node = Group.Replica.Protocol.stream_origin(stream_id)
    generation = Group.Replica.Protocol.stream_generation(stream_id)
    epoch = Group.Replica.Protocol.stream_epoch(stream_id)

    :ets.select(reg_claim_by_key_table(name, shard), [
      {{{cluster, :"$1", origin_node, generation, epoch}, :"$2", :"$3", :"$4", :_}, [],
       [{{:"$1", :"$2", :"$3", :"$4"}}]}
    ])
  end

  def replace_registry_claims_for_stream(name, shard, stream_id, snapshot_seq, claims) do
    cluster = Group.Replica.Protocol.stream_cluster(stream_id)
    origin_node = Group.Replica.Protocol.stream_origin(stream_id)
    generation = Group.Replica.Protocol.stream_generation(stream_id)
    epoch = Group.Replica.Protocol.stream_epoch(stream_id)
    existing = registry_claims_for_stream(name, shard, stream_id)

    Enum.each(existing, fn {key, pid, _meta, _time} ->
      :ets.delete(
        reg_claim_by_key_table(name, shard),
        {cluster, key, origin_node, generation, epoch}
      )

      :ets.delete(
        reg_claim_by_pid_table(name, shard),
        {pid, cluster, key, origin_node, generation, epoch}
      )
    end)

    Enum.each(claims, fn {key, pid, meta, time} ->
      put_registry_claim(name, shard, stream_id, snapshot_seq, key, pid, meta, time)
    end)

    Enum.uniq(Enum.map(existing, &elem(&1, 0)) ++ Enum.map(claims, &elem(&1, 0)))
  end

  def purge_registry_claims_for_origin(name, shard, origin_node) do
    claims =
      :ets.select(reg_claim_by_key_table(name, shard), [
        {{{:"$1", :"$2", origin_node, :"$3", :"$4"}, :"$5", :"$6", :"$7", :_}, [],
         [{{:"$1", :"$2", :"$5", :"$6", :"$7", :"$3", :"$4"}}]}
      ])

    Enum.each(claims, fn {cluster, key, pid, _meta, _time, generation, epoch} ->
      :ets.delete(
        reg_claim_by_key_table(name, shard),
        {cluster, key, origin_node, generation, epoch}
      )

      :ets.delete(
        reg_claim_by_pid_table(name, shard),
        {pid, cluster, key, origin_node, generation, epoch}
      )
    end)

    Enum.uniq(Enum.map(claims, fn {cluster, key, _, _, _, _, _} -> {cluster, key} end))
  end

  def purge_registry_claims_for_streams(_name, _shard, []), do: []

  def purge_registry_claims_for_streams(name, shard, stream_ids) do
    streams =
      MapSet.new(stream_ids, fn stream_id ->
        {
          Group.Replica.Protocol.stream_cluster(stream_id),
          Group.Replica.Protocol.stream_origin(stream_id),
          Group.Replica.Protocol.stream_generation(stream_id),
          Group.Replica.Protocol.stream_epoch(stream_id)
        }
      end)

    claims =
      :ets.tab2list(reg_claim_by_key_table(name, shard))
      |> Enum.filter(fn {{cluster, _key, origin, generation, epoch}, _pid, _meta, _time, _seq} ->
        MapSet.member?(streams, {cluster, origin, generation, epoch})
      end)

    Enum.each(claims, fn {{cluster, key, origin, generation, epoch}, pid, _meta, _time, _seq} ->
      :ets.delete(
        reg_claim_by_key_table(name, shard),
        {cluster, key, origin, generation, epoch}
      )

      :ets.delete(
        reg_claim_by_pid_table(name, shard),
        {pid, cluster, key, origin, generation, epoch}
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
      :ets.delete(
        reg_claim_by_key_table(name, shard),
        {cluster, key, claim_origin, generation, epoch}
      )

      :ets.delete(
        reg_claim_by_pid_table(name, shard),
        {pid, cluster, key, claim_origin, generation, epoch}
      )
    end)

    Enum.uniq(Enum.map(claims, &elem(&1, 0)))
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

  def local_data_by_cluster(name, shard, clusters) do
    cluster_set = MapSet.new(clusters)
    local_node = node()

    reg_table = reg_by_key_table(name, shard)

    reg_by_cluster =
      :ets.select(reg_table, [
        {{{:"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6"}, [{:==, :"$6", local_node}],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.filter(fn {cluster, _, _, _, _} -> MapSet.member?(cluster_set, cluster) end)
      |> Enum.group_by(&elem(&1, 0), fn {_, key, pid, meta, time} -> {key, pid, meta, time} end)

    pg_table = pg_by_key_table(name, shard)

    pg_by_cluster =
      :ets.select(pg_table, [
        {{{:"$1", :"$2", :"$3"}, :"$4", :"$5", :"$6"}, [{:==, :"$6", local_node}],
         [{{:"$1", :"$2", :"$3", :"$4", :"$5"}}]}
      ])
      |> Enum.filter(fn {cluster, _, _, _, _} -> MapSet.member?(cluster_set, cluster) end)
      |> Enum.group_by(&elem(&1, 0), fn {_, key, pid, meta, time} -> {key, pid, meta, time} end)

    {reg_by_cluster, pg_by_cluster}
  end

  def pg_entries_for_origin(name, shard, cluster, origin_node) do
    :ets.select(pg_by_key_table(name, shard), [
      {{{cluster, :"$1", :"$2"}, :"$3", :"$4", origin_node}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
    ])
  end

  def delete_pg_for_origin_cluster(name, shard, cluster, origin_node) do
    entries = pg_entries_for_origin(name, shard, cluster, origin_node)

    Enum.each(entries, fn {key, pid, _meta, _time} ->
      :ets.delete(pg_by_key_table(name, shard), {cluster, key, pid})
      :ets.delete(pg_by_pid_table(name, shard), {pid, cluster, key})
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
      :ets.delete(pg_by_key_table(name, shard), {cluster, key, pid})
      :ets.delete(pg_by_pid_table(name, shard), {pid, cluster, key})
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
      :ets.delete(pg_by_key_table(name, shard), {cluster, key, pid})
      :ets.delete(pg_by_pid_table(name, shard), {pid, cluster, key})
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

  @impl true
  def handle_call({:add_cluster_node, clusters, node}, _from, state) do
    :ets.insert(cluster_nodes_table(state.name), Enum.map(clusters, &{&1, node}))
    :ets.insert(node_clusters_table(state.name), Enum.map(clusters, &{node, &1}))
    {:reply, :ok, state}
  end

  def handle_call({:remove_cluster_node, clusters, node}, _from, state) do
    Enum.each(clusters, fn cluster ->
      :ets.delete_object(cluster_nodes_table(state.name), {cluster, node})
      :ets.delete_object(node_clusters_table(state.name), {node, cluster})
    end)

    {:reply, :ok, state}
  end

  def handle_call({:remove_clusters, clusters}, _from, state) do
    Enum.each(clusters, fn cluster ->
      nodes = cluster_nodes(state.name, cluster)
      :ets.delete(cluster_nodes_table(state.name), cluster)

      Enum.each(nodes, fn cluster_node ->
        :ets.delete_object(
          node_clusters_table(state.name),
          {cluster_node, cluster}
        )
      end)
    end)

    {:reply, :ok, state}
  end

  def handle_call({:purge_cluster_node, dead_node}, _from, state) do
    # Scan the forward index directly so this also repairs a one-sided row left
    # by an interrupted or older dual-index mutation.
    :ets.select_delete(cluster_nodes_table(state.name), [
      {{:_, dead_node}, [], [true]}
    ])

    :ets.delete(node_clusters_table(state.name), dead_node)
    {:reply, :ok, state}
  end

  def handle_call({:activate_local_clusters, clusters}, _from, state) do
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

    {:reply, epochs, state}
  end

  def handle_call(:local_replica_authority, _from, state) do
    generation = generation(state.name)
    revision = local_cluster_epoch_revision(state.name)
    epochs = [{nil, generation} | :ets.tab2list(local_cluster_epochs_table(state.name))]
    {:reply, {generation, revision, epochs}, state}
  end

  def handle_call({:deactivate_local_clusters, clusters}, _from, state) do
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
        if epoch, do: :ets.insert(closed_local_cluster_epochs_table(state.name), {cluster, epoch})
        {cluster, epoch}
      end)

    {:reply, epochs, state}
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

    :ets.update_counter(
      replication_meta_table(state.name),
      {:remote_authority_installs, remote_node},
      {2, 1},
      {{:remote_authority_installs, remote_node}, 0}
    )

    for view_shard <- 0..(state.num_shards - 1) do
      :ets.insert(
        replication_meta_table(state.name),
        {{:remote_view_info, view_shard, remote_node}, generation, epoch_revision, epoch_revision}
      )
    end

    rows =
      for {cluster, epoch} <- epochs, not is_nil(cluster), do: {{remote_node, cluster}, epoch}

    :ets.insert(remote_cluster_epochs_table(state.name), rows)

    {:reply, {seen_generation, stale_epochs}, state}
  end

  def handle_call(
        {:put_remote_view_info, shard, remote_node, generation, authoritative, observed},
        _from,
        state
      ) do
    :ets.insert(
      replication_meta_table(state.name),
      {{:remote_view_info, shard, remote_node}, generation, authoritative, observed}
    )

    {:reply, :ok, state}
  end

  def handle_call(
        {:put_remote_cluster_epochs, shard, remote_node, revision, epochs},
        _from,
        state
      ) do
    _ = shard
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

    current_revision = remote_cluster_epoch_revision(state.name, remote_node)

    :ets.insert(
      replication_meta_table(state.name),
      {{:remote_epoch_revision, remote_node}, max(current_revision || revision, revision)}
    )

    {:reply, stale_epochs, state}
  end

  def handle_call(
        {:close_remote_cluster_epochs, shard, remote_node, revision, epochs},
        _from,
        state
      ) do
    0 = shard
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

    current_revision = remote_cluster_epoch_revision(state.name, remote_node)

    :ets.insert(
      replication_meta_table(state.name),
      {{:remote_epoch_revision, remote_node}, max(current_revision || revision, revision)}
    )

    {:reply, closed, state}
  end

  def handle_call(
        {:forget_remote_cluster_epochs, shard, remote_node, epochs},
        _from,
        state
      ) do
    # Kept for the rolling-compatibility receive path. The node-wide authority
    # table is intentionally not mutated by a shard-local purge.
    _ = {shard, remote_node, epochs}
    {:reply, :ok, state}
  end

  def handle_call({:delete_remote_replica_info, shard, remote_node}, _from, state) do
    :ets.delete(
      replication_meta_table(state.name),
      {:remote_view_info, shard, remote_node}
    )

    if shard == 0 do
      :ets.delete(replication_meta_table(state.name), {:remote_generation, remote_node})
      :ets.delete(replication_meta_table(state.name), {:remote_epoch_revision, remote_node})
      :ets.delete(replication_meta_table(state.name), {:remote_epoch_exact, remote_node})
      :ets.delete(replication_meta_table(state.name), {:remote_epoch_observed, remote_node})
      :ets.delete(replication_meta_table(state.name), {:remote_authority_installs, remote_node})

      if state.num_shards > 1 do
        for view_shard <- 1..(state.num_shards - 1) do
          :ets.delete(
            replication_meta_table(state.name),
            {:remote_view_info, view_shard, remote_node}
          )
        end
      end

      :ets.select_delete(remote_cluster_epochs_table(state.name), [
        {{{remote_node, :_}, :_}, [], [true]}
      ])
    end

    {:reply, :ok, state}
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
    :ets.insert(replication_meta_table(name), {:generation, make_ref()})
    :ets.insert(replication_meta_table(name), {:cluster_epoch_revision, 0})

    {:ok, %{name: name, num_shards: num_shards}}
  end

  defp observe_remote_cluster_revision(name, remote_node, revision, num_shards) do
    key = {:remote_epoch_observed, remote_node}

    case :ets.lookup(replication_meta_table(name), key) do
      [{^key, current}] when current >= revision -> :ok
      _ -> :ets.insert(replication_meta_table(name), {key, revision})
    end

    for shard <- 0..(num_shards - 1) do
      view_key = {:remote_view_info, shard, remote_node}

      case :ets.lookup(replication_meta_table(name), view_key) do
        [{^view_key, _generation, _authoritative, observed}] when observed >= revision ->
          :ok

        [{^view_key, generation, authoritative, _observed}] ->
          :ets.insert(
            replication_meta_table(name),
            {view_key, generation, authoritative, revision}
          )

        [] ->
          :ets.insert(
            replication_meta_table(name),
            {view_key, remote_generation(name, remote_node),
             remote_cluster_epoch_revision(name, remote_node), revision}
          )
      end
    end

    :ok
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
