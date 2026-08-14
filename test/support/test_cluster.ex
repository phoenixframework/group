defmodule Group.TestCluster do
  @moduledoc false

  @doc "Start N peer nodes with Group app loaded and ready"
  def start_peers(count, opts \\ []) do
    cookie = Keyword.get(opts, :cookie, Node.get_cookie())
    code_paths = :code.get_path()
    schedulers = Keyword.get(opts, :schedulers)

    scheduler_args =
      if schedulers, do: [~c"+S", ~c"#{schedulers}:#{schedulers}"], else: []

    args =
      scheduler_args ++
        [
          ~c"-setcookie",
          ~c"#{cookie}",
          ~c"-kernel",
          ~c"prevent_overlapping_partitions",
          ~c"false"
        ] ++
        Enum.flat_map(code_paths, fn p -> [~c"-pa", p] end)

    for _i <- 1..count do
      name = :"peer#{System.unique_integer([:positive])}"

      # A fixed inet_dist_listen_min/max inherited through ERL_AFLAGS makes
      # every child contend for the parent VM's distribution port. Peer args
      # above carry every setting the test nodes require explicitly.
      {:ok, pid, node} =
        :peer.start(%{
          name: name,
          host: ~c"127.0.0.1",
          longnames: true,
          args: args,
          env: [{~c"ERL_AFLAGS", ~c""}]
        })

      {:ok, _} = :rpc.call(node, :application, :ensure_all_started, [:elixir])
      {:ok, _} = :rpc.call(node, :application, :ensure_all_started, [:group])
      {pid, node}
    end
  end

  def stop_peers(peers) do
    Enum.each(peers, fn {pid, _node} ->
      if pid do
        try do
          :peer.stop(pid)
        catch
          :exit, _ -> :ok
        end
      end
    end)
  end

  @doc false
  def resume_if_alive(pid) when is_pid(pid) do
    if Process.alive?(pid), do: :sys.resume(pid)
    :ok
  end

  @doc false
  def expire_replica_lane(name, shard, remote_node) do
    replica = Process.whereis(Group.Replica.shard_name(name, shard))
    now = System.monotonic_time(:millisecond)

    :sys.replace_state(replica, fn state ->
      expired_at = now - state.replicated_peer_lease_timeout - 1
      %{state | peer_last_seen: Map.put(state.peer_last_seen, remote_node, expired_at)}
    end)

    state = :sys.get_state(replica)
    send(replica, {:group_replica_anti_entropy, state.anti_entropy_ref})
    _state = :sys.get_state(replica)
    :ok
  end

  @doc false
  def put_pending_registry_reprojection(replica, remote_node, cluster, key) do
    :sys.replace_state(replica, fn state ->
      pending =
        Map.update(
          state.pending_registry_reprojections,
          remote_node,
          MapSet.new([{cluster, key}]),
          &MapSet.put(&1, {cluster, key})
        )

      %{state | pending_registry_reprojections: pending}
    end)

    :ok
  end

  @doc "Call a function on a remote node, raise on badrpc"
  def rpc!(node, mod, fun, args) do
    case :rpc.call(node, mod, fun, args) do
      {:badrpc, reason} -> raise "RPC to #{node} failed: #{inspect(reason)}"
      result -> result
    end
  end

  @doc false
  def spawn_trace_forwarder(node, target_pid) do
    :erpc.call(node, fn -> spawn(fn -> forward_trace_messages(target_pid) end) end)
  end

  defp forward_trace_messages(target_pid) do
    receive do
      message ->
        send(target_pid, {:forwarded_trace, message})
        forward_trace_messages(target_pid)
    end
  end

  @doc "Start Group on a remote node"
  def start_group(node, opts) do
    opts = Keyword.put_new(opts, :log, false)

    :erpc.call(node, fn ->
      {:ok, pid} = Group.start_link(opts)
      Process.unlink(pid)
      {:ok, pid}
    end)
  end

  @doc "Start Group on a remote node and immediately connect to a cluster,
  all within a single call. This ensures Group.connect runs before peer
  discovery can complete (no round-trip gap between start and connect)."
  def start_group_and_connect(node, opts, cluster) do
    :erpc.call(node, __MODULE__, :do_start_group_and_connect, [opts, cluster])
  end

  @doc false
  def do_start_group_and_connect(opts, cluster) do
    opts = Keyword.put_new(opts, :log, false)
    {:ok, pid} = Group.start_link(opts)
    Process.unlink(pid)
    name = Keyword.fetch!(opts, :name)
    Group.connect(name, cluster)
    {:ok, pid}
  end

  @doc "Spawn a process on a remote node that registers and sleeps forever.

  Waits for the registration to complete before returning.

  Options:
    - `flush_shards: num_shards` — after the registration completes, flush the
      target shard with `:sys.get_state` so later assertions observe settled
      shard state. This does not act as a pre-write barrier.
  "
  def spawn_register(node, name, key, meta, opts \\ []) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, key, meta)
          send(parent, {:registered, self()})
          registration_owner_loop(name, key)
        end)

      receive do
        {:registered, ^pid} -> :ok
      after
        5000 -> raise "spawn_register timed out"
      end

      if num_shards = opts[:flush_shards] do
        cluster = opts[:cluster]
        shard_index = :erlang.phash2({cluster, key}, num_shards)
        :sys.get_state(:"#{name}_replica_#{shard_index}")
      end

      pid
    end)
  end

  @doc false
  def unregister_owner(pid) when is_pid(pid) do
    send(pid, {:unregister, self()})

    receive do
      {:unregistered, ^pid, result} -> result
    after
      5_000 -> raise "unregister_owner timed out"
    end
  end

  defp registration_owner_loop(name, key) do
    receive do
      {:unregister, reply_to} when is_pid(reply_to) ->
        result = Group.unregister(name, key)
        send(reply_to, {:unregistered, self(), result})
        registration_owner_loop(name, key)
    end
  end

  @doc "Spawn a process on a remote node that joins and sleeps forever.
  Waits for the join to complete before returning."
  def spawn_join(node, name, key, meta, opts \\ []) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.join(name, key, meta, opts)
          send(parent, {:joined, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, ^pid} -> pid
      after
        5000 -> raise "spawn_join timed out"
      end
    end)
  end

  @doc "Spawn a process on a remote node that registers, joins, and sleeps forever.
  Waits for both operations to complete before returning."
  def spawn_register_and_join(node, name, reg_key, reg_meta, join_key, join_meta, opts \\ []) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, reg_key, reg_meta, opts)
          :ok = Group.join(name, join_key, join_meta, opts)
          send(parent, {:ready, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:ready, ^pid} -> pid
      after
        5000 -> raise "spawn_register_and_join timed out"
      end
    end)
  end

  @doc "Spawn a process on a remote node that monitors a pattern and forwards events"
  def spawn_monitor_forwarder(node, name, pattern, target_pid, opts \\ []) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.monitor(name, pattern, opts)
        send(target_pid, {:monitor_ready, self()})
        forward_events(target_pid)
      end)
    end)
  end

  defp forward_events(target_pid) do
    receive do
      {:group, events, _info} ->
        for event <- events, do: send(target_pid, {:got_event, event})
        forward_events(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc "Like spawn_monitor_forwarder, but preserves batch structure.
  Sends `{:got_batch, pid, events}` per received `{:group, events, info}` message."
  def spawn_batch_forwarder(node, name, pattern, target_pid, opts \\ []) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.monitor(name, pattern, opts)
        send(target_pid, {:monitor_ready, self()})
        forward_batches(target_pid)
      end)
    end)
  end

  defp forward_batches(target_pid) do
    receive do
      {:group, events, _info} ->
        send(target_pid, {:got_batch, self(), events})
        forward_batches(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc "Disconnect two peer nodes from each other"
  def disconnect_nodes(node_a, node_b) do
    rpc!(node_a, :erlang, :disconnect_node, [node_b])
  end

  @doc "Reconnect two peer nodes"
  def reconnect_nodes(node_a, node_b) do
    rpc!(node_a, Node, :connect, [node_b])
  end

  @doc "Spawn a process that registers and then exits after optional delay"
  def spawn_register_then_kill(node, name, key, meta, delay \\ 0) do
    :erpc.call(node, fn ->
      pid =
        spawn(fn ->
          :ok = Group.register(name, key, meta)
          if delay > 0, do: Process.sleep(delay)
        end)

      pid
    end)
  end

  @doc "Spawn a process that registers, re-registers with new meta, then unregisters"
  def spawn_register_update_unregister(node, name, key, meta1, meta2) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :ok = Group.register(name, key, meta1)
        Process.sleep(10)
        :ok = Group.register(name, key, meta2)
        Process.sleep(10)
        :ok = Group.unregister(name, key)
      end)
    end)
  end

  @doc "Find two keys that hash to different shards for the default cluster"
  def keys_for_different_shards(num_shards) do
    key1 = "shard_test/a"
    shard1 = :erlang.phash2({nil, key1}, num_shards)

    key2_suffix =
      Enum.find(
        Stream.iterate(0, &(&1 + 1)),
        fn i ->
          k = "shard_test/b_#{i}"
          :erlang.phash2({nil, k}, num_shards) != shard1
        end
      )

    {key1, "shard_test/b_#{key2_suffix}"}
  end

  @doc "Spawn a process that registers under one key and joins another, then sleeps.
  Waits for both operations to complete before returning."
  def spawn_register_and_join_keys(node, name, reg_key, reg_meta, join_key, join_meta) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, reg_key, reg_meta)
          :ok = Group.join(name, join_key, join_meta)
          send(parent, {:ready, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:ready, ^pid} -> pid
      after
        5000 -> raise "spawn_register_and_join_keys timed out"
      end
    end)
  end

  @doc "Kill many remote processes in one RPC so their DOWNs arrive tightly."
  def kill_pids(node, pids, reason \\ :kill) do
    :erpc.call(node, __MODULE__, :do_kill_pids, [pids, reason])
  end

  @doc false
  def do_kill_pids(pids, reason) do
    Enum.each(pids, &Process.exit(&1, reason))
    :ok
  end

  @doc "Spawn a process on a remote node that registers in a named cluster and sleeps.
  Waits for the registration to complete before returning."
  def spawn_register_in_cluster(node, name, key, meta, cluster) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.register(name, key, meta, cluster: cluster)
          send(parent, {:registered, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:registered, ^pid} -> pid
      after
        5000 -> raise "spawn_register_in_cluster timed out"
      end
    end)
  end

  @doc "Spawn a process on a remote node that joins in a named cluster and sleeps."
  def spawn_join_in_cluster(node, name, key, meta, cluster) do
    :erpc.call(node, fn ->
      parent = self()

      pid =
        spawn(fn ->
          :ok = Group.join(name, key, meta, cluster: cluster)
          send(parent, {:joined, self()})
          Process.sleep(:infinity)
        end)

      receive do
        {:joined, ^pid} -> pid
      after
        5000 -> raise "spawn_join_in_cluster timed out"
      end
    end)
  end

  @doc "Connects every cluster through an independent concurrent caller."
  def connect_many_concurrently(node, name, clusters) do
    :erpc.call(node, __MODULE__, :do_connect_many_concurrently, [name, clusters], 60_000)
  end

  @doc false
  def do_connect_many_concurrently(name, clusters) do
    clusters
    |> Task.async_stream(
      fn cluster -> Group.connect(name, cluster) end,
      max_concurrency: 64,
      ordered: false,
      timeout: 30_000
    )
    |> Enum.each(fn {:ok, :ok} -> :ok end)

    :ok
  end

  @doc "Spawns one long-lived registration owner in each named cluster."
  def spawn_register_many_clusters(node, name, clusters, key_prefix) do
    :erpc.call(
      node,
      __MODULE__,
      :do_spawn_register_many_clusters,
      [name, clusters, key_prefix],
      60_000
    )
  end

  @doc false
  def do_spawn_register_many_clusters(name, clusters, key_prefix) do
    parent = self()

    entries =
      Enum.map(clusters, fn cluster ->
        key = "#{key_prefix}/#{cluster}"

        pid =
          spawn(fn ->
            :ok = Group.register(name, key, %{cluster: cluster}, cluster: cluster)
            send(parent, {:registered_many, self()})
            Process.sleep(:infinity)
          end)

        {cluster, key, pid}
      end)

    Enum.each(entries, fn {_cluster, _key, pid} ->
      receive do
        {:registered_many, ^pid} -> :ok
      after
        30_000 -> raise "spawn_register_many_clusters timed out"
      end
    end)

    entries
  end

  @doc false
  def registry_entries_present?(name, entries) do
    Enum.all?(entries, fn {cluster, key, pid} ->
      match?({^pid, %{cluster: ^cluster}}, Group.lookup(name, key, cluster: cluster))
    end)
  end

  @doc "Monitor nodedown events from a remote node, forwarding to caller"
  def monitor_nodes_on(node, target_pid) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :net_kernel.monitor_nodes(true)
        forward_nodedown(target_pid)
      end)
    end)
  end

  defp forward_nodedown(target_pid) do
    receive do
      {:nodedown, node} ->
        send(target_pid, {:nodedown_on_remote, node})
        forward_nodedown(target_pid)

      {:nodeup, _node} ->
        forward_nodedown(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc "Spawn a process on a remote node that joins a group and forwards messages to target"
  def spawn_join_forwarder(node, name, key, target_pid, opts) do
    :erpc.call(node, __MODULE__, :do_spawn_join_forwarder, [name, key, target_pid, opts])
  end

  @doc "Spawn a process on a remote node that joins a group and reports the result to target_pid."
  def spawn_join_reporter(node, name, key, meta, target_pid, opts \\ []) do
    :erpc.call(node, __MODULE__, :do_spawn_join_reporter, [name, key, meta, target_pid, opts])
  end

  @doc false
  def do_spawn_join_reporter(name, key, meta, target_pid, opts) do
    spawn(fn ->
      result = Group.join(name, key, meta, opts)
      send(target_pid, {:join_result, self(), result})
      Process.sleep(:infinity)
    end)
  end

  @doc false
  def do_spawn_join_forwarder(name, key, target_pid, opts) do
    parent = self()

    pid =
      spawn(fn ->
        :ok = Group.join(name, key, %{}, opts)
        send(parent, {:joined, self()})
        forward_messages(target_pid)
      end)

    receive do
      {:joined, ^pid} -> pid
    after
      5000 -> raise "spawn_join_forwarder timed out"
    end
  end

  defp forward_messages(target_pid) do
    receive do
      msg ->
        send(target_pid, {:forwarded, msg})
        forward_messages(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc """
  Synchronously flushes all shard GenServers on a remote node.

  Sends a barrier through each shard's normal mailbox so any buffered replicated
  PG joins/leaves are flushed before the call returns. Use after
  `assert_eventually` to drain any remaining async fan-out or cleanup messages
  before checking ETS state.
  """
  def flush_shards(node, name) do
    :erpc.call(node, fn ->
      num_shards = Group.get_config(name).num_shards

      for shard <- 0..(num_shards - 1) do
        shard_name = :"#{name}_replica_#{shard}"
        ref = make_ref()
        send(shard_name, {:group_dispatch, [self()], {:test_cluster_flush_ack, ref}})

        receive do
          {:test_cluster_flush_ack, ^ref} -> :ok
        after
          5_000 -> raise "flush_shards timed out for #{inspect(shard_name)}"
        end
      end

      :ok
    end)
  end

  @doc false
  def kill_shard_with_snapshot_staging(name, shard_index) do
    shard = Process.whereis(Group.Replica.shard_name(name, shard_index))
    state = :sys.get_state(shard)
    {_key, transfer} = Enum.at(state.snapshot_transfers, 0)
    monitor = Process.monitor(shard)
    Process.exit(shard, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^shard, :killed} ->
        {shard, :ets.info(transfer.table)}
    after
      5_000 -> raise "snapshot staging owner did not terminate"
    end
  end

  @doc false
  def snapshot_staging_tables(name, shard_index) do
    owner = Process.whereis(Group.Replica.shard_name(name, shard_index))

    :ets.all()
    |> Enum.filter(fn table -> :ets.info(table, :owner) == owner end)
  end

  @doc false
  def delete_remote_view_info(name, shard_index, remote_node) do
    :ets.delete(
      Group.Replica.Data.replication_meta_table(name),
      {:remote_view_info, shard_index, remote_node}
    )

    :ok
  end

  @doc "Returns the current message_queue_len for a shard on a remote node."
  def shard_message_queue_len(node, name, shard) do
    :erpc.call(node, __MODULE__, :do_shard_message_queue_len, [name, shard])
  end

  @doc false
  def do_shard_message_queue_len(name, shard) do
    shard_name = :"#{name}_replica_#{shard}"

    case Process.info(Process.whereis(shard_name), :message_queue_len) do
      {:message_queue_len, len} -> len
      nil -> 0
    end
  end

  @doc "Returns the current mailbox messages for a shard on a remote node."
  def shard_messages(node, name, shard) do
    :erpc.call(node, __MODULE__, :do_shard_messages, [name, shard])
  end

  @doc false
  def do_shard_messages(name, shard) do
    shard_name = :"#{name}_replica_#{shard}"

    case Process.info(Process.whereis(shard_name), :messages) do
      {:messages, messages} -> messages
      nil -> []
    end
  end

  @doc "Resumes a suspended shard on a remote node if it is still alive."
  def resume_shard_if_alive(node, name, shard) do
    :erpc.call(node, __MODULE__, :do_resume_shard_if_alive, [name, shard])
  end

  @doc false
  def do_resume_shard_if_alive(name, shard) do
    shard_name = :"#{name}_replica_#{shard}"

    if Process.whereis(shard_name) do
      :sys.resume(shard_name)
    else
      :ok
    end
  end

  @doc "Expires a named-cluster ttl lease on a remote node and forces an immediate sweep."
  def expire_cluster_lease_and_force_sweep(node, name, cluster) do
    :erpc.call(node, __MODULE__, :do_expire_cluster_lease_and_force_sweep, [name, cluster])
  end

  @doc false
  def do_expire_cluster_lease_and_force_sweep(name, cluster) do
    {ttl_ms, _expires_at} = Group.Replica.Data.cluster_lease(name, cluster)

    Group.Replica.Data.put_cluster_lease(
      name,
      cluster,
      ttl_ms,
      System.monotonic_time(:millisecond) - 1
    )

    lease_manager = Group.ClusterLease.lease_name(name)
    send(lease_manager, :force_sweep)
    :sys.get_state(lease_manager)
    :ok
  end

  @doc """
  Asserts that all ETS dual-index tables are in sync for a Group instance.

  Verifies:
  - reg_by_key ↔ reg_by_pid contain the same entries (across all shards)
  - pg_by_key ↔ pg_by_pid contain the same entries (across all shards)
  - cluster_nodes ↔ node_clusters contain the same pairs

  Raises on inconsistency with details about orphaned/missing entries.
  """
  def assert_ets_consistent(name) do
    num_shards = Group.get_config(name).num_shards

    for shard <- 0..(num_shards - 1) do
      # Registry: by_key entries should match by_pid entries
      reg_key_table = Group.Replica.Data.reg_by_key_table(name, shard)
      reg_pid_table = Group.Replica.Data.reg_by_pid_table(name, shard)

      # by_key: {{cluster, key}, pid, meta, time, node}
      key_set =
        :ets.tab2list(reg_key_table)
        |> MapSet.new(fn {{cluster, key}, pid, meta, time, nd} ->
          {cluster, key, pid, meta, time, nd}
        end)

      # by_pid: {{pid, cluster, key}, meta, time, node}
      pid_set =
        :ets.tab2list(reg_pid_table)
        |> MapSet.new(fn {{pid, cluster, key}, meta, time, nd} ->
          {cluster, key, pid, meta, time, nd}
        end)

      if key_set != pid_set do
        orphaned = MapSet.difference(pid_set, key_set) |> MapSet.to_list()
        missing = MapSet.difference(key_set, pid_set) |> MapSet.to_list()

        raise "ETS inconsistency in #{name} shard #{shard} (registry)!\n" <>
                "  Orphaned in reg_by_pid (no matching by_key): #{inspect(orphaned)}\n" <>
                "  Missing from reg_by_pid (in by_key only): #{inspect(missing)}"
      end

      # PG: by_key entries should match by_pid entries
      pg_key_table = Group.Replica.Data.pg_by_key_table(name, shard)
      pg_pid_table = Group.Replica.Data.pg_by_pid_table(name, shard)

      # pg_by_key: {{cluster, key, pid}, meta, time, node}
      pg_key_set =
        :ets.tab2list(pg_key_table)
        |> MapSet.new(fn {{cluster, key, pid}, meta, time, nd} ->
          {cluster, key, pid, meta, time, nd}
        end)

      # pg_by_pid: {{pid, cluster, key}, meta, time, node}
      pg_pid_set =
        :ets.tab2list(pg_pid_table)
        |> MapSet.new(fn {{pid, cluster, key}, meta, time, nd} ->
          {cluster, key, pid, meta, time, nd}
        end)

      if pg_key_set != pg_pid_set do
        orphaned = MapSet.difference(pg_pid_set, pg_key_set) |> MapSet.to_list()
        missing = MapSet.difference(pg_key_set, pg_pid_set) |> MapSet.to_list()

        raise "ETS inconsistency in #{name} shard #{shard} (PG)!\n" <>
                "  Orphaned in pg_by_pid (no matching by_key): #{inspect(orphaned)}\n" <>
                "  Missing from pg_by_pid (in by_key only): #{inspect(missing)}"
      end
    end

    # Cluster membership: cluster_nodes ↔ node_clusters
    cn_table = Group.Replica.Data.cluster_nodes_table(name)
    nc_table = Group.Replica.Data.node_clusters_table(name)

    cn_set = :ets.tab2list(cn_table) |> MapSet.new(fn {cluster, nd} -> {cluster, nd} end)
    nc_set = :ets.tab2list(nc_table) |> MapSet.new(fn {nd, cluster} -> {cluster, nd} end)

    if cn_set != nc_set do
      only_cn = MapSet.difference(cn_set, nc_set) |> MapSet.to_list()
      only_nc = MapSet.difference(nc_set, cn_set) |> MapSet.to_list()

      raise "cluster_nodes / node_clusters inconsistency in #{name}!\n" <>
              "  Only in cluster_nodes: #{inspect(only_cn)}\n" <>
              "  Only in node_clusters: #{inspect(only_nc)}"
    end

    :ok
  end

  @doc """
  Asserts the replica-only authority and journal invariants in addition to the
  public dual-index invariants checked by `assert_ets_consistent/1`.

  This is intended for quiescent convergence points in adversarial tests.
  """
  def assert_replica_consistent(name) do
    :ok = assert_ets_consistent(name)
    num_shards = Group.get_config(name).num_shards

    for shard <- 0..(num_shards - 1) do
      assert_rows_on_matching_shard(name, shard, num_shards)
      assert_registry_claim_indexes(name, shard)
      assert_registry_projection_has_authority(name, shard)
      assert_registry_claim_authority(name, shard)
      assert_pg_row_authority(name, shard)
      assert_oplog_indexes(name, shard)
      assert_replica_cursor_authority(name, shard)
    end

    :ok
  end

  defp assert_rows_on_matching_shard(name, shard, num_shards) do
    checks = [
      {Group.Replica.Data.reg_by_key_table(name, shard),
       fn
         {{cluster, key}, _pid, _meta, _time, _origin} -> {cluster, key}
       end},
      {Group.Replica.Data.reg_claim_by_key_table(name, shard),
       fn
         {{cluster, key, _origin, _generation, _epoch}, _pid, _meta, _time, _seq} ->
           {cluster, key}
       end},
      {Group.Replica.Data.pg_by_key_table(name, shard),
       fn
         {{cluster, key, _pid}, _meta, _time, _origin} -> {cluster, key}
       end}
    ]

    Enum.each(checks, fn {table, key_fun} ->
      case Enum.find(:ets.tab2list(table), fn row ->
             {cluster, key} = key_fun.(row)
             Group.Replica.shard_index_for(cluster, key, num_shards) != shard
           end) do
        nil ->
          :ok

        row ->
          raise "row stored on wrong shard in #{name} shard #{shard}: #{inspect(row)}"
      end
    end)
  end

  @doc false
  def assert_replica_origin_purged(name, origin) do
    num_shards = Group.get_config(name).num_shards

    for shard <- 0..(num_shards - 1) do
      retained_claims =
        Group.Replica.Data.reg_claim_by_key_table(name, shard)
        |> :ets.tab2list()
        |> Enum.filter(fn {{_cluster, _key, row_origin, _generation, _epoch}, _pid, _meta, _time,
                           _seq} ->
          row_origin == origin
        end)

      retained_registry =
        Group.Replica.Data.reg_by_key_table(name, shard)
        |> :ets.tab2list()
        |> Enum.filter(fn {_key, _pid, _meta, _time, entry_node} -> entry_node == origin end)

      retained_pg =
        Group.Replica.Data.pg_by_key_table(name, shard)
        |> :ets.tab2list()
        |> Enum.filter(fn {_key, _meta, _time, entry_node} -> entry_node == origin end)

      retained_cursors =
        Group.Replica.Data.replica_cursor_table(name, shard)
        |> :ets.tab2list()
        |> Enum.filter(fn {stream_id, _seq} ->
          Group.Replica.WireProtocol.stream_origin(stream_id) == origin
        end)

      retained_view = Group.Replica.Data.remote_view_generation(name, shard, origin)

      unless retained_claims == [] and retained_registry == [] and retained_pg == [] and
               retained_cursors == [] and is_nil(retained_view) do
        raise "replica origin was not fully purged from #{name} shard #{shard}: " <>
                inspect(%{
                  claims: retained_claims,
                  registry: retained_registry,
                  pg: retained_pg,
                  cursors: retained_cursors,
                  view_generation: retained_view
                })
      end
    end

    unless is_nil(Group.Replica.Data.remote_generation(name, origin)) and
             is_nil(Group.Replica.Data.remote_replica_authority_hint(name, origin)) and
             Group.Replica.Data.clusters_for_node(name, origin) == [] do
      raise "replica origin retained shared authority after purge: #{inspect(origin)}"
    end

    :ok
  end

  @doc """
  Returns every PID currently retained as replica authority or visible PG state.

  Adversarial tests use this at a quiescent convergence point to prove that no
  dead owner remains hidden behind otherwise-consistent dual indexes.
  """
  def replica_owner_pids(name) do
    num_shards = Group.get_config(name).num_shards

    0..(num_shards - 1)
    |> Enum.flat_map(fn shard ->
      registry_pids =
        Group.Replica.Data.reg_claim_by_key_table(name, shard)
        |> :ets.tab2list()
        |> Enum.map(fn {{_cluster, _key, _origin, _generation, _epoch}, pid, _meta, _time, _seq} ->
          pid
        end)

      pg_pids =
        Group.Replica.Data.pg_by_key_table(name, shard)
        |> :ets.tab2list()
        |> Enum.map(fn {{_cluster, _key, pid}, _meta, _time, _node} -> pid end)

      registry_pids ++ pg_pids
    end)
    |> Enum.uniq()
  end

  @doc false
  def replica_protocol_state(name) do
    num_shards = Group.get_config(name).num_shards

    for shard <- 0..(num_shards - 1) do
      %{
        shard: shard,
        heads: Group.Replica.Data.replica_stream_heads(name, shard),
        cursors:
          Group.Replica.Data.replica_cursor_table(name, shard)
          |> :ets.tab2list()
          |> Enum.sort()
      }
    end
  end

  @doc false
  def replica_registry_key_state(name, cluster, key) do
    num_shards = Group.get_config(name).num_shards
    shard = Group.Replica.shard_index_for(cluster, key, num_shards)

    %{
      shard: shard,
      projection: Group.Replica.Data.registry_lookup(name, shard, cluster, key),
      claims: Group.Replica.Data.registry_claims(name, shard, cluster, key)
    }
  end

  @doc false
  def replica_registry_replication_state(name, cluster, key, origin) do
    num_shards = Group.get_config(name).num_shards
    shard = Group.Replica.shard_index_for(cluster, key, num_shards)
    replica = Process.whereis(Group.Replica.shard_name(name, shard))
    replica_state = :sys.get_state(replica)
    local_stream_id = Group.Replica.Data.local_stream_id(name, shard, cluster)

    streams =
      Group.Replica.Data.replica_cursor_streams_for_origin_cluster(
        name,
        shard,
        origin,
        cluster
      )

    peer_authority =
      Node.list()
      |> Map.new(fn peer ->
        {peer,
         %{
           generation: Group.Replica.Data.remote_generation(name, peer),
           epoch: Group.Replica.Data.remote_cluster_epoch(name, peer, cluster),
           revision: Group.Replica.Data.remote_cluster_epoch_revision(name, peer),
           exact: Group.Replica.Data.remote_cluster_epoch_exact_revision(name, peer),
           observed: Group.Replica.Data.remote_cluster_epoch_observed_revision(name, peer),
           hint: Group.Replica.Data.remote_replica_authority_hint(name, peer),
           lane_view: {
             Group.Replica.Data.remote_view_generation(name, shard, peer),
             Group.Replica.Data.remote_view_cluster_epoch_revision(name, shard, peer),
             Group.Replica.Data.remote_view_observed_revision(name, shard, peer)
           }
         }}
      end)

    %{
      key: replica_registry_key_state(name, cluster, key),
      origin: origin,
      cluster_nodes: Group.nodes(name, cluster),
      local_generation: Group.Replica.Data.generation(name),
      local_epoch: Group.Replica.Data.local_cluster_epoch(name, cluster),
      local_revision: Group.Replica.Data.local_cluster_epoch_revision(name),
      local_stream:
        if(local_stream_id,
          do:
            {local_stream_id,
             Group.Replica.Data.replica_stream_head(name, shard, local_stream_id)},
          else: nil
        ),
      peer_authority: peer_authority,
      remote_generation: Group.Replica.Data.remote_generation(name, origin),
      remote_epoch: Group.Replica.Data.remote_cluster_epoch(name, origin, cluster),
      remote_revision: Group.Replica.Data.remote_cluster_epoch_revision(name, origin),
      remote_exact_revision: Group.Replica.Data.remote_cluster_epoch_exact_revision(name, origin),
      remote_observed_revision:
        Group.Replica.Data.remote_cluster_epoch_observed_revision(name, origin),
      remote_authority_hint: Group.Replica.Data.remote_replica_authority_hint(name, origin),
      lane_view: {
        Group.Replica.Data.remote_view_generation(name, shard, origin),
        Group.Replica.Data.remote_view_cluster_epoch_revision(name, shard, origin),
        Group.Replica.Data.remote_view_observed_revision(name, shard, origin)
      },
      cursors:
        Enum.map(streams, fn stream_id ->
          {stream_id, Group.Replica.Data.replica_cursor(name, shard, stream_id)}
        end),
      remote_shards: Map.keys(replica_state.remote_shards),
      peer_last_seen_nodes: Map.keys(replica_state.peer_last_seen),
      peer_last_seen: Map.get(replica_state.peer_last_seen, origin),
      pending_reprojection?:
        replica_state.pending_registry_reprojections
        |> Map.get(origin, MapSet.new())
        |> MapSet.member?({cluster, key})
    }
  end

  defp assert_registry_claim_indexes(name, shard) do
    by_key = Group.Replica.Data.reg_claim_by_key_table(name, shard)
    by_pid = Group.Replica.Data.reg_claim_by_pid_table(name, shard)

    key_set =
      :ets.tab2list(by_key)
      |> MapSet.new(fn
        {{cluster, key, origin, generation, epoch}, pid, meta, time, seq} ->
          {cluster, key, pid, meta, time, origin, generation, epoch, seq}
      end)

    pid_set =
      :ets.tab2list(by_pid)
      |> MapSet.new(fn
        {{pid, cluster, key, origin, generation, epoch}, meta, time, seq} ->
          {cluster, key, pid, meta, time, origin, generation, epoch, seq}
      end)

    if key_set != pid_set do
      raise "registry claim index inconsistency in #{name} shard #{shard}: " <>
              "by_key_only=#{inspect(MapSet.difference(key_set, pid_set) |> MapSet.to_list())} " <>
              "by_pid_only=#{inspect(MapSet.difference(pid_set, key_set) |> MapSet.to_list())}"
    end

    case Enum.find(key_set, fn {_cluster, _key, pid, _meta, _time, origin, _gen, _epoch, _seq} ->
           node(pid) != origin
         end) do
      nil -> :ok
      invalid -> raise "registry claim has invalid origin authority: #{inspect(invalid)}"
    end
  end

  defp assert_registry_projection_has_authority(name, shard) do
    claim_rows =
      Group.Replica.Data.reg_claim_by_key_table(name, shard)
      |> :ets.tab2list()

    resolver = Map.get(Group.get_config(name), :resolve_registry_conflict)

    expected =
      claim_rows
      |> Enum.group_by(fn
        {{cluster, key, _origin, _generation, _epoch}, _pid, _meta, _time, _seq} ->
          {cluster, key}
      end)
      |> MapSet.new(fn {{cluster, key}, claims} ->
        {{^cluster, ^key, origin, _generation, _epoch}, pid, meta, time, _seq} =
          Enum.max_by(claims, fn
            {{^cluster, ^key, _origin, _generation, _epoch}, claim_pid, claim_meta, claim_time,
             _seq} ->
              registry_claim_order_key(
                name,
                key,
                {claim_pid, claim_meta, claim_time},
                resolver
              )
          end)

        {cluster, key, pid, meta, time, origin}
      end)

    visible =
      Group.Replica.Data.reg_by_key_table(name, shard)
      |> :ets.tab2list()
      |> MapSet.new(fn {{cluster, key}, pid, meta, time, origin} ->
        {cluster, key, pid, meta, time, origin}
      end)

    if visible != expected do
      raise "registry projection does not match the deterministic claim winner in #{name} " <>
              "shard #{shard}: expected=#{inspect(MapSet.to_list(expected))} " <>
              "visible=#{inspect(MapSet.to_list(visible))}"
    end
  end

  defp registry_claim_order_key(_name, _key, {pid, _meta, time}, nil), do: {time, pid}

  defp registry_claim_order_key(name, key, {pid, _meta, _time} = claim, {
         mod,
         func,
         extra_args
       }) do
    {apply(mod, func, [name, key, claim | extra_args]), pid}
  end

  defp assert_registry_claim_authority(name, shard) do
    Group.Replica.Data.reg_claim_by_key_table(name, shard)
    |> :ets.tab2list()
    |> Enum.each(fn
      {{cluster, key, origin, generation, epoch}, pid, _meta, _time, seq} = claim ->
        valid? =
          if origin == node() do
            generation == Group.Replica.Data.generation(name) and
              epoch == Group.Replica.Data.local_cluster_epoch(name, cluster)
          else
            generation == Group.Replica.Data.remote_generation(name, origin) and
              remote_lane_current?(name, shard, origin) and
              epoch == Group.Replica.Data.remote_cluster_epoch(name, origin, cluster)
          end

        unless valid? and node(pid) == origin and seq > 0 do
          raise "registry claim is not fenced by current authority in #{name} shard #{shard}: " <>
                  inspect({claim, key})
        end
    end)
  end

  defp assert_pg_row_authority(name, shard) do
    Group.Replica.Data.pg_by_key_table(name, shard)
    |> :ets.tab2list()
    |> Enum.each(fn {{cluster, key, pid}, _meta, _time, origin} = row ->
      valid? =
        node(pid) == origin and
          if origin == node() do
            not is_nil(Group.Replica.Data.local_cluster_epoch(name, cluster))
          else
            generation = Group.Replica.Data.remote_generation(name, origin)

            not is_nil(generation) and
              remote_lane_current?(name, shard, origin) and
              not is_nil(Group.Replica.Data.remote_cluster_epoch(name, origin, cluster))
          end

      unless valid? do
        raise "PG row is not fenced by current authority in #{name} shard #{shard}: " <>
                inspect({row, key})
      end
    end)
  end

  defp assert_oplog_indexes(name, shard) do
    oplog =
      Group.Replica.Data.replica_oplog_table(name, shard)
      |> :ets.tab2list()
      |> MapSet.new(fn {{stream_id, seq}, append_id, _mutations} ->
        {append_id, stream_id, seq}
      end)

    order =
      Group.Replica.Data.replica_oplog_order_table(name, shard)
      |> :ets.tab2list()
      |> MapSet.new()

    if oplog != order do
      raise "oplog/order index inconsistency in #{name} shard #{shard}: " <>
              "oplog_only=#{inspect(MapSet.difference(oplog, order) |> MapSet.to_list())} " <>
              "order_only=#{inspect(MapSet.difference(order, oplog) |> MapSet.to_list())}"
    end

    max_entries = Group.get_config(name).replicated_oplog_max_entries

    if MapSet.size(order) > max_entries do
      raise "oplog bound exceeded in #{name} shard #{shard}: " <>
              "size=#{MapSet.size(order)} max=#{max_entries}"
    end

    Group.Replica.Data.replica_stream_meta_table(name, shard)
    |> :ets.tab2list()
    |> Enum.each(fn {stream_id, head, floor, applied} ->
      unless floor >= 1 and floor <= head + 1 and applied >= 0 and applied <= head do
        raise "invalid stream bounds in #{name} shard #{shard}: " <>
                inspect({stream_id, head, floor, applied})
      end

      retained =
        oplog
        |> Enum.filter(fn {_append_id, row_stream, _seq} -> row_stream == stream_id end)
        |> Enum.map(&elem(&1, 2))
        |> Enum.sort()

      expected = if floor <= head, do: Enum.to_list(floor..head), else: []

      if retained != expected do
        raise "non-contiguous retained oplog in #{name} shard #{shard}: " <>
                "stream=#{inspect(stream_id)} retained=#{inspect(retained)} " <>
                "expected=#{inspect(expected)}"
      end
    end)
  end

  defp assert_replica_cursor_authority(name, shard) do
    Group.Replica.Data.replica_cursor_table(name, shard)
    |> :ets.tab2list()
    |> Enum.each(fn {stream_id, seq} ->
      origin = Group.Replica.WireProtocol.stream_origin(stream_id)
      generation = Group.Replica.WireProtocol.stream_generation(stream_id)
      cluster = Group.Replica.WireProtocol.stream_cluster(stream_id)
      epoch = Group.Replica.WireProtocol.stream_epoch(stream_id)

      valid? =
        Group.Replica.WireProtocol.stream_name(stream_id) == name and
          Group.Replica.WireProtocol.stream_shard(stream_id) == shard and
          origin != node() and
          generation == Group.Replica.Data.remote_generation(name, origin) and
          remote_lane_current?(name, shard, origin) and
          epoch == Group.Replica.Data.remote_cluster_epoch(name, origin, cluster) and
          seq >= 0

      unless valid? do
        raise "replica cursor is not fenced by current authority in #{name} shard #{shard}: " <>
                inspect({stream_id, seq})
      end
    end)
  end

  defp remote_lane_current?(name, shard, origin) do
    generation = Group.Replica.Data.remote_generation(name, origin)
    observed = Group.Replica.Data.remote_cluster_epoch_observed_revision(name, origin)

    Group.Replica.Data.remote_replica_authority_hint(name, origin) ==
      {generation, observed} and
      Group.Replica.Data.remote_cluster_epoch_revision(name, origin) == observed and
      Group.Replica.Data.remote_view_generation(name, shard, origin) == generation and
      Group.Replica.Data.remote_view_cluster_epoch_revision(name, shard, origin) ==
        Group.Replica.Data.remote_cluster_epoch_exact_revision(name, origin) and
      Group.Replica.Data.remote_view_observed_revision(name, shard, origin) ==
        observed
  end

  @doc "Wait for a condition to become true, with retries"
  def assert_eventually(fun, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2000)
    interval = Keyword.get(opts, :interval, 50)
    diagnostic = Keyword.get(opts, :diagnostic)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_assert_eventually(fun, interval, deadline, diagnostic)
  end

  defp do_assert_eventually(fun, interval, deadline, diagnostic) do
    case fun.() do
      true ->
        true

      false ->
        if System.monotonic_time(:millisecond) >= deadline do
          details =
            if is_function(diagnostic, 0) do
              "\ndiagnostic: " <> inspect(diagnostic.(), pretty: true, limit: :infinity)
            else
              ""
            end

          raise "assert_eventually timed out#{details}"
        end

        Process.sleep(interval)
        do_assert_eventually(fun, interval, deadline, diagnostic)
    end
  end
end
