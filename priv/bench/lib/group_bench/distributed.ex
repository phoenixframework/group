defmodule GroupBench.Distributed do
  @moduledoc """
  Coordinator for distributed benchmarks.

  Expects replica1@127.0.0.1 and replica2@127.0.0.1 to already be running
  (started by run_distributed.sh). Connects to them, then drives benchmark
  scenarios via :erpc.call with MFA.
  """

  import GroupBench.Helpers

  @name :bench
  @replicas [:"replica1@127.0.0.1", :"replica2@127.0.0.1"]

  def run(opts \\ []) do
    shards = Keyword.get(opts, :shards, 8)
    Process.put(:bench_shards, shards)

    header("Distributed Benchmarks")
    IO.puts("  coordinator: #{node()}")
    IO.puts("  shards:      #{shards}")
    IO.puts("  schedulers:  #{System.schedulers_online()}")

    connect_replicas()

    bench_replication_latency(@replicas)
    bench_bulk_sync(@replicas)
    bench_concurrent_cross_node(@replicas)
    bench_named_cluster_replication(@replicas)
    bench_death_cleanup(@replicas)
    bench_churn(@replicas)
    bench_join_death_cleanup(@replicas)
    bench_many_clusters(@replicas)
    bench_busy_app(@replicas)
    bench_local_requests_under_replicated_pg_pressure(@replicas)

    IO.puts("\n  Done.\n")
  end

  def run_registry_pressure_only(opts \\ []) do
    shards = Keyword.get(opts, :shards, 1)
    Process.put(:bench_shards, shards)

    header("Distributed Registry Pressure Benchmark")
    IO.puts("  coordinator: #{node()}")
    IO.puts("  shards:      #{shards}")
    IO.puts("  schedulers:  #{System.schedulers_online()}")

    connect_replicas()
    bench_local_requests_under_replicated_registry_pressure(@replicas)

    IO.puts("\n  Done.\n")
  end

  def run_many_clusters_only(opts \\ []) do
    shards = Keyword.get(opts, :shards, 4)
    Process.put(:bench_shards, shards)

    header("Distributed Many-Clusters Benchmark")
    IO.puts("  coordinator: #{node()}")
    IO.puts("  shards:      #{shards}")
    IO.puts("  schedulers:  #{System.schedulers_online()}")

    connect_replicas()
    bench_many_clusters(@replicas)

    IO.puts("\n  Done.\n")
  end

  def run_snapshot_sync_only(opts \\ []) do
    shards = Keyword.get(opts, :shards, 1)
    entries = Keyword.get(opts, :entries, 50_000)
    Process.put(:bench_shards, shards)

    header("Distributed Exact-Snapshot Benchmark")
    IO.puts("  coordinator: #{node()}")
    IO.puts("  shards:      #{shards}")
    IO.puts("  entries:     #{format_number(entries)}")
    IO.puts("  schedulers:  #{System.schedulers_online()}")

    connect_replicas()
    bench_snapshot_sync(@replicas, entries)

    IO.puts("\n  Done.\n")
  end

  def run_scale_recovery_only(opts \\ []) do
    shards = Keyword.get(opts, :shards, 32)
    entries = Keyword.get(opts, :entries, 1_000_000)
    mode = Keyword.get(opts, :mode, :registry)
    Process.put(:bench_shards, shards)

    unless mode in [:registry, :pg_hotspot] do
      raise ArgumentError, "expected :mode to be :registry or :pg_hotspot"
    end

    header("Distributed Million-Row Recovery Benchmark")
    IO.puts("  coordinator: #{node()}")
    IO.puts("  shards:      #{shards}")
    IO.puts("  entries:     #{format_number(entries)}")
    IO.puts("  mode:        #{mode}")
    IO.puts("  schedulers:  #{System.schedulers_online()}")

    connect_replicas()
    bench_scale_recovery(@replicas, mode, entries)

    IO.puts("\n  Done.\n")
  end

  # ── Connection ────────────────────────────────────────────────────────

  defp connect_replicas do
    Enum.each(@replicas, fn node ->
      wait_for_connection(node)
    end)

    IO.puts("  All replicas connected.\n")
  end

  defp wait_for_connection(node_name, attempts \\ 50) do
    if attempts <= 0 do
      raise "Failed to connect to #{node_name}"
    end

    case Node.connect(node_name) do
      true ->
        IO.puts("  Connected to #{node_name}")

      _ ->
        Process.sleep(200)
        wait_for_connection(node_name, attempts - 1)
    end
  end

  defp bench_scale_recovery([source, receiver] = replicas, mode, entries) do
    header("Exact snapshot, shard restart, and permanent Group loss")

    group_opts = [
      replicated_oplog_max_entries: 64,
      replicated_snapshot_chunk_target_bytes: 1_048_576,
      replicated_anti_entropy_interval: 250,
      replicated_peer_lease_timeout: 5_000
    ]

    stop_groups(replicas)
    start_group_on(source, group_opts)

    {seed_us, seed_result} =
      :timer.tc(fn ->
        case mode do
          :registry ->
            :erpc.call(
              source,
              GroupBench.Replica,
              :seed_registry_slice,
              [@name, entries, "scale/registry/", 10_000],
              1_800_000
            )

          :pg_hotspot ->
            :erpc.call(
              source,
              GroupBench.Replica,
              :seed_pg_hotspot,
              [@name, entries, "scale/pg-hotspot", 10_000],
              1_800_000
            )
        end
      end)

    source_memory = :erpc.call(source, GroupBench.Replica, :memory_snapshot, [@name])

    {snapshot_us, _} =
      :timer.tc(fn ->
        start_group_on(receiver, group_opts)

        poll_until(
          fn -> replicated_row_count(receiver, mode) == entries end,
          900_000
        )

        :ok =
          :erpc.call(receiver, GroupBench.Replica, :flush_shards, [@name], 900_000)
      end)

    receiver_memory = :erpc.call(receiver, GroupBench.Replica, :memory_snapshot, [@name])

    restart_shard =
      case mode do
        :registry ->
          source
          |> :erpc.call(GroupBench.Replica, :registry_counts_by_shard, [@name])
          |> Enum.max_by(&elem(&1, 1))
          |> elem(0)

        :pg_hotspot ->
          seed_result
      end

    restart =
      :erpc.call(
        receiver,
        GroupBench.Replica,
        :restart_shard,
        [@name, restart_shard],
        900_000
      )

    unless replicated_row_count(receiver, mode) == entries do
      raise "row count changed across receiver shard restart"
    end

    {eviction_us, _} =
      :timer.tc(fn ->
        stop_group_on(source)

        poll_until(
          fn ->
            counts = :erpc.call(receiver, GroupBench.Replica, :replica_row_counts, [@name])

            counts.registry_rows == 0 and counts.registry_claim_rows == 0 and
              counts.pg_rows == 0 and counts.replica_cursor_rows == 0
          end,
          900_000
        )
      end)

    after_eviction_memory =
      :erpc.call(receiver, GroupBench.Replica, :memory_snapshot, [@name])

    unless after_eviction_memory.registry_rows == 0 and after_eviction_memory.pg_rows == 0 and
             after_eviction_memory.registry_claim_rows == 0 and
             after_eviction_memory.replica_cursor_rows == 0 do
      raise "retired source left visible rows, authoritative claims, or receive cursors: " <>
              inspect(
                Map.take(after_eviction_memory, [
                  :registry_rows,
                  :registry_claim_rows,
                  :pg_rows,
                  :replica_cursor_rows
                ])
              )
    end

    if mode == :registry and is_pid(seed_result) do
      :erpc.call(source, Process, :exit, [seed_result, :kill])
    end

    result = %{
      mode: mode,
      entries: entries,
      shards: Process.get(:bench_shards),
      seed_ms: div(seed_us, 1_000),
      snapshot_ms: div(snapshot_us, 1_000),
      snapshot_rows_per_second: round(entries * 1_000_000 / max(snapshot_us, 1)),
      restart_shard: restart_shard,
      restart_ms: div(restart.elapsed_us, 1_000),
      eviction_ms: div(eviction_us, 1_000),
      source_memory: source_memory,
      receiver_memory: receiver_memory,
      after_eviction_memory: after_eviction_memory
    }

    IO.puts("\n  PERF_RESULT #{inspect(result, pretty: true, limit: :infinity)}")
    stop_groups(replicas)
    result
  end

  defp replicated_row_count(node, :registry) do
    :erpc.call(node, GroupBench.Replica, :total_registry_count, [@name])
  end

  defp replicated_row_count(node, :pg_hotspot) do
    :erpc.call(node, GroupBench.Replica, :total_pg_count, [@name])
  end

  # ── Group lifecycle helpers (all MFA) ─────────────────────────────────

  defp start_group_on(node, opts \\ []) do
    shards = Process.get(:bench_shards, 8)
    opts = Keyword.merge([name: @name, shards: shards, log: false], opts)
    :erpc.call(node, GroupBench.Replica, :start_group, [opts])
  end

  defp stop_group_on(node) do
    :erpc.call(node, GroupBench.Replica, :stop_group, [@name])
  catch
    _, _ -> :ok
  end

  defp stop_groups(replicas) do
    Enum.each(replicas, &stop_group_on/1)
    Process.sleep(100)
  end

  defp wait_for_peer_discovery(replicas) do
    expected = MapSet.new(replicas)

    poll_until(fn ->
      Enum.all?(replicas, fn node ->
        nodes = :erpc.call(node, Group, :nodes, [@name])
        node_set = MapSet.new([node | nodes])
        MapSet.equal?(node_set, expected)
      end)
    end)
  end

  defp poll_until(fun, timeout_ms \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_poll(fun, deadline)
  end

  defp do_poll(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        raise "poll_until timed out"
      end

      Process.sleep(10)
      do_poll(fun, deadline)
    end
  end

  # ── 1. Replication latency ───────────────────────────────────────────

  defp bench_replication_latency([r1, r2] = replicas) do
    header("1. Replication Latency (nil cluster)")

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    n = 1_000

    samples =
      Enum.map(1..n, fn i ->
        key = "repl-#{i}"

        {us, _} =
          :timer.tc(fn ->
            :erpc.call(r1, GroupBench.Replica, :spawn_register, [@name, key])

            poll_until(fn ->
              :erpc.call(r2, Group, :lookup, [@name, key, []]) != nil
            end)
          end)

        us
      end)
      |> Enum.sort()

    report_latency("register on r1 → visible on r2", samples)

    stop_groups(replicas)
  end

  # ── 2. Bulk sync (new peer catches up) ───────────────────────────────

  defp bench_bulk_sync([r1, r2] = replicas) do
    header("2. Bulk Sync (new peer catches up)")

    for key_count <- [1_000, 10_000] do
      subheader("#{format_number(key_count)} keys")

      start_group_on(r1)

      :erpc.call(r1, GroupBench.Replica, :bulk_register, [@name, key_count, "bulk-"], 60_000)

      {sync_us, _} =
        :timer.tc(fn ->
          start_group_on(r2)

          poll_until(
            fn ->
              count = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
              count >= key_count
            end,
            30_000
          )
        end)

      rate = if sync_us > 0, do: round(key_count * 1_000_000 / sync_us), else: 0

      IO.puts("  sync time:  #{format_number(div(sync_us, 1000))} ms")
      IO.puts("  keys/sec:   #{format_number(rate)}")

      stop_groups(replicas)
    end
  end

  defp bench_snapshot_sync([r1, r2] = replicas, key_count) do
    header("Exact Snapshot (receiver below oplog floor)")
    subheader("#{format_number(key_count)} keys")

    start_group_on(r1,
      replicated_oplog_max_entries: 64,
      replicated_anti_entropy_interval: 100,
      replicated_peer_lease_timeout: 15_000
    )

    :erpc.call(
      r1,
      GroupBench.Replica,
      :bulk_register,
      [@name, key_count, "snapshot-"],
      180_000
    )

    {sync_us, _} =
      :timer.tc(fn ->
        start_group_on(r2,
          replicated_oplog_max_entries: 64,
          replicated_anti_entropy_interval: 100,
          replicated_peer_lease_timeout: 15_000
        )

        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) >= key_count
          end,
          120_000
        )
      end)

    rate = if sync_us > 0, do: round(key_count * 1_000_000 / sync_us), else: 0

    IO.puts("  sync time:  #{format_number(div(sync_us, 1000))} ms")
    IO.puts("  keys/sec:   #{format_number(rate)}")

    stop_groups(replicas)
  end

  # ── 3. Concurrent cross-node writes ──────────────────────────────────

  defp bench_concurrent_cross_node([r1, r2] = replicas) do
    header("3. Concurrent Cross-Node Writes")

    n = 5_000

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    {wall_us, _} =
      :timer.tc(fn ->
        t1 =
          Task.async(fn ->
            :erpc.call(r1, GroupBench.Replica, :bulk_register, [@name, n, "r1-"], 60_000)
          end)

        t2 =
          Task.async(fn ->
            :erpc.call(r2, GroupBench.Replica, :bulk_register, [@name, n, "r2-"], 60_000)
          end)

        Task.await_many([t1, t2], 60_000)

        total = 2 * n

        poll_until(
          fn ->
            c1 = :erpc.call(r1, GroupBench.Replica, :total_registry_count, [@name])
            c2 = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
            c1 >= total and c2 >= total
          end,
          30_000
        )
      end)

    total = 2 * n
    report_throughput("concurrent writes + convergence", total, wall_us)

    stop_groups(replicas)
  end

  # ── 4. Named cluster replication ─────────────────────────────────────

  defp bench_named_cluster_replication([r1, r2] = replicas) do
    header("4. Named Cluster Replication Latency")

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    :erpc.call(r1, Group, :connect, [@name, "game"])
    :erpc.call(r2, Group, :connect, [@name, "game"])

    poll_until(fn ->
      n1 = :erpc.call(r1, Group, :nodes, [@name, "game"])
      n2 = :erpc.call(r2, Group, :nodes, [@name, "game"])
      length(n1) >= 1 and length(n2) >= 1
    end)

    n = 1_000

    samples =
      Enum.map(1..n, fn i ->
        key = "game-repl-#{i}"

        {us, _} =
          :timer.tc(fn ->
            :erpc.call(r1, GroupBench.Replica, :spawn_register, [
              @name,
              key,
              [cluster: "game"]
            ])

            poll_until(fn ->
              :erpc.call(r2, Group, :lookup, [@name, key, [cluster: "game"]]) != nil
            end)
          end)

        us
      end)
      |> Enum.sort()

    report_latency("register on r1 → visible on r2 (cluster: \"game\")", samples)

    stop_groups(replicas)
  end

  # ── 5. Process death cleanup ─────────────────────────────────────

  defp bench_death_cleanup([r1, r2] = replicas) do
    header("5. Process Death Cleanup Replication")

    for n <- [1_000, 5_000] do
      subheader("#{format_number(n)} processes")

      start_group_on(r1)
      start_group_on(r2)
      wait_for_peer_discovery(replicas)

      # Register N on r1
      pids = :erpc.call(r1, GroupBench.Replica, :bulk_register, [@name, n, "death-"], 60_000)

      # Wait for replication to r2
      poll_until(
        fn ->
          :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) >= n
        end,
        30_000
      )

      # Kill all registered processes, measure cleanup convergence
      {cleanup_us, _} =
        :timer.tc(fn ->
          :erpc.call(r1, GroupBench.Replica, :kill_processes, [pids])

          poll_until(
            fn ->
              :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) == 0
            end,
            30_000
          )
        end)

      rate = if cleanup_us > 0, do: round(n * 1_000_000 / cleanup_us), else: 0

      IO.puts("  cleanup:    #{format_number(div(cleanup_us, 1000))} ms")
      IO.puts("  deaths/sec: #{format_number(rate)}")

      stop_groups(replicas)
    end
  end

  # ── 6. Register/die churn ────────────────────────────────────────

  defp bench_churn([r1, r2] = replicas) do
    header("6. Register/Die Churn Throughput")

    waves = 10
    per_wave = 500
    total = waves * per_wave

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    {wall_us, _} =
      :timer.tc(fn ->
        for w <- 1..waves do
          pids =
            :erpc.call(
              r1,
              GroupBench.Replica,
              :bulk_register,
              [@name, per_wave, "churn-w#{w}-"],
              60_000
            )

          :erpc.call(r1, GroupBench.Replica, :kill_processes, [pids])
        end

        # Wait for r2 to converge to 0
        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) == 0
          end,
          30_000
        )
      end)

    report_throughput("register+die waves (#{waves}x#{per_wave})", total, wall_us)

    stop_groups(replicas)
  end

  # ── 7. Join/die cleanup ──────────────────────────────────────────

  defp bench_join_death_cleanup([r1, r2] = replicas) do
    header("7. Join/Die Cleanup Replication")

    n = 1_000

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    # Spawn N processes on r1, each joins the same group
    pids = :erpc.call(r1, GroupBench.Replica, :bulk_join, [@name, n, "room"], 60_000)

    # Wait for replication to r2
    poll_until(
      fn ->
        :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name]) >= n
      end,
      30_000
    )

    # Kill all joined processes, measure cleanup convergence
    {cleanup_us, _} =
      :timer.tc(fn ->
        :erpc.call(r1, GroupBench.Replica, :kill_processes, [pids])

        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name]) == 0
          end,
          30_000
        )
      end)

    rate = if cleanup_us > 0, do: round(n * 1_000_000 / cleanup_us), else: 0

    IO.puts("  cleanup:    #{format_number(div(cleanup_us, 1000))} ms")
    IO.puts("  deaths/sec: #{format_number(rate)}")

    stop_groups(replicas)
  end

  # ── 8. Many clusters ───────────────────────────────────────────

  defp bench_many_clusters([r1, r2] = replicas) do
    header("8. Many Clusters (10,000 clusters)")

    num_clusters = 10_000
    prefix = "c-"

    # -- 8a. Cluster connect throughput --

    subheader("connect #{format_number(num_clusters)} clusters")

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    {local_connect_us, _} =
      :timer.tc(fn ->
        t1 =
          Task.async(fn ->
            :erpc.call(
              r1,
              GroupBench.Replica,
              :bulk_connect,
              [@name, num_clusters, prefix],
              120_000
            )
          end)

        t2 =
          Task.async(fn ->
            :erpc.call(
              r2,
              GroupBench.Replica,
              :bulk_connect,
              [@name, num_clusters, prefix],
              120_000
            )
          end)

        Task.await_many([t1, t2], 120_000)
      end)

    # Local completion only means both nodes accepted all Group.connect calls.
    # Controls may be reordered or repaired asynchronously, so seeing one
    # sentinel cluster cannot prove that the other 9,999 have converged.
    expected_cluster_count = num_clusters + 1

    {control_convergence_us, _} =
      try do
        :timer.tc(fn ->
          wait_for_cluster_control_convergence(r1, r2, expected_cluster_count)
        end)
      rescue
        error ->
          r1_status = :erpc.call(r1, GroupBench.Replica, :cluster_control_status, [@name, r2])
          r2_status = :erpc.call(r2, GroupBench.Replica, :cluster_control_status, [@name, r1])

          reraise RuntimeError,
                  [
                    message:
                      "#{Exception.message(error)}; r1=#{inspect(r1_status)} " <>
                        "r2=#{inspect(r2_status)}"
                  ],
                  __STACKTRACE__
      end

    connect_total_us = local_connect_us + control_convergence_us

    IO.puts("  local calls:          #{format_number(div(local_connect_us, 1000))} ms")
    IO.puts("  control convergence:  #{format_number(div(control_convergence_us, 1000))} ms")
    IO.puts("  end-to-end connect:    #{format_number(div(connect_total_us, 1000))} ms")

    IO.puts(
      "  local clusters/sec:   #{format_number(round(num_clusters * 1_000_000 / local_connect_us))}"
    )

    IO.puts(
      "  converged clusters/sec: #{format_number(round(num_clusters * 1_000_000 / connect_total_us))}"
    )

    # -- 8b. Register 1 key per cluster --

    subheader("register across #{format_number(num_clusters)} clusters")

    {local_reg_us, pids} =
      :timer.tc(fn ->
        :erpc.call(
          r1,
          GroupBench.Replica,
          :bulk_register_per_cluster,
          [@name, num_clusters, prefix],
          120_000
        )
      end)

    remote_count_at_handoff =
      :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])

    {reg_convergence_us, _} =
      :timer.tc(fn ->
        poll_until(
          fn ->
            :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) >= num_clusters
          end,
          60_000
        )
      end)

    reg_total_us = local_reg_us + reg_convergence_us

    IO.puts("  local registration:  #{format_number(div(local_reg_us, 1000))} ms")
    IO.puts("  remote at handoff:    #{format_number(remote_count_at_handoff)} / 10,000")
    IO.puts("  data convergence:     #{format_number(div(reg_convergence_us, 1000))} ms")
    IO.puts("  register end-to-end:  #{format_number(div(reg_total_us, 1000))} ms")

    IO.puts(
      "  converged ops/sec:    #{format_number(round(num_clusters * 1_000_000 / reg_total_us))}"
    )

    # -- 8c. Peer re-discovery with many clusters --

    subheader("peer re-discovery with #{format_number(num_clusters)} clusters")

    # Stop Group on r2 (simulates crash), restart it, reconnect all clusters
    stop_group_on(r2)
    Process.sleep(500)

    {restart_us, _} =
      :timer.tc(fn ->
        start_group_on(r2)
        wait_for_peer_discovery(replicas)
      end)

    {local_reconnect_us, _} =
      :timer.tc(fn ->
        :erpc.call(r2, GroupBench.Replica, :bulk_connect, [@name, num_clusters, prefix], 120_000)
      end)

    {reconnect_control_us, _} =
      :timer.tc(fn ->
        wait_for_cluster_control_convergence(r1, r2, expected_cluster_count)
      end)

    {rediscovery_data_us, _} =
      try do
        :timer.tc(fn ->
          poll_until(
            fn ->
              :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) >= num_clusters
            end,
            120_000
          )
        end)
      rescue
        error ->
          counts =
            :erpc.call(r2, GroupBench.Replica, :registry_counts_by_shard, [@name])

          diagnostics =
            :erpc.call(r2, GroupBench.Replica, :replica_process_diagnostics, [@name])

          r1_revision =
            :erpc.call(r1, Group.Replica.Data, :local_cluster_epoch_revision, [@name])

          r2_remote_revision =
            :erpc.call(r2, Group.Replica.Data, :remote_cluster_epoch_revision, [@name, r1])

          r1_remote_revision =
            :erpc.call(r1, Group.Replica.Data, :remote_cluster_epoch_revision, [@name, r2])

          reraise RuntimeError,
                  [
                    message:
                      "#{Exception.message(error)}; rediscovery_counts=#{inspect(counts)} " <>
                        "r1_revision=#{r1_revision} " <>
                        "r2_remote_revision=#{inspect(r2_remote_revision)} " <>
                        "r1_remote_revision=#{inspect(r1_remote_revision)} " <>
                        "replica=#{inspect(diagnostics)}"
                  ],
                  __STACKTRACE__
      end

    rediscovery_total_us =
      restart_us + local_reconnect_us + reconnect_control_us + rediscovery_data_us

    IO.puts("  restart + discovery:  #{format_number(div(restart_us, 1000))} ms")
    IO.puts("  local reconnect:      #{format_number(div(local_reconnect_us, 1000))} ms")
    IO.puts("  control convergence:  #{format_number(div(reconnect_control_us, 1000))} ms")
    IO.puts("  data convergence:     #{format_number(div(rediscovery_data_us, 1000))} ms")
    IO.puts("  re-discovery total:    #{format_number(div(rediscovery_total_us, 1000))} ms")

    # -- 8d. Disconnect cleanup --

    subheader("disconnect #{format_number(num_clusters)} clusters")

    {local_disconnect_us, _} =
      :timer.tc(fn ->
        :erpc.call(
          r1,
          GroupBench.Replica,
          :bulk_disconnect,
          [@name, num_clusters, prefix],
          120_000
        )
      end)

    {cleanup_us, _} =
      :timer.tc(fn ->
        # Wait for r2 to see r1's entries cleaned from all clusters
        try do
          poll_until(
            fn ->
              :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) == 0
            end,
            60_000
          )
        rescue
          error ->
            count = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
            sample = :erpc.call(r2, GroupBench.Replica, :registry_sample, [@name])

            local_revision =
              :erpc.call(r1, Group.Replica.Data, :local_cluster_epoch_revision, [@name])

            remote_revision =
              :erpc.call(r2, Group.Replica.Data, :remote_cluster_epoch_revision, [@name, r1])

            observed_revision =
              :erpc.call(
                r2,
                Group.Replica.Data,
                :remote_cluster_epoch_observed_revision,
                [@name, r1]
              )

            reraise RuntimeError,
                    [
                      message:
                        "#{Exception.message(error)}; remaining=#{count} " <>
                          "local_revision=#{local_revision} " <>
                          "remote_revision=#{inspect(remote_revision)} " <>
                          "observed_revision=#{inspect(observed_revision)} " <>
                          "sample=#{inspect(sample)}"
                    ],
                    __STACKTRACE__
        end
      end)

    disconnect_total_us = local_disconnect_us + cleanup_us

    IO.puts("  local disconnect:    #{format_number(div(local_disconnect_us, 1000))} ms")
    IO.puts("  remote cleanup:      #{format_number(div(cleanup_us, 1000))} ms")
    IO.puts("  disconnect total:    #{format_number(div(disconnect_total_us, 1000))} ms")

    IO.puts(
      "  converged clusters/sec: #{format_number(round(num_clusters * 1_000_000 / disconnect_total_us))}"
    )

    # Kill leftover processes
    :erpc.call(r1, GroupBench.Replica, :kill_processes, [pids])
    stop_groups(replicas)
  end

  defp wait_for_cluster_control_convergence(r1, r2, expected_cluster_count) do
    r1_revision =
      :erpc.call(r1, GroupBench.Replica, :cluster_control_revision, [@name])

    r2_revision =
      :erpc.call(r2, GroupBench.Replica, :cluster_control_revision, [@name])

    poll_until(
      fn ->
        r1_converged? =
          :erpc.call(
            r1,
            GroupBench.Replica,
            :cluster_control_converged?,
            [@name, r2, expected_cluster_count, r2_revision]
          )

        r2_converged? =
          :erpc.call(
            r2,
            GroupBench.Replica,
            :cluster_control_converged?,
            [@name, r1, expected_cluster_count, r1_revision]
          )

        r1_converged? and r2_converged?
      end,
      120_000
    )
  end

  # ── 9. Busy app simulation ──────────────────────────────────────────

  defp bench_busy_app([r1, r2] = replicas) do
    header("9. Busy App Simulation")

    num_clusters = 50
    workers_per_node = 10
    users_per_worker = 20
    rooms_per_worker = 5
    churn_rounds = 3
    lookups_per_round = 50
    dispatches_per_round = 10

    total_users = num_clusters * workers_per_node * 2 * users_per_worker

    total_rooms =
      num_clusters * workers_per_node * 2 * rooms_per_worker *
        div(users_per_worker, rooms_per_worker)

    IO.puts("  clusters:      #{num_clusters}")
    IO.puts("  workers/node:  #{workers_per_node}")
    IO.puts("  users/worker:  #{users_per_worker}")
    IO.puts("  rooms/worker:  #{rooms_per_worker}")
    IO.puts("  churn rounds:  #{churn_rounds}")

    IO.puts(
      "  initial pids:  #{format_number(total_users + total_rooms)} (#{format_number(total_users)} reg + #{format_number(total_rooms)} pg)"
    )

    IO.puts("")

    start_group_on(r1)
    start_group_on(r2)
    wait_for_peer_discovery(replicas)

    # Connect all clusters on both nodes
    clusters = for i <- 1..num_clusters, do: "org/#{i}"

    for node <- replicas, cluster <- clusters do
      :erpc.call(node, Group, :connect, [@name, cluster])
    end

    # Wait for cluster membership to converge
    poll_until(fn ->
      Enum.all?(replicas, fn node ->
        n = :erpc.call(node, Group, :nodes, [@name, List.last(clusters)])
        length(n) >= 1
      end)
    end)

    worker_opts = [
      users: users_per_worker,
      rooms: rooms_per_worker,
      churn_rounds: churn_rounds,
      lookups_per_round: lookups_per_round,
      dispatches_per_round: dispatches_per_round
    ]

    # Run all workers concurrently across both nodes
    {wall_us, all_pids} =
      :timer.tc(fn ->
        tasks =
          for {node, ni} <- Enum.with_index(replicas),
              {cluster, ci} <- Enum.with_index(clusters),
              wi <- 1..workers_per_node do
            worker_id = "n#{ni}_c#{ci}_w#{wi}"

            Task.async(fn ->
              :erpc.call(
                node,
                GroupBench.Replica,
                :run_busy_worker,
                [@name, cluster, worker_id, worker_opts],
                120_000
              )
            end)
          end

        results = Task.await_many(tasks, 120_000)
        List.flatten(results)
      end)

    total_ops =
      num_clusters * workers_per_node * 2 *
        (users_per_worker + div(users_per_worker, rooms_per_worker) * rooms_per_worker +
           churn_rounds *
             (lookups_per_round + dispatches_per_round + div(users_per_worker, 4) * 2))

    wall_ms = div(wall_us, 1000)
    ops_sec = if wall_us > 0, do: round(total_ops * 1_000_000 / wall_us), else: 0

    IO.puts("  --- results ---")
    IO.puts("  wall time:     #{format_number(wall_ms)} ms")
    IO.puts("  total ops:     ~#{format_number(total_ops)}")
    IO.puts("  ops/sec:       ~#{format_number(ops_sec)}")

    # Convergence check: wait for both nodes to agree on registry count
    subheader("convergence")

    {converge_us, _} =
      :timer.tc(fn ->
        poll_until(
          fn ->
            c1 = :erpc.call(r1, GroupBench.Replica, :total_registry_count, [@name])
            c2 = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
            c1 == c2 and c1 > 0
          end,
          30_000
        )
      end)

    final_r1 = :erpc.call(r1, GroupBench.Replica, :total_registry_count, [@name])
    final_r2 = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
    pg_r1 = :erpc.call(r1, GroupBench.Replica, :total_pg_count, [@name])
    pg_r2 = :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name])

    IO.puts("  converge:      #{format_number(div(converge_us, 1000))} ms")
    IO.puts("  registry r1:   #{format_number(final_r1)}")
    IO.puts("  registry r2:   #{format_number(final_r2)}")
    IO.puts("  pg r1:         #{format_number(pg_r1)}")
    IO.puts("  pg r2:         #{format_number(pg_r2)}")

    IO.puts(
      "  match:         #{if final_r1 == final_r2 and pg_r1 == pg_r2, do: "YES", else: "NO"}"
    )

    # Cleanup
    for node <- replicas do
      local_pids = Enum.filter(all_pids, fn pid -> node(pid) == node end)
      :erpc.call(node, GroupBench.Replica, :kill_processes, [local_pids])
    end

    stop_groups(replicas)
  end

  # ── 10. Local register/join throughput under replicated PG pressure ─

  defp bench_local_requests_under_replicated_pg_pressure([r1, r2] = replicas) do
    header("10. Local Register/Join Throughput Under Replicated PG Pressure")

    total_ops = 1_000
    inflight_levels = [1, 4, 8, 16, 32]
    spam_workers = 32
    hot_shard = 0
    request_timeout = 5_000

    IO.puts("  setup:       1 shard, #{spam_workers} remote PG update spammers")
    IO.puts("  total ops:   #{total_ops} per inflight level")
    IO.puts("  inflight:    #{inspect(inflight_levels)}")
    IO.puts("  hot shard:   #{hot_shard}")
    IO.puts("  timeout:     #{request_timeout} ms")

    start_group_on(r1, shards: 1, replicated_pg_receiver_local_request_quota: 8)
    start_group_on(r2, shards: 1, replicated_pg_receiver_local_request_quota: 8)
    wait_for_peer_discovery(replicas)

    spammers =
      :erpc.call(
        r1,
        GroupBench.Replica,
        :start_pg_update_spammers,
        [@name, spam_workers, "pressure/spam/"],
        60_000
      )

    poll_until(
      fn ->
        case :erpc.call(r2, GroupBench.Replica, :replicated_pg_pressure, [@name, hot_shard]) do
          %{message_queue_len: queue_len, pending_replicated_pg_len: pending_len} ->
            queue_len > 0 or pending_len > 0
        end
      end,
      10_000
    )

    try do
      registry_baseline = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
      pg_baseline = :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name])

      register_results =
        bench_local_load_sweep(
          r2,
          :local_register_load,
          "local Group.register/4 on receiver",
          total_ops,
          inflight_levels,
          "pressure/register/",
          [timeout: request_timeout],
          fn ->
            poll_until(
              fn ->
                :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) ==
                  registry_baseline
              end,
              30_000
            )
          end
        )

      report_best_no_timeout_result("register", register_results)

      join_results =
        bench_local_load_sweep(
          r2,
          :local_join_load,
          "local Group.join/4 on receiver",
          total_ops,
          inflight_levels,
          "pressure/join/",
          [timeout: request_timeout],
          fn ->
            poll_until(
              fn ->
                :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name]) == pg_baseline
              end,
              30_000
            )
          end
        )

      report_best_no_timeout_result("join", join_results)
    after
      :erpc.call(r1, GroupBench.Replica, :kill_processes, [spammers], 60_000)
      stop_groups(replicas)
    end
  end

  defp bench_local_requests_under_replicated_registry_pressure([r1, r2] = replicas) do
    header("Local Register/Join Throughput Under Replicated Registry Pressure")

    total_ops = 1_000
    inflight_levels = [1, 4, 8, 16, 32]
    spam_workers = 32
    hot_shard = 0
    request_timeout = 5_000

    IO.puts("  setup:       1 shard, #{spam_workers} remote registry update spammers")
    IO.puts("  total ops:   #{total_ops} per inflight level")
    IO.puts("  inflight:    #{inspect(inflight_levels)}")
    IO.puts("  hot shard:   #{hot_shard}")
    IO.puts("  timeout:     #{request_timeout} ms")

    start_group_on(r1, shards: 1, replicated_pg_receiver_local_request_quota: 8)
    start_group_on(r2, shards: 1, replicated_pg_receiver_local_request_quota: 8)
    wait_for_peer_discovery(replicas)

    spammers =
      :erpc.call(
        r1,
        GroupBench.Replica,
        :start_registry_update_spammers,
        [@name, spam_workers, "pressure/registry-spam/"],
        60_000
      )

    poll_until(
      fn ->
        case :erpc.call(r2, GroupBench.Replica, :replicated_registry_pressure, [@name, hot_shard]) do
          %{message_queue_len: queue_len, pending_replicated_registry_len: pending_len} ->
            queue_len > 0 or pending_len > 0
        end
      end,
      10_000
    )

    try do
      registry_baseline = :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name])
      pg_baseline = :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name])

      register_results =
        bench_local_load_sweep(
          r2,
          :local_register_load,
          "local Group.register/4 on receiver",
          total_ops,
          inflight_levels,
          "pressure/registry-register/",
          [timeout: request_timeout],
          fn ->
            poll_until(
              fn ->
                :erpc.call(r2, GroupBench.Replica, :total_registry_count, [@name]) ==
                  registry_baseline
              end,
              30_000
            )
          end
        )

      report_best_no_timeout_result("register", register_results)

      join_results =
        bench_local_load_sweep(
          r2,
          :local_join_load,
          "local Group.join/4 on receiver",
          total_ops,
          inflight_levels,
          "pressure/registry-join/",
          [timeout: request_timeout],
          fn ->
            poll_until(
              fn ->
                :erpc.call(r2, GroupBench.Replica, :total_pg_count, [@name]) == pg_baseline
              end,
              30_000
            )
          end
        )

      report_best_no_timeout_result("join", join_results)
    after
      :erpc.call(r1, GroupBench.Replica, :kill_processes, [spammers], 60_000)
      stop_groups(replicas)
    end
  end

  defp bench_local_load_sweep(
         node,
         replica_fun,
         label,
         total_ops,
         inflight_levels,
         key_prefix,
         request_opts,
         wait_for_cleanup
       ) do
    for inflight <- inflight_levels do
      result =
        :erpc.call(
          node,
          GroupBench.Replica,
          replica_fun,
          [@name, total_ops, inflight, "#{key_prefix}#{inflight}/", request_opts],
          120_000
        )

      report_load_profile(
        "#{label} (inflight=#{inflight})",
        result.successful_ops,
        result.wall_us,
        result.samples,
        result.timeout_count,
        result.error_count
      )

      :erpc.call(node, GroupBench.Replica, :kill_processes, [result.pids], 60_000)
      wait_for_cleanup.()

      {inflight, result}
    end
  end

  defp report_best_no_timeout_result(op_name, results) do
    case Enum.filter(results, fn {_inflight, result} ->
           result.timeout_count == 0 and result.error_count == 0
         end)
         |> Enum.max_by(
           fn {_inflight, result} -> result.successful_ops / max(result.wall_us, 1) end,
           fn -> nil end
         ) do
      nil ->
        IO.puts("  best no-timeout #{op_name}: none")

      {inflight, result} ->
        ops_sec = round(result.successful_ops * 1_000_000 / max(result.wall_us, 1))

        IO.puts(
          "  best no-timeout #{op_name}: inflight=#{inflight}, #{format_number(ops_sec)} ops/sec"
        )
    end
  end
end
