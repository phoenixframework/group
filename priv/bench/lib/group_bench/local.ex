defmodule GroupBench.Local do
  @moduledoc """
  Local (single-node) benchmarks for Group.
  """

  import GroupBench.Helpers

  @name :bench
  @default_shards 8
  @shard_counts [1, 2, 4, 8, 16, 32, 64]

  def run do
    header("Local Benchmarks")
    IO.puts("  schedulers_online: #{System.schedulers_online()}")
    IO.puts("  default_shards:    #{@default_shards}")
    IO.puts("  shard_sweep:       #{Enum.join(@shard_counts, ", ")}")

    bench_lookup()
    bench_members()
    bench_register_shards()
    bench_register_unregister_cycle()
    bench_join_shards()
    bench_join_leave_cycle()
    bench_monitor_events()

    IO.puts("\n  Done.\n")
  end

  def run_member_counts do
    shards = System.get_env("GROUP_BENCH_SHARDS", "32") |> String.to_integer()
    samples = System.get_env("GROUP_BENCH_COUNT_SAMPLES", "100000") |> String.to_integer()

    header("Materialized Membership Count Scale")
    IO.puts("  schedulers_online: #{System.schedulers_online()}")
    IO.puts("  shards:            #{shards}")
    IO.puts("  samples/query:     #{format_number(samples)}")

    Enum.each([1_000, 100_000, 1_000_000], fn cardinality ->
      with_group([name: @name, shards: shards], fn ->
        subheader("#{format_number(cardinality)} distinct membership keys")
        {seed_us, :ok} = time_us(fn -> seed_member_count_index(cardinality, shards) end)

        exact_key = "count/tenant/item-1"
        prefix = "count/tenant/"
        1 = Group.member_count(@name, exact_key)
        ^cardinality = Group.member_count(@name, prefix)
        1 = Group.local_member_count(@name, exact_key)
        ^cardinality = Group.local_member_count(@name, prefix)

        memory_bytes =
          Enum.reduce(0..(shards - 1), 0, fn shard, total ->
            words = :ets.info(Group.Replica.Data.pg_count_table(@name, shard), :memory)
            total + words * :erlang.system_info(:wordsize)
          end)

        IO.puts("  seed time: #{format_number(div(seed_us, 1_000))} ms")
        IO.puts("  count-index memory: #{Float.round(memory_bytes / 1_048_576, 1)} MiB")

        warmup(1_000, fn -> Group.member_count(@name, exact_key) end)
        exact_samples = collect_samples(samples, fn -> Group.member_count(@name, exact_key) end)
        report_latency("exact Group.member_count/3", exact_samples)

        warmup(1_000, fn -> Group.member_count(@name, prefix) end)
        prefix_samples = collect_samples(samples, fn -> Group.member_count(@name, prefix) end)
        report_latency("prefix Group.member_count/3", prefix_samples)
      end)
    end)

    IO.puts("\n  Done.\n")
  end

  # ── 1. lookup throughput ──────────────────────────────────────────────

  defp bench_lookup do
    header("1. Lookup Throughput (ETS read)")

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        key_count = 10_000
        measure_count = 100_000

        # Each process registers itself
        pids =
          register_from_spawned_processes(key_count, fn i ->
            Group.register(@name, "key-#{i}", %{i: i}, cluster_opts(cluster_opt))
          end)

        try do
          # warmup
          warmup(1_000, fn -> Group.lookup(@name, "key-1", cluster_opts(cluster_opt)) end)

          # measure
          samples =
            collect_samples(measure_count, fn ->
              i = :rand.uniform(key_count)
              Group.lookup(@name, "key-#{i}", cluster_opts(cluster_opt))
            end)

          report_latency("Group.lookup/3", samples)
        after
          stop_spawned_processes(pids)
        end
      end)
    end
  end

  # ── 2. members throughput ─────────────────────────────────────────────

  defp bench_members do
    header("2. Members Throughput (ETS read)")

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        group_count = 100
        members_per_group = 100
        measure_count = 100_000

        total = group_count * members_per_group

        # Each process joins a group
        pids =
          register_from_spawned_processes(total, fn i ->
            gi = rem(i - 1, group_count) + 1
            Group.join(@name, "group-#{gi}", %{}, cluster_opts(cluster_opt))
          end)

        try do
          warmup(1_000, fn -> Group.members(@name, "group-1", cluster_opts(cluster_opt)) end)

          samples =
            collect_samples(measure_count, fn ->
              gi = :rand.uniform(group_count)
              Group.members(@name, "group-#{gi}", cluster_opts(cluster_opt))
            end)

          report_latency("Group.members/3", samples)
        after
          stop_spawned_processes(pids)
        end
      end)
    end
  end

  # ── 3. register throughput (shard scaling) ────────────────────────────

  defp bench_register_shards do
    header("3. Register Throughput (shard scaling)")

    n = 10_000

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      for shards <- @shard_counts do
        with_group([name: @name, shards: shards], fn ->
          maybe_connect_cluster(cluster_opt)

          {wall_us, pids} =
            time_us(fn ->
              register_from_spawned_processes(n, fn i ->
                Group.register(@name, "reg-#{i}", %{}, cluster_opts(cluster_opt))
              end)
            end)

          try do
            report_throughput("shards=#{shards}", n, wall_us)
          after
            stop_spawned_processes(pids)
          end
        end)
      end
    end
  end

  # ── 4. register/unregister cycle ──────────────────────────────────────

  defp bench_register_unregister_cycle do
    header("4. Register/Unregister Cycle")

    n = 10_000

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        opts = cluster_opts(cluster_opt)

        # register/unregister from self — self() is the calling process
        samples =
          collect_samples(n, fn ->
            key = "cycle-#{:erlang.unique_integer([:positive])}"
            :ok = Group.register(@name, key, %{}, opts)
            :ok = Group.unregister(@name, key, opts)
          end)

        report_latency("register+unregister", samples)
      end)
    end
  end

  # ── 5. join throughput (shard scaling) ────────────────────────────────

  defp bench_join_shards do
    header("5. Join Throughput (shard scaling)")

    n = 10_000

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      for shards <- @shard_counts do
        with_group([name: @name, shards: shards], fn ->
          maybe_connect_cluster(cluster_opt)

          {wall_us, pids} =
            time_us(fn ->
              register_from_spawned_processes(n, fn i ->
                Group.join(@name, "join-group-#{rem(i, 100)}", %{}, cluster_opts(cluster_opt))
              end)
            end)

          try do
            report_throughput("shards=#{shards}", n, wall_us)
          after
            stop_spawned_processes(pids)
          end
        end)
      end
    end
  end

  # ── 6. join/leave cycle ────────────────────────────────────────────────

  defp bench_join_leave_cycle do
    header("6. Join/Leave Cycle")

    n = 10_000

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        opts = cluster_opts(cluster_opt)

        samples =
          collect_samples(n, fn ->
            key = "cycle/group/#{:erlang.unique_integer([:positive])}"
            :ok = Group.join(@name, key, %{}, opts)
            :ok = Group.leave(@name, key, opts)
          end)

        report_latency("join+leave (two slash-prefixes)", samples)
      end)
    end
  end

  # ── 7. monitor event delivery ─────────────────────────────────────────

  defp bench_monitor_events do
    header("7. Monitor Event Delivery")

    n = 5_000

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn ->
        maybe_connect_cluster(cluster_opt)
        :ok = Group.monitor(@name, :all, cluster_opts(cluster_opt))
        drain_stale_group_events()

        {wall_us, pids} =
          time_us(fn ->
            pids =
              register_from_spawned_processes(n, fn i ->
                Group.register(@name, "mon-#{i}", %{}, cluster_opts(cluster_opt))
              end)

            # drain all N events
            drain_events(n)
            pids
          end)

        try do
          report_throughput("events (register → receive)", n, wall_us)
        after
          stop_spawned_processes(pids)
        end
      end)
    end
  end

  # ── Helpers ───────────────────────────────────────────────────────────

  defp clusters do
    [{"nil (default)", nil}, {"named (\"game\")", "game"}]
  end

  defp maybe_connect_cluster(nil), do: :ok
  defp maybe_connect_cluster(cluster), do: Group.connect(@name, cluster)

  defp cluster_opts(nil), do: []
  defp cluster_opts(cluster), do: [cluster: cluster]

  @doc false
  # Spawns N processes, each of which calls `fun.(index)` where index is 1..n.
  # Processes stay alive after the call. Waits for all to complete.
  defp register_from_spawned_processes(n, fun) do
    parent = self()

    pids =
      Enum.map(1..n, fn i ->
        spawn(fn ->
          result = fun.(i)
          send(parent, {:done, self(), result})

          # Stay alive so the registration/membership persists
          Process.sleep(:infinity)
        end)
      end)

    # Wait for all to complete
    Enum.each(pids, fn pid ->
      receive do
        {:done, ^pid, _result} -> :ok
      after
        10_000 -> raise "Timed out waiting for #{inspect(pid)}"
      end
    end)

    pids
  end

  defp stop_spawned_processes(pids) do
    refs = Enum.map(pids, &{&1, Process.monitor(&1)})
    Enum.each(pids, &Process.exit(&1, :kill))

    Enum.each(refs, fn {pid, ref} ->
      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      after
        5_000 -> raise "Timed out stopping #{inspect(pid)}"
      end
    end)
  end

  # The count read benchmark intentionally seeds only the derived read index.
  # Mutation and rebuild costs are measured by the normal join/leave and
  # distributed recovery scenarios; this isolates whether lookup latency stays
  # flat as the dynamic count table itself grows to a million exact keys.
  defp seed_member_count_index(cardinality, shards) do
    totals =
      1..cardinality
      |> Stream.chunk_every(10_000)
      |> Enum.reduce(%{}, fn indexes, totals ->
        {rows_by_shard, totals} =
          Enum.reduce(indexes, {%{}, totals}, fn index, {rows_by_shard, counts} ->
            key = "count/tenant/item-#{index}"
            shard = Group.Replica.shard_index_for(nil, key, shards)
            row = {{nil, :exact, key}, 1, 1}

            {
              Map.update(rows_by_shard, shard, [row], &[row | &1]),
              Map.update(counts, shard, 1, &(&1 + 1))
            }
          end)

        Enum.each(rows_by_shard, fn {shard, rows} ->
          true = :ets.insert(Group.Replica.Data.pg_count_table(@name, shard), rows)
        end)

        totals
      end)

    Enum.each(totals, fn {shard, count} ->
      table = Group.Replica.Data.pg_count_table(@name, shard)

      true =
        :ets.insert(table, [
          {{nil, :prefix, "count/"}, count, count},
          {{nil, :prefix, "count/tenant/"}, count, count}
        ])
    end)

    :ok
  end

  defp drain_events(0), do: :ok

  defp drain_events(remaining) do
    receive do
      {:group, events, _info} ->
        count = Enum.count(events, &match?(%Group.Event{type: :registered}, &1))
        drain_events(remaining - count)
    after
      5_000 -> IO.puts("    WARNING: timed out waiting for events, #{remaining} remaining")
    end
  end

  defp drain_stale_group_events do
    receive do
      {:group, _events, _info} -> drain_stale_group_events()
    after
      0 -> :ok
    end
  end
end
