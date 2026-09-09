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
      with_group([name: @name, shards: shards], fn _workers ->
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

        exact_samples =
          collect_samples(samples, fn -> Group.member_count(@name, exact_key) end, fn 1 -> :ok end)

        report_latency("exact Group.member_count/3", exact_samples)

        warmup(1_000, fn -> Group.member_count(@name, prefix) end)

        prefix_samples =
          collect_samples(samples, fn -> Group.member_count(@name, prefix) end, fn ^cardinality ->
            :ok
          end)

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

      with_group([name: @name, shards: @default_shards], fn workers ->
        maybe_connect_cluster(cluster_opt)
        key_count = 10_000
        measure_count = 100_000

        # Each process registers itself
        {_, pids} =
          run_workers(workers, key_count, fn i ->
            Group.register(@name, "key-#{i}", %{i: i}, cluster_opts(cluster_opt))
          end)

        entries = indexed_entries(pids, &"key-#{&1}", &%{i: &1})
        verify_registry(@name, entries, cluster_opts(cluster_opt))
        expected = entries |> Enum.map(fn {_, pid, meta} -> {pid, meta} end) |> List.to_tuple()

        # warmup
        warmup(1_000, fn -> Group.lookup(@name, "key-1", cluster_opts(cluster_opt)) end)

        # measure
        samples =
          collect_samples(
            measure_count,
            fn ->
              i = :rand.uniform(key_count)
              {i, Group.lookup(@name, "key-#{i}", cluster_opts(cluster_opt))}
            end,
            fn {i, actual} ->
              if actual != elem(expected, i - 1),
                do: raise("incorrect lookup sample for key-#{i}")

              :ok
            end
          )

        verify_registry(@name, entries, cluster_opts(cluster_opt))
        report_latency("Group.lookup/3", samples)
      end)
    end
  end

  # ── 2. members throughput ─────────────────────────────────────────────

  defp bench_members do
    header("2. Members Throughput (ETS read)")

    for {cluster_label, cluster_opt} <- clusters() do
      subheader("cluster: #{cluster_label}")

      with_group([name: @name, shards: @default_shards], fn workers ->
        maybe_connect_cluster(cluster_opt)
        group_count = 100
        members_per_group = 100
        measure_count = 100_000

        total = group_count * members_per_group

        # Each process joins a group
        group_key = fn i -> "group-#{rem(i - 1, group_count) + 1}" end

        {_, pids} =
          run_workers(workers, total, fn i ->
            Group.join(@name, group_key.(i), %{}, cluster_opts(cluster_opt))
          end)

        entries = indexed_entries(pids, group_key, fn _ -> %{} end)
        verify_members(@name, entries, cluster_opts(cluster_opt))

        expected =
          entries
          |> Enum.group_by(&elem(&1, 0), fn {_, pid, meta} -> {pid, meta} end)
          |> Map.new(fn {key, members} -> {key, Enum.sort(members)} end)

        warmup(1_000, fn -> Group.members(@name, "group-1", cluster_opts(cluster_opt)) end)

        samples =
          collect_samples(
            measure_count,
            fn ->
              key = "group-#{:rand.uniform(group_count)}"
              {key, Group.members(@name, key, cluster_opts(cluster_opt))}
            end,
            fn {key, actual} ->
              if Enum.sort(actual) != Map.fetch!(expected, key),
                do: raise("incorrect members sample for #{key}")

              :ok
            end
          )

        verify_members(@name, entries, cluster_opts(cluster_opt))
        report_latency("Group.members/3", samples)
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
        with_group([name: @name, shards: shards], fn workers ->
          maybe_connect_cluster(cluster_opt)

          {wall_us, pids} =
            run_workers(workers, n, fn i ->
              Group.register(@name, "reg-#{i}", %{}, cluster_opts(cluster_opt))
            end)

          entries = indexed_entries(pids, &"reg-#{&1}", fn _ -> %{} end)
          verify_registry(@name, entries, cluster_opts(cluster_opt))
          report_throughput("shards=#{shards}", n, wall_us)
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

      with_group([name: @name, shards: @default_shards], fn _workers ->
        maybe_connect_cluster(cluster_opt)
        opts = cluster_opts(cluster_opt)

        # register/unregister from self — self() is the calling process
        samples =
          collect_samples(n, fn ->
            key = "cycle-#{:erlang.unique_integer([:positive])}"
            :ok = Group.register(@name, key, %{}, opts)
            :ok = Group.unregister(@name, key, opts)
          end)

        verify_registry(@name, [], opts)
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
        with_group([name: @name, shards: shards], fn workers ->
          maybe_connect_cluster(cluster_opt)
          group_key = fn i -> "join-group-#{rem(i, 100)}" end

          {wall_us, pids} =
            run_workers(workers, n, fn i ->
              Group.join(@name, group_key.(i), %{}, cluster_opts(cluster_opt))
            end)

          entries = indexed_entries(pids, group_key, fn _ -> %{} end)
          verify_members(@name, entries, cluster_opts(cluster_opt))
          report_throughput("shards=#{shards}", n, wall_us)
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

      with_group([name: @name, shards: @default_shards], fn _workers ->
        maybe_connect_cluster(cluster_opt)
        opts = cluster_opts(cluster_opt)

        samples =
          collect_samples(n, fn ->
            key = "cycle/group/#{:erlang.unique_integer([:positive])}"
            :ok = Group.join(@name, key, %{}, opts)
            :ok = Group.leave(@name, key, opts)
          end)

        0 = Group.member_count(@name, "cycle/group/", opts)
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

      with_group([name: @name, shards: @default_shards], fn workers ->
        maybe_connect_cluster(cluster_opt)
        :ok = Group.monitor(@name, :all, cluster_opts(cluster_opt))
        drain_stale_group_events()

        {wall_us, pids} =
          run_workers(
            workers,
            n,
            fn i ->
              Group.register(@name, "mon-#{i}", %{}, cluster_opts(cluster_opt))
            end,
            fn pids ->
              entries = indexed_entries(pids, &"mon-#{&1}", fn _ -> %{} end)
              await_registered_events(@name, cluster_opt, entries)
            end
          )

        entries = indexed_entries(pids, &"mon-#{&1}", fn _ -> %{} end)
        verify_registry(@name, entries, cluster_opts(cluster_opt))
        report_throughput("events (register → validated receipt)", n, wall_us)
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

  defp indexed_entries(pids, key, meta) do
    pids
    |> Enum.with_index(1)
    |> Enum.map(fn {pid, i} ->
      {key.(i), pid, meta.(i)}
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

  defp drain_stale_group_events do
    receive do
      {:group, _events, _info} -> drain_stale_group_events()
    after
      0 -> :ok
    end
  end
end
