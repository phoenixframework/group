defmodule Group.MutationCampaign do
  @moduledoc """
  Runs protocol mutations in isolated repository copies.

  A mutant is useful only when it compiles and its designated regression test
  fails. The source checkout is never edited.
  """

  @repo Path.expand("../..", __DIR__)
  @timeout_seconds "120"

  # Each entry replaces one correct production fragment with an intentionally
  # faulty fragment, but only inside an isolated campaign checkout.
  @mutations [
    %{
      name: "accept_old_generation",
      file: "lib/group/replica.ex",
      correct_source:
        "Protocol.stream_generation(stream_id) == Data.remote_generation(state.name, source_node) and",
      faulty_source: "true and",
      test: ["test/distributed_test.exs:5328"]
    },
    %{
      name: "accept_old_epoch",
      file: "lib/group/replica.ex",
      correct_source: """
      Protocol.stream_epoch(stream_id) ==
              Data.remote_cluster_epoch(state.name, source_node, cluster) and
      """,
      faulty_source: """
      true and
      """,
      test: ["test/distributed_test.exs:4774"]
    },
    %{
      name: "advance_cursor_across_gap",
      file: "lib/group/replica.ex",
      correct_source: """
              [{first_seq, _mutations} | _] when first_seq > cursor + 1 ->
                request_replica_need(state, source_node, stream_id, cursor + 1)
      """,
      faulty_source: """
              [{first_seq, _mutations} | _] when first_seq > cursor + 1 ->
                :ok =
                  Data.put_replica_cursor(
                    state.name,
                    state.shard_index,
                    stream_id,
                    first_seq - 1
                  )

                apply_replica_delta_run(
                  state,
                  source_node,
                  stream_id,
                  records,
                  advertised_head
                )
      """,
      test: ["test/distributed_test.exs:5386"]
    },
    %{
      name: "registry_snapshot_is_additive",
      file: "lib/group/replica/data.ex",
      correct_source: "existing = registry_claims_for_stream(name, shard, stream_id)",
      faulty_source: "existing = []",
      test: ["test/distributed_test.exs:4030"]
    },
    %{
      name: "pg_snapshot_is_additive",
      file: "lib/group/replica.ex",
      correct_source: """
          current =
            state.name
            |> Data.pg_entries_for_origin(state.shard_index, cluster, source_node)
            |> Map.new(fn {key, pid, meta, time} -> {{key, pid}, {meta, time}} end)
      """,
      faulty_source: """
          current = %{}
      """,
      test: ["test/distributed_test.exs:4030"]
    },
    %{
      name: "disable_below_floor_snapshot",
      file: "lib/group/replica.ex",
      correct_source: """
            true ->
              {:state, send_replica_snapshot(state, target_node, stream_id, head)}
      """,
      faulty_source: """
            true ->
              {:state, state}
      """,
      test: ["test/replica_model_property_test.exs:180"]
    },
    %{
      name: "do_not_sequence_process_down",
      file: "lib/group/replica.ex",
      correct_source: """
              sequenced_downs =
                append_process_down_records(state, reason_by_pid, pending_reg, pending_pg)
      """,
      faulty_source:
        "        sequenced_downs =\n          if false,\n            do: append_process_down_records(state, reason_by_pid, pending_reg, pending_pg),\n            else: []\n",
      test: ["test/distributed_test.exs:3940"]
    },
    %{
      name: "do_not_exit_conflict_loser",
      file: "lib/group/replica.ex",
      correct_source: """
                winner_meta = if winner, do: elem(winner, 1), else: nil
                exit_local_conflict_loser(pid, key, winner_meta)
                acc
      """,
      faulty_source: """
                _winner_meta = if winner, do: elem(winner, 1), else: nil
                _ = Process.alive?(pid)
                acc
      """,
      test: ["test/replica_model_property_test.exs:77"]
    },
    %{
      name: "heartbeat_promotes_observed_authority",
      file: "lib/group/replica.ex",
      correct_source:
        "            replica_view_current?(state, remote_node) ->\n" <>
          "          state\n" <>
          "          |> put_remote_shard(remote_node, remote_pid)\n" <>
          "          |> touch_replica_peer(remote_node)",
      faulty_source:
        "            replica_view_current?(state, remote_node) ->\n" <>
          "          :ok =\n" <>
          "            Data.put_remote_view_info(\n" <>
          "              state.name,\n" <>
          "              state.shard_index,\n" <>
          "              remote_node,\n" <>
          "              generation,\n" <>
          "              epoch_revision,\n" <>
          "              epoch_revision\n" <>
          "            )\n\n" <>
          "          state\n" <>
          "          |> put_remote_shard(remote_node, remote_pid)\n" <>
          "          |> touch_replica_peer(remote_node)",
      test: ["test/distributed_test.exs:4247"]
    },
    %{
      name: "skip_authority_fanout",
      file: "lib/group/replica.ex",
      correct_source: """
          fan_out_to_siblings(
            state,
            {:replica_authority_installed_local, remote_node, generation, epoch_revision,
             old_generation, stale_epochs}
          )
      """,
      faulty_source: """
          :ok
      """,
      test: ["test/distributed_test.exs:5531"]
    },
    %{
      name: "skip_generation_purge",
      file: "lib/group/replica.ex",
      correct_source: """
        defp maybe_purge_remote_generation(state, remote_node, _old_generation, _generation) do
          {_reg, _pg} = Data.purge_node(state.name, state.shard_index, remote_node)

          affected =
            Data.purge_registry_claims_for_origin(
              state.name,
              state.shard_index,
              remote_node
            )

          {state, events} =
            Enum.reduce(affected, {state, []}, fn {cluster, key}, {acc, inner_events} ->
              reconcile_registry_projection(acc, cluster, key, :nodedown, inner_events)
            end)

          notify_monitors(state.name, events)
          Data.delete_replica_cursors_for_origin(state.name, state.shard_index, remote_node)
          state
        end
      """,
      faulty_source: """
        defp maybe_purge_remote_generation(state, _remote_node, _old_generation, _generation),
          do: state
      """,
      test: ["test/distributed_test.exs:5531"]
    },
    %{
      name: "disable_periodic_heads",
      file: "lib/group/replica.ex",
      correct_source: """
        defp broadcast_replica_heads(state) do
          Enum.reduce(state.peer_last_seen, state, fn {target_node, _last_seen}, acc ->
            send_replica_heads(acc, target_node)
          end)
        end
      """,
      faulty_source: """
        defp broadcast_replica_heads(state), do: state
      """,
      test: ["test/distributed_test.exs:3940"]
    },
    %{
      name: "skip_journal_crash_repair",
      file: "lib/group/replica.ex",
      correct_source: ":ok = Data.repair_local_replica_journal(name, shard_index)",
      faulty_source: ":ok",
      test: ["test/group_test.exs:3045"]
    },
    %{
      name: "skip_index_crash_repair",
      file: "lib/group/replica.ex",
      correct_source: ":ok = Data.repair_shard_indexes(name, shard_index)",
      faulty_source: ":ok",
      test: ["test/group_test.exs:3091"]
    },
    %{
      name: "skip_inactive_cluster_repair",
      file: "lib/group/replica/data.ex",
      correct_source: """
        def repair_shard_indexes(name, shard) do
          purge_inactive_cluster_rows(name, shard)
      """,
      faulty_source: """
        def repair_shard_indexes(name, shard) do
          if false, do: purge_inactive_cluster_rows(name, shard)
      """,
      test: ["test/group_test.exs:3145"]
    },
    %{
      name: "skip_closed_cluster_completion",
      file: "lib/group/replica.ex",
      correct_source: """
          completed_clusters =
            Data.mark_closed_cluster_shard(name, Data.closed_local_clusters(name), shard_index)
      """,
      faulty_source: """
          completed_clusters = []
      """,
      test: ["test/group_test.exs:3145"]
    },
    %{
      name: "accept_shared_authority_before_lane_install",
      file: "lib/group/replica.ex",
      correct_source: """
          Protocol.stream_shard(stream_id) == state.shard_index and
            replica_view_current?(state, source_node) and
      """,
      faulty_source: """
          Protocol.stream_shard(stream_id) == state.shard_index and
            true and
      """,
      test: ["test/distributed_test.exs:5531"]
    }
  ]

  def run(args) do
    selected = select_mutations(args)
    campaign_dir = campaign_dir()
    File.mkdir_p!(campaign_dir)

    IO.puts("mutation artifacts: #{campaign_dir}")
    verify_baselines!(selected, campaign_dir)

    results = Enum.map(selected, &run_mutant(&1, campaign_dir))
    print_summary(results)

    if Enum.any?(results, fn {_name, status, _path} -> status != :killed end) do
      System.halt(1)
    end
  end

  defp select_mutations(["--list"]) do
    Enum.each(@mutations, &IO.puts(&1.name))
    System.halt(0)
  end

  defp select_mutations([]), do: @mutations

  defp select_mutations(names) do
    by_name = Map.new(@mutations, &{&1.name, &1})
    unknown = names -- Map.keys(by_name)

    if unknown != [] do
      raise "unknown mutations: #{Enum.join(unknown, ", ")}"
    end

    Enum.map(names, &Map.fetch!(by_name, &1))
  end

  defp verify_baselines!(mutations, campaign_dir) do
    mutations
    |> Enum.map(& &1.test)
    |> Enum.uniq()
    |> Enum.each(fn test ->
      label = test |> hd() |> String.replace(~r/[^A-Za-z0-9_.-]/, "_")
      log = Path.join(campaign_dir, "baseline-#{label}.log")
      IO.write("baseline #{Enum.join(test, " ")} ... ")
      {output, status} = run_test(@repo, test)
      File.write!(log, output)

      if status == 0 do
        IO.puts("pass")
      else
        IO.puts("FAIL")
        raise "baseline failed; see #{log}"
      end
    end)
  end

  defp run_mutant(mutation, campaign_dir) do
    work = Path.join(campaign_dir, mutation.name)
    File.mkdir_p!(work)
    copy_checkout!(work)

    source_path = Path.join(work, mutation.file)
    source = File.read!(source_path)
    matches = :binary.matches(source, mutation.correct_source)

    if length(matches) != 1 do
      log = Path.join(work, "mutation-error.log")
      File.write!(log, "expected one match, found #{length(matches)}\n")
      IO.puts("#{mutation.name}: INVALID (replacement matched #{length(matches)} times)")
      {mutation.name, :invalid, work}
    else
      File.write!(
        source_path,
        String.replace(source, mutation.correct_source, mutation.faulty_source)
      )

      compile_log = Path.join(work, "compile.log")
      {compile_output, compile_status} = run_mix(work, ["compile", "--warnings-as-errors"])
      File.write!(compile_log, compile_output)

      if compile_status != 0 do
        IO.puts("#{mutation.name}: INVALID (does not compile)")
        {mutation.name, :invalid, work}
      else
        test_log = Path.join(work, "test.log")
        {test_output, test_status} = run_test(work, mutation.test)
        File.write!(test_log, test_output)

        case test_status do
          0 ->
            IO.puts("#{mutation.name}: SURVIVED")
            {mutation.name, :survived, work}

          124 ->
            IO.puts("#{mutation.name}: killed (timeout)")
            {mutation.name, :killed, work}

          _ ->
            IO.puts("#{mutation.name}: killed")
            {mutation.name, :killed, work}
        end
      end
    end
  end

  defp copy_checkout!(target) do
    rsync = System.find_executable("rsync") || raise "rsync is required"

    {_output, 0} =
      System.cmd(
        rsync,
        [
          "-a",
          "--exclude=.git",
          "--exclude=deps",
          "--exclude=tmp",
          "#{@repo}/",
          "#{target}/"
        ],
        stderr_to_stdout: true
      )

    File.ln_s!(Path.join(@repo, "deps"), Path.join(target, "deps"))
  end

  defp run_test(directory, test) do
    run_with_timeout(directory, ["mix", "test" | test],
      env: [{"GROUP_MODEL_RUNS", "1"}, {"GROUP_MODEL_COMMANDS", "8"}]
    )
  end

  defp run_mix(directory, args) do
    System.cmd("mix", args,
      cd: directory,
      env: [{"MIX_ENV", "test"}],
      stderr_to_stdout: true
    )
  end

  defp run_with_timeout(directory, command, opts) do
    timeout = System.find_executable("timeout") || raise "timeout is required"
    env = Keyword.fetch!(opts, :env)

    System.cmd(timeout, [@timeout_seconds | command],
      cd: directory,
      env: [{"MIX_ENV", "test"} | env],
      stderr_to_stdout: true
    )
  end

  defp campaign_dir do
    stamp = Calendar.strftime(DateTime.utc_now(), "%Y%m%dT%H%M%S")
    Path.join([@repo, "tmp", "mutation", "#{stamp}-#{System.unique_integer([:positive])}"])
  end

  defp print_summary(results) do
    IO.puts("\nmutation summary")

    Enum.each(results, fn {name, status, path} ->
      IO.puts("  #{String.pad_trailing(name, 38)} #{status}  #{path}")
    end)
  end
end

Group.MutationCampaign.run(System.argv())
