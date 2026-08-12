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
        "WireProtocol.stream_generation(stream_id) == Data.remote_generation(state.name, source_node) and",
      faulty_source: "true and",
      test: ["test/distributed_test.exs:5328"]
    },
    %{
      name: "accept_old_epoch",
      file: "lib/group/replica.ex",
      correct_source: """
      WireProtocol.stream_epoch(stream_id) ==
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
      correct_source: "Enum.reduce(existing, MapSet.new(), fn {key, pid, _meta, _time}, keys ->",
      faulty_source:
        "Enum.reduce(Enum.take(existing, 0), MapSet.new(), fn {key, pid, _meta, _time}, keys ->",
      test: ["test/replica_snapshot_distributed_test.exs:16"]
    },
    %{
      name: "pg_snapshot_is_additive",
      file: "lib/group/replica.ex",
      correct_source: "Enum.reduce(current, events, fn {key, pid, old_meta, _old_time}, acc ->",
      faulty_source:
        "Enum.reduce(Enum.take(current, 0), events, fn {key, pid, old_meta, _old_time}, acc ->",
      test: ["test/replica_snapshot_distributed_test.exs:16"]
    },
    %{
      name: "single_chunk_registry_snapshot_is_additive",
      file: "lib/group/replica/data.ex",
      correct_source: "Enum.each(existing, fn {key, pid, _meta, _time} ->",
      faulty_source: "Enum.each(Enum.take(existing, 0), fn {key, pid, _meta, _time} ->",
      test: ["test/distributed_test.exs:4030"]
    },
    %{
      name: "single_chunk_pg_snapshot_is_additive",
      file: "lib/group/replica.ex",
      correct_source: "      current\n      |> Map.keys()\n",
      faulty_source: "      %{}\n      |> Map.keys()\n",
      test: ["test/distributed_test.exs:4030"]
    },
    %{
      name: "commit_incomplete_snapshot",
      file: "lib/group/replica.ex",
      correct_source: """
          if MapSet.size(transfer.received) == transfer.chunk_count do
            if transfer.registry_seen == transfer.registry_count and
                 transfer.pg_seen == transfer.pg_count do
              commit_snapshot_transfer(state, key, source_node, stream_id, transfer)
            else
              discard_snapshot_transfer(state, key)
            end
          else
            state
          end
      """,
      faulty_source: """
          if MapSet.size(transfer.received) >= 1 do
            commit_snapshot_transfer(state, key, source_node, stream_id, transfer)
          else
            state
          end
      """,
      test: ["test/replica_snapshot_distributed_test.exs:16"]
    },
    %{
      name: "allow_duplicate_snapshot_rows",
      file: "lib/group/replica/snapshot.ex",
      correct_source: """
          if :ets.insert_new(table, objects) and
               :ets.info(table, :size) - size_before == length(objects) do
      """,
      faulty_source: """
          if :ets.insert(table, objects) and size_before >= 0 do
      """,
      test: ["test/replica_snapshot_distributed_test.exs:164"]
    },
    %{
      name: "do_not_supersede_partial_snapshot",
      file: "lib/group/replica.ex",
      correct_source: """
            %{snapshot_seq: existing_seq} when existing_seq < snapshot_seq ->
              state = discard_snapshot_transfer(state, key)
              {:ok, state, new_snapshot_transfer(snapshot_seq, manifest)}
      """,
      faulty_source: """
            %{snapshot_seq: existing_seq} when existing_seq < snapshot_seq ->
              _ = existing_seq
              {:ignore, state}
      """,
      test: ["test/replica_snapshot_distributed_test.exs:107"]
    },
    %{
      name: "accept_stale_snapshot_authority",
      file: "lib/group/replica.ex",
      correct_source: """
        defp valid_snapshot_stream?(state, source_node, stream_id, snapshot_seq) do
          valid_remote_stream?(state, source_node, stream_id) and
            snapshot_seq > Data.replica_cursor(state.name, state.shard_index, stream_id)
        end
      """,
      faulty_source: """
        defp valid_snapshot_stream?(state, _source_node, stream_id, snapshot_seq) do
          snapshot_seq > Data.replica_cursor(state.name, state.shard_index, stream_id)
        end
      """,
      test: ["test/replica_snapshot_distributed_test.exs:202"]
    },
    %{
      name: "disable_snapshot_staging_expiry",
      file: "lib/group/replica.ex",
      correct_source: """
            if now - transfer.last_progress > acc.replicated_peer_lease_timeout do
              discard_snapshot_transfer(acc, key)
            else
              acc
            end
      """,
      faulty_source: """
            _ = {now, transfer}

            if Process.get(:force_snapshot_staging_expiry, false) do
              discard_snapshot_transfer(acc, key)
            else
              acc
            end
      """,
      test: ["test/replica_snapshot_distributed_test.exs:202"]
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
      name: "assume_authority_fanout_reaches_late_lane",
      file: "lib/group/replica.ex",
      correct_source: """
        defp install_current_replica_lane(state, remote_node, generation) do
          old_generation =
            Data.remote_view_generation(state.name, state.shard_index, remote_node)

          state = maybe_purge_remote_generation(state, remote_node, old_generation, generation)
          state = purge_remote_streams_outside_authority(state, remote_node)
          :ok = install_replica_view(state, remote_node, generation)

          state
          |> touch_replica_peer(remote_node)
          |> Map.update!(:cluster_control_dirty, &Map.delete(&1, remote_node))
          |> send_replica_heads(remote_node)
        end
      """,
      faulty_source: """
        defp install_current_replica_lane(state, remote_node, generation) do
          _ = {remote_node, generation}
          state
        end
      """,
      test: ["test/replica_snapshot_distributed_test.exs:329"]
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
          WireProtocol.stream_shard(stream_id) == state.shard_index and
            replica_view_current?(state, source_node) and
      """,
      faulty_source: """
          WireProtocol.stream_shard(stream_id) == state.shard_index and
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
