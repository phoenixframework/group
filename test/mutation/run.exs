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
        "        WireProtocol.stream_generation(stream_id) ==\n" <>
          "          Data.remote_generation(state.name, source_node) and",
      faulty_source: "        true and",
      test: ["test/distributed_test.exs:5466"]
    },
    %{
      name: "accept_old_epoch",
      file: "lib/group/replica.ex",
      correct_source:
        "        WireProtocol.stream_epoch(stream_id) ==\n" <>
          "          Data.remote_cluster_epoch(state.name, source_node, cluster) and",
      faulty_source: "        true and",
      test: ["test/distributed_test.exs:4869"]
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
      test: ["test/distributed_test.exs:5543"]
    },
    %{
      name: "registry_snapshot_is_additive",
      file: "lib/group/replica/data.ex",
      correct_source: """
          acc =
            fold_registry_claims_for_stream(name, shard, stream_id, acc, fn
      """,
      faulty_source: """
          acc =
            Enum.reduce([], acc, fn
      """,
      test: [
        "test/replica_snapshot_distributed_test.exs:16",
        "test/distributed_test.exs:4073"
      ]
    },
    %{
      name: "pg_snapshot_is_additive",
      file: "lib/group/replica.ex",
      correct_source: "          if Snapshot.member_pg?(staging_table, key, pid) do",
      faulty_source: "          if Process.alive?(self()) do",
      test: [
        "test/replica_snapshot_distributed_test.exs:16",
        "test/distributed_test.exs:4073"
      ]
    },
    %{
      name: "commit_incomplete_snapshot",
      file: "lib/group/replica.ex",
      correct_source:
        "        if MapSet.size(transfer.received) == chunk_count and\n" <>
          "             transfer.registry_seen == registry_count and transfer.pg_seen == pg_count do",
      faulty_source:
        "        if MapSet.size(transfer.received) >= 1 and chunk_count >= 1 and\n" <>
          "             registry_count >= 0 and pg_count >= 0 do",
      test: ["test/replica_snapshot_distributed_test.exs:223"]
    },
    %{
      name: "commit_snapshot_without_terminal_manifest",
      file: "lib/group/replica.ex",
      correct_source:
        "      nil ->\n" <>
          "        state\n" <>
          "    end\n" <>
          "  end\n\n" <>
          "  defp snapshot_chunk_within_manifest?",
      faulty_source:
        "      nil ->\n" <>
          "        chunk_count = MapSet.size(transfer.received)\n" <>
          "        transfer = %{transfer | manifest: {chunk_count, transfer.registry_seen, transfer.pg_seen}}\n" <>
          "        commit_snapshot_transfer(state, key, source_node, stream_id, transfer)\n" <>
          "    end\n" <>
          "  end\n\n" <>
          "  defp snapshot_chunk_within_manifest?",
      test: ["test/replica_snapshot_distributed_test.exs:16"]
    },
    %{
      name: "accept_conflicting_snapshot_chunk_retransmission",
      file: "lib/group/replica.ex",
      correct_source:
        "          MapSet.member?(transfer.received, chunk_index) and\n" <>
          "              Snapshot.chunk_matches?(transfer.table, chunk_index, reg_data, pg_data) ->",
      faulty_source:
        "          MapSet.member?(transfer.received, chunk_index) and\n" <>
          "              Process.alive?(self()) ->",
      test: ["test/replica_snapshot_distributed_test.exs:385"]
    },
    %{
      name: "retain_conflicting_snapshot_manifest",
      file: "lib/group/replica.ex",
      correct_source: """
            {:ok, state, _conflicting_transfer} ->
              discard_snapshot_transfer(state, key)
      """,
      faulty_source: """
            {:ok, state, _conflicting_transfer} ->
              state
      """,
      test: ["test/replica_snapshot_distributed_test.exs:191"]
    },
    %{
      name: "commit_snapshot_after_source_changes_during_scan",
      file: "lib/group/replica.ex",
      correct_source:
        "      if current_snapshot_send?(state, target_node, stream_id, head) do\n" <>
          "        send_replica_snapshot_commit(state, target_node, stream_id, head, manifest)\n" <>
          "      else\n" <>
          "        :complete\n" <>
          "      end\n" <>
          "    catch",
      faulty_source:
        "      if Process.alive?(self()) do\n" <>
          "        send_replica_snapshot_commit(state, target_node, stream_id, head, manifest)\n" <>
          "      else\n" <>
          "        :complete\n" <>
          "      end\n" <>
          "    catch",
      test: ["test/replica_snapshot_distributed_test.exs:101"]
    },
    %{
      name: "drop_final_snapshot_event_batch",
      file: "lib/group/replica.ex",
      correct_source: "        _event_buffer = Snapshot.finish_event_buffer(event_buffer)",
      faulty_source: "        _event_buffer = event_buffer",
      test: ["test/replica_snapshot_distributed_test.exs:223"]
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
      test: ["test/replica_snapshot_distributed_test.exs:385"]
    },
    %{
      name: "do_not_supersede_partial_snapshot",
      file: "lib/group/replica.ex",
      correct_source:
        "      %{snapshot_seq: existing_seq} when existing_seq < snapshot_seq ->\n" <>
          "        state = discard_snapshot_transfer(state, key)\n" <>
          "        {state, transfer} = new_snapshot_transfer(state, snapshot_seq)\n" <>
          "        {:ok, put_snapshot_transfer(state, key, transfer), transfer}",
      faulty_source:
        "      %{snapshot_seq: existing_seq} when existing_seq < snapshot_seq ->\n" <>
          "        _ = existing_seq\n" <>
          "        {:ignore, state}",
      test: ["test/replica_snapshot_distributed_test.exs:328"]
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
      test: ["test/replica_snapshot_distributed_test.exs:556"]
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
      test: ["test/replica_snapshot_distributed_test.exs:482"]
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
      test: ["test/replica_model_property_test.exs:174"]
    },
    %{
      name: "do_not_sequence_process_down",
      file: "lib/group/replica.ex",
      correct_source:
        "      sequenced_downs =\n" <>
          "        append_process_down_records(state, reason_by_pid, pending_reg, pending_pg)\n",
      faulty_source:
        "      sequenced_downs =\n        if false,\n          do: append_process_down_records(state, reason_by_pid, pending_reg, pending_pg),\n          else: []\n",
      test: ["test/distributed_test.exs:3983"]
    },
    %{
      name: "do_not_exit_conflict_loser",
      file: "lib/group/replica.ex",
      correct_source:
        "              winner_meta = if winner, do: elem(winner, 1), else: nil\n" <>
          "              exit_local_conflict_loser(pid, key, winner_meta)\n" <>
          "              acc\n",
      faulty_source:
        "              _winner_meta = if winner, do: elem(winner, 1), else: nil\n" <>
          "              _ = Process.alive?(pid)\n" <>
          "              acc\n",
      test: ["test/replica_model_property_test.exs:71"]
    },
    %{
      name: "heartbeat_does_not_fence_newer_authority",
      file: "lib/group/replica/data.ex",
      correct_source: "        hint_generation == generation and\n",
      faulty_source: "        false and hint_generation == generation and\n",
      test: ["test/anti_entropy_fault_regression_test.exs:2199"]
    },
    %{
      name: "heartbeat_does_not_fence_newer_generation",
      file: "lib/group/replica/data.ex",
      correct_source:
        "        not is_nil(hint_generation) and\n" <>
          "            WireProtocol.generation_newer?(generation, hint_generation) ->\n",
      faulty_source:
        "        not is_nil(hint_generation) and\n" <>
          "            WireProtocol.generation_newer?(generation, hint_generation) and\n" <>
          "            Process.get(:fence_newer_generation, false) ->\n",
      test: ["test/anti_entropy_fault_regression_test.exs:2362"]
    },
    %{
      name: "drop_new_generation_authority_hint",
      file: "lib/group/replica/data.ex",
      correct_source:
        "          # below are being updated. Existing materialized projections may remain\n" <>
          "          # visible until exact-authority repair or bounded lease retirement.\n" <>
          "          put_remote_authority_hint(state.name, remote_node, generation, revision)",
      faulty_source:
        "          # below are being updated. Existing materialized projections may remain\n" <>
          "          # visible until exact-authority repair or bounded lease retirement.\n" <>
          "          _ = {state.name, remote_node, generation, revision}",
      test: ["test/anti_entropy_fault_regression_test.exs:2362"]
    },
    %{
      name: "accept_authority_older_than_generation_hint",
      file: "lib/group/replica/data.ex",
      correct_source: "    hinted_stale? or known_stale? or revision_stale?",
      faulty_source:
        "    _ = hinted_stale?\n" <>
          "    known_stale? or revision_stale?",
      test: ["test/anti_entropy_fault_regression_test.exs:2362"]
    },
    %{
      name: "install_lane_view_behind_generation_hint",
      file: "lib/group/replica/data.ex",
      correct_source:
        "         remote_replica_authority_hint(state.name, remote_node) == {generation, observed} do",
      faulty_source:
        "         elem(remote_replica_authority_hint(state.name, remote_node), 1) == observed do",
      test: ["test/group_test.exs:3024"]
    },
    %{
      name: "install_incremental_after_newer_hint",
      file: "lib/group/replica/data.ex",
      correct_source:
        "      remote_cluster_epoch_observed_revision(name, remote_node) == expected_revision and\n" <>
          "      remote_replica_authority_hint(name, remote_node) == {generation, expected_revision}\n",
      faulty_source:
        "      Process.get(:ignore_incremental_authority_race, true) and\n" <>
          "      is_tuple(remote_replica_authority_hint(name, remote_node))\n",
      test: ["test/group_test.exs:2958"]
    },
    %{
      name: "accept_hint_without_exact_authority",
      file: "lib/group/replica/data.ex",
      correct_source:
        "        not is_nil(hint_generation) and\n" <>
          "            WireProtocol.generation_newer?(generation, hint_generation) ->\n",
      faulty_source:
        "        (is_nil(hint_generation) or\n" <>
          "           WireProtocol.generation_newer?(generation, hint_generation)) ->\n",
      test: ["test/anti_entropy_fault_regression_test.exs:3458"]
    },
    %{
      name: "admit_retired_lane_route_without_authority",
      file: "lib/group/replica.ex",
      correct_source:
        "          # A lane hello is only a hint until node-wide exact authority exists.\n" <>
          "          # In particular, a delayed hello after retirement must not recreate\n" <>
          "          # an unleased route that can live forever and suppress rediscovery.\n" <>
          "          {:noreply, request_replica_authority(state, remote_node)}",
      faulty_source:
        "          state = put_remote_shard(state, remote_node, remote_pid)\n" <>
          "          {:noreply, request_replica_authority(state, remote_node)}",
      test: ["test/anti_entropy_fault_regression_test.exs:3458"]
    },
    %{
      name: "do_not_restore_hint_lease_after_lane_restart",
      file: "lib/group/replica/data.ex",
      correct_source:
        "      {{{:remote_view_info, shard, :\"$1\"}, :_, :_, :_}, [], [:\"$1\"]},\n" <>
          "      # A hint is the durable fence left before the observing lane records its\n" <>
          "      # in-memory lease deadline. Every restarting lane must recognize it so a\n" <>
          "      # crash in that window cannot strand the peer forever.\n" <>
          "      {{{:remote_authority_hint, :\"$1\"}, :_, :_}, [], [:\"$1\"]}\n",
      faulty_source: "      {{{:remote_view_info, shard, :\"$1\"}, :_, :_, :_}, [], [:\"$1\"]}\n",
      test: ["test/group_test.exs:2897"]
    },
    %{
      name: "retain_retired_authority_repair",
      file: "lib/group/replica.ex",
      correct_source:
        "          is_nil(Data.remote_generation(state.name, remote_node)) and\n" <>
          "              is_nil(Data.remote_replica_authority_hint(state.name, remote_node)) ->\n" <>
          "            acc\n",
      faulty_source:
        "          is_nil(Data.remote_generation(state.name, remote_node)) and\n" <>
          "              is_nil(Data.remote_replica_authority_hint(state.name, remote_node)) ->\n" <>
          "            Map.put(acc, remote_node, last_activity)\n",
      test: ["test/anti_entropy_fault_regression_test.exs:3458"]
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
      test: ["test/distributed_test.exs:5688"]
    },
    %{
      name: "wait_for_periodic_lane_probe_after_authority_fanout",
      file: "lib/group/replica.ex",
      correct_source: """
              else
                # A lane hello can legitimately outrun shard zero's exact authority.
                # The hello is not retained as a route, because a delayed hello after
                # retirement must not recreate an unleased peer. Once exact authority
                # reaches this lane, repeat shard-local discovery immediately instead
                # of waiting for the next anti-entropy probe.
                send_remote_shard_message(
                  state,
                  remote_node,
                  {:peer_connect, self(), state.shard_index, state.num_shards,
                   Data.my_clusters(state.name)}
                )

                state
              end
      """,
      faulty_source: """
              else
                state
              end
      """,
      test: ["test/anti_entropy_fault_regression_test.exs:3963"]
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
          state = install_replica_view(state, remote_node, generation)

          if replica_view_current?(state, remote_node) do
            state
            |> touch_replica_peer(remote_node)
            |> Map.update!(:cluster_control_dirty, &Map.delete(&1, remote_node))
            |> send_replica_heads(remote_node)
          else
            state
          end
        end
      """,
      faulty_source: """
        defp install_current_replica_lane(state, remote_node, generation) do
          _ = {remote_node, generation}
          state
        end
      """,
      test: ["test/anti_entropy_fault_regression_test.exs:3963"]
    },
    %{
      name: "skip_generation_purge",
      file: "lib/group/replica.ex",
      correct_source: """
        defp maybe_purge_remote_generation(state, remote_node, _old_generation, _generation) do
          state = discard_snapshot_transfers_for_source(state, remote_node)
          state = discard_pending_registry_reprojections(state, remote_node)
          Data.delete_replica_cursors_for_origin(state.name, state.shard_index, remote_node)
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
          state
        end
      """,
      faulty_source: """
        defp maybe_purge_remote_generation(state, _remote_node, _old_generation, _generation),
          do: state
      """,
      test: ["test/distributed_test.exs:5688"]
    },
    %{
      name: "disable_periodic_heads",
      file: "lib/group/replica.ex",
      correct_source: """
        defp broadcast_replica_heads(state) do
          peers = Map.keys(state.peer_last_seen)
      """,
      faulty_source: """
        defp broadcast_replica_heads(state) do
          _ = state.peer_last_seen
          peers = []
      """,
      test: ["test/distributed_test.exs:3983"]
    },
    %{
      name: "skip_journal_crash_repair",
      file: "lib/group/replica.ex",
      correct_source: ":ok = Data.repair_local_replica_journal(name, shard_index)",
      faulty_source: ":ok",
      test: ["test/group_test.exs:2547"]
    },
    %{
      name: "skip_index_crash_repair",
      file: "lib/group/replica.ex",
      correct_source: ":ok = Data.repair_shard_indexes(name, shard_index)",
      faulty_source: ":ok",
      test: ["test/group_test.exs:2593"]
    },
    %{
      name: "skip_pg_count_projection_update",
      file: "lib/group/replica/data.ex",
      correct_source:
        "        [total_count, local_count] =\n" <>
          "          :ets.update_counter(\n" <>
          "            table,\n" <>
          "            count_key,\n" <>
          "            [{2, total_delta}, {3, local_delta}],\n" <>
          "            {count_key, 0, 0}\n" <>
          "          )\n",
      faulty_source:
        "        _ = {table, count_key, total_delta, local_delta}\n" <>
          "        [total_count, local_count] = [0, 0]\n",
      test: ["test/group_test.exs:1910"]
    },
    %{
      name: "retain_stale_pg_counts_on_shard_repair",
      file: "lib/group/replica/data.ex",
      correct_source: "    :ets.delete_all_objects(pg_counts)",
      faulty_source: "    _ = pg_counts",
      test: ["test/group_test.exs:2593"]
    },
    %{
      name: "consult_stale_pg_counts_during_snapshot_repair",
      file: "lib/group/replica/data.ex",
      correct_source: """
        defp delete_pg_for_origin_clusters_before_rebuild(name, shard, clusters, origin_node) do
          entries = pg_entries_for_origin_clusters(name, shard, clusters, origin_node)
          primary = pg_by_key_table(name, shard)
          reverse = pg_by_pid_table(name, shard)

          Enum.each(entries, fn {cluster, key, pid, _meta, _time} ->
            :ets.delete(primary, {cluster, key, pid})
            :ets.delete(reverse, {pid, cluster, key})
          end)

          :ok
        end
      """,
      faulty_source: """
        defp delete_pg_for_origin_clusters_before_rebuild(name, shard, clusters, origin_node) do
          _entries = delete_pg_for_origin_clusters(name, shard, clusters, origin_node)
          :ok
        end
      """,
      test: ["test/anti_entropy_fault_regression_test.exs:3278"]
    },
    %{
      name: "replay_local_journal_before_pg_count_repair",
      file: "lib/group/replica.ex",
      correct_source: """
          :ok = Data.repair_local_replica_journal(name, shard_index)
          :ok = Data.repair_shard_indexes(name, shard_index)
          state = replay_local_journal(state)
      """,
      faulty_source: """
          :ok = Data.repair_local_replica_journal(name, shard_index)
          state = replay_local_journal(state)
          :ok = Data.repair_shard_indexes(name, shard_index)
      """,
      test: ["test/group_test.exs:3094"]
    },
    %{
      name: "skip_inactive_cluster_repair",
      file: "lib/group/replica/data.ex",
      correct_source: "    repair_primary_replica_rows(name, shard)",
      faulty_source:
        "    if Process.get(:run_primary_replica_repair, false),\n" <>
          "      do: repair_primary_replica_rows(name, shard),\n" <>
          "      else: :ok",
      test: ["test/group_test.exs:2701"]
    },
    %{
      name: "skip_closed_cluster_completion",
      file: "lib/group/replica.ex",
      correct_source: """
          _completed_clusters =
            Data.mark_closed_cluster_shard(
              name,
              Data.closed_local_cluster_epochs(name),
              shard_index
            )
      """,
      faulty_source: """
          _completed_clusters = []
      """,
      test: ["test/group_test.exs:2701"]
    },
    %{
      name: "accept_unfenced_cluster_disconnect",
      file: "lib/group/replica.ex",
      correct_source:
        "        _ ->\n" <>
          "          false\n" <>
          "      end)\n\n" <>
          "    case epochs do\n",
      faulty_source:
        "        _ ->\n" <>
          "          Process.get(:accept_unfenced_cluster_disconnect, true)\n" <>
          "      end)\n\n" <>
          "    case epochs do\n",
      test: ["test/group_test.exs:1485"]
    },
    %{
      name: "accept_completed_cluster_disconnect",
      file: "lib/group/replica.ex",
      correct_source:
        "          Data.closed_local_cluster_pending?(\n" <>
          "            state.name,\n" <>
          "            cluster,\n" <>
          "            epoch,\n" <>
          "            state.shard_index\n" <>
          "          )",
      faulty_source:
        "          _ = cluster\n" <>
          "          Process.get(:accept_completed_cluster_disconnect, true)",
      test: ["test/group_test.exs:1485"]
    },
    %{
      name: "acknowledge_wrong_cluster_close_epoch",
      file: "lib/group/replica/data.ex",
      correct_source: "          [{^cluster, ^request_epoch, pending_shards}] ->\n",
      faulty_source: "          [{^cluster, _stored_epoch, pending_shards}] ->\n",
      test: ["test/group_test.exs:2742"]
    },
    %{
      name: "accept_shared_authority_before_lane_install",
      file: "lib/group/replica.ex",
      correct_source:
        "        WireProtocol.stream_shard(stream_id) == state.shard_index and\n" <>
          "        replica_view_current?(state, source_node) and",
      faulty_source:
        "        WireProtocol.stream_shard(stream_id) == state.shard_index and\n" <>
          "        true and",
      test: ["test/anti_entropy_fault_regression_test.exs:1313"]
    },
    %{
      name: "apply_incremental_authority_across_revision_gap",
      file: "lib/group/replica.ex",
      correct_source: "            if contiguous_cluster_controls?(accepted, next_revision) do",
      faulty_source:
        "            if contiguous_cluster_controls?(accepted, next_revision) or\n" <>
          "                 Enum.any?(accepted, fn {revision, _epochs} -> revision >= next_revision end) do",
      test: ["test/anti_entropy_fault_regression_test.exs:1505"]
    },
    %{
      name: "allow_non_owner_lane_to_mutate_shared_authority",
      file: "lib/group/replica.ex",
      correct_source: "    if state.shard_index == 0 do\n      remote_node = node(remote_pid)",
      faulty_source: "    if true do\n      remote_node = node(remote_pid)",
      test: ["test/anti_entropy_fault_regression_test.exs:1505"]
    },
    %{
      name: "crash_lane_when_local_authority_owner_is_missing",
      file: "lib/group/replica.ex",
      correct_source: "      _ = send_local_control_message(state, control)",
      faulty_source: "      send(shard_name(state.name, 0), control)",
      test: ["test/anti_entropy_fault_regression_test.exs:1452"]
    },
    %{
      name: "retire_local_owner_after_remote_authority_changed",
      file: "lib/group/replica.ex",
      correct_source: "      registry_winner_authoritative?(state, cluster, winner) ->",
      faulty_source:
        "      Process.get(:skip_remote_registry_authority, true) or\n" <>
          "          registry_winner_authoritative?(state, cluster, winner) ->",
      test: ["test/anti_entropy_fault_regression_test.exs:1664"]
    },
    %{
      name: "skip_registry_reprojection_after_authority_restore",
      file: "lib/group/replica.ex",
      correct_source: """
        defp reproject_pending_registry_keys(state, remote_node) do
          case Map.pop(state.pending_registry_reprojections, remote_node) do
            {nil, _pending} ->
              state

            {keys, pending} ->
              state = %{state | pending_registry_reprojections: pending}
              {state, events} = reconcile_registry_keys(state, keys, :reconcile, [])
              notify_monitors(state.name, events)
              state
          end
        end
      """,
      faulty_source: """
        defp reproject_pending_registry_keys(state, remote_node) do
          _ = remote_node
          state
        end
      """,
      test: ["test/anti_entropy_fault_regression_test.exs:1847"]
    },
    %{
      name: "retain_registry_reprojection_after_peer_expiry",
      file: "lib/group/replica.ex",
      correct_source: """
        defp expire_replica_peer(state, remote_node) do
          state = discard_snapshot_transfers_for_source(state, remote_node)
          state = discard_snapshot_send_offsets_for_target(state, remote_node)
          state = discard_pending_registry_reprojections(state, remote_node)
      """,
      faulty_source: """
        defp expire_replica_peer(state, remote_node) do
          state = discard_snapshot_transfers_for_source(state, remote_node)
          state = discard_snapshot_send_offsets_for_target(state, remote_node)
      """,
      test: ["test/anti_entropy_fault_regression_test.exs:2023"]
    },
    %{
      name: "retain_registry_reprojection_after_nodedown",
      file: "lib/group/replica.ex",
      correct_source: """
        def handle_info({:nodedown, dead_node}, state) do
          state = flush_pending_replicated_message_barrier(state)
          state = discard_snapshot_transfers_for_source(state, dead_node)
          state = discard_snapshot_send_offsets_for_target(state, dead_node)
          state = discard_pending_registry_reprojections(state, dead_node)
      """,
      faulty_source: """
        def handle_info({:nodedown, dead_node}, state) do
          state = flush_pending_replicated_message_barrier(state)
          state = discard_snapshot_transfers_for_source(state, dead_node)
          state = discard_snapshot_send_offsets_for_target(state, dead_node)
      """,
      test: ["test/anti_entropy_fault_regression_test.exs:3741"]
    },
    %{
      name: "separate_exact_authority_from_cluster_projection",
      file: "lib/group/replica/data.ex",
      correct_source:
        "    replace_remote_cluster_projection(state.name, remote_node, current_epochs)\n",
      faulty_source:
        "    _ = {&replace_remote_cluster_projection/3, state.name, remote_node, current_epochs}\n",
      test: ["test/anti_entropy_fault_regression_test.exs:3777"]
    },
    %{
      name: "separate_local_activation_from_cluster_projection",
      file: "lib/group/replica/data.ex",
      correct_source:
        "    if durable?, do: project_activated_local_clusters(state.name, clusters)\n",
      faulty_source:
        "    _ = {durable?, &project_activated_local_clusters/2, state.name, clusters}\n",
      test: ["test/anti_entropy_fault_regression_test.exs:3832"]
    },
    %{
      name: "drop_durable_cluster_deactivation_cleanup",
      file: "lib/group/replica/data.ex",
      correct_source:
        "      cast_cluster_lifecycle(\n" <>
          "        state.name,\n" <>
          "        0..(state.num_shards - 1),\n" <>
          "        {:cluster_disconnect, clusters, epochs}\n" <>
          "      )\n",
      faulty_source:
        "      _ = {&cast_cluster_lifecycle/3, state.name, state.num_shards, clusters, epochs}\n",
      test: ["test/anti_entropy_fault_regression_test.exs:3881"]
    },
    %{
      name: "delete_close_marker_before_terminal_route_cleanup",
      file: "lib/group/replica/data.ex",
      correct_source:
        "              :ok = delete_cluster_routes(state.name, [cluster])\n" <>
          "              :ets.delete(closed_local_cluster_epochs_table(state.name), cluster)\n",
      faulty_source:
        "              :ets.delete(closed_local_cluster_epochs_table(state.name), cluster)\n",
      test: ["test/group_test.exs:2742"]
    },
    %{
      name: "retire_peer_authority_before_terminal_route_cleanup",
      file: "lib/group/replica/data.ex",
      correct_source:
        "    :ets.delete(replication_meta_table(name), {:remote_generation, remote_node})\n" <>
          "    :ok = delete_peer_routes(name, remote_node)\n",
      faulty_source:
        "    :ets.delete(replication_meta_table(name), {:remote_generation, remote_node})\n",
      test: ["test/group_test.exs:2796"]
    },
    %{
      name: "stale_peer_cleanup_removes_rediscovered_routes",
      file: "lib/group/replica/data.ex",
      correct_source:
        "    if is_nil(remote_generation(state.name, dead_node)) and\n" <>
          "         is_nil(remote_replica_authority_hint(state.name, dead_node)) do\n",
      faulty_source:
        "    if Process.get(:purge_rediscovered_peer_routes, true) or\n" <>
          "         (is_nil(remote_generation(state.name, dead_node)) and\n" <>
          "            is_nil(remote_replica_authority_hint(state.name, dead_node))) do\n",
      test: ["test/group_test.exs:2849"]
    },
    %{
      name: "stale_restart_cleanup_removes_reactivated_routes",
      file: "lib/group/replica/data.ex",
      correct_source:
        "      Enum.filter(clusters, &is_nil(local_cluster_epoch(state.name, &1)))\n",
      faulty_source: "      clusters\n",
      test: ["test/group_test.exs:2776"]
    },
    %{
      name: "retain_authority_repair_after_nodedown",
      file: "lib/group/replica.ex",
      correct_source:
        "        cluster_control_dirty: Map.delete(state.cluster_control_dirty, dead_node),\n" <>
          "        authority_dirty_notified: MapSet.delete(state.authority_dirty_notified, dead_node)\n",
      faulty_source:
        "        cluster_control_dirty: state.cluster_control_dirty,\n" <>
          "        authority_dirty_notified: MapSet.delete(state.authority_dirty_notified, dead_node)\n",
      test: ["test/group_test.exs:2835"]
    },
    %{
      name: "retain_receive_cursor_for_inactive_local_cluster",
      file: "lib/group/replica/data.ex",
      correct_source:
        "      WireProtocol.stream_origin(stream_id) != node() and\n" <>
          "      active_local_cluster?(name, WireProtocol.stream_cluster(stream_id)) and",
      faulty_source:
        "      WireProtocol.stream_origin(stream_id) != node() and\n" <>
          "      true and",
      test: ["test/anti_entropy_fault_regression_test.exs:2876"]
    },
    %{
      name: "retire_shared_authority_with_live_lanes",
      file: "lib/group/replica/data.ex",
      correct_source: "    result =\n      if remaining_lanes == 0 do",
      faulty_source: "    _ = remaining_lanes\n\n    result =\n      if true do",
      test: ["test/anti_entropy_fault_regression_test.exs:857"]
    },
    %{
      name: "shard_zero_deletes_sibling_restart_views",
      file: "lib/group/replica/data.ex",
      correct_source: """
          if shard == 0 do
            delete_remote_authority(state.name, remote_node)
          end
      """,
      faulty_source: """
          if shard == 0 do
            if state.num_shards > 1 do
              for view_shard <- 1..(state.num_shards - 1) do
                :ets.delete(
                  replication_meta_table(state.name),
                  {:remote_view_info, view_shard, remote_node}
                )
              end
            end

            delete_remote_authority(state.name, remote_node)
          end
      """,
      test: ["test/anti_entropy_fault_regression_test.exs:240"]
    },
    %{
      name: "forget_partial_authority_after_restart",
      file: "lib/group/replica.ex",
      correct_source: """
          cluster_control_dirty =
            if shard_index == 0 do
              Enum.reduce(retained_origins, %{}, fn origin, dirty ->
                exact = Data.remote_cluster_epoch_exact_revision(name, origin)
                observed = Data.remote_cluster_epoch_observed_revision(name, origin)
                known_generation = Data.remote_generation(name, origin)
                hint = Data.remote_replica_authority_hint(name, origin)

                if (not is_nil(observed) and observed != exact) or
                     (not is_nil(hint) and elem(hint, 0) != known_generation) do
                  Map.put(dirty, origin, restarted_at)
                else
                  dirty
                end
              end)
            else
              %{}
            end
      """,
      faulty_source: "cluster_control_dirty = %{}\n",
      test: ["test/anti_entropy_fault_regression_test.exs:350"]
    },
    %{
      name: "accept_wrong_shard_delta_rows",
      file: "lib/group/replica.ex",
      correct_source:
        "            Enum.split_while(contiguous, fn {_seq, mutations} ->\n" <>
          "              valid_replica_mutations?(state, stream_id, mutations)\n" <>
          "            end)",
      faulty_source:
        "            Enum.split_while(contiguous, fn {_seq, mutations} ->\n" <>
          "              valid_replica_mutations?(%{state | num_shards: 1}, stream_id, mutations)\n" <>
          "            end)",
      test: ["test/anti_entropy_fault_regression_test.exs:512"]
    },
    %{
      name: "restore_unsequenced_cluster_disconnect",
      file: "lib/group/replica.ex",
      correct_source: """
        def handle_info(_msg, state) do
          state = flush_pending_replicated_message_barrier(state)
          {:noreply, state}
        end
      """,
      faulty_source: """
      def handle_info({:cluster_disconnect, clusters, remote_pid}, state) do
        Enum.each(clusters, fn cluster ->
          Data.purge_registry_claims_for_cluster(
            state.name,
            state.shard_index,
            cluster,
            node(remote_pid)
          )

          purge_cluster_entries(
            state.name,
            state.shard_index,
            cluster,
            node(remote_pid)
          )
        end)

        {:noreply, state}
      end

      def handle_info(_msg, state) do
        state = flush_pending_replicated_message_barrier(state)
        {:noreply, state}
      end
      """,
      test: ["test/group_test.exs:2393"]
    },
    %{
      name: "skip_cursorless_restart_authority_repair",
      file: "lib/group/replica/data.ex",
      correct_source: "    repair_primary_replica_rows(name, shard)",
      faulty_source:
        "    if Process.get(:run_primary_replica_repair, false),\n" <>
          "      do: repair_primary_replica_rows(name, shard),\n" <>
          "      else: :ok",
      test: ["test/anti_entropy_fault_regression_test.exs:3166"]
    },
    %{
      name: "project_stale_claims_before_restart_repair",
      file: "lib/group/replica.ex",
      correct_source: """
          :ok = Data.repair_shard_indexes(name, shard_index)
          state = replay_local_journal(state)
          {state, _events} = rebuild_registry_projections(state)
      """,
      faulty_source: """
          state = replay_local_journal(state)
          {state, _events} = rebuild_registry_projections(state)
          :ok = Data.repair_shard_indexes(name, shard_index)
      """,
      test: ["test/anti_entropy_fault_regression_test.exs:3598"]
    },
    %{
      name: "skip_interrupted_snapshot_install_repair",
      file: "lib/group/replica/data.ex",
      correct_source: "    repair_interrupted_snapshot_installs(name, shard)",
      faulty_source:
        "    if Process.get(:run_snapshot_install_repair, false),\n" <>
          "      do: repair_interrupted_snapshot_installs(name, shard),\n" <>
          "      else: :ok",
      test: ["test/anti_entropy_fault_regression_test.exs:3278"]
    },
    %{
      name: "retain_cursorless_remote_registry_claims",
      file: "lib/group/replica/data.ex",
      correct_source:
        "      :ets.member(replica_cursor_table(name, shard), stream_id)\n    else\n      false\n    end\n  end\n\n  defp valid_remote_pg_authority?",
      faulty_source:
        "      is_tuple(stream_id)\n    else\n      false\n    end\n  end\n\n  defp valid_remote_pg_authority?",
      test: ["test/anti_entropy_fault_regression_test.exs:3166"]
    },
    %{
      name: "restart_snapshot_from_first_chunk_after_busy",
      file: "lib/group/replica.ex",
      correct_source: "    resume = Map.get(offsets, snapshot_key, {:chunk, 1})",
      faulty_source: """
          resume =
            case Map.get(offsets, snapshot_key) do
              {:commit, _manifest} = commit -> commit
              _chunk_resume -> {:chunk, 1}
            end
      """,
      test: ["test/replica_snapshot_distributed_test.exs:877"]
    },
    %{
      name: "drain_oversized_ingress_batch_without_yield",
      file: "lib/group/replica.ex",
      correct_source: "    {turn, remaining} = Enum.split(messages, @incoming_batch_quota)",
      faulty_source: "    _ = @incoming_batch_quota\n    turn = messages\n    remaining = []",
      test: ["test/group_test.exs:37"]
    }
  ]

  def run(["--list"]) do
    verify_mutation_definitions!(@mutations)
    Enum.each(@mutations, &IO.puts(&1.name))
  end

  def run(args) do
    selected = select_mutations(args)
    verify_mutation_definitions!(selected)
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

  defp select_mutations([]), do: @mutations

  defp select_mutations(names) do
    by_name = Map.new(@mutations, &{&1.name, &1})
    unknown = names -- Map.keys(by_name)

    if unknown != [] do
      raise "unknown mutations: #{Enum.join(unknown, ", ")}"
    end

    Enum.map(names, &Map.fetch!(by_name, &1))
  end

  defp verify_mutation_definitions!(mutations) do
    invalid =
      Enum.flat_map(mutations, fn mutation ->
        verify_test_selectors!(mutation.test)

        source = mutation.file |> then(&Path.join(@repo, &1)) |> File.read!()
        matches = :binary.matches(source, mutation.correct_source)

        if length(matches) == 1, do: [], else: [{mutation.name, length(matches)}]
      end)

    if invalid != [] do
      details = Enum.map_join(invalid, ", ", fn {name, count} -> "#{name}=#{count}" end)
      raise "mutation definitions must match exactly one production fragment: #{details}"
    end
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

  # ExUnit accepts a line anywhere inside a test body, which can silently run
  # the preceding test after source edits shift declarations. Mutation tests
  # must point at the declaration itself so a stale selector is invalid rather
  # than being mistaken for evidence that a mutant survived.
  defp verify_test_selectors!(selectors) do
    Enum.each(selectors, fn selector ->
      with [_, relative, line] <- Regex.run(~r/^(.*):(\d+)$/, selector),
           {line, ""} <- Integer.parse(line),
           source when is_binary(source) <- File.read!(Path.join(@repo, relative)),
           declaration when is_binary(declaration) <-
             Enum.at(String.split(source, "\n"), line - 1),
           true <-
             String.starts_with?(String.trim_leading(declaration), ["test \"", "property \""]) do
        :ok
      else
        _ ->
          raise "mutation selector must name an exact test/property declaration: #{selector}"
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
          # Jepsen histories and caches are runtime artifacts. Copying them into
          # every mutant can multiply a long soak's disk usage by the number of
          # mutations without contributing anything to compilation or tests.
          "--exclude=test/jepsen/store",
          "--exclude=test/jepsen/.cache",
          "#{@repo}/",
          "#{target}/"
        ],
        stderr_to_stdout: true
      )

    File.ln_s!(Path.join(@repo, "deps"), Path.join(target, "deps"))
  end

  defp run_test(directory, test) do
    run_with_timeout(directory, ["mix", "test" | test],
      env: [
        {"GROUP_MODEL_RUNS", "1"},
        {"GROUP_MODEL_COMMANDS", "8"},
        {"GROUP_JEPSEN_SKIP_CHECKER", "1"}
      ]
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
