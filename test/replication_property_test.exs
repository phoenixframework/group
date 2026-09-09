defmodule Group.ReplicationPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Group.PropertyFixture
  alias Group.Replica
  alias Group.TestCluster

  @name :replication_property
  @keys ["hot/a", "hot/b", "hot/nested/a", "other/a"]

  for kind <- [:registry, :pg] do
    property "#{kind} batch boundaries preserve settled contents and per-key events" do
      check all(
              shards <- member_of([1, 2, 4]),
              buffer <- member_of([2, 4, 8]),
              cluster <- member_of([nil, "shared"]),
              count <- member_of([buffer - 1, buffer, buffer + 1, 2 * buffer + 1]),
              commands <-
                list_of(
                  tuple(
                    {member_of([:put, :put, :remove]), integer(0..3), integer(0..1),
                     integer(0..2)}
                  ),
                  length: count
                ),
              max_runs: 100
            ) do
        # Compare each partition against a map-based oracle, not merely another
        # Group run that could make the same mistake. Include one entire batch.
        for chunk_size <- Enum.uniq([1, buffer - 1, buffer, buffer + 1, count]) do
          with_group(@name, options(shards, buffer), fn actors ->
            if cluster, do: Group.connect(@name, cluster)
            subscribe(actors.observer, cluster)

            {ops, entries, expected_events} =
              compile_history(unquote(kind), cluster, commands, actors)

            deliver(unquote(kind), ops, chunk_size, shards)
            actual_events = events_after_barrier(@name, actors.observer)
            assert_per_key_events(actual_events, expected_events)
            assert_entries(entries, unquote(kind), cluster)
            assert :ok = TestCluster.assert_ets_consistent(@name)
          end)
        end
      end
    end

    property "#{kind} buffers and snapshots cannot repopulate a cluster after disconnect" do
      check all(
              shards <- member_of([1, 2, 4]),
              buffer <- member_of([2, 4, 8]),
              count <- integer(1..(buffer - 1)),
              value <- integer(0..10),
              cycles <- integer(1..3),
              max_runs: 50
            ) do
        with_group(@name, options(shards, buffer), fn actors ->
          cluster = "shared"
          assert :ok = Group.connect(@name, [cluster, "other"])

          # Surviving entries use the same key in two other clusters.
          for c <- [nil, "other"] do
            in_process(actors[0], fn ->
              :ok = Group.register(@name, "hot/a", %{survivor: true}, cluster: c)
              :ok = Group.join(@name, "hot/a", %{survivor: true}, cluster: c)
            end)
          end

          survivors = Enum.sort(Group.local_entries(@name))
          subscribe(actors.observer, cluster)
          commands = for i <- 0..(count - 1), do: {:put, rem(i, 4), rem(i, 2), value + i}

          {ops, entries, _events} =
            compile_history(unquote(kind), cluster, commands, actors)

          deliver(unquote(kind), ops, 1, shards)

          # System messages inspect without flushing the normal mailbox lane.
          # Prove the operations really are buffered before membership is removed.
          pending =
            for shard <- 0..(shards - 1) do
              state = :sys.get_state(Replica.shard_name(@name, shard))

              case unquote(kind) do
                :registry -> state.pending_replicated_registry_len
                :pg -> state.pending_replicated_pg_len
              end
            end

          assert Enum.sum(pending) == length(ops)
          assert :ok = Group.disconnect(@name, cluster)
          # Checking events catches an incorrect apply-then-purge, even if both
          # indexes end up empty and therefore appear consistent.
          assert events_after_barrier(@name, actors.observer) == []
          assert Enum.sort(Group.local_entries(@name)) == survivors

          for _cycle <- 1..cycles do
            deliver(unquote(kind), ops, buffer + 1, shards)
            snapshot(unquote(kind), cluster, entries, shards)
            assert events_after_barrier(@name, actors.observer) == []
            assert Enum.sort(Group.local_entries(@name)) == survivors
            refute Group.connected?(@name, cluster)

            assert :ok = Group.connect(@name, cluster)
            snapshot(unquote(kind), cluster, entries, shards)
            actual_events = events_after_barrier(@name, actors.observer)

            # Snapshots contain only the final entry for each key/owner.
            snapshot_events =
              entries
              |> Enum.map(fn {{key, pid}, meta} ->
                event(unquote(kind), cluster, key, pid, meta, nil, nil)
              end)

            assert_per_key_events(actual_events, snapshot_events)
            assert_entries(entries, unquote(kind), cluster, survivors)

            assert :ok = Group.disconnect(@name, cluster)

            removals =
              Enum.map(snapshot_events, fn e ->
                %{e | type: removal_type(unquote(kind)), reason: :cluster_disconnect}
              end)

            assert_per_key_events(events_after_barrier(@name, actors.observer), removals)
            assert Enum.sort(Group.local_entries(@name)) == survivors
            assert :ok = TestCluster.assert_ets_consistent(@name)
          end
        end)
      end
    end

    property "#{kind} backlog yields to a queued local write at the first receiver flush" do
      check all(
              buffer <- member_of([1, 2, 4, 8]),
              chunk_size <- member_of(Enum.uniq([1, max(buffer - 1, 1), buffer, buffer + 1])),
              count <- member_of([2 * buffer + chunk_size, 4 * buffer + chunk_size]),
              local_operation <- member_of([:register, :join]),
              max_runs: 50
            ) do
        # One shard gives a single event timeline. Assert the position of local
        # work in that timeline, not the scheduler-sensitive queue length when
        # the caller happens to wake up.
        with_group(@name, options(1, buffer), fn actors ->
          subscribe(actors.observer, nil)
          commands = for i <- 1..count, do: {:put, 0, 0, i}
          {ops, entries, remote_events} = compile_history(unquote(kind), nil, commands, actors)

          tag =
            if unquote(kind) == :registry,
              do: :replicate_registry_batch,
              else: :replicate_pg_batch

          shard = Process.whereis(Replica.shard_name(@name, 0))
          :ok = :sys.suspend(shard)

          try do
            for chunk <- Enum.chunk_every(ops, chunk_size), do: send(shard, {tag, chunk})

            caller =
              Task.async(fn ->
                in_process(actors[1], fn ->
                  apply(Group, local_operation, [@name, "local/a", %{local: true}])
                end)
              end)

            try do
              TestCluster.assert_eventually(
                fn ->
                  {:messages, messages} = Process.info(shard, :messages)
                  Enum.any?(messages, &(is_tuple(&1) and elem(&1, 0) == :group_local_request))
                end,
                interval: 1
              )

              :ok = :sys.resume(shard)
              assert :ok = Task.await(caller, 5_000)
              events = events_after_barrier(@name, actors.observer)
              first_flush = div(buffer + chunk_size - 1, chunk_size) * chunk_size
              assert Enum.find_index(events, &(&1.key == "local/a")) == first_flush
              assert first_flush < count
              assert Enum.reject(events, &(&1.key == "local/a")) == remote_events

              local_kind = if local_operation == :register, do: :registry, else: :pg
              local_event = event(local_kind, nil, "local/a", actors[1], %{local: true}, nil, nil)
              assert Enum.filter(events, &(&1.key == "local/a")) == [local_event]
              local_entry = {local_kind, nil, "local/a", actors[1], %{local: true}}
              assert_entries(entries, unquote(kind), nil, [local_entry])
              assert :ok = TestCluster.assert_ets_consistent(@name)
            after
              Task.shutdown(caller, :brutal_kill)
            end
          after
            :sys.resume(shard)
          end
        end)
      end
    end
  end

  defp options(shards, buffer) do
    [
      shards: shards,
      log: false,
      replicated_pg_receiver_buffer_size: buffer,
      replicated_registry_receiver_buffer_size: buffer,
      replicated_pg_receiver_flush_interval: 60_000,
      replicated_registry_receiver_flush_interval: 60_000
    ]
  end

  defp subscribe(observer, cluster) do
    assert :ok = in_process(observer, fn -> Group.monitor(@name, :all, cluster: cluster) end)
  end

  # Build well-ordered wire histories with monotonic timestamps. Registry keys
  # keep one owner (conflicts are a separate property); PG keys can have two.
  defp compile_history(kind, cluster, commands, actors) do
    {ops, entries, events} =
      commands
      |> Enum.with_index(1)
      |> Enum.reduce({[], %{}, []}, fn {{action, key_id, owner, value}, time},
                                       {ops, entries, events} ->
        key = Enum.at(@keys, key_id)
        pid = actors[if(kind == :registry, do: rem(key_id, 2), else: owner)]
        meta = %{v: value}
        previous = Map.get(entries, {key, pid})

        case action do
          :put ->
            op =
              case kind do
                :registry ->
                  {:register, cluster, key, pid, meta, time, node(pid)}

                :pg ->
                  reason = if previous, do: :update, else: :join
                  {:join, cluster, key, pid, meta, time, reason, node(pid)}
              end

            e = event(kind, cluster, key, pid, meta, previous, nil)
            {[op | ops], Map.put(entries, {key, pid}, meta), [e | events]}

          :remove ->
            reason = if kind == :registry, do: :unregister, else: :leave
            op = {reason, cluster, key, pid, previous || meta, reason}

            events =
              if previous,
                do: [event(kind, cluster, key, pid, previous, nil, reason) | events],
                else: events

            {[op | ops], Map.delete(entries, {key, pid}), events}
        end
      end)

    {Enum.reverse(ops), entries, Enum.reverse(events)}
  end

  defp event(kind, cluster, key, pid, meta, previous, reason) do
    type =
      if reason,
        do: removal_type(kind),
        else: if(kind == :registry, do: :registered, else: :joined)

    %Group.Event{
      supervisor: @name,
      type: type,
      cluster: cluster,
      key: key,
      pid: pid,
      meta: meta,
      previous_meta: previous,
      reason: reason
    }
  end

  defp removal_type(:registry), do: :unregistered
  defp removal_type(:pg), do: :left

  defp deliver(kind, ops, chunk_size, shards) do
    tag = if kind == :registry, do: :replicate_registry_batch, else: :replicate_pg_batch

    ops
    |> Enum.group_by(fn op -> Replica.shard_index_for(elem(op, 1), elem(op, 2), shards) end)
    |> Enum.each(fn {shard, shard_ops} ->
      for chunk <- Enum.chunk_every(shard_ops, chunk_size) do
        send(Replica.shard_name(@name, shard), {tag, chunk})
      end

      # Order our sends before the observer's barrier (a different sender).
      # This system message does not itself flush a partial receiver buffer.
      :sys.get_state(Replica.shard_name(@name, shard))
    end)
  end

  defp snapshot(kind, cluster, entries, shards) do
    data = Enum.map(entries, fn {{key, pid}, meta} -> {key, pid, meta, 1_000} end)
    {reg, pg} = if kind == :registry, do: {data, []}, else: {[], data}

    for shard <- 0..(shards - 1) do
      send(Replica.shard_name(@name, shard), {:cluster_state, cluster, reg, pg})
      :sys.get_state(Replica.shard_name(@name, shard))
    end
  end

  defp assert_per_key_events(actual, expected) do
    group = fn events -> Enum.group_by(events, &{&1.cluster, &1.key}) end
    assert group.(actual) == group.(expected)
  end

  defp assert_entries(entries, kind, cluster, survivors \\ []) do
    expected =
      Enum.map(entries, fn {{key, pid}, meta} -> {kind, cluster, key, pid, meta} end)

    assert Enum.sort(Group.local_entries(@name)) == Enum.sort(survivors ++ expected)

    for key <- @keys do
      members = for {{^key, pid}, meta} <- entries, do: {pid, meta}

      case kind do
        :registry ->
          assert Group.lookup(@name, key, cluster: cluster) == List.first(members)

        :pg ->
          assert Enum.sort(Group.members(@name, key, cluster: cluster)) == Enum.sort(members)
      end
    end
  end
end
