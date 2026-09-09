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

  defp assert_per_key_events(actual, expected) do
    group = fn events -> Enum.group_by(events, &{&1.cluster, &1.key}) end
    assert group.(actual) == group.(expected)
  end

  defp assert_entries(entries, kind, cluster) do
    expected =
      Enum.map(entries, fn {{key, pid}, meta} -> {kind, cluster, key, pid, meta} end)

    assert Enum.sort(Group.local_entries(@name)) == Enum.sort(expected)

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
