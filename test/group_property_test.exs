defmodule Group.PropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Group.TestCluster

  @moduletag :capture_log
  # Reuse a bounded set of names: generating atoms per example leaks atoms while shrinking.
  @name :group_property_model
  @clusters [nil, "alpha", "beta"]
  @keys ["room/a", "room/b", "room/nested/a", "rooms/a", "other/a"]
  @prefixes ["room/", "room/nested/", "rooms/", "other/", "missing/"]

  property "local command histories agree with an independent model" do
    check all(
            shards <- member_of([1, 2, 4]),
            commands <- list_of(command(), min_length: 1, max_length: 60),
            max_runs: 100
          ) do
      run_history(shards, commands)
    end
  end

  test "a failed example releases its fixtures before the next shrink attempt" do
    assert_raise ExUnit.AssertionError, fn ->
      run_history(4, [
        {:register, nil, "room/a", 0, %{v: 0}},
        # Intentionally outside the generator's valid-key domain: the model
        # expects success, but Group rejects the trailing slash.
        {:register, nil, "room/", 0, %{v: 0}}
      ])
    end

    run_history(1, [{:register, nil, "room/a", 1, %{v: 1}}])
  end

  # Exercise important transitions even when a short random history misses them.
  test "model covers updates, collisions, cleanup, subscriptions, and reconnects" do
    for shards <- [1, 4] do
      run_history(shards, [
        {:register, nil, "room/a", 0, %{v: 0}},
        {:register, nil, "room/a", 0, %{v: 0}},
        {:register, nil, "room/a", 0, %{v: 1}},
        {:register, nil, "room/a", 1, %{v: 2}},
        {:join, nil, "room/a", 0, %{v: 0}},
        {:join, nil, "room/a", 0, %{v: 0}},
        {:join, nil, "room/a", 0, %{v: 1}},
        {:join, nil, "room/b", 1, %{v: 2}},
        {:unregister, nil, "room/a", 1},
        {:leave, nil, "room/b", 0},
        {:leave, nil, "room/b", 1},
        {:connect, "alpha"},
        {:connect, "alpha"},
        {:register, "alpha", "room/a", 0, %{v: 0}},
        {:join, "alpha", "room/a", 1, %{v: 1}},
        {:connect, "beta"},
        {:join, "beta", "room/a", 1, %{v: 2}},
        {:kill, 0},
        {:disconnect, "alpha"},
        {:join, "alpha", "room/a", 1, %{v: 0}},
        {:connect, "alpha"},
        {:demonitor, "alpha", :all},
        {:monitor, "alpha", "room/"},
        {:monitor, "alpha", "room/a"},
        {:monitor, "alpha", "room/a"},
        {:join, "alpha", "room/a", 0, %{v: 1}},
        {:demonitor, "alpha", "room/a"},
        {:demonitor, "alpha", "room/"},
        {:join, "alpha", "room/a", 0, %{v: 2}},
        {:kill, 1}
      ])
    end
  end

  defp command do
    cluster = member_of(@clusters)
    key = member_of(@keys)
    actor = integer(0..2)
    meta = map(integer(0..2), &%{v: &1})

    frequency([
      {6, tuple({member_of([:register, :join]), cluster, key, actor, meta})},
      {3, tuple({member_of([:unregister, :leave]), cluster, key, actor})},
      {2, tuple({member_of([:connect, :disconnect]), member_of(["alpha", "beta"])})},
      {2,
       tuple({member_of([:monitor, :demonitor]), cluster, member_of([:all | @keys ++ @prefixes])})},
      {1, tuple({constant(:kill), actor})}
    ])
  end

  defp run_history(shards, commands) do
    # These fixtures belong inside the property body, not ExUnit setup/on_exit:
    # StreamData runs this again for every example AND every shrink candidate.
    pool = start_supervised!({Task.Supervisor, []})

    try do
      start_supervised!({Group, name: @name, shards: shards, log: false})
      actors = Map.new(0..2, &{&1, start_actor(pool)})
      # Registry subscriptions link to Registry partitions. Keep them out of
      # the ExUnit process, which must survive fixture teardown for shrinking.
      observer = start_actor(pool)

      model = %{
        entries: %{},
        connected: MapSet.new([nil]),
        subscriptions: MapSet.new(@clusters, &{&1, :all})
      }

      call_actor(observer, fn ->
        for cluster <- @clusters, do: Group.monitor(@name, :all, cluster: cluster)
      end)

      assert_state(model, actors)

      Enum.reduce(commands, {model, actors}, fn command, {model, actors} ->
        {expected_result, next_model, events} = transition(model, command)
        {result, next_actors} = execute(command, actors, pool, shards, observer)
        assert result == expected_result, "result for #{inspect(command)}"

        actual_events =
          call_actor(observer, fn ->
            barrier(shards)
            drain_events()
          end)

        assert_events(actual_events, events, model.subscriptions, actors)
        assert_state(next_model, next_actors)
        {next_model, next_actors}
      end)
    after
      stop_supervised(Task.Supervisor)
      stop_supervised({Group, @name})
      :persistent_term.erase({Group, @name})
    end
  end

  defp start_actor(pool) do
    {:ok, pid} = Task.Supervisor.start_child(pool, &actor_loop/0)
    pid
  end

  defp actor_loop do
    receive do
      {:call, caller, ref, fun} ->
        result =
          try do
            {:ok, fun.()}
          rescue
            error -> {:error, error, __STACKTRACE__}
          end

        send(caller, {ref, result})
        actor_loop()
    end
  end

  defp call_actor(pid, fun) do
    ref = Process.monitor(pid)
    send(pid, {:call, self(), ref, fun})

    try do
      receive do
        {^ref, {:ok, result}} -> result
        {^ref, {:error, error, stacktrace}} -> reraise error, stacktrace
        {:DOWN, ^ref, :process, ^pid, reason} -> flunk("actor exited: #{inspect(reason)}")
      after
        5_000 -> flunk("actor call timed out")
      end
    after
      Process.demonitor(ref, [:flush])
    end
  end

  defp execute({:kill, actor}, actors, pool, shards, _observer) do
    pid = Map.fetch!(actors, actor)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000

    # A DOWN received here does not order the shards' DOWN messages. Wait for
    # each shard to finish cleanup before sending the event-delivery barriers.
    TestCluster.assert_eventually(
      fn ->
        Enum.all?(0..(shards - 1), fn shard ->
          state = :sys.get_state(Group.Replica.shard_name(@name, shard))
          not Map.has_key?(state.monitors, pid)
        end)
      end,
      interval: 1
    )

    {:ok, Map.put(actors, actor, start_actor(pool))}
  end

  defp execute({operation, cluster}, actors, _pool, _shards, _observer) do
    {apply(Group, operation, [@name, cluster]), actors}
  end

  defp execute({operation, cluster, pattern}, actors, _pool, _shards, observer) do
    result =
      call_actor(observer, fn -> apply(Group, operation, [@name, pattern, [cluster: cluster]]) end)

    {result, actors}
  end

  defp execute(command, actors, _pool, _shards, _observer) do
    [operation, cluster, key, actor | metadata] = Tuple.to_list(command)

    result =
      call_actor(Map.fetch!(actors, actor), fn ->
        try do
          apply(Group, operation, [@name, key | metadata] ++ [[cluster: cluster]])
        rescue
          ArgumentError -> {:raised, ArgumentError}
        end
      end)

    {result, actors}
  end

  # Pure reference model. Entries use symbolic owners, never PIDs or ETS data.
  # Each key is {registry | pg, cluster, key, actor_id}.
  defp transition(model, {:kill, actor}) do
    purge(model, fn {_kind, _cluster, _key, owner} -> owner == actor end, :killed)
  end

  defp transition(model, {:connect, cluster}) do
    {:ok, %{model | connected: MapSet.put(model.connected, cluster)}, []}
  end

  defp transition(model, {:disconnect, cluster}) do
    model = %{model | connected: MapSet.delete(model.connected, cluster)}
    purge(model, fn {_kind, c, _key, _owner} -> c == cluster end, :cluster_disconnect)
  end

  defp transition(model, {operation, cluster, pattern}) do
    subscription = {cluster, pattern}

    subscriptions =
      case operation do
        :monitor -> MapSet.put(model.subscriptions, subscription)
        :demonitor -> MapSet.delete(model.subscriptions, subscription)
      end

    {:ok, %{model | subscriptions: subscriptions}, []}
  end

  defp transition(model, command) do
    [operation, cluster, key, actor | metadata] = Tuple.to_list(command)

    if MapSet.member?(model.connected, cluster) do
      kind = if operation in [:register, :unregister], do: :registry, else: :pg
      entry = {kind, cluster, key, actor}

      existing =
        Enum.find(model.entries, fn {{k, c, name, owner}, _meta} ->
          {k, c, name} == {kind, cluster, key} and (kind == :registry or owner == actor)
        end)

      case {operation, existing, metadata} do
        {:register, {{:registry, ^cluster, ^key, other}, _}, _} when other != actor ->
          {{:error, :taken}, model, []}

        {op, _, [meta]} when op in [:register, :join] ->
          previous = if existing, do: elem(existing, 1), else: nil
          events = if previous == meta, do: [], else: [event(entry, meta, previous, nil)]
          {:ok, %{model | entries: Map.put(model.entries, entry, meta)}, events}

        {:unregister, nil, []} ->
          {{:error, :undefined}, model, []}

        {:leave, nil, []} ->
          {{:error, :not_in_group}, model, []}

        {op, {existing_entry, meta}, []} when op in [:unregister, :leave] ->
          {:ok, %{model | entries: Map.delete(model.entries, existing_entry)},
           [event(existing_entry, meta, nil, op)]}
      end
    else
      {{:raised, ArgumentError}, model, []}
    end
  end

  defp purge(model, predicate, reason) do
    {removed, kept} = Enum.split_with(model.entries, fn {entry, _meta} -> predicate.(entry) end)
    events = Enum.map(removed, fn {entry, meta} -> event(entry, meta, nil, reason) end)
    {:ok, %{model | entries: Map.new(kept)}, events}
  end

  defp event({kind, cluster, key, actor}, meta, previous, reason) do
    type =
      case {kind, reason} do
        {:registry, nil} -> :registered
        {:registry, _} -> :unregistered
        {:pg, nil} -> :joined
        {:pg, _} -> :left
      end

    %Group.Event{
      supervisor: @name,
      type: type,
      cluster: cluster,
      key: key,
      pid: actor,
      meta: meta,
      previous_meta: previous,
      reason: reason
    }
  end

  defp assert_events(actual, events, subscriptions, actors) do
    expected =
      events
      |> Enum.filter(fn event ->
        Enum.any?(subscriptions, fn {cluster, pattern} ->
          cluster == event.cluster and matches?(event.key, pattern)
        end)
      end)
      |> Enum.map(fn event -> %{event | pid: Map.fetch!(actors, event.pid)} end)

    # Cleanup spans shards, so there is no global ordering contract. Compare
    # multisets (not sets) per command: duplicates and missing events must fail.
    # Settling after each command also checks ordering between successive writes.
    assert Enum.sort(actual) == Enum.sort(expected)
  end

  defp assert_state(model, actors) do
    expected =
      Enum.map(model.entries, fn {{kind, cluster, key, actor}, meta} ->
        {kind, cluster, key, Map.fetch!(actors, actor), meta}
      end)

    assert Enum.sort(Group.local_entries(@name)) == Enum.sort(expected)

    for cluster <- @clusters do
      opts = [cluster: cluster]
      entries = Enum.filter(expected, &(elem(&1, 1) == cluster))
      registry = for {:registry, _, key, pid, meta} <- entries, into: %{}, do: {key, {pid, meta}}
      assert Group.registry_count(@name, opts) == map_size(registry)
      assert Group.local_registry_count(@name, opts) == map_size(registry)

      if cluster != nil do
        assert Group.connected?(@name, cluster) == MapSet.member?(model.connected, cluster)
      end

      for key <- @keys do
        assert Group.lookup(@name, key, opts) == Map.get(registry, key)
      end

      for query <- @keys ++ @prefixes do
        members =
          for {:pg, _, key, pid, meta} <- entries, matches?(key, query), do: {pid, meta}

        assert Enum.sort(Group.members(@name, query, opts)) == Enum.sort(members)
        assert Enum.sort(Group.local_members(@name, query, opts)) == Enum.sort(members)
        assert Group.member_count(@name, query, opts) == length(members)
        assert Group.local_member_count(@name, query, opts) == length(members)
      end
    end

    assert :ok == TestCluster.assert_ets_consistent(@name)
  end

  defp matches?(_key, :all), do: true

  defp matches?(key, query) do
    if String.ends_with?(query, "/"), do: String.starts_with?(key, query), else: key == query
  end

  defp barrier(shards) do
    # Use this process as the ack recipient, so same-sender signal ordering
    # guarantees that all earlier events from each shard are in our mailbox.
    for shard <- 0..(shards - 1) do
      ref = make_ref()
      send(Group.Replica.shard_name(@name, shard), {:group_dispatch, [self()], {:settled, ref}})
      assert_receive {:settled, ^ref}, 1_000
    end
  end

  defp drain_events do
    receive do
      {:group, events, %{name: @name}} -> events ++ drain_events()
    after
      0 -> []
    end
  end
end
