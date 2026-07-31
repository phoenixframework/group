defmodule Group.ReplicaAdversarialTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias Group.TestCluster

  @seeds [10_007, 20_011, 40_009]
  @clusters ["red", "blue"]

  for seed <- @seeds do
    @seed seed
    @tag chaos_seed: seed

    test "seeded mixed-operation transport chaos converges without zombies (seed #{@seed})" do
      seed = @seed
      :rand.seed(:exsss, {seed, seed * 3 + 1, seed * 7 + 2})

      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      name = :"replica_chaos_#{seed}_#{System.unique_integer([:positive])}"

      opts = [
        name: name,
        shards: 3,
        replica_transport: Group.TestReplicaTransport,
        replicated_sender_buffer_size: 2,
        replicated_anti_entropy_interval: 25,
        replicated_peer_lease_timeout: 2_000,
        replicated_oplog_max_entries: 12
      ]

      for {_peer, node} <- peers do
        {:ok, _pid} = TestCluster.start_group(node, opts)
        :ok = TestCluster.rpc!(node, Group, :connect, [name, @clusters])
      end

      TestCluster.assert_eventually(
        fn ->
          Enum.all?([node_a, node_b], fn node ->
            length(TestCluster.rpc!(node, Group, :nodes, [name])) == 1 and
              Enum.all?(@clusters, fn cluster ->
                length(TestCluster.rpc!(node, Group, :nodes, [name, cluster])) == 2
              end)
          end)
        end,
        timeout: 10_000
      )

      :ok =
        TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [
          name,
          {:chaos, [drop_every: 5, duplicate_every: 7, max_delay: 40]}
        ])

      :ok =
        TestCluster.rpc!(node_b, Group.TestReplicaTransport, :set_mode, [
          name,
          {:chaos, [drop_every: 7, duplicate_every: 5, max_delay: 55]}
        ])

      initial = %{
        active: %{node_a => MapSet.new(@clusters), node_b => MapSet.new(@clusters)},
        counter: 0,
        pg_keys: MapSet.new(),
        pids: [],
        reg_keys: MapSet.new(),
        trace: []
      }

      state =
        Enum.reduce(1..72, initial, fn step, state ->
          apply_random_operation(state, step, seed, name, node_a, node_b)
        end)

      for node <- [node_a, node_b] do
        :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :pass])
        :ok = TestCluster.rpc!(node, Group, :connect, [name, @clusters])
      end

      assert_converges(name, node_a, node_b, state)

      # Let delayed frames from the chaos phase arrive, then prove they are
      # duplicates/stale rather than a source of resurrection.
      Process.sleep(150)
      TestCluster.flush_shards(node_a, name)
      TestCluster.flush_shards(node_b, name)
      assert_converges(name, node_a, node_b, state)

      for node <- [node_a, node_b] do
        assert :ok =
                 TestCluster.rpc!(node, Group.TestCluster, :assert_replica_consistent, [name])
      end

      TestCluster.assert_eventually(
        fn -> retained_owners_alive?(name, [node_a, node_b]) end,
        timeout: 15_000
      )
    end
  end

  defp apply_random_operation(state, step, seed, name, node_a, node_b) do
    nodes = [node_a, node_b]

    case :rand.uniform(12) do
      choice when choice in 1..3 ->
        add_registration(state, step, seed, name, random(nodes))

      choice when choice in 4..6 ->
        add_membership(state, step, seed, name, random(nodes))

      7 ->
        kill_random_owner(state)

      8 ->
        add_registry_conflict(state, step, seed, name, node_a, node_b)

      9 ->
        toggle_cluster(state, step, name, random(nodes), random(@clusters))

      10 ->
        change_transport_mode(state, step, name, random(nodes))

      _ ->
        Enum.reduce(1..3, state, fn offset, acc ->
          add_registration(acc, step * 10 + offset, seed, name, random(nodes))
        end)
    end
  end

  defp add_registration(state, step, seed, name, origin) do
    cluster = random([nil | MapSet.to_list(state.active[origin])])
    {key, state} = next_key(state, seed, "reg", step)
    meta = %{seed: seed, step: step, origin: origin}

    pid =
      if cluster do
        TestCluster.spawn_register_in_cluster(origin, name, key, meta, cluster)
      else
        TestCluster.spawn_register(origin, name, key, meta)
      end

    state
    |> Map.update!(:pids, &[{origin, pid} | &1])
    |> Map.update!(:reg_keys, &MapSet.put(&1, {cluster, key}))
    |> trace({:register, origin, cluster, key, pid})
  end

  defp add_membership(state, step, seed, name, origin) do
    cluster = random([nil | MapSet.to_list(state.active[origin])])
    {key, state} = next_key(state, seed, "pg", step)
    meta = %{seed: seed, step: step, origin: origin}

    pid =
      if cluster do
        TestCluster.spawn_join_in_cluster(origin, name, key, meta, cluster)
      else
        TestCluster.spawn_join(origin, name, key, meta)
      end

    state
    |> Map.update!(:pids, &[{origin, pid} | &1])
    |> Map.update!(:pg_keys, &MapSet.put(&1, {cluster, key}))
    |> trace({:join, origin, cluster, key, pid})
  end

  defp kill_random_owner(%{pids: []} = state), do: trace(state, :kill_noop)

  defp kill_random_owner(state) do
    {origin, pid} = random(state.pids)
    TestCluster.rpc!(origin, Process, :exit, [pid, :kill])

    state
    |> Map.update!(:pids, &List.delete(&1, {origin, pid}))
    |> trace({:kill, origin, pid})
  end

  defp add_registry_conflict(state, step, seed, name, node_a, node_b) do
    {key, state} = next_key(state, seed, "conflict", step)

    # Establish the competing claims before either origin can observe the
    # other. Drain any already-scheduled delayed frame first; otherwise this
    # operation nondeterministically degenerates into a local :taken result.
    for node <- [node_a, node_b] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    Process.sleep(75)
    pid_a = TestCluster.spawn_register(node_a, name, key, %{side: :a, seed: seed})
    pid_b = TestCluster.spawn_register(node_b, name, key, %{side: :b, seed: seed})

    :ok =
      TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [
        name,
        {:chaos, [drop_every: 5, duplicate_every: 7, max_delay: 40]}
      ])

    :ok =
      TestCluster.rpc!(node_b, Group.TestReplicaTransport, :set_mode, [
        name,
        {:chaos, [drop_every: 7, duplicate_every: 5, max_delay: 55]}
      ])

    state
    |> Map.update!(:pids, &[{node_a, pid_a}, {node_b, pid_b} | &1])
    |> Map.update!(:reg_keys, &MapSet.put(&1, {nil, key}))
    |> trace({:conflict, key, pid_a, pid_b})
  end

  defp toggle_cluster(state, step, name, origin, cluster) do
    if MapSet.member?(state.active[origin], cluster) do
      :ok = TestCluster.rpc!(origin, Group, :disconnect, [name, cluster])

      state
      |> put_in([:active, origin], MapSet.delete(state.active[origin], cluster))
      |> trace({:disconnect, step, origin, cluster})
    else
      :ok = TestCluster.rpc!(origin, Group, :connect, [name, cluster])

      state
      |> put_in([:active, origin], MapSet.put(state.active[origin], cluster))
      |> trace({:connect, step, origin, cluster})
    end
  end

  defp change_transport_mode(state, step, name, origin) do
    mode =
      random([
        :drop,
        :busy,
        {:chaos, [drop_every: 4, duplicate_every: 3, max_delay: 65]},
        {:chaos, [drop_every: 9, duplicate_every: 2, max_delay: 30]}
      ])

    :ok = TestCluster.rpc!(origin, Group.TestReplicaTransport, :set_mode, [name, mode])
    trace(state, {:transport, step, origin, mode})
  end

  defp next_key(state, seed, kind, step) do
    counter = state.counter + 1
    {"chaos/#{seed}/#{kind}/#{step}/#{counter}", %{state | counter: counter}}
  end

  defp assert_converges(name, node_a, node_b, state) do
    TestCluster.assert_eventually(
      fn ->
        length(TestCluster.rpc!(node_a, Group, :nodes, [name])) == 1 and
          length(TestCluster.rpc!(node_b, Group, :nodes, [name])) == 1 and
          Enum.all?(@clusters, fn cluster ->
            length(TestCluster.rpc!(node_a, Group, :nodes, [name, cluster])) == 2 and
              length(TestCluster.rpc!(node_b, Group, :nodes, [name, cluster])) == 2
          end) and
          registry_equal?(name, node_a, node_b, state.reg_keys) and
          memberships_equal?(name, node_a, node_b, state.pg_keys)
      end,
      timeout: 20_000,
      interval: 75
    )
  rescue
    error ->
      flunk(
        "chaos convergence failed: #{Exception.message(error)}\n" <>
          "differences=#{inspect(convergence_differences(name, node_a, node_b, state), limit: :infinity)}\n" <>
          "recent operations=#{inspect(Enum.take(state.trace, 20), limit: :infinity)}"
      )
  end

  defp convergence_differences(name, node_a, node_b, state) do
    registry =
      state.reg_keys
      |> Enum.flat_map(fn {cluster, key} ->
        args = [name, key, cluster_opts(cluster)]
        value_a = TestCluster.rpc!(node_a, Group, :lookup, args)
        value_b = TestCluster.rpc!(node_b, Group, :lookup, args)
        if value_a == value_b, do: [], else: [{:registry, cluster, key, value_a, value_b}]
      end)
      |> Enum.take(10)

    pg =
      state.pg_keys
      |> Enum.flat_map(fn {cluster, key} ->
        args = [name, key, cluster_opts(cluster)]
        value_a = TestCluster.rpc!(node_a, Group, :members, args) |> Enum.sort()
        value_b = TestCluster.rpc!(node_b, Group, :members, args) |> Enum.sort()
        if value_a == value_b, do: [], else: [{:pg, cluster, key, value_a, value_b}]
      end)
      |> Enum.take(10)

    nodes =
      for cluster <- [nil | @clusters] do
        value_a = group_nodes(node_a, name, cluster)
        value_b = group_nodes(node_b, name, cluster)
        {cluster, value_a, value_b}
      end

    cluster_trace =
      state.trace
      |> Enum.filter(fn
        {:connect, _step, _origin, _cluster} -> true
        {:disconnect, _step, _origin, _cluster} -> true
        _ -> false
      end)

    protocol =
      for node <- [node_a, node_b] do
        {node, TestCluster.rpc!(node, Group.TestCluster, :replica_protocol_state, [name])}
      end

    [
      nodes: nodes,
      registry: registry,
      pg: pg,
      cluster_trace: cluster_trace,
      protocol: protocol
    ]
  end

  defp group_nodes(node, name, nil), do: TestCluster.rpc!(node, Group, :nodes, [name])

  defp group_nodes(node, name, cluster),
    do: TestCluster.rpc!(node, Group, :nodes, [name, cluster])

  defp registry_equal?(name, node_a, node_b, keys) do
    Enum.all?(keys, fn {cluster, key} ->
      args = [name, key, cluster_opts(cluster)]

      TestCluster.rpc!(node_a, Group, :lookup, args) ==
        TestCluster.rpc!(node_b, Group, :lookup, args)
    end)
  end

  defp memberships_equal?(name, node_a, node_b, keys) do
    Enum.all?(keys, fn {cluster, key} ->
      args = [name, key, cluster_opts(cluster)]
      members_a = TestCluster.rpc!(node_a, Group, :members, args) |> Enum.sort()
      members_b = TestCluster.rpc!(node_b, Group, :members, args) |> Enum.sort()
      members_a == members_b
    end)
  end

  defp retained_owners_alive?(name, nodes) do
    nodes
    |> Enum.flat_map(fn node ->
      TestCluster.rpc!(node, Group.TestCluster, :replica_owner_pids, [name])
    end)
    |> Enum.uniq()
    |> Enum.all?(fn pid -> TestCluster.rpc!(node(pid), Process, :alive?, [pid]) end)
  end

  defp cluster_opts(nil), do: []
  defp cluster_opts(cluster), do: [cluster: cluster]

  defp trace(state, operation), do: Map.update!(state, :trace, &[operation | &1])
  defp random(values), do: Enum.at(values, :rand.uniform(length(values)) - 1)
end
