defmodule Group.ReplicaAckScaleTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 360_000

  alias Group.TestCluster

  @tag skip: System.get_env("GROUP_SCALE_TEST") != "1"
  test "20,000 connected subclusters advertise only unsettled heads" do
    peers = TestCluster.start_peers(2)
    on_exit(fn -> TestCluster.stop_peers(peers) end)
    [{_, source}, {_, receiver}] = peers

    name = :"replica_ack_scale_#{System.unique_integer([:positive])}"
    clusters = for index <- 1..20_000, do: "tenant/#{index}"

    opts = [
      name: name,
      shards: 4,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 5_000,
      replicated_peer_lease_timeout: 120_000
    ]

    for node <- [source, receiver] do
      {:ok, _pid} = TestCluster.start_group(node, opts)
    end

    for batch <- Enum.chunk_every(clusters, 1_000) do
      tasks =
        for node <- [source, receiver] do
          Task.async(fn -> TestCluster.connect_many_concurrently(node, name, batch) end)
        end

      assert [:ok, :ok] = Task.await_many(tasks, 120_000)
    end

    TestCluster.assert_eventually(
      fn ->
        Enum.all?([source, receiver], fn node ->
          length(TestCluster.rpc!(node, Group.Replica.Data, :my_clusters, [name])) == 20_001 and
            length(TestCluster.rpc!(node, Group, :nodes, [name, "tenant/20000"])) == 2
        end)
      end,
      timeout: 120_000,
      interval: 100
    )

    entries =
      for index <- 1..12 do
        cluster = "tenant/#{index * 1_666}"
        key = "sprite/#{index}"
        pid = TestCluster.spawn_register_in_cluster(source, name, key, %{index: index}, cluster)
        {cluster, key, pid}
      end

    TestCluster.assert_eventually(
      fn ->
        Enum.all?(entries, fn {cluster, key, pid} ->
          match?(
            {^pid, _meta},
            TestCluster.rpc!(receiver, Group, :lookup, [name, key, [cluster: cluster]])
          )
        end)
      end,
      timeout: 120_000,
      interval: 100
    )

    TestCluster.assert_eventually(
      fn ->
        Enum.all?(0..3, fn shard ->
          source
          |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(name, shard)])
          |> Map.get(:pending_replica_heads)
          |> map_size() == 0
        end)
      end,
      timeout: 120_000,
      interval: 100
    )

    :ok =
      TestCluster.rpc!(source, Group.TestReplicaTransport, :set_mode, [
        name,
        {:capture_pass, [:heads]}
      ])

    for shard <- 0..3 do
      shard_name = Group.Replica.shard_name(name, shard)
      state = TestCluster.rpc!(source, :sys, :get_state, [shard_name])
      shard_pid = TestCluster.rpc!(source, Process, :whereis, [shard_name])
      send(shard_pid, {:group_replica_anti_entropy, state.anti_entropy_ref})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(source, :sys, :get_state, [shard_name]).anti_entropy_ref !=
          state.anti_entropy_ref
      end)
    end

    assert [] == TestCluster.rpc!(source, Group.TestReplicaTransport, :captured, [name])
  end
end
