defmodule Group.AntiEntropyFaultRegressionTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias Group.TestCluster

  setup_all do
    peers = TestCluster.start_peers(3, schedulers: 4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)
    [{_, node_a}, {_, node_b}, {_, node_c}] = peers
    {:ok, peers: peers, node_a: node_a, node_b: node_b, node_c: node_c}
  end

  test "local unregister promotes a retained remote claim after its delta is pruned", context do
    %{name: name, pid_a: pid_a, pid_b: pid_b, stream_b: stream_b} =
      establish_hidden_remote_claim(context, :unregister)

    {floor, head, _applied} =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_stream_head, [
        name,
        0,
        stream_b
      ])

    assert floor > 1

    assert TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_cursor, [
             name,
             0,
             stream_b
           ]) == head

    assert :ok = TestCluster.unregister_owner(pid_a)
    TestCluster.flush_shards(context.node_a, name)

    assert TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims, [
             name,
             0,
             nil,
             "hidden/unregister"
           ])
           |> Enum.map(&elem(&1, 0)) == [pid_b]

    TestCluster.assert_eventually(
      fn ->
        match?(
          {^pid_b, %{rank: 1}},
          TestCluster.rpc!(context.node_a, Group, :lookup, [name, "hidden/unregister"])
        )
      end,
      timeout: 500,
      interval: 25
    )

    for node <- [context.node_a, context.node_b, context.node_c] do
      assert :ok = TestCluster.rpc!(node, TestCluster, :assert_replica_consistent, [name])
    end
  end

  test "journal replay promotes a retained remote claim before marking the delete applied",
       context do
    %{name: name, pid_a: pid_a, pid_b: pid_b} =
      establish_hidden_remote_claim(context, :journal)

    stream_a =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 0, nil])

    {seq, _mutations} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :append_replica_record, [
        name,
        0,
        stream_a,
        [{:unregister, nil, "hidden/journal", pid_a, %{rank: 2}, :injected_crash}]
      ])

    assert [{^stream_a, ^seq, _}] =
             TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_unapplied, [
               name,
               0
             ])

    old_shard =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    true = TestCluster.rpc!(context.node_a, Process, :exit, [old_shard, :kill])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_a, Process, :whereis, [
             Group.Replica.shard_name(name, 0)
           ]) do
        pid when is_pid(pid) -> pid != old_shard
        nil -> false
      end
    end)

    TestCluster.flush_shards(context.node_a, name)

    claims =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims, [
        name,
        0,
        nil,
        "hidden/journal"
      ])

    journal_state = %{
      claims: claims,
      head:
        TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
          name,
          0,
          stream_a
        ]),
      unapplied:
        TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_unapplied, [
          name,
          0
        ])
    }

    assert Enum.map(claims, &elem(&1, 0)) == [pid_b], inspect(journal_state)

    assert {^pid_b, %{rank: 1}} =
             TestCluster.rpc!(context.node_a, Group, :lookup, [name, "hidden/journal"])

    assert [] =
             TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_unapplied, [
               name,
               0
             ])

    assert :ok =
             TestCluster.rpc!(context.node_a, TestCluster, :assert_replica_consistent, [name])
  end

  test "local process DOWN promotes a retained remote claim", context do
    %{name: name, pid_a: pid_a, pid_b: pid_b} =
      establish_hidden_remote_claim(context, :process_down)

    true = TestCluster.rpc!(context.node_a, Process, :exit, [pid_a, :kill])
    TestCluster.flush_shards(context.node_a, name)

    assert [^pid_b] =
             TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims, [
               name,
               0,
               nil,
               "hidden/process_down"
             ])
             |> Enum.map(&elem(&1, 0))

    assert {^pid_b, %{rank: 1}} =
             TestCluster.rpc!(context.node_a, Group, :lookup, [name, "hidden/process_down"])

    assert :ok =
             TestCluster.rpc!(context.node_a, TestCluster, :assert_replica_consistent, [name])
  end

  test "local cluster disconnect removes a claimless legacy registry row", context do
    name = unique_name(:claimless_disconnect)
    cluster = "legacy-slice"

    opts = [
      name: name,
      shards: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    {:ok, _pid} = TestCluster.start_group(context.node_a, opts)
    :ok = TestCluster.rpc!(context.node_a, Group, :connect, [name, cluster])

    owner =
      TestCluster.spawn_register_in_cluster(
        context.node_a,
        name,
        "legacy/claimless",
        %{legacy: true},
        cluster
      )

    assert ["legacy/claimless"] =
             TestCluster.rpc!(
               context.node_a,
               Group.Replica.Data,
               :purge_registry_claims_for_cluster,
               [name, 0, cluster]
             )

    assert {^owner, %{legacy: true}} =
             TestCluster.rpc!(context.node_a, Group, :lookup, [
               name,
               "legacy/claimless",
               [cluster: cluster]
             ])

    :ok = TestCluster.rpc!(context.node_a, Group, :disconnect, [name, cluster])

    assert nil ==
             TestCluster.rpc!(context.node_a, Group, :lookup, [
               name,
               "legacy/claimless",
               [cluster: cluster]
             ])
  end

  test "expiring one sideband lane keeps the shared node route while other lanes are live",
       context do
    name = unique_name(:sideband_lane)

    opts = [
      name: name,
      shards: 3,
      replica_transport:
        {Group.TestTCPTransport,
         [connect_timeout: 250, send_timeout: 250, reconnect_interval: 10]},
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(context.node_b, Group.TestTCPTransport, :connected?, [
          name,
          context.node_a
        ])
      end,
      timeout: 10_000
    )

    lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    send(lane, {:replica_authority_removed_local, context.node_a})
    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [lane])
    Process.sleep(100)

    assert TestCluster.rpc!(context.node_b, Group.TestTCPTransport, :connected?, [
             name,
             context.node_a
           ])

    status = TestCluster.rpc!(context.node_b, Group.TestTCPTransport, :status, [name])
    assert context.node_a in status.peers

    for shard <- [0, 2] do
      replica =
        TestCluster.rpc!(context.node_b, Process, :whereis, [
          Group.Replica.shard_name(name, shard)
        ])

      send(replica, {:replica_authority_removed_local, context.node_a})
      _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [replica])
    end

    TestCluster.assert_eventually(fn ->
      not TestCluster.rpc!(context.node_b, Group.TestTCPTransport, :connected?, [
        name,
        context.node_a
      ]) and
        context.node_a not in TestCluster.rpc!(context.node_b, Group.TestTCPTransport, :status, [
          name
        ]).peers
    end)
  end

  test "an exact shard-zero hello repairs a missing local authority view", context do
    name = unique_name(:control_view_repair)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    {generation, revision, epochs} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_authority, [name])

    assert generation ==
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
               name,
               0,
               context.node_a
             ])

    :ok =
      TestCluster.rpc!(context.node_b, TestCluster, :delete_remote_view_info, [
        name,
        0,
        context.node_a
      ])

    assert nil ==
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
               name,
               0,
               context.node_a
             ])

    assert generation ==
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
               name,
               context.node_a
             ])

    source =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    send(
      target,
      {:replica_hello, source, Group.Replica.WireProtocol.version(), generation, revision, epochs,
       Group.TestReplicaTransport.id(), Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target])

    assert generation ==
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
               name,
               0,
               context.node_a
             ])

    assert revision ==
             TestCluster.rpc!(
               context.node_b,
               Group.Replica.Data,
               :remote_view_cluster_epoch_revision,
               [name, 0, context.node_a]
             )
  end

  defp establish_hidden_remote_claim(context, suffix) do
    name = unique_name(suffix)
    key = "hidden/#{suffix}"

    opts = [
      name: name,
      shards: 1,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.ModelConflictResolver, :resolve, []},
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 5_000,
      replicated_oplog_max_entries: 2
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    pid_a = TestCluster.spawn_register(context.node_a, name, key, %{rank: 2})
    pid_b = TestCluster.spawn_register(context.node_b, name, key, %{rank: 1})
    :ok = TestCluster.rpc!(context.node_b, Group.TestReplicaTransport, :set_mode, [name, :pass])

    TestCluster.assert_eventually(fn ->
      claims =
        TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims, [
          name,
          0,
          nil,
          key
        ])

      match?({^pid_a, %{rank: 2}}, TestCluster.rpc!(context.node_a, Group, :lookup, [name, key])) and
        MapSet.new(Enum.map(claims, &elem(&1, 0))) == MapSet.new([pid_a, pid_b])
    end)

    for index <- 1..4 do
      churn =
        TestCluster.spawn_register(context.node_b, name, "#{key}/churn/#{index}", %{rank: 1})

      true = TestCluster.rpc!(context.node_b, Process, :exit, [churn, :kill])
    end

    TestCluster.flush_shards(context.node_b, name)

    stream_b =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :local_stream_id, [name, 0, nil])

    TestCluster.assert_eventually(fn ->
      {_floor, head, _applied} =
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_stream_head, [
          name,
          0,
          stream_b
        ])

      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_cursor, [
        name,
        0,
        stream_b
      ]) == head
    end)

    %{name: name, pid_a: pid_a, pid_b: pid_b, stream_b: stream_b}
  end

  defp start_group_on_peers(peers, opts) do
    Enum.each(peers, fn {_peer, node} ->
      {:ok, _pid} = TestCluster.start_group(node, opts)
    end)

    nodes = Enum.map(peers, &elem(&1, 1))

    TestCluster.assert_eventually(fn ->
      Enum.all?(nodes, fn node ->
        Enum.sort(TestCluster.rpc!(node, Group, :nodes, [Keyword.fetch!(opts, :name)])) ==
          Enum.sort(nodes -- [node])
      end)
    end)
  end

  defp unique_name(suffix) do
    :"ae_fault_#{suffix}_#{System.unique_integer([:positive])}"
  end
end
