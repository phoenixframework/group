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

  test "a receiver shard restart purges a permanently disappeared Group", context do
    name = unique_name(:receiver_restart_permanent_loss)

    opts = [
      name: name,
      shards: 2,
      replicated_sender_buffer_size: 1,
      replicated_pg_receiver_buffer_size: 1,
      replicated_registry_receiver_buffer_size: 1,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 150
    ]

    start_group_on_peers(context.peers, opts)

    registry_key = "receiver-restart/registry"
    pg_key = "receiver-restart/pg"

    owner =
      TestCluster.spawn_register_and_join(
        context.node_a,
        name,
        registry_key,
        %{owner: :a},
        pg_key,
        %{owner: :a}
      )

    for receiver <- [context.node_b, context.node_c] do
      TestCluster.assert_eventually(fn ->
        match?(
          {^owner, %{owner: :a}},
          TestCluster.rpc!(receiver, Group, :lookup, [name, registry_key])
        ) and
          match?(
            [{^owner, %{owner: :a}}],
            TestCluster.rpc!(receiver, Group, :members, [name, pg_key])
          )
      end)
    end

    old_receiver =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [old_receiver])

    # A's BEAM intentionally remains connected. Only its Group disappears, so
    # B must learn retirement from the lease it misses while its shard is down.
    supervisor = TestCluster.rpc!(context.node_a, Process, :whereis, [:"#{name}_group_sup"])
    :ok = TestCluster.rpc!(context.node_a, Supervisor, :stop, [supervisor, :normal, 5_000])

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(context.node_c, Group, :lookup, [name, registry_key]) == nil and
          TestCluster.rpc!(context.node_c, Group, :members, [name, pg_key]) == [] and
          context.node_a not in TestCluster.rpc!(context.node_c, Group, :nodes, [name])
      end,
      timeout: 5_000
    )

    Process.sleep(250)
    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_receiver, :kill])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 0)
           ]) do
        pid when is_pid(pid) -> pid != old_receiver
        _ -> false
      end
    end)

    # No peer is allowed to resurrect A or supply a later delete. Correctness
    # therefore depends entirely on restart-time authority reconciliation.
    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(context.node_b, Group, :lookup, [name, registry_key]) == nil and
          TestCluster.rpc!(context.node_b, Group, :members, [name, pg_key]) == [] and
          context.node_a not in TestCluster.rpc!(context.node_b, Group, :nodes, [name])
      end,
      timeout: 1_000,
      interval: 25
    )

    assert :ok =
             TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_origin_purged, [
               name,
               context.node_a
             ])

    assert :ok =
             TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "a sibling restart cannot lose the retirement lease after shard zero expires a peer",
       context do
    name = unique_name(:sibling_restart_during_peer_expiry)

    opts = [
      name: name,
      shards: 2,
      replicated_sender_buffer_size: 1,
      replicated_pg_receiver_buffer_size: 1,
      replicated_registry_receiver_buffer_size: 1,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 150
    ]

    start_group_on_peers(context.peers, opts)

    key =
      1..1_000
      |> Enum.map(&"sibling-expiry/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    owner =
      TestCluster.spawn_register_and_join(
        context.node_a,
        name,
        key,
        %{owner: :a},
        key,
        %{owner: :a}
      )

    for receiver <- [context.node_b, context.node_c] do
      TestCluster.assert_eventually(fn ->
        match?({^owner, %{owner: :a}}, TestCluster.rpc!(receiver, Group, :lookup, [name, key])) and
          match?(
            [{^owner, %{owner: :a}}],
            TestCluster.rpc!(receiver, Group, :members, [name, key])
          )
      end)
    end

    old_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [old_lane])

    supervisor = TestCluster.rpc!(context.node_a, Process, :whereis, [:"#{name}_group_sup"])
    :ok = TestCluster.rpc!(context.node_a, Supervisor, :stop, [supervisor, :normal, 5_000])

    # Shard zero retires only its lane while the suspended sibling remains a
    # live restart breadcrumb. Shared authority must survive until that final
    # lane performs its own bounded retirement.
    TestCluster.assert_eventually(
      fn ->
        is_nil(
          TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
            name,
            0,
            context.node_a
          ])
        )
      end,
      timeout: 5_000
    )

    refute is_nil(
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
               name,
               context.node_a
             ])
           )

    # The lane view is its restart breadcrumb. Shard zero must not erase it on
    # behalf of a lane that has not purged its own retained rows and cursor.
    refute is_nil(
             TestCluster.rpc!(
               context.node_b,
               Group.Replica.Data,
               :remote_view_generation,
               [name, 1, context.node_a]
             )
           )

    assert {^owner, %{owner: :a}} =
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, key])

    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_lane, :kill])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 1)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(context.node_b, Group, :lookup, [name, key]) == nil and
          TestCluster.rpc!(context.node_b, Group, :members, [name, key]) == []
      end,
      timeout: 1_000,
      interval: 25
    )

    # Restart repair can remove invalid rows before the reconstructed lease
    # retires its final constant-size view breadcrumb.
    TestCluster.assert_eventually(
      fn ->
        is_nil(
          TestCluster.rpc!(
            context.node_b,
            Group.Replica.Data,
            :remote_view_generation,
            [name, 1, context.node_a]
          )
        )
      end,
      timeout: 1_000,
      interval: 25
    )

    assert :ok =
             TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_origin_purged, [
               name,
               context.node_a
             ])
  end

  test "nodedown on shard zero preserves a suspended sibling's restart breadcrumb", context do
    name = unique_name(:sibling_restart_during_nodedown)

    opts = [
      name: name,
      shards: 2,
      replicated_sender_buffer_size: 1,
      replicated_pg_receiver_buffer_size: 1,
      replicated_registry_receiver_buffer_size: 1,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 150
    ]

    start_group_on_peers(context.peers, opts)

    key =
      1..1_000
      |> Enum.map(&"sibling-nodedown/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    owner =
      TestCluster.spawn_register_and_join(
        context.node_a,
        name,
        key,
        %{owner: :a},
        key,
        %{owner: :a}
      )

    TestCluster.assert_eventually(fn ->
      match?({^owner, _}, TestCluster.rpc!(context.node_b, Group, :lookup, [name, key])) and
        match?([{^owner, _}], TestCluster.rpc!(context.node_b, Group, :members, [name, key]))
    end)

    old_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [old_lane])

    on_exit(fn ->
      TestCluster.reconnect_nodes(context.node_b, context.node_a)
    end)

    TestCluster.disconnect_nodes(context.node_b, context.node_a)

    TestCluster.assert_eventually(fn ->
      is_nil(
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
          name,
          context.node_a
        ])
      )
    end)

    # Shard zero owns node-wide authority, but it must not erase another lane's
    # durable view before that lane has purged its own rows and cursor.
    refute is_nil(
             TestCluster.rpc!(
               context.node_b,
               Group.Replica.Data,
               :remote_view_generation,
               [name, 1, context.node_a]
             )
           )

    assert {^owner, %{owner: :a}} =
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, key])

    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_lane, :kill])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 1)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(context.node_b, Group, :lookup, [name, key]) == nil and
        TestCluster.rpc!(context.node_b, Group, :members, [name, key]) == []
    end)

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :registry_claims, [
             name,
             1,
             nil,
             key
           ]) == []

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :pg_entries_for_origin, [
             name,
             1,
             nil,
             context.node_a
           ]) == []

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :replica_cursor_streams_for_origin,
             [name, 1, context.node_a]
           ) == []

    assert :ok =
             TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "shard zero restart retains the obligation to repair partially observed authority",
       context do
    name = unique_name(:authority_dirty_restart)
    cluster = "authority-dirty/restart"

    opts = [
      name: name,
      shards: 1,
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    exact_before =
      TestCluster.rpc!(
        context.node_b,
        Group.Replica.Data,
        :remote_cluster_epoch_exact_revision,
        [name, context.node_a]
      )

    :ok = TestCluster.rpc!(context.node_a, Group, :connect, [name, cluster])

    TestCluster.assert_eventually(fn ->
      observed =
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_cluster_epoch_observed_revision,
          [name, context.node_a]
        )

      exact =
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_cluster_epoch_exact_revision,
          [name, context.node_a]
        )

      is_integer(observed) and observed > exact_before and exact == exact_before
    end)

    source =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source])

    on_exit(fn ->
      _ = TestCluster.resume_shard_if_alive(context.node_a, name, 0)
    end)

    old_receiver =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_receiver, :kill])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 0)
           ]) do
        pid when is_pid(pid) -> pid != old_receiver
        _ -> false
      end
    end)

    receiver =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    state = TestCluster.rpc!(context.node_b, :sys, :get_state, [receiver])
    assert Map.has_key?(state.cluster_control_dirty, context.node_a)

    :ok = TestCluster.rpc!(context.node_a, :sys, :resume, [source])

    TestCluster.assert_eventually(
      fn ->
        exact =
          TestCluster.rpc!(
            context.node_b,
            Group.Replica.Data,
            :remote_cluster_epoch_exact_revision,
            [name, context.node_a]
          )

        observed =
          TestCluster.rpc!(
            context.node_b,
            Group.Replica.Data,
            :remote_cluster_epoch_observed_revision,
            [name, context.node_a]
          )

        exact == observed and exact > exact_before
      end,
      timeout: 5_000
    )
  end

  test "malformed replica frames are rejected without restarting the receiving shard", context do
    name = unique_name(:malformed_replica_frames)

    opts = [
      name: name,
      shards: 1,
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 5_000
    ]

    start_group_on_peers(context.peers, opts)

    receiver =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 0, nil])

    version = Group.Replica.WireProtocol.version()

    frames = [
      {:heads, version, [:not_a_head]},
      {:delta_batch, version, [:not_a_delta_run]},
      {:need, version, :not_a_stream, 1},
      {:needs, version, [:not_a_need]},
      {:snapshot_chunk, version, :not_a_stream, 1, 1, 1, 0, 0, [], []},
      {:delta_batch, version,
       [
         {stream_id, 1,
          [{1, [{:register, nil, "malformed/pid", :not_a_pid, %{}, 0, context.node_a}]}], 1}
       ]}
    ]

    for frame <- frames do
      assert :ok =
               TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
                 name,
                 context.node_a,
                 0,
                 frame
               ])

      Process.sleep(25)

      assert TestCluster.rpc!(context.node_b, Process, :alive?, [receiver]),
             "receiver restarted after malformed frame: #{inspect(frame)}"
    end

    assert receiver ==
             TestCluster.rpc!(context.node_b, Process, :whereis, [
               Group.Replica.shard_name(name, 0)
             ])

    assert nil ==
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, "malformed/pid"])

    assert :ok =
             TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "a delta cannot place registry or PG rows on the wrong shard", context do
    name = unique_name(:wrong_shard_delta)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_sender_buffer_size: 1,
      replicated_pg_receiver_buffer_size: 1,
      replicated_registry_receiver_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    wrong_key =
      1..1_000
      |> Enum.map(&"wrong-shard/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    source_key =
      1..1_000
      |> Enum.map(&"source-shard/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 0))

    :ok =
      TestCluster.rpc!(context.node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    owner = TestCluster.spawn_register(context.node_a, name, source_key, %{owner: :a})

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 0, nil])

    frame =
      {:delta_batch, Group.Replica.WireProtocol.version(),
       [
         {stream_id, 1,
          [
            {1,
             [
               {:register, nil, wrong_key, owner, %{owner: :a}, 1, context.node_a},
               {:join, nil, wrong_key, owner, %{owner: :a}, 1, :join, context.node_a}
             ]}
          ], 1}
       ]}

    assert :ok =
             TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
               name,
               context.node_a,
               0,
               frame
             ])

    TestCluster.flush_shards(context.node_b, name)

    assert nil == TestCluster.rpc!(context.node_b, Group, :lookup, [name, wrong_key])
    assert [] == TestCluster.rpc!(context.node_b, Group, :members, [name, wrong_key])

    assert 0 ==
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
               name,
               0,
               stream_id
             ])

    assert :ok =
             TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "full snapshot capture never runs on the replica shard", context do
    name = unique_name(:snapshot_capture_isolation)

    opts = [
      name: name,
      shards: 1,
      replica_transport: Group.TestReplicaTransport,
      replicated_sender_buffer_size: 1,
      replicated_oplog_max_entries: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    for index <- 1..4 do
      owner = TestCluster.spawn_register(context.node_a, name, "snapshot/isolation/#{index}", %{})
      true = TestCluster.rpc!(context.node_a, Process, :exit, [owner, :kill])
    end

    TestCluster.flush_shards(context.node_a, name)

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 0, nil])

    {floor, _head, _applied} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        0,
        stream_id
      ])

    assert floor > 1

    shard =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    tracer = TestCluster.spawn_trace_forwarder(context.node_a, self())

    assert 1 ==
             TestCluster.rpc!(context.node_a, :erlang, :trace_pattern, [
               {Group.Replica.Data, :reduce_registry_claim_batches_for_stream, 5},
               true,
               [:local]
             ])

    assert 1 ==
             TestCluster.rpc!(context.node_a, :erlang, :trace, [
               shard,
               true,
               [:call, :set_on_spawn, {:tracer, tracer}]
             ])

    on_exit(fn ->
      TestCluster.rpc!(context.node_a, :erlang, :trace, [shard, false, [:all]])

      TestCluster.rpc!(context.node_a, :erlang, :trace_pattern, [
        {Group.Replica.Data, :reduce_registry_claim_batches_for_stream, 5},
        false,
        [:local]
      ])
    end)

    assert :ok =
             TestCluster.rpc!(context.node_a, Group.Transport, :incoming, [
               name,
               context.node_b,
               0,
               {:needs, Group.Replica.WireProtocol.version(), [{stream_id, 1}]}
             ])

    assert_receive {:forwarded_trace,
                    {:trace, capture_pid, :call,
                     {Group.Replica.Data, :reduce_registry_claim_batches_for_stream,
                      [^name, 0, ^stream_id, _capture, _fun]}}},
                   1_000

    refute capture_pid == shard
    assert TestCluster.rpc!(context.node_a, Process, :alive?, [shard])
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

    source_lanes =
      for shard <- 0..2 do
        lane =
          TestCluster.rpc!(context.node_a, Process, :whereis, [
            Group.Replica.shard_name(name, shard)
          ])

        :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [lane])
        lane
      end

    on_exit(fn ->
      Enum.each(source_lanes, fn lane ->
        TestCluster.rpc!(context.node_a, TestCluster, :resume_if_alive, [lane])
      end)
    end)

    :ok =
      TestCluster.rpc!(context.node_b, TestCluster, :expire_replica_lane, [
        name,
        0,
        context.node_a
      ])

    Process.sleep(100)

    assert TestCluster.rpc!(context.node_b, Group.TestTCPTransport, :connected?, [
             name,
             context.node_a
           ])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
             name,
             context.node_a
           ]) ==
             TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    for live_shard <- [1, 2] do
      assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
               name,
               live_shard,
               context.node_a
             ]) != nil
    end

    status = TestCluster.rpc!(context.node_b, Group.TestTCPTransport, :status, [name])
    assert context.node_a in status.peers

    for shard <- [1, 2] do
      :ok =
        TestCluster.rpc!(context.node_b, TestCluster, :expire_replica_lane, [
          name,
          shard,
          context.node_a
        ])
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

  test "a restarted sideband manager rediscovers live peers without nodeup", context do
    name = unique_name(:sideband_manager_restart)

    opts = [
      name: name,
      shards: 2,
      replica_transport:
        {Group.TestTCPTransport,
         [connect_timeout: 250, send_timeout: 250, reconnect_interval: 10]},
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 1_000
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

    seed = TestCluster.spawn_register(context.node_a, name, "manager-restart/seed", %{})

    TestCluster.assert_eventually(fn ->
      match?(
        {^seed, %{}},
        TestCluster.rpc!(context.node_b, Group, :lookup, [name, "manager-restart/seed"])
      )
    end)

    manager_name = :"#{name}_replica_tcp_transport"
    old_manager = TestCluster.rpc!(context.node_b, Process, :whereis, [manager_name])
    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_manager, :kill])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [manager_name]) do
        manager when is_pid(manager) -> manager != old_manager
        _ -> false
      end
    end)

    # Group and every replica lane stayed alive; there is deliberately no
    # nodeup or authority-generation change to trigger rediscovery.
    fresh = TestCluster.spawn_register(context.node_a, name, "manager-restart/fresh", %{})

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(context.node_b, Group.TestTCPTransport, :connected?, [
          name,
          context.node_a
        ]) and
          match?(
            {^fresh, %{}},
            TestCluster.rpc!(context.node_b, Group, :lookup, [name, "manager-restart/fresh"])
          )
      end,
      timeout: 5_000,
      interval: 25
    )

    for node <- [context.node_a, context.node_b, context.node_c] do
      assert :ok = TestCluster.rpc!(node, TestCluster, :assert_replica_consistent, [name])
    end
  end

  test "three-origin conflict resolution cannot retire every owner", context do
    name = unique_name(:three_origin_conflict)
    key = "three-origin/conflict"

    opts = [
      name: name,
      shards: 1,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.CyclicConflictResolver, :resolve, []},
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    nodes = [context.node_a, context.node_b, context.node_c]

    for node <- nodes do
      :ok =
        TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [
          name,
          {:capture_drop, [:delta_batch]}
        ])
    end

    owner_a = TestCluster.spawn_register(context.node_a, name, key, %{rank: :a})
    owner_b = TestCluster.spawn_register(context.node_b, name, key, %{rank: :b})
    owner_c = TestCluster.spawn_register(context.node_c, name, key, %{rank: :c})
    TestCluster.flush_shards(context.node_a, name)
    TestCluster.flush_shards(context.node_b, name)
    TestCluster.flush_shards(context.node_c, name)

    # Deliver one edge of the cyclic preference to each owner. With the old
    # pairwise callback A loses to C, B loses to A, and C loses to B before any
    # node has observed all three claims.
    for {source, target} <- [
          {context.node_c, context.node_a},
          {context.node_a, context.node_b},
          {context.node_b, context.node_c}
        ] do
      {_target, 0, frame} =
        source
        |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [name])
        |> Enum.find(fn {captured_target, shard, frame} ->
          captured_target == target and shard == 0 and elem(frame, 0) == :delta_batch
        end)

      :ok = TestCluster.rpc!(target, Group.Transport, :incoming, [name, source, 0, frame])
    end

    for node <- nodes, do: TestCluster.flush_shards(node, name)

    for node <- nodes do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :pass])
    end

    TestCluster.assert_eventually(fn ->
      alive =
        Enum.filter([owner_a, owner_b, owner_c], fn owner ->
          TestCluster.rpc!(node(owner), Process, :alive?, [owner])
        end)

      case alive do
        [winner] ->
          Enum.all?(nodes, fn receiver ->
            match?({^winner, _}, TestCluster.rpc!(receiver, Group, :lookup, [name, key]))
          end)

        _ ->
          false
      end
    end)

    for node <- nodes do
      assert :ok = TestCluster.rpc!(node, TestCluster, :assert_replica_consistent, [name])
    end
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

  test "shared authority cannot admit a delta before its receiving lane is installed", context do
    name = unique_name(:lane_view_fence)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    key =
      1..1_000
      |> Enum.map(&"lane-view-fence/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    :ok =
      TestCluster.rpc!(context.node_a, Group.TestReplicaTransport, :set_mode, [
        name,
        {:capture_drop, [:delta_batch]}
      ])

    pid = TestCluster.spawn_register(context.node_a, name, key, %{lane: 1})
    TestCluster.flush_shards(context.node_a, name)

    {stream_id, frame} =
      TestCluster.rpc!(context.node_a, Group.TestReplicaTransport, :captured, [name])
      |> Enum.find_value(fn
        {target, 1, {:delta_batch, _version, [{stream_id, _first_seq, _records, _head}]} = frame}
        when target == context.node_b ->
          {stream_id, frame}

        _other ->
          nil
      end)

    generation =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
        name,
        context.node_a
      ])

    assert generation == Group.Replica.WireProtocol.stream_generation(stream_id)

    :ok =
      TestCluster.rpc!(context.node_b, TestCluster, :delete_remote_view_info, [
        name,
        1,
        context.node_a
      ])

    assert nil ==
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
               name,
               1,
               context.node_a
             ])

    cursor_before =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
        name,
        1,
        stream_id
      ])

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        1,
        frame
      ])

    TestCluster.flush_shards(context.node_b, name)

    assert cursor_before ==
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
               name,
               1,
               stream_id
             ])

    assert TestCluster.rpc!(context.node_b, Group, :lookup, [name, key]) == nil

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :registry_claims, [
             name,
             1,
             nil,
             key
           ]) == []

    # A matching lane hello installs the per-shard fence. The exact same
    # previously rejected frame is then admissible and advances contiguously.
    source_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    send(
      target_lane,
      {:replica_lane_hello, source_lane, Group.Replica.WireProtocol.version(), generation,
       revision, Group.TestReplicaTransport.id(), Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    assert generation ==
             TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
               name,
               1,
               context.node_a
             ])

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        1,
        frame
      ])

    TestCluster.flush_shards(context.node_b, name)

    assert match?(
             {^pid, %{lane: 1}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, key])
           )

    assert :ok =
             TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "a missing local authority owner does not crash a healthy replica lane", context do
    name = unique_name(:missing_local_authority_owner)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    source_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    replica_supervisor =
      TestCluster.rpc!(context.node_b, Process, :whereis, [:"#{name}_replica_sup"])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [replica_supervisor])

    on_exit(fn ->
      _ = TestCluster.rpc!(context.node_b, TestCluster, :resume_if_alive, [replica_supervisor])
    end)

    monitor = Process.monitor(target_control)
    true = TestCluster.rpc!(context.node_b, Process, :exit, [target_control, :kill])
    assert_receive {:DOWN, ^monitor, :process, ^target_control, :killed}, 5_000

    assert TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 0)
           ]) == nil

    {generation, revision, [{nil, base_epoch}]} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_authority, [name])

    send(
      target_lane,
      {:replica_cluster_open, source_lane, generation, revision, [{nil, base_epoch}]}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])
    assert TestCluster.rpc!(context.node_b, Process, :alive?, [target_lane])

    :ok = TestCluster.rpc!(context.node_b, :sys, :resume, [replica_supervisor])
  end

  test "incremental authority from different lanes cannot race across a revision gap", context do
    name = unique_name(:cross_lane_authority_gap)
    cluster = "cross-lane-authority-gap"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)
    :ok = TestCluster.rpc!(context.node_b, Group, :connect, [name, cluster])

    [{^cluster, old_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        [cluster]
      ])

    old_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    [{^cluster, ^old_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :deactivate_local_clusters, [
        name,
        [cluster]
      ])

    [{^cluster, current_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        [cluster]
      ])

    generation = TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    current_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    assert current_revision > old_revision

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    source_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    data_owner =
      TestCluster.rpc!(context.node_b, Process, :whereis, [
        Group.Replica.Data.data_name(name)
      ])

    :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source_control])
    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [data_owner])

    on_exit(fn ->
      _ =
        TestCluster.rpc!(context.node_a, TestCluster, :resume_if_alive, [source_control])

      _ = TestCluster.rpc!(context.node_b, TestCluster, :resume_if_alive, [data_owner])
    end)

    # Shard 0 sees revision 3 first, detects that revisions 1 and 2 are absent,
    # and blocks while recording the gap in the shared Data owner.
    send(
      target_control,
      {:replica_cluster_open, source_control, generation, current_revision,
       [{cluster, current_epoch}]}
    )

    TestCluster.assert_eventually(fn ->
      {:messages, messages} =
        TestCluster.rpc!(context.node_b, Process, :info, [data_owner, :messages])

      Enum.any?(messages, fn
        {:"$gen_call", _from, {:observe_remote_cluster_epoch_revision, source, revision}} ->
          source == context.node_a and revision == current_revision

        _ ->
          false
      end)
    end)

    # Before the shared gap fence is visible, another lane inspects revision 0
    # and queues an older revision-1 update behind it. A lane must not be able
    # to install that stale epoch after shard 0 records the newer gap.
    send(
      target_lane,
      {:replica_cluster_open, source_lane, generation, old_revision, [{cluster, old_epoch}]}
    )

    TestCluster.assert_eventually(fn ->
      {:messages, messages} =
        TestCluster.rpc!(context.node_b, Process, :info, [target_control, :messages])

      Enum.any?(messages, fn
        {:replica_cluster_open, ^source_lane, ^generation, ^old_revision,
         [{^cluster, ^old_epoch}]} ->
          true

        _ ->
          false
      end)
    end)

    :ok = TestCluster.rpc!(context.node_b, :sys, :resume, [data_owner])
    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])
    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
             name,
             context.node_a,
             cluster
           ]) == nil

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_cluster_epoch_observed_revision,
             [name, context.node_a]
           ) == current_revision

    refute TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_view_observed_revision,
             [name, 1, context.node_a]
           ) == current_revision

    {^generation, ^current_revision, authority} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_authority, [name])

    send(
      target_control,
      {:replica_hello, source_control, Group.Replica.WireProtocol.version(), generation,
       current_revision, authority, Group.TestReplicaTransport.id(),
       Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])
    :ok = TestCluster.rpc!(context.node_a, :sys, :resume, [source_control])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
             name,
             context.node_a,
             cluster
           ]) == current_epoch

    assert :ok = TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "authority replacement fences an in-flight stale registry conflict", context do
    name = unique_name(:inflight_stale_conflict)
    cluster = "inflight-stale-conflict"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.PausingConflictResolver, :resolve, [self()]},
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    TestCluster.assert_eventually(fn ->
      generation =
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
          name,
          context.node_a
        ])

      observed =
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_cluster_epoch_observed_revision,
          [name, context.node_a]
        )

      not is_nil(generation) and
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
          name,
          1,
          context.node_a
        ]) == generation and
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_view_observed_revision,
          [name, 1, context.node_a]
        ) == observed
    end)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group, :connect, [name, cluster])
    end

    TestCluster.assert_eventually(fn ->
      not is_nil(
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
          name,
          context.node_a,
          cluster
        ])
      )
    end)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    key =
      1..1_000
      |> Enum.map(&"inflight-stale-conflict/key/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(cluster, &1, 2) == 1))

    local_owner =
      TestCluster.spawn_register_in_cluster(
        context.node_b,
        name,
        key,
        %{rank: 1},
        cluster
      )

    remote_owner =
      TestCluster.spawn_register_in_cluster(
        context.node_a,
        name,
        key,
        %{rank: 2, pause: true},
        cluster
      )

    generation = TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    epoch =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch, [
        name,
        cluster
      ])

    stream_id =
      Group.Replica.WireProtocol.stream_id(
        name,
        context.node_a,
        generation,
        1,
        cluster,
        epoch
      )

    [{^key, ^remote_owner, remote_meta, remote_time}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims_for_stream, [
        name,
        1,
        stream_id
      ])

    {_floor, head, applied} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        1,
        stream_id
      ])

    assert head == applied

    record =
      {head,
       [
         {:register, cluster, key, remote_owner, remote_meta, remote_time, context.node_a}
       ]}

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        1,
        {:delta_batch, Group.Replica.WireProtocol.version(), [{stream_id, head, [record], head}]}
      ])

    assert_receive {:conflict_resolver_waiting, resolver, ref, ^key, ^remote_owner}, 5_000

    on_exit(fn -> send(resolver, {:continue_conflict_resolution, ref}) end)

    [{^cluster, ^epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :deactivate_local_clusters, [
        name,
        [cluster]
      ])

    close_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    send(
      target_control,
      {:replica_cluster_close, source_control, generation, close_revision, [{cluster, epoch}]}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
             name,
             context.node_a,
             cluster
           ]) == nil

    send(resolver, {:continue_conflict_resolution, ref})
    TestCluster.flush_shards(context.node_b, name)

    assert TestCluster.rpc!(context.node_b, Process, :alive?, [local_owner])

    assert match?(
             {^local_owner, %{rank: 1}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [
               name,
               key,
               [cluster: cluster]
             ])
           )

    assert :ok = TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "restoring lane authority reprojects a conflict retained during the authority gap",
       context do
    name = unique_name(:authority_restore_reprojects_conflict)
    authority_bump_cluster = "authority-restore-bump"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.PausingConflictResolver, :resolve, [self()]},
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    TestCluster.assert_eventually(fn ->
      generation =
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
          name,
          context.node_a
        ])

      observed =
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_cluster_epoch_observed_revision,
          [name, context.node_a]
        )

      source_generation =
        TestCluster.rpc!(context.node_a, Group.Replica.Data, :remote_generation, [
          name,
          context.node_b
        ])

      not is_nil(generation) and not is_nil(source_generation) and
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
          name,
          1,
          context.node_a
        ]) == generation and
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_view_observed_revision,
          [name, 1, context.node_a]
        ) == observed
    end)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    key =
      1..1_000
      |> Enum.map(&"authority-restore-conflict/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    local_owner = TestCluster.spawn_register(context.node_b, name, key, %{rank: 1})

    remote_owner =
      TestCluster.spawn_register(context.node_a, name, key, %{rank: 2, pause: true})

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 1, nil])

    [{^key, ^remote_owner, remote_meta, remote_time}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims_for_stream, [
        name,
        1,
        stream_id
      ])

    {_floor, head, applied} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        1,
        stream_id
      ])

    assert head == applied

    record =
      {head,
       [
         {:register, nil, key, remote_owner, remote_meta, remote_time, context.node_a}
       ]}

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        1,
        {:delta_batch, Group.Replica.WireProtocol.version(), [{stream_id, head, [record], head}]}
      ])

    assert_receive {:conflict_resolver_waiting, resolver, ref, ^key, ^remote_owner}, 5_000
    on_exit(fn -> send(resolver, {:continue_conflict_resolution, ref}) end)

    :ok = TestCluster.rpc!(context.node_a, Group, :connect, [name, authority_bump_cluster])

    generation = TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    bumped_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    bump_epoch =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch, [
        name,
        authority_bump_cluster
      ])

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    send(
      target_control,
      {:replica_cluster_open, source_control, generation, bumped_revision,
       [{authority_bump_cluster, bump_epoch}]}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_cluster_epoch_observed_revision,
             [name, context.node_a]
           ) == bumped_revision

    refute TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_view_observed_revision,
             [name, 1, context.node_a]
           ) == bumped_revision

    send(resolver, {:continue_conflict_resolution, ref})

    assert_receive {:conflict_resolver_waiting, ^resolver, repair_ref, ^key, ^remote_owner}, 5_000
    send(resolver, {:continue_conflict_resolution, repair_ref})

    TestCluster.flush_shards(context.node_b, name)

    assert match?(
             {^remote_owner, %{rank: 2, pause: true}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, key])
           )

    refute TestCluster.rpc!(context.node_b, Process, :alive?, [local_owner])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
             name,
             1,
             stream_id
           ]) == head

    consistency =
      Task.async(fn ->
        TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
      end)

    assert_receive {:conflict_resolver_waiting, consistency_resolver, consistency_ref, ^key,
                    ^remote_owner},
                   5_000

    send(consistency_resolver, {:continue_conflict_resolution, consistency_ref})
    assert :ok = Task.await(consistency, 5_000)
  end

  test "lease expiry discards conflict reprojection state when the source never returns",
       context do
    name = unique_name(:authority_gap_expiry)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.PausingConflictResolver, :resolve, [self()]},
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 500
    ]

    start_group_on_peers(context.peers, opts)

    TestCluster.assert_eventually(fn ->
      not is_nil(
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
          name,
          context.node_a
        ])
      )
    end)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source_control])

    on_exit(fn ->
      TestCluster.rpc!(context.node_a, TestCluster, :resume_if_alive, [source_control])
    end)

    key =
      1..1_000
      |> Enum.map(&"authority-gap-expiry/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    local_owner = TestCluster.spawn_register(context.node_b, name, key, %{rank: 1})
    remote_owner = TestCluster.spawn_register(context.node_a, name, key, %{rank: 2, pause: true})

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 1, nil])

    [{^key, ^remote_owner, remote_meta, remote_time}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims_for_stream, [
        name,
        1,
        stream_id
      ])

    {_floor, head, applied} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        1,
        stream_id
      ])

    assert head == applied

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        1,
        {:delta_batch, Group.Replica.WireProtocol.version(),
         [
           {stream_id, head,
            [
              {head,
               [
                 {:register, nil, key, remote_owner, remote_meta, remote_time, context.node_a}
               ]}
            ], head}
         ]}
      ])

    assert_receive {:conflict_resolver_waiting, resolver, ref, ^key, ^remote_owner}, 5_000
    on_exit(fn -> send(resolver, {:continue_conflict_resolution, ref}) end)

    initial_revision =
      TestCluster.rpc!(
        context.node_b,
        Group.Replica.Data,
        :remote_cluster_epoch_observed_revision,
        [
          name,
          context.node_a
        ]
      )

    [{"authority-gap-expiry-1", _epoch1}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        ["authority-gap-expiry-1"]
      ])

    [{"authority-gap-expiry-2", epoch2}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        ["authority-gap-expiry-2"]
      ])

    gap_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    assert gap_revision >= initial_revision + 2

    generation = TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    send(
      target_control,
      {:replica_cluster_open, source_control, generation, gap_revision,
       [{"authority-gap-expiry-2", epoch2}]}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_cluster_epoch_observed_revision,
             [name, context.node_a]
           ) == gap_revision

    send(resolver, {:continue_conflict_resolution, ref})

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    TestCluster.assert_eventually(fn ->
      state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

      state.pending_registry_reprojections
      |> Map.get(context.node_a, MapSet.new())
      |> MapSet.member?({nil, key})
    end)

    TestCluster.assert_eventually(
      fn ->
        state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

        not Map.has_key?(state.pending_registry_reprojections, context.node_a) and
          TestCluster.rpc!(context.node_b, Group.Replica.Data, :registry_claims, [
            name,
            1,
            nil,
            key
          ])
          |> Enum.all?(&(elem(&1, 3) != context.node_a)) and
          TestCluster.rpc!(
            context.node_b,
            Group.Replica.Data,
            :replica_cursor_streams_for_origin,
            [name, 1, context.node_a]
          ) == []
      end,
      timeout: 10_000
    )

    assert TestCluster.rpc!(context.node_b, Process, :alive?, [local_owner])

    assert match?(
             {^local_owner, %{rank: 1}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, key])
           )
  end

  test "a newer heartbeat fences old-epoch data before exact authority arrives", context do
    name = unique_name(:heartbeat_authority_fence)
    cluster = "heartbeat-authority-fence"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.ModelConflictResolver, :resolve, []},
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group, :connect, [name, cluster])
    end

    TestCluster.assert_eventually(fn ->
      not is_nil(
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
          name,
          context.node_a,
          cluster
        ])
      )
    end)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    key =
      1..1_000
      |> Enum.map(&"heartbeat-authority-fence/key/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(cluster, &1, 2) == 1))

    local_owner =
      TestCluster.spawn_register_in_cluster(context.node_b, name, key, %{rank: 1}, cluster)

    stale_owner =
      TestCluster.spawn_register_in_cluster(context.node_a, name, key, %{rank: 2}, cluster)

    generation = TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    old_epoch =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch, [
        name,
        cluster
      ])

    old_stream =
      Group.Replica.WireProtocol.stream_id(
        name,
        context.node_a,
        generation,
        1,
        cluster,
        old_epoch
      )

    [{^key, ^stale_owner, stale_meta, stale_time}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims_for_stream, [
        name,
        1,
        old_stream
      ])

    {_floor, old_head, old_head} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        1,
        old_stream
      ])

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    source_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source_control])

    on_exit(fn ->
      TestCluster.rpc!(context.node_a, TestCluster, :resume_if_alive, [source_control])
    end)

    # Change only the source's durable authority. The incremental close/open
    # controls are intentionally absent, while the matching data lane exposes
    # the newer revision through its normal heartbeat.
    [{^cluster, ^old_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :deactivate_local_clusters, [
        name,
        [cluster]
      ])

    [{^cluster, new_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        [cluster]
      ])

    refute new_epoch == old_epoch

    new_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    send(
      target_lane,
      {:replica_heartbeat, source_lane, Group.Replica.WireProtocol.version(), generation,
       new_revision, Group.TestReplicaTransport.id(),
       Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_cluster_epoch_observed_revision,
             [name, context.node_a]
           ) == new_revision

    old_record =
      {old_head,
       [
         {:register, cluster, key, stale_owner, stale_meta, stale_time, context.node_a}
       ]}

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        1,
        {:delta_batch, Group.Replica.WireProtocol.version(),
         [{old_stream, old_head, [old_record], old_head}]}
      ])

    TestCluster.flush_shards(context.node_b, name)

    assert TestCluster.rpc!(context.node_b, Process, :alive?, [local_owner])

    assert match?(
             {^local_owner, %{rank: 1}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [
               name,
               key,
               [cluster: cluster]
             ])
           )

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
             name,
             1,
             old_stream
           ]) == 0
  end

  test "a newer-generation heartbeat fences prior-generation data before exact authority",
       context do
    name = unique_name(:heartbeat_generation_fence)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.ModelConflictResolver, :resolve, []},
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    key =
      1..1_000
      |> Enum.map(&"heartbeat-generation-fence/key/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    local_owner = TestCluster.spawn_register(context.node_b, name, key, %{rank: 1})
    stale_owner = TestCluster.spawn_register(context.node_a, name, key, %{rank: 2})
    stale_cluster = "heartbeat-generation-fence/stale-cluster"

    [{^stale_cluster, stale_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        [stale_cluster]
      ])

    old_stream =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 1, nil])

    old_generation = Group.Replica.WireProtocol.stream_generation(old_stream)

    {^old_generation, old_revision, old_authority} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_authority, [name])

    [{^key, ^stale_owner, stale_meta, stale_time}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :registry_claims_for_stream, [
        name,
        1,
        old_stream
      ])

    {_floor, old_head, old_head} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        1,
        old_stream
      ])

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    source_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source_control])

    on_exit(fn ->
      TestCluster.rpc!(context.node_a, TestCluster, :resume_if_alive, [source_control])
    end)

    new_generation =
      TestCluster.rpc!(context.node_a, Group.Replica.WireProtocol, :new_generation, [])

    assert Group.Replica.WireProtocol.generation_newer?(new_generation, old_generation)

    send(
      target_lane,
      {:replica_heartbeat, source_lane, Group.Replica.WireProtocol.version(), new_generation, 0,
       Group.TestReplicaTransport.id(), Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    refute TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_view_cluster_epoch_revision,
             [
               name,
               1,
               context.node_a
             ]
           ) ==
             TestCluster.rpc!(
               context.node_b,
               Group.Replica.Data,
               :remote_cluster_epoch_exact_revision,
               [name, context.node_a]
             )

    send(
      target_control,
      {:replica_cluster_open, source_control, old_generation, old_revision,
       [{stale_cluster, stale_epoch}]}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
             name,
             context.node_a,
             stale_cluster
           ]) == nil

    assert :stale =
             TestCluster.rpc!(
               context.node_b,
               Group.Replica.Data,
               :put_remote_replica_info,
               [
                 name,
                 0,
                 context.node_a,
                 old_generation,
                 old_revision,
                 old_authority
               ]
             )

    assert :stale =
             TestCluster.rpc!(
               context.node_b,
               Group.Replica.Data,
               :put_remote_view_info,
               [name, 0, context.node_a, old_generation, old_revision, old_revision]
             )

    # The newer generation is only a hint until its exact authority arrives,
    # but it must still fence a delayed exact hello from the prior generation.
    send(
      target_control,
      {:replica_hello, source_control, Group.Replica.WireProtocol.version(), old_generation,
       old_revision, old_authority, Group.TestReplicaTransport.id(),
       Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])

    refute TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_view_cluster_epoch_revision,
             [name, 0, context.node_a]
           ) ==
             TestCluster.rpc!(
               context.node_b,
               Group.Replica.Data,
               :remote_cluster_epoch_exact_revision,
               [name, context.node_a]
             )

    old_record =
      {old_head,
       [
         {:register, nil, key, stale_owner, stale_meta, stale_time, context.node_a}
       ]}

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        1,
        {:delta_batch, Group.Replica.WireProtocol.version(),
         [{old_stream, old_head, [old_record], old_head}]}
      ])

    TestCluster.flush_shards(context.node_b, name)

    assert TestCluster.rpc!(context.node_b, Process, :alive?, [local_owner])

    assert match?(
             {^local_owner, %{rank: 1}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, key])
           )

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
             name,
             1,
             old_stream
           ]) == 0
  end

  test "a newer lane hello cannot reinstall a view from stale exact authority", context do
    name = unique_name(:lane_hello_exact_fence)
    bump_cluster = "lane-hello-exact-fence/bump"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    key =
      1..1_000
      |> Enum.map(&"lane-hello-exact-fence/member/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    owner = TestCluster.spawn_join(context.node_a, name, key, %{owner: :a})

    stream =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 1, nil])

    generation = Group.Replica.WireProtocol.stream_generation(stream)

    {_floor, head, head} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        1,
        stream
      ])

    [{^head, mutations}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_records, [
        name,
        1,
        stream,
        head,
        1
      ])

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    source_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source_control])

    on_exit(fn ->
      TestCluster.rpc!(context.node_a, TestCluster, :resume_if_alive, [source_control])
    end)

    [{^bump_cluster, _epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        [bump_cluster]
      ])

    revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    send(
      target_lane,
      {:replica_lane_hello, source_lane, Group.Replica.WireProtocol.version(), generation,
       revision, Group.TestReplicaTransport.id(), Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    refute TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_view_observed_revision,
             [name, 1, context.node_a]
           ) == revision

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        1,
        {:delta_batch, Group.Replica.WireProtocol.version(),
         [{stream, head, [{head, mutations}], head}]}
      ])

    TestCluster.flush_shards(context.node_b, name)

    assert TestCluster.rpc!(context.node_b, Group, :members, [name, key]) == []
    assert TestCluster.rpc!(context.node_a, Process, :alive?, [owner])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
             name,
             1,
             stream
           ]) == 0
  end

  test "a delayed incremental control cannot roll observed authority backward", context do
    name = unique_name(:incremental_authority_rollback)
    cluster = "incremental-authority-rollback"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.ModelConflictResolver, :resolve, []},
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    :ok = TestCluster.rpc!(context.node_b, Group, :connect, [name, cluster])

    [{^cluster, old_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        [cluster]
      ])

    old_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    [{^cluster, ^old_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :deactivate_local_clusters, [
        name,
        [cluster]
      ])

    [{^cluster, current_epoch}] =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :activate_local_clusters, [
        name,
        [cluster]
      ])

    refute current_epoch == old_epoch

    :ok =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :add_cluster_node, [
        name,
        [cluster],
        context.node_a
      ])

    generation = TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    current_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    assert current_revision > old_revision

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    source_data_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target_data_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    # Keep the exact-authority request caused by the intentional revision gap
    # queued at its source until this test has exercised the delayed control and
    # stale data frame. The Group control plane intentionally bypasses the test
    # replica transport, so transport :drop alone cannot create this window.
    :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source_control])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
             name,
             context.node_a,
             cluster
           ]) == nil

    initial_observed_revision =
      TestCluster.rpc!(
        context.node_b,
        Group.Replica.Data,
        :remote_cluster_epoch_observed_revision,
        [name, context.node_a]
      )

    assert is_integer(initial_observed_revision)
    assert initial_observed_revision < old_revision

    send(
      target_data_lane,
      {:replica_cluster_open, source_data_lane, generation, current_revision,
       [{cluster, current_epoch}]}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_data_lane])
    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
             name,
             context.node_a,
             cluster
           ]) == nil

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_cluster_epoch_observed_revision,
             [name, context.node_a]
           ) == current_revision

    # The old open was delayed behind the newer open. Accepting it can make an
    # obsolete stream authoritative long enough to retire a valid local owner;
    # the later exact hello can purge the stale claim but cannot resurrect that
    # killed owner.
    send(
      target_control,
      {:replica_cluster_open, source_control, generation, old_revision, [{cluster, old_epoch}]}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
             name,
             context.node_a,
             cluster
           ]) == nil

    key =
      1..1_000
      |> Enum.map(&"incremental-authority-rollback/key/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(cluster, &1, 2) == 0))

    local_owner =
      TestCluster.spawn_register_in_cluster(
        context.node_b,
        name,
        key,
        %{rank: 1},
        cluster
      )

    stale_owner = TestCluster.spawn_monitor_forwarder(context.node_a, name, :all, self())
    assert_receive {:monitor_ready, ^stale_owner}, 5_000

    stale_stream =
      Group.Replica.WireProtocol.stream_id(
        name,
        context.node_a,
        generation,
        0,
        cluster,
        old_epoch
      )

    stale_record =
      {1,
       [
         {:register, cluster, key, stale_owner, %{rank: 2}, System.system_time(), context.node_a}
       ]}

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        0,
        {:delta_batch, Group.Replica.WireProtocol.version(),
         [{stale_stream, 1, [stale_record], 1}]}
      ])

    TestCluster.flush_shards(context.node_b, name)

    {^generation, ^current_revision, current_authority} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_authority, [name])

    send(
      target_control,
      {:replica_hello, source_control, Group.Replica.WireProtocol.version(), generation,
       current_revision, current_authority, Group.TestReplicaTransport.id(),
       Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])

    :ok = TestCluster.rpc!(context.node_a, :sys, :resume, [source_control])

    assert TestCluster.rpc!(context.node_b, Process, :alive?, [local_owner])

    assert match?(
             {^local_owner, %{rank: 1}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [
               name,
               key,
               [cluster: cluster]
             ])
           )

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
             name,
             context.node_a,
             cluster
           ]) == current_epoch

    assert :ok = TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "a receiver restart forgets cursors for a locally deactivated cluster", context do
    name = unique_name(:inactive_cluster_cursor_restart)
    cluster = "inactive-cluster-cursor-restart"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group, :connect, [name, cluster])
    end

    TestCluster.assert_eventually(
      fn ->
        Enum.all?([context.node_a, context.node_b, context.node_c], fn node ->
          length(TestCluster.rpc!(node, Group, :nodes, [name, cluster])) == 3
        end)
      end,
      timeout: 30_000,
      interval: 50
    )

    key =
      1..1_000
      |> Enum.map(&"inactive-cluster-cursor/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(cluster, &1, 2) == 1))

    registry_owner =
      TestCluster.spawn_register_in_cluster(
        context.node_a,
        name,
        key,
        %{kind: :registry},
        cluster
      )

    pg_owner =
      TestCluster.spawn_join_in_cluster(
        context.node_a,
        name,
        key,
        %{kind: :pg},
        cluster
      )

    TestCluster.assert_eventually(fn ->
      match?(
        {^registry_owner, %{kind: :registry}},
        TestCluster.rpc!(context.node_b, Group, :lookup, [name, key, [cluster: cluster]])
      ) and
        match?(
          [{^pg_owner, %{kind: :pg}}],
          TestCluster.rpc!(context.node_b, Group, :members, [name, key, [cluster: cluster]])
        )
    end)

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [
        name,
        1,
        cluster
      ])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
             name,
             1,
             stream_id
           ]) > 0

    # This is the durable first half of Group.disconnect. Kill the lane before
    # its normal disconnect request can erase receive cursors and rows.
    [{^cluster, _local_epoch}] =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :deactivate_local_clusters, [
        name,
        [cluster]
      ])

    old_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    monitor = Process.monitor(old_lane)
    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_lane, :kill])
    assert_receive {:DOWN, ^monitor, :process, ^old_lane, :killed}, 5_000

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 1)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    new_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    # A registered GenServer name only proves the replacement process exists;
    # init still has to finish its retained-ETS repair before direct ETS reads
    # can assert the post-restart state.
    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [new_lane])

    assert TestCluster.rpc!(context.node_b, :ets, :lookup, [
             Group.Replica.Data.replica_cursor_table(name, 1),
             stream_id
           ]) == []

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :registry_lookup, [
             name,
             1,
             cluster,
             key
           ]) == nil

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :pg_lookup, [
             name,
             1,
             cluster,
             key,
             pg_owner
           ]) == nil

    assert :ok = TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "a receiver restart removes cursorless rows whose named-stream authority closed",
       context do
    name = unique_name(:cursorless_closed_stream_restart)
    cluster = "cursorless-restart"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 5_000
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group, :connect, [name, cluster])
    end

    TestCluster.assert_eventually(
      fn ->
        Enum.all?([context.node_a, context.node_b, context.node_c], fn node ->
          length(TestCluster.rpc!(node, Group, :nodes, [name, cluster])) == 3
        end)
      end,
      timeout: 30_000,
      interval: 50
    )

    key =
      1..1_000
      |> Enum.map(&"cursorless-restart/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(cluster, &1, 2) == 1))

    owner =
      TestCluster.spawn_register_in_cluster(
        context.node_a,
        name,
        key,
        %{kind: :registry},
        cluster
      )

    member =
      TestCluster.spawn_join_in_cluster(
        context.node_a,
        name,
        key,
        %{kind: :pg},
        cluster
      )

    for receiver <- [context.node_b, context.node_c] do
      TestCluster.assert_eventually(fn ->
        match?(
          {^owner, %{kind: :registry}},
          TestCluster.rpc!(receiver, Group, :lookup, [name, key, [cluster: cluster]])
        ) and
          match?(
            [{^member, %{kind: :pg}}],
            TestCluster.rpc!(receiver, Group, :members, [name, key, [cluster: cluster]])
          )
      end)
    end

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [
        name,
        1,
        cluster
      ])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor, [
             name,
             1,
             stream_id
           ]) > 0

    # Model either receiver crash window: rows were materialized before the
    # cursor was recorded, or authority cleanup deleted the cursor before all
    # rows. Restart repair must never depend solely on that missing breadcrumb.
    :ok =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :delete_replica_cursor, [
        name,
        1,
        stream_id
      ])

    replica_supervisor =
      TestCluster.rpc!(context.node_b, Process, :whereis, [:"#{name}_replica_sup"])

    old_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [replica_supervisor])

    on_exit(fn ->
      TestCluster.rpc!(context.node_b, TestCluster, :resume_if_alive, [replica_supervisor])
    end)

    lane_monitor = Process.monitor(old_lane)
    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_lane, :kill])
    assert_receive {:DOWN, ^lane_monitor, :process, ^old_lane, :killed}, 5_000

    :ok = TestCluster.rpc!(context.node_a, Group, :disconnect, [name, cluster])

    exact_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_cluster_epoch_exact_revision,
          [name, context.node_a]
        ) == exact_revision and
          TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
            name,
            context.node_a,
            cluster
          ]) == nil
      end,
      timeout: 5_000,
      interval: 25
    )

    :ok = TestCluster.rpc!(context.node_b, :sys, :resume, [replica_supervisor])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 1)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(context.node_b, Group, :lookup, [name, key, [cluster: cluster]]) == nil and
          TestCluster.rpc!(context.node_b, Group, :members, [name, key, [cluster: cluster]]) == []
      end,
      timeout: 2_000,
      interval: 25
    )

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :registry_claims, [
             name,
             1,
             cluster,
             key
           ]) == []

    assert :ok =
             TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "a receiver restart rejects cursorless rows under otherwise current authority", context do
    name = unique_name(:cursorless_current_authority_restart)

    opts = [
      name: name,
      shards: 1,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    key = "cursorless-current-authority/key"
    registry_owner = TestCluster.spawn_register(context.node_a, name, key, %{kind: :registry})
    pg_owner = TestCluster.spawn_join(context.node_a, name, key, %{kind: :pg})

    for receiver <- [context.node_b, context.node_c] do
      TestCluster.assert_eventually(fn ->
        match?(
          {^registry_owner, %{kind: :registry}},
          TestCluster.rpc!(receiver, Group, :lookup, [name, key])
        ) and
          match?(
            [{^pg_owner, %{kind: :pg}}],
            TestCluster.rpc!(receiver, Group, :members, [name, key])
          )
      end)
    end

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 0, nil])

    :ok =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :delete_replica_cursor, [
        name,
        0,
        stream_id
      ])

    old_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    monitor = Process.monitor(old_lane)
    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_lane, :kill])
    assert_receive {:DOWN, ^monitor, :process, ^old_lane, :killed}, 5_000

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 0)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    new_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [new_lane])

    assert TestCluster.rpc!(context.node_b, Group, :lookup, [name, key]) == nil
    assert TestCluster.rpc!(context.node_b, Group, :members, [name, key]) == []

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :registry_claims, [
             name,
             0,
             nil,
             key
           ]) == []

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :pass])
    end

    {floor, head, _applied} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        0,
        stream_id
      ])

    :ok =
      TestCluster.rpc!(context.node_b, Group.Transport, :incoming, [
        name,
        context.node_a,
        0,
        {:heads, Group.Replica.WireProtocol.version(), [{stream_id, floor, head}]}
      ])

    TestCluster.assert_eventually(
      fn ->
        match?(
          {^registry_owner, %{kind: :registry}},
          TestCluster.rpc!(context.node_b, Group, :lookup, [name, key])
        ) and
          match?(
            [{^pg_owner, %{kind: :pg}}],
            TestCluster.rpc!(context.node_b, Group, :members, [name, key])
          )
      end,
      timeout: 10_000,
      interval: 25
    )

    assert :ok = TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "a receiver restart never projects a partially installed exact snapshot", context do
    name = unique_name(:partial_snapshot_install_restart)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_oplog_max_entries: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    registry_key =
      1..1_000
      |> Enum.map(&"partial-install/registry/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    pg_key =
      1..1_000
      |> Enum.map(&"partial-install/pg/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(nil, &1, 2) == 1))

    registry_owner =
      TestCluster.spawn_register(context.node_a, name, registry_key, %{kind: :registry})

    pg_owner = TestCluster.spawn_join(context.node_a, name, pg_key, %{kind: :pg})
    TestCluster.flush_shards(context.node_a, name)

    stream_id =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [name, 1, nil])

    {floor, head, applied} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :replica_stream_head, [
        name,
        1,
        stream_id
      ])

    assert floor > 1
    assert applied == head

    # Model the exact crash boundary in both row domains: the receiver has
    # written only a subset of the exact image after recording its sequence-0
    # admission marker, but dies before publishing the snapshot cursor. These
    # are the same public ETS writes used by the commit path.
    :ok =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :begin_replica_snapshot_install, [
        name,
        1,
        stream_id,
        head
      ])

    :ok =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :put_registry_claim, [
        name,
        1,
        stream_id,
        head,
        registry_key,
        registry_owner,
        %{kind: :registry},
        System.system_time()
      ])

    :ok =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :pg_insert, [
        name,
        1,
        nil,
        pg_key,
        pg_owner,
        %{kind: :pg},
        System.system_time(),
        context.node_a
      ])

    replica_supervisor =
      TestCluster.rpc!(context.node_b, Process, :whereis, [:"#{name}_replica_sup"])

    old_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [replica_supervisor])

    on_exit(fn ->
      TestCluster.rpc!(context.node_b, TestCluster, :resume_if_alive, [replica_supervisor])
    end)

    monitor = Process.monitor(old_lane)
    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_lane, :kill])
    assert_receive {:DOWN, ^monitor, :process, ^old_lane, :killed}, 5_000

    :ok = TestCluster.rpc!(context.node_b, :sys, :resume, [replica_supervisor])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 1)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    assert TestCluster.rpc!(context.node_b, Group, :lookup, [name, registry_key]) == nil
    assert TestCluster.rpc!(context.node_b, Group, :members, [name, pg_key]) == []

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :registry_claims, [
             name,
             1,
             nil,
             registry_key
           ]) == []

    cursor_table =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :replica_cursor_table, [name, 1])

    refute TestCluster.rpc!(context.node_b, :ets, :member, [cursor_table, stream_id])
    assert :ok = TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "a delayed prior-generation hello cannot roll authority backward", context do
    name = unique_name(:generation_rollback)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    {old_generation, old_revision, old_epochs} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_authority, [name])

    old_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    supervisor = TestCluster.rpc!(context.node_a, Process, :whereis, [:"#{name}_group_sup"])
    :ok = TestCluster.rpc!(context.node_a, Supervisor, :stop, [supervisor, :normal, 5_000])
    {:ok, _pid} = TestCluster.start_group(context.node_a, opts)

    new_generation =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    refute new_generation == old_generation

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
        name,
        context.node_a
      ]) == new_generation
    end)

    target =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    send(
      target,
      {:replica_hello, old_control, Group.Replica.WireProtocol.version(), old_generation,
       old_revision, old_epochs, Group.TestReplicaTransport.id(),
       Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target])

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
             name,
             context.node_a
           ]) == new_generation
  end

  test "an unresolved authority hint is retired and cannot recreate a retired peer", context do
    name = unique_name(:delayed_hint_retirement)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 150
    ]

    start_group_on_peers(context.peers, opts)

    old_generation =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
        name,
        context.node_a
      ]) == old_generation
    end)

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    source_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    for source <- [source_control, source_lane] do
      :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source])
    end

    on_exit(fn ->
      for source <- [source_control, source_lane] do
        TestCluster.rpc!(context.node_a, TestCluster, :resume_if_alive, [source])
      end
    end)

    new_generation =
      TestCluster.rpc!(context.node_a, Group.Replica.WireProtocol, :new_generation, [])

    assert Group.Replica.WireProtocol.generation_newer?(new_generation, old_generation)

    send(
      target_lane,
      {:replica_heartbeat, source_lane, Group.Replica.WireProtocol.version(), new_generation, 0,
       Group.TestReplicaTransport.id(), Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_replica_authority_hint,
             [name, context.node_a]
           ) == {new_generation, 0}

    TestCluster.assert_eventually(
      fn ->
        hint =
          TestCluster.rpc!(
            context.node_b,
            Group.Replica.Data,
            :remote_replica_authority_hint,
            [name, context.node_a]
          )

        control_state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])
        lane_state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

        is_nil(hint) and
          not Map.has_key?(control_state.cluster_control_dirty, context.node_a) and
          not Map.has_key?(lane_state.peer_last_seen, context.node_a)
      end,
      timeout: 3_000,
      interval: 25
    )

    # A dirty notification can have been sent locally before the final lane
    # retired but reach shard zero afterward. The next sweep must forget that
    # now-authority-less repair obligation instead of retaining one map entry
    # forever for the departed node.
    send(target_control, {:replica_authority_dirty_local, context.node_a})
    control_state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])
    assert Map.has_key?(control_state.cluster_control_dirty, context.node_a)

    send(target_control, {:group_replica_anti_entropy, control_state.anti_entropy_ref})
    control_state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])
    refute Map.has_key?(control_state.cluster_control_dirty, context.node_a)

    # Once lease retirement removes exact authority, even a delayed heartbeat
    # from that incarnation is only a discovery prompt. It cannot recreate a
    # hint, route, or new lease without the dist-Erlang exact hello.
    send(
      target_lane,
      {:replica_heartbeat, source_lane, Group.Replica.WireProtocol.version(), new_generation, 0,
       Group.TestReplicaTransport.id(), Group.TestReplicaTransport.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_replica_authority_hint,
             [name, context.node_a]
           ) == nil

    refute Map.has_key?(
             TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane]).peer_last_seen,
             context.node_a
           )

    send(
      target_lane,
      {:replica_lane_hello, source_lane, Group.Replica.WireProtocol.version(), new_generation, 0,
       Group.TestReplicaTransport.id(), Group.TestReplicaTransport.descriptor(name, [])}
    )

    lane_state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    assert TestCluster.rpc!(
             context.node_b,
             Group.Replica.Data,
             :remote_replica_authority_hint,
             [name, context.node_a]
           ) == nil

    refute Map.has_key?(lane_state.remote_shards, context.node_a)
    refute Map.has_key?(lane_state.peer_last_seen, context.node_a)
  end

  test "restart authority repair runs before stale claims can retire a local owner", context do
    name = unique_name(:repair_before_projection)
    cluster = "repair-before-projection"

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      resolve_registry_conflict: {Group.ModelConflictResolver, :resolve, []},
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 5_000
    ]

    start_group_on_peers(context.peers, opts)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group, :connect, [name, cluster])
    end

    TestCluster.assert_eventually(fn ->
      Enum.all?([context.node_a, context.node_b, context.node_c], fn node ->
        length(TestCluster.rpc!(node, Group, :nodes, [name, cluster])) == 3
      end)
    end)

    for node <- [context.node_a, context.node_b, context.node_c] do
      :ok = TestCluster.rpc!(node, Group.TestReplicaTransport, :set_mode, [name, :drop])
    end

    key =
      1..1_000
      |> Enum.map(&"repair-before-projection/#{&1}")
      |> Enum.find(&(Group.Replica.shard_index_for(cluster, &1, 2) == 1))

    local_owner =
      TestCluster.spawn_register_in_cluster(
        context.node_b,
        name,
        key,
        %{rank: 1},
        cluster
      )

    stale_remote_owner =
      TestCluster.spawn_register_in_cluster(
        context.node_a,
        name,
        key,
        %{rank: 2},
        cluster
      )

    TestCluster.flush_shards(context.node_a, name)

    stale_stream =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_stream_id, [
        name,
        1,
        cluster
      ])

    :ok =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :put_registry_claim, [
        name,
        1,
        stale_stream,
        1,
        key,
        stale_remote_owner,
        %{rank: 2},
        System.system_time()
      ])

    assert match?(
             {^local_owner, %{rank: 1}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, key, [cluster: cluster]])
           )

    replica_supervisor =
      TestCluster.rpc!(context.node_b, Process, :whereis, [:"#{name}_replica_sup"])

    old_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [replica_supervisor])

    on_exit(fn ->
      TestCluster.rpc!(context.node_b, TestCluster, :resume_if_alive, [replica_supervisor])
    end)

    monitor = Process.monitor(old_lane)
    true = TestCluster.rpc!(context.node_b, Process, :exit, [old_lane, :kill])
    assert_receive {:DOWN, ^monitor, :process, ^old_lane, :killed}, 5_000

    :ok = TestCluster.rpc!(context.node_a, Group, :disconnect, [name, cluster])

    exact_revision =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_cluster_epoch_revision, [name])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(
        context.node_b,
        Group.Replica.Data,
        :remote_cluster_epoch_exact_revision,
        [name, context.node_a]
      ) == exact_revision and
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
          name,
          context.node_a,
          cluster
        ]) == nil
    end)

    :ok = TestCluster.rpc!(context.node_b, :sys, :resume, [replica_supervisor])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.node_b, Process, :whereis, [
             Group.Replica.shard_name(name, 1)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    assert TestCluster.rpc!(context.node_b, Process, :alive?, [local_owner])

    assert match?(
             {^local_owner, %{rank: 1}},
             TestCluster.rpc!(context.node_b, Group, :lookup, [name, key, [cluster: cluster]])
           )

    claims =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :registry_claims, [
        name,
        1,
        cluster,
        key
      ])

    assert Enum.map(claims, &elem(&1, 0)) == [local_owner]
    assert :ok = TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
  end

  test "nodedown discards deferred registry reprojection state", context do
    name = unique_name(:nodedown_deferred_projection)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    key = "nodedown/deferred-projection"

    :ok =
      TestCluster.rpc!(context.node_b, TestCluster, :put_pending_registry_reprojection, [
        target_lane,
        context.node_a,
        nil,
        key
      ])

    send(target_lane, {:nodedown, context.node_a})

    TestCluster.assert_eventually(fn ->
      state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])
      not Map.has_key?(state.pending_registry_reprojections, context.node_a)
    end)

    assert TestCluster.rpc!(context.node_b, Process, :alive?, [target_lane])
  end

  test "exact authority installation atomically projects shared clusters", context do
    name = unique_name(:atomic_authority_projection)
    cluster = "authority/install-race"

    opts = [
      name: name,
      shards: 2,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    start_group_on_peers(context.peers, opts)

    TestCluster.assert_eventually(fn ->
      Enum.sort(TestCluster.rpc!(context.node_b, Group, :nodes, [name])) ==
        Enum.sort([context.node_a, context.node_c])
    end)

    local_epoch = make_ref()

    local_epochs_table =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :local_cluster_epochs_table, [name])

    true =
      TestCluster.rpc!(context.node_b, :ets, :insert, [
        local_epochs_table,
        {cluster, local_epoch}
      ])

    :ok =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :add_cluster_node, [
        name,
        [cluster],
        context.node_b
      ])

    generation = TestCluster.rpc!(context.node_a, Group.Replica.Data, :generation, [name])
    revision = 1
    remote_epoch = make_ref()
    epochs = [{nil, generation}, {cluster, remote_epoch}]

    {_old_generation, _stale_epochs} =
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :put_remote_replica_info, [
        name,
        0,
        context.node_a,
        generation,
        revision,
        epochs
      ])

    assert Enum.sort(TestCluster.rpc!(context.node_b, Group, :nodes, [name, cluster])) ==
             Enum.sort([context.node_a, context.node_b])
  end

  test "local cluster activation survives its caller before shard notification", context do
    name = unique_name(:interrupted_cluster_activation)
    cluster = "cluster/activation-interrupted"

    opts = [
      name: name,
      shards: 2,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 5_000
    ]

    start_group_on_peers(context.peers, opts)
    :ok = TestCluster.rpc!(context.node_a, Group, :connect, [name, cluster])

    TestCluster.assert_eventually(fn ->
      not is_nil(
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_cluster_epoch, [
          name,
          context.node_a,
          cluster
        ])
      )
    end)

    control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [control])

    try do
      # This is the durable first step of Group.connect/3. Simulate its caller
      # disappearing while the queued shard notification cannot run.
      [_epoch] =
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :activate_local_clusters_durable, [
          name,
          [cluster]
        ])

      assert Enum.sort(TestCluster.rpc!(context.node_b, Group, :nodes, [name, cluster])) ==
               Enum.sort([context.node_a, context.node_b])
    after
      :ok = TestCluster.rpc!(context.node_b, TestCluster, :resume_if_alive, [control])
    end

    TestCluster.assert_eventually(fn ->
      context.node_b in TestCluster.rpc!(context.node_a, Group, :nodes, [name, cluster])
    end)
  end

  test "local cluster deactivation survives its caller before shard cleanup", context do
    name = unique_name(:interrupted_cluster_deactivation)
    cluster = "cluster/deactivation-interrupted"
    reg_key = "deactivation/interrupted/registry"
    pg_key = "deactivation/interrupted/pg"
    opts = [name: name, shards: 2]

    start_group_on_peers(context.peers, opts)
    :ok = TestCluster.rpc!(context.node_b, Group, :connect, [name, cluster])

    owner =
      TestCluster.spawn_register_and_join(
        context.node_b,
        name,
        reg_key,
        %{kind: :registry},
        pg_key,
        %{kind: :pg},
        cluster: cluster
      )

    assert {^owner, %{kind: :registry}} =
             TestCluster.rpc!(context.node_b, Group, :lookup, [
               name,
               reg_key,
               [cluster: cluster]
             ])

    assert [{^owner, %{kind: :pg}}] =
             TestCluster.rpc!(context.node_b, Group, :members, [
               name,
               pg_key,
               [cluster: cluster]
             ])

    lanes =
      for shard <- 0..1 do
        lane =
          TestCluster.rpc!(context.node_b, Process, :whereis, [
            Group.Replica.shard_name(name, shard)
          ])

        :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [lane])
        lane
      end

    try do
      # This is the durable first step of Group.disconnect/3. Simulate its
      # caller disappearing while no shard can process the queued cleanup.
      [_epoch] =
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :deactivate_local_clusters_durable, [
          name,
          [cluster]
        ])

      refute context.node_b in TestCluster.rpc!(context.node_b, Group, :nodes, [name, cluster])
    after
      Enum.each(lanes, fn lane ->
        :ok = TestCluster.rpc!(context.node_b, TestCluster, :resume_if_alive, [lane])
      end)
    end

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(context.node_b, Group, :lookup, [
          name,
          reg_key,
          [cluster: cluster]
        ]) == nil and
          TestCluster.rpc!(context.node_b, Group, :members, [
            name,
            pg_key,
            [cluster: cluster]
          ]) == [] and
          TestCluster.rpc!(context.node_b, Group, :nodes, [name, cluster]) == [] and
          TestCluster.rpc!(context.node_b, Group.Replica.Data, :closed_local_clusters, [name]) ==
            []
      end,
      timeout: 5_000
    )
  end

  test "exact authority immediately re-probes a lane whose hello arrived first", context do
    name = unique_name(:authority_reprobes_early_lane)

    opts = [
      name: name,
      shards: 2,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    {:ok, _pid} = TestCluster.start_group(context.node_b, opts)
    {:ok, _pid} = TestCluster.start_group(context.node_c, opts)

    TestCluster.assert_eventually(fn ->
      context.node_c in TestCluster.rpc!(context.node_b, Group, :nodes, [name])
    end)

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target_lane =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [target_control])

    on_exit(fn ->
      TestCluster.rpc!(context.node_b, TestCluster, :resume_if_alive, [target_control])
    end)

    {:ok, _pid} = TestCluster.start_group(context.node_a, opts)

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    source_lane =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    {generation, revision, _epochs} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_authority, [name])

    :ok = TestCluster.rpc!(context.node_a, :sys, :suspend, [source_lane])

    on_exit(fn ->
      TestCluster.rpc!(context.node_a, TestCluster, :resume_if_alive, [source_lane])
    end)

    # Drain any discovery traffic emitted while A was starting, then force the
    # lane hello to be processed while shard zero is still suspended.
    Process.sleep(100)
    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

    send(
      target_lane,
      {:replica_lane_hello, source_lane, Group.Replica.WireProtocol.version(), generation,
       revision, Group.TestReplicaTransport.id(), Group.TestReplicaTransport.descriptor(name, [])}
    )

    target_lane_state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])
    refute Map.has_key?(target_lane_state.remote_shards, context.node_a)

    assert TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_generation, [
             name,
             context.node_a
           ]) == nil

    TestCluster.assert_eventually(fn ->
      {:messages, messages} =
        TestCluster.rpc!(context.node_b, Process, :info, [target_control, :messages])

      Enum.any?(messages, fn
        {:replica_hello, ^source_control, _version, ^generation, ^revision, _epochs, _transport,
         _descriptor} ->
          true

        _message ->
          false
      end)
    end)

    :ok = TestCluster.rpc!(context.node_b, :sys, :resume, [target_control])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
        name,
        1,
        context.node_a
      ]) == generation
    end)

    # The source lane is suspended, so the only way this peer_connect can be
    # present is the immediate post-authority re-probe from the target lane.
    TestCluster.assert_eventually(fn ->
      {:messages, messages} =
        TestCluster.rpc!(context.node_a, Process, :info, [source_lane, :messages])

      Enum.any?(messages, fn
        {:peer_connect, ^target_lane, 1, 2, _clusters} -> true
        _message -> false
      end)
    end)

    :ok = TestCluster.rpc!(context.node_a, :sys, :resume, [source_lane])

    TestCluster.assert_eventually(fn ->
      lane_state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_lane])

      Map.has_key?(lane_state.remote_shards, context.node_a) and
        Map.has_key?(lane_state.peer_last_seen, context.node_a)
    end)

    assert :ok = TestCluster.rpc!(context.node_b, TestCluster, :assert_replica_consistent, [name])
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
