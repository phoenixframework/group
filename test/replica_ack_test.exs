defmodule Group.ReplicaAckTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 60_000

  alias Group.TestCluster

  setup do
    peers = TestCluster.start_peers(2)
    on_exit(fn -> TestCluster.stop_peers(peers) end)
    [{_, source}, {_, receiver}] = peers

    name = :"replica_ack_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      shards: 1,
      replica_transport: Group.TestReplicaTransport,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 5_000
    ]

    for node <- [source, receiver] do
      {:ok, _pid} = TestCluster.start_group(node, opts)
      :ok = TestCluster.rpc!(node, Group, :connect, [name, "org"])
    end

    TestCluster.assert_eventually(fn ->
      Enum.all?([source, receiver], fn node ->
        length(TestCluster.rpc!(node, Group, :nodes, [name, "org"])) == 2
      end)
    end)

    on_exit(fn ->
      for node <- [source, receiver] do
        TestCluster.rpc!(node, Group.TestReplicaTransport, :clear, [name])
      end
    end)

    {:ok, name: name, source: source, receiver: receiver}
  end

  test "settled streams do not keep advertising heads", context do
    owner =
      TestCluster.spawn_join_in_cluster(
        context.source,
        context.name,
        "sprite",
        %{up: true},
        "org"
      )

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, %{up: true}}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() == 0
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:capture_pass, [:heads]}
      ])

    drive_anti_entropy(context.source, context.name, 5)

    assert [] ==
             TestCluster.rpc!(context.source, Group.TestReplicaTransport, :captured, [
               context.name
             ])
  end

  test "a lost applied ACK retries the head and then becomes quiet", context do
    :ok =
      TestCluster.rpc!(context.receiver, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:drop_types, [:applied]}
      ])

    owner =
      TestCluster.spawn_join_in_cluster(
        context.source,
        context.name,
        "sprite",
        %{up: true},
        "org"
      )

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:capture_pass, [:heads]}
      ])

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [context.name])
      |> length() >= 2
    end)

    :ok =
      TestCluster.rpc!(context.receiver, Group.TestReplicaTransport, :set_mode, [
        context.name,
        :pass
      ])

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() == 0
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :clear_captured, [context.name])

    drive_anti_entropy(context.source, context.name, 5)

    assert [] ==
             TestCluster.rpc!(context.source, Group.TestReplicaTransport, :captured, [
               context.name
             ])
  end

  test "busy ACK transport backs off while pending heads continue repair", context do
    :ok =
      TestCluster.rpc!(context.receiver, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:capture_busy_types, [:applied]}
      ])

    owner =
      TestCluster.spawn_join_in_cluster(
        context.source,
        context.name,
        "sprite",
        %{up: true},
        "org"
      )

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    TestCluster.assert_eventually(fn ->
      context.receiver
      |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [context.name])
      |> length() > 0
    end)

    drive_anti_entropy(context.source, context.name, 5)
    TestCluster.flush_shards(context.receiver, context.name)

    attempts =
      TestCluster.rpc!(context.receiver, Group.TestReplicaTransport, :captured, [context.name])

    assert length(attempts) <= 2

    :ok =
      TestCluster.rpc!(context.receiver, Group.TestReplicaTransport, :set_mode, [
        context.name,
        :pass
      ])

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() == 0
    end)
  end

  test "repeated discovery probes do not resend an acknowledged catalog", context do
    owner =
      TestCluster.spawn_join_in_cluster(context.source, context.name, "member", %{}, "org")

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "member",
          [cluster: "org"]
        ])
      )
    end)

    source_shard =
      TestCluster.rpc!(context.source, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    receiver_shard =
      TestCluster.rpc!(context.receiver, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    # Prime one discovery round so the sender has seen this receiver lane.
    send(source_shard, peer_connect(context, receiver_shard, 0))
    _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [source_shard])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() == 0
    end)

    TestCluster.flush_shards(context.receiver, context.name)
    :ok = TestCluster.rpc!(context.receiver, :sys, :suspend, [receiver_shard])

    on_exit(fn ->
      TestCluster.rpc!(context.receiver, TestCluster, :resume_if_alive, [receiver_shard])
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:capture_pass, [:heads]}
      ])

    # Even after the discovery retry window, duplicate probes only need a
    # constant-size ACK when the sender already has this exact receiver view.
    :ok =
      TestCluster.rpc!(context.source, TestCluster, :backdate_discovery_hello, [
        context.name,
        0,
        context.receiver,
        10_000
      ])

    for _ <- 1..5 do
      send(source_shard, peer_connect(context, receiver_shard, 0))
      _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    end

    assert [] ==
             TestCluster.rpc!(context.source, Group.TestReplicaTransport, :captured, [
               context.name
             ])

    {:messages, messages} =
      TestCluster.rpc!(context.receiver, Process, :info, [receiver_shard, :messages])

    refute Enum.any?(messages, fn
             {:replica_hello, _, _, _, _, _, _, _} -> true
             _ -> false
           end)
  end

  test "a probe from an uninstalled receiver PID cannot certify head reseeding", context do
    owner =
      TestCluster.spawn_join_in_cluster(context.source, context.name, "member", %{}, "org")

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "member",
          [cluster: "org"]
        ])
      )
    end)

    source_shard =
      TestCluster.rpc!(context.source, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    receiver_shard =
      TestCluster.rpc!(context.receiver, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    TestCluster.assert_eventually(fn ->
      state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
      map_size(Map.get(state.pending_replica_heads, context.receiver, %{})) == 0
    end)

    :ok = TestCluster.rpc!(context.receiver, :sys, :suspend, [receiver_shard])

    on_exit(fn ->
      TestCluster.rpc!(context.receiver, TestCluster, :resume_if_alive, [receiver_shard])
    end)

    new_receiver_pid = TestCluster.spawn_trace_forwarder(context.receiver, self())
    send(source_shard, peer_connect(context, new_receiver_pid, 1))
    source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

    assert map_size(Map.get(source_state.pending_replica_heads, context.receiver, %{})) == 0

    {:messages, messages} =
      TestCluster.rpc!(context.receiver, Process, :info, [receiver_shard, :messages])

    assert Enum.any?(messages, fn
             {:peer_connect_ack, ^source_shard, 0, 1, 1, false} -> true
             _ -> false
           end)

    {:peer_connect, ^receiver_shard, 0, 1, 2, generation, revision} =
      peer_connect(context, receiver_shard, 2)

    send(
      source_shard,
      {:peer_connect, receiver_shard, 0, 1, 2, generation, revision + 1}
    )

    source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    assert map_size(Map.get(source_state.pending_replica_heads, context.receiver, %{})) == 0

    {:messages, messages} =
      TestCluster.rpc!(context.receiver, Process, :info, [receiver_shard, :messages])

    assert Enum.any?(messages, fn
             {:peer_connect_ack, ^source_shard, 0, 1, 2, false} -> true
             _ -> false
           end)
  end

  test "a lost discovery hello is retried without a catalog on every probe", context do
    source_shard =
      TestCluster.rpc!(context.source, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    receiver_shard =
      TestCluster.rpc!(context.receiver, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    TestCluster.flush_shards(context.receiver, context.name)
    :ok = TestCluster.rpc!(context.receiver, :sys, :suspend, [receiver_shard])

    on_exit(fn ->
      TestCluster.rpc!(context.receiver, TestCluster, :resume_if_alive, [receiver_shard])
    end)

    send(source_shard, peer_connect(context, receiver_shard, 1))
    _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

    for _ <- 1..5 do
      send(source_shard, peer_connect(context, receiver_shard, 1))
      _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    end

    request_token = make_ref()

    for _ <- 1..5 do
      send(source_shard, {:replica_hello_request, receiver_shard, request_token})
      _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    end

    assert 2 == queued_replica_hellos(context.receiver, receiver_shard)

    :ok =
      TestCluster.rpc!(context.source, TestCluster, :backdate_discovery_hello, [
        context.name,
        0,
        context.receiver,
        10_000
      ])

    send(source_shard, {:replica_hello_request, receiver_shard, request_token})
    _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

    assert 3 == queued_replica_hellos(context.receiver, receiver_shard)

    :ok = TestCluster.rpc!(context.source, Group, :connect, [context.name, "new-org"])
    send(source_shard, {:replica_hello_request, receiver_shard, request_token})
    _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

    assert 4 == queued_replica_hellos(context.receiver, receiver_shard)

    send(source_shard, {:replica_hello_request, receiver_shard, make_ref()})
    _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

    assert 5 == queued_replica_hellos(context.receiver, receiver_shard)

    :ok =
      TestCluster.rpc!(context.source, TestCluster, :forget_peer_connect_ack, [
        context.name,
        0,
        context.receiver
      ])

    for _ <- 1..5 do
      send(source_shard, {:peer_connect_ack, receiver_shard, 0, 1, 0, true})
      _state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    end

    assert 6 == queued_replica_hellos(context.receiver, receiver_shard)
  end

  test "a heartbeat that restores a lane advertises writes missed without a route", context do
    source_shard = Group.Replica.shard_name(context.name, 0)

    receiver_shard =
      TestCluster.rpc!(context.receiver, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    :ok = TestCluster.rpc!(context.receiver, :sys, :suspend, [receiver_shard])

    on_exit(fn ->
      TestCluster.rpc!(context.receiver, TestCluster, :resume_if_alive, [receiver_shard])
    end)

    :ok =
      TestCluster.rpc!(context.source, TestCluster, :forget_replica_peer_route, [
        context.name,
        0,
        context.receiver
      ])

    owner = TestCluster.spawn_join(context.source, context.name, "missed", %{})

    source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    assert Map.get(source_state.pending_replica_heads, context.receiver, %{}) == %{}

    {generation, revision, _epochs} =
      TestCluster.rpc!(context.receiver, Group.Replica.Data, :local_replica_authority, [
        context.name
      ])

    send(
      TestCluster.rpc!(context.source, Process, :whereis, [source_shard]),
      {:replica_heartbeat, receiver_shard, Group.Replica.WireProtocol.version(), generation,
       revision, Group.TestReplicaTransport.id(),
       Group.TestReplicaTransport.descriptor(context.name, [])}
    )

    TestCluster.assert_eventually(fn ->
      state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
      map_size(Map.get(state.pending_replica_heads, context.receiver, %{})) > 0
    end)

    :ok = TestCluster.rpc!(context.receiver, :sys, :resume, [receiver_shard])

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _}],
        TestCluster.rpc!(context.receiver, Group, :members, [context.name, "missed"])
      )
    end)
  end

  test "an out-of-order delta cannot advance past a missing mutation", context do
    first =
      TestCluster.spawn_join_in_cluster(context.source, context.name, "first", %{seq: 1}, "org")

    TestCluster.assert_eventually(fn ->
      match?(
        [{^first, %{seq: 1}}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "first",
          [cluster: "org"]
        ])
      )
    end)

    stream =
      TestCluster.rpc!(context.source, Group.Replica.Data, :local_stream_id, [
        context.name,
        0,
        "org"
      ])

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:capture_drop_types, [:delta_batch]}
      ])

    second =
      TestCluster.spawn_join_in_cluster(context.source, context.name, "second", %{seq: 2}, "org")

    TestCluster.flush_shards(context.source, context.name)

    third =
      TestCluster.spawn_join_in_cluster(context.source, context.name, "third", %{seq: 3}, "org")

    TestCluster.flush_shards(context.source, context.name)

    TestCluster.assert_eventually(fn -> captured_record(context, stream, 3) != nil end)
    record = captured_record(context, stream, 3)

    send(
      {Group.Replica.shard_name(context.name, 0), context.receiver},
      {:group_replica_frame, context.source,
       {:delta_batch, Group.Replica.WireProtocol.version(), [{stream, 3, [record], 3}]}}
    )

    TestCluster.flush_shards(context.receiver, context.name)

    assert 1 ==
             TestCluster.rpc!(context.receiver, Group.Replica.Data, :replica_cursor, [
               context.name,
               0,
               stream
             ])

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        :pass
      ])

    TestCluster.assert_eventually(fn ->
      match?(
        [{^second, %{seq: 2}}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "second",
          [cluster: "org"]
        ])
      ) and
        match?(
          [{^third, %{seq: 3}}],
          TestCluster.rpc!(context.receiver, Group, :members, [
            context.name,
            "third",
            [cluster: "org"]
          ])
        )
    end)
  end

  test "a dropped final leave repairs without a zombie", context do
    owner =
      TestCluster.spawn_join_in_cluster(
        context.source,
        context.name,
        "sprite",
        %{up: true},
        "org"
      )

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:drop_types, [:delta_batch]}
      ])

    true = TestCluster.rpc!(context.source, Process, :exit, [owner, :kill])
    TestCluster.flush_shards(context.source, context.name)

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() > 0
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        :pass
      ])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(context.receiver, Group, :members, [
        context.name,
        "sprite",
        [cluster: "org"]
      ]) == []
    end)
  end

  test "receiver cursor loss and shard restart reseed an acknowledged stream", context do
    owner =
      TestCluster.spawn_join_in_cluster(
        context.source,
        context.name,
        "sprite",
        %{up: true},
        "org"
      )

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() == 0
    end)

    stream_id =
      TestCluster.rpc!(context.source, Group.Replica.Data, :local_stream_id, [
        context.name,
        0,
        "org"
      ])

    :ok =
      TestCluster.rpc!(context.receiver, Group.Replica.Data, :delete_replica_cursor, [
        context.name,
        0,
        stream_id
      ])

    old_lane =
      TestCluster.rpc!(context.receiver, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    true = TestCluster.rpc!(context.receiver, Process, :exit, [old_lane, :kill])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.receiver, Process, :whereis, [
             Group.Replica.shard_name(context.name, 0)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)
  end

  test "one-sided lease expiry reseeds a stream and rejects a delayed ACK", context do
    :ok =
      TestCluster.rpc!(context.receiver, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:capture_pass, [:applied]}
      ])

    owner =
      TestCluster.spawn_join_in_cluster(
        context.source,
        context.name,
        "sprite",
        %{up: true},
        "org"
      )

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() == 0
    end)

    {_target, 0, old_ack} =
      context.receiver
      |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [context.name])
      |> Enum.find(fn {_target, shard, frame} -> shard == 0 and elem(frame, 0) == :applied end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:drop_types, [:heads]}
      ])

    :ok =
      TestCluster.rpc!(context.receiver, Group.TestCluster, :expire_replica_lane, [
        context.name,
        0,
        context.source
      ])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(context.receiver, Group, :members, [
        context.name,
        "sprite",
        [cluster: "org"]
      ]) == []
    end)

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() > 0
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.Transport, :incoming, [
        context.name,
        context.receiver,
        0,
        old_ack
      ])

    TestCluster.flush_shards(context.source, context.name)

    source_state =
      TestCluster.rpc!(context.source, :sys, :get_state, [
        Group.Replica.shard_name(context.name, 0)
      ])

    assert map_size(Map.get(source_state.pending_replica_heads, context.receiver, %{})) > 0

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        :pass
      ])

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)
  end

  test "a stale hello after expiry cannot suppress an unacknowledged recovery probe", context do
    owner =
      TestCluster.spawn_join_in_cluster(context.source, context.name, "sprite", %{}, "org")

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    TestCluster.assert_eventually(fn ->
      state =
        TestCluster.rpc!(context.source, :sys, :get_state, [
          Group.Replica.shard_name(context.name, 0)
        ])

      map_size(Map.get(state.pending_replica_heads, context.receiver, %{})) == 0
    end)

    source_shard =
      TestCluster.rpc!(context.source, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    :ok = TestCluster.rpc!(context.source, :sys, :suspend, [source_shard])

    on_exit(fn ->
      TestCluster.rpc!(context.source, TestCluster, :resume_if_alive, [source_shard])
    end)

    {receiver_shard, probe_epoch, tick_ref} =
      TestCluster.rpc!(
        context.receiver,
        TestCluster,
        :expire_replica_lane_without_probe,
        [context.name, 0, context.source]
      )

    {generation, revision, epochs} =
      TestCluster.rpc!(context.source, Group.Replica.Data, :local_replica_authority, [
        context.name
      ])

    send(
      receiver_shard,
      {:replica_hello, source_shard, Group.Replica.WireProtocol.version(), generation, revision,
       epochs, Group.TestReplicaTransport.id(),
       Group.TestReplicaTransport.descriptor(context.name, [])}
    )

    receiver_state = TestCluster.rpc!(context.receiver, :sys, :get_state, [receiver_shard])
    assert Map.get(receiver_state.remote_shards, context.source) == source_shard

    assert [] ==
             TestCluster.rpc!(context.receiver, Group, :members, [
               context.name,
               "sprite",
               [cluster: "org"]
             ])

    send(receiver_shard, {:peer_connect_ack, source_shard, 0, 1, probe_epoch - 1, true})
    receiver_state = TestCluster.rpc!(context.receiver, :sys, :get_state, [receiver_shard])
    assert Map.get(receiver_state.pending_peer_probes, context.source) == probe_epoch

    send(receiver_shard, {:peer_connect_ack, source_shard, 0, 1, probe_epoch, false})
    receiver_state = TestCluster.rpc!(context.receiver, :sys, :get_state, [receiver_shard])
    assert Map.get(receiver_state.pending_peer_probes, context.source) == probe_epoch

    send(receiver_shard, {:group_replica_anti_entropy, tick_ref})
    _receiver_state = TestCluster.rpc!(context.receiver, :sys, :get_state, [receiver_shard])

    {:messages, messages} =
      TestCluster.rpc!(context.source, Process, :info, [source_shard, :messages])

    assert Enum.any?(messages, fn
             {:peer_connect, ^receiver_shard, 0, 1, ^probe_epoch, _generation, _revision} -> true
             _ -> false
           end)

    :ok = TestCluster.rpc!(context.source, :sys, :resume, [source_shard])

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    TestCluster.assert_eventually(fn ->
      state = TestCluster.rpc!(context.receiver, :sys, :get_state, [receiver_shard])
      not Map.has_key?(state.pending_peer_probes, context.source)
    end)
  end

  test "a second one-sided expiry advances the recovery epoch and rejects the first ACK",
       context do
    owner =
      TestCluster.spawn_join_in_cluster(context.source, context.name, "sprite", %{}, "org")

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    source_shard = Group.Replica.shard_name(context.name, 0)
    receiver_shard = Group.Replica.shard_name(context.name, 0)

    TestCluster.assert_eventually(fn ->
      state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
      map_size(Map.get(state.pending_replica_heads, context.receiver, %{})) == 0
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:drop_types, [:heads]}
      ])

    :ok =
      TestCluster.rpc!(context.receiver, TestCluster, :expire_replica_lane, [
        context.name,
        0,
        context.source
      ])

    TestCluster.assert_eventually(fn ->
      receiver_state = TestCluster.rpc!(context.receiver, :sys, :get_state, [receiver_shard])
      source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

      Map.get(receiver_state.peer_probe_epochs, context.source) == 1 and
        map_size(Map.get(source_state.pending_replica_heads, context.receiver, %{})) > 0
    end)

    first_token =
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [source_shard])
      |> Map.fetch!(:replica_send_tokens)
      |> Map.fetch!(context.receiver)

    :ok =
      TestCluster.rpc!(context.receiver, TestCluster, :expire_replica_lane, [
        context.name,
        0,
        context.source
      ])

    TestCluster.assert_eventually(fn ->
      receiver_state = TestCluster.rpc!(context.receiver, :sys, :get_state, [receiver_shard])
      source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

      Map.get(receiver_state.peer_probe_epochs, context.source) == 2 and
        Map.get(source_state.replica_send_tokens, context.receiver) != first_token
    end)

    second_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    second_token = Map.fetch!(second_state.replica_send_tokens, context.receiver)
    assert second_token != first_token

    receiver_pid = TestCluster.rpc!(context.receiver, Process, :whereis, [receiver_shard])

    send(
      TestCluster.rpc!(context.source, Process, :whereis, [source_shard]),
      peer_connect(context, receiver_pid, 1)
    )

    later_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    assert Map.fetch!(later_state.replica_send_tokens, context.receiver) == second_token
    assert map_size(Map.get(later_state.pending_replica_heads, context.receiver, %{})) > 0

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        :pass
      ])

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)
  end

  defp queued_replica_hellos(node, receiver_shard) do
    {:messages, messages} = TestCluster.rpc!(node, Process, :info, [receiver_shard, :messages])

    Enum.count(messages, fn
      {:replica_hello, _, _, _, _, _, _, _} -> true
      _ -> false
    end)
  end

  test "an ACK from a closed subscription cannot clear a reopened stream", context do
    :ok =
      TestCluster.rpc!(context.receiver, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:capture_drop_types, [:applied]}
      ])

    owner =
      TestCluster.spawn_join_in_cluster(
        context.source,
        context.name,
        "sprite",
        %{up: true},
        "org"
      )

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    TestCluster.assert_eventually(fn ->
      context.receiver
      |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [context.name])
      |> Enum.any?(fn {_target, _shard, frame} -> elem(frame, 0) == :applied end)
    end)

    {_target, 0, old_ack} =
      context.receiver
      |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [context.name])
      |> Enum.find(fn {_target, shard, frame} -> shard == 0 and elem(frame, 0) == :applied end)

    old_epoch =
      TestCluster.rpc!(context.receiver, Group.Replica.Data, :local_cluster_epoch, [
        context.name,
        "org"
      ])

    :ok = TestCluster.rpc!(context.receiver, Group, :disconnect, [context.name, "org"])
    :ok = TestCluster.rpc!(context.receiver, Group, :connect, [context.name, "org"])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(context.source, Group.Replica.Data, :remote_cluster_epoch, [
        context.name,
        context.receiver,
        "org"
      ]) != old_epoch
    end)

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() > 0
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.Transport, :incoming, [
        context.name,
        context.receiver,
        0,
        old_ack
      ])

    TestCluster.flush_shards(context.source, context.name)

    source_state =
      TestCluster.rpc!(context.source, :sys, :get_state, [
        Group.Replica.shard_name(context.name, 0)
      ])

    assert map_size(Map.get(source_state.pending_replica_heads, context.receiver, %{})) > 0

    :ok =
      TestCluster.rpc!(context.receiver, Group.TestReplicaTransport, :set_mode, [
        context.name,
        :pass
      ])

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() == 0
    end)
  end

  test "sender shard restart rebuilds a lost pending head", context do
    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        {:drop_types, [:delta_batch, :heads]}
      ])

    owner =
      TestCluster.spawn_join_in_cluster(
        context.source,
        context.name,
        "sprite",
        %{up: true},
        "org"
      )

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(context.name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() > 0
    end)

    assert [] ==
             TestCluster.rpc!(context.receiver, Group, :members, [
               context.name,
               "sprite",
               [cluster: "org"]
             ])

    old_lane =
      TestCluster.rpc!(context.source, Process, :whereis, [
        Group.Replica.shard_name(context.name, 0)
      ])

    true = TestCluster.rpc!(context.source, Process, :exit, [old_lane, :kill])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(context.source, Process, :whereis, [
             Group.Replica.shard_name(context.name, 0)
           ]) do
        pid when is_pid(pid) -> pid != old_lane
        _ -> false
      end
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        context.name,
        :pass
      ])

    TestCluster.assert_eventually(fn ->
      match?(
        [{^owner, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          context.name,
          "sprite",
          [cluster: "org"]
        ])
      )
    end)
  end

  test "an unacknowledged pruned stream repairs by exact snapshot", context do
    name = :"replica_ack_snapshot_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      shards: 1,
      replica_transport: Group.TestReplicaTransport,
      replicated_oplog_max_entries: 2,
      replicated_anti_entropy_interval: 25,
      replicated_peer_lease_timeout: 5_000
    ]

    for node <- [context.source, context.receiver] do
      {:ok, _pid} = TestCluster.start_group(node, opts)
      :ok = TestCluster.rpc!(node, Group, :connect, [name, "snapshot-org"])
    end

    TestCluster.assert_eventually(fn ->
      length(TestCluster.rpc!(context.source, Group, :nodes, [name, "snapshot-org"])) == 2
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        name,
        {:drop_types, [:delta_batch, :heads]}
      ])

    entries =
      for index <- 1..8 do
        key = "member/#{index}"

        {key,
         TestCluster.spawn_join_in_cluster(
           context.source,
           name,
           key,
           %{index: index},
           "snapshot-org"
         )}
      end

    TestCluster.flush_shards(context.source, name)

    stream_id =
      TestCluster.rpc!(context.source, Group.Replica.Data, :local_stream_id, [
        name,
        0,
        "snapshot-org"
      ])

    {floor, head, _applied} =
      TestCluster.rpc!(context.source, Group.Replica.Data, :replica_stream_head, [
        name,
        0,
        stream_id
      ])

    assert floor > 1
    assert head >= length(entries)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        name,
        {:capture_pass, [:snapshot_commit]}
      ])

    TestCluster.assert_eventually(fn ->
      Enum.all?(entries, fn {key, pid} ->
        match?(
          [{^pid, _meta}],
          TestCluster.rpc!(context.receiver, Group, :members, [
            name,
            key,
            [cluster: "snapshot-org"]
          ])
        )
      end)
    end)

    TestCluster.assert_eventually(fn ->
      context.source
      |> TestCluster.rpc!(:sys, :get_state, [Group.Replica.shard_name(name, 0)])
      |> Map.get(:pending_replica_heads)
      |> Map.get(context.receiver, %{})
      |> map_size() == 0
    end)

    assert Enum.any?(
             TestCluster.rpc!(context.source, Group.TestReplicaTransport, :captured, [name]),
             fn
               {_target, _shard, {:snapshot_commit, _, ^stream_id, _, _, _, _}} -> true
               _ -> false
             end
           )

    commits_before =
      context.source
      |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [name])
      |> Enum.count(fn
        {_target, _shard, {:snapshot_commit, _, ^stream_id, _, _, _, _}} -> true
        _ -> false
      end)

    source_shard =
      TestCluster.rpc!(context.source, Process, :whereis, [
        Group.Replica.shard_name(name, 0)
      ])

    receiver_shard =
      TestCluster.rpc!(context.receiver, Process, :whereis, [
        Group.Replica.shard_name(name, 0)
      ])

    # A delayed need from before the applied ACK cannot start another full snapshot.
    send(
      source_shard,
      {:group_replica_frame, receiver_shard,
       {:needs, Group.Replica.WireProtocol.version(), [{stream_id, 1, head}]}}
    )

    source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    assert source_state.snapshot_send == nil

    assert commits_before ==
             context.source
             |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [name])
             |> Enum.count(fn
               {_target, _shard, {:snapshot_commit, _, ^stream_id, _, _, _, _}} -> true
               _ -> false
             end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        name,
        {:capture_drop_types, [:heads, :delta_batch, :snapshot_commit]}
      ])

    new_member =
      TestCluster.spawn_join_in_cluster(
        context.source,
        name,
        "member/new",
        %{},
        "snapshot-org"
      )

    TestCluster.assert_eventually(fn ->
      source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
      Map.has_key?(Map.get(source_state.pending_replica_heads, context.receiver, %{}), stream_id)
    end)

    # This need describes the previous head. A newer pending head must not
    # turn it into a full snapshot request.
    send(
      source_shard,
      {:group_replica_frame, receiver_shard,
       {:needs, Group.Replica.WireProtocol.version(), [{stream_id, 1, head}]}}
    )

    source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
    assert source_state.snapshot_send == nil
    assert snapshot_commits(context.source, name, stream_id) == commits_before

    :ok = TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [name, :pass])
    drive_anti_entropy(context.source, name, 1)

    TestCluster.assert_eventually(fn ->
      match?(
        [{^new_member, _meta}],
        TestCluster.rpc!(context.receiver, Group, :members, [
          name,
          "member/new",
          [cluster: "snapshot-org"]
        ])
      )
    end)
  end

  test "a lost snapshot commit retries after a bound, not on every need", context do
    name = :"replica_ack_snapshot_retry_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      shards: 1,
      replica_transport: Group.TestReplicaTransport,
      replicated_oplog_max_entries: 2,
      replicated_anti_entropy_interval: 2_000,
      replicated_peer_lease_timeout: 5_000
    ]

    for node <- [context.source, context.receiver] do
      {:ok, _pid} = TestCluster.start_group(node, opts)
      :ok = TestCluster.rpc!(node, Group, :connect, [name, "snapshot-org"])
    end

    TestCluster.assert_eventually(fn ->
      length(TestCluster.rpc!(context.source, Group, :nodes, [name, "snapshot-org"])) == 2
    end)

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        name,
        {:drop_types, [:heads, :delta_batch]}
      ])

    for index <- 1..8 do
      TestCluster.spawn_join_in_cluster(
        context.source,
        name,
        "member/#{index}",
        %{index: index},
        "snapshot-org"
      )
    end

    TestCluster.flush_shards(context.source, name)

    stream_id =
      TestCluster.rpc!(context.source, Group.Replica.Data, :local_stream_id, [
        name,
        0,
        "snapshot-org"
      ])

    {floor, head, _applied} =
      TestCluster.rpc!(
        context.source,
        Group.Replica.Data,
        :replica_stream_head,
        [name, 0, stream_id]
      )

    assert floor > 1

    :ok =
      TestCluster.rpc!(context.source, Group.TestReplicaTransport, :set_mode, [
        name,
        {:capture_drop_types, [:snapshot_commit]}
      ])

    source_shard =
      TestCluster.rpc!(context.source, Process, :whereis, [
        Group.Replica.shard_name(name, 0)
      ])

    receiver_shard =
      TestCluster.rpc!(context.receiver, Process, :whereis, [
        Group.Replica.shard_name(name, 0)
      ])

    drive_anti_entropy(context.source, name, 1)

    TestCluster.assert_eventually(fn ->
      snapshot_commits(context.source, name, stream_id) == 1 and
        TestCluster.rpc!(context.source, :sys, :get_state, [source_shard]).snapshot_send == nil
    end)

    need =
      {:group_replica_frame, receiver_shard,
       {:needs, Group.Replica.WireProtocol.version(), [{stream_id, 1, head}]}}

    for _ <- 1..3 do
      send(source_shard, need)
      state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])
      assert state.snapshot_send == nil
    end

    assert snapshot_commits(context.source, name, stream_id) == 1

    :ok =
      TestCluster.rpc!(context.source, TestCluster, :backdate_completed_snapshot, [
        name,
        0,
        context.receiver,
        stream_id,
        head,
        3_000
      ])

    send(source_shard, need)

    TestCluster.assert_eventually(fn ->
      snapshot_commits(context.source, name, stream_id) == 2
    end)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(context.source, :sys, :get_state, [source_shard]).snapshot_send == nil
    end)

    source_state = TestCluster.rpc!(context.source, :sys, :get_state, [source_shard])

    {^receiver_shard, probe_epoch} =
      Map.fetch!(source_state.remote_probe_epochs, context.receiver)

    # A receiver that has lost its view must not wait for the old retry hold.
    send(source_shard, peer_connect(context, receiver_shard, probe_epoch + 1, name))

    TestCluster.assert_eventually(fn ->
      snapshot_commits(context.source, name, stream_id) == 3
    end)
  end

  defp peer_connect(context, receiver_pid, probe_epoch, name \\ nil) do
    name = name || context.name

    {generation, revision, _epochs} =
      TestCluster.rpc!(context.receiver, Group.Replica.Data, :local_replica_authority, [
        name
      ])

    {:peer_connect, receiver_pid, 0, 1, probe_epoch, generation, revision}
  end

  defp snapshot_commits(source, name, stream_id) do
    source
    |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [name])
    |> Enum.count(fn
      {_target, _shard, {:snapshot_commit, _, ^stream_id, _, _, _, _}} -> true
      _ -> false
    end)
  end

  defp captured_record(context, stream, sequence) do
    context.source
    |> TestCluster.rpc!(Group.TestReplicaTransport, :captured, [context.name])
    |> Enum.find_value(fn
      {target, 0, {:delta_batch, _version, runs}} when target == context.receiver ->
        Enum.find_value(runs, fn
          {^stream, _first, records, _head} ->
            Enum.find(records, fn {seq, _mutations} -> seq == sequence end)

          _other ->
            nil
        end)

      _other ->
        nil
    end)
  end

  defp drive_anti_entropy(node, name, turns) do
    shard_name = Group.Replica.shard_name(name, 0)

    for _ <- 1..turns do
      state = TestCluster.rpc!(node, :sys, :get_state, [shard_name])
      shard = TestCluster.rpc!(node, Process, :whereis, [shard_name])
      send(shard, {:group_replica_anti_entropy, state.anti_entropy_ref})

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node, :sys, :get_state, [shard_name]).anti_entropy_ref !=
          state.anti_entropy_ref
      end)

      TestCluster.flush_shards(node, name)
    end
  end
end
