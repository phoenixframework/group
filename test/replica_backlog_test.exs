defmodule Group.ReplicaBacklogTest do
  # These tests change VM-global trace patterns.
  use Group.LocalCase, async: false

  @source :"backlog-source@test"

  setup do
    name = start_single_shard_group()
    shard = Process.whereis(Group.Replica.shard_name(name, 0))

    on_exit(fn ->
      :erlang.trace_pattern({Group.Replica, :handle_replica_message, 3}, false, [:local])
      :erlang.trace_pattern({Group.Replica, :process_local_request_turn, 2}, false, [:local])
    end)

    {:ok, name: name, shard: shard}
  end

  describe "a backlog of replicated batches" do
    test "costs the same per batch however deep it is", %{name: name} do
      small = reductions_per_batch(name, 2_000)
      large = reductions_per_batch(name, 20_000)

      assert large < 2 * small,
             "a batch cost #{small} reductions behind 2,000 queued batches " <>
               "and #{large} behind 20,000"
    end

    test "is handled in the order it arrived", %{shard: shard} do
      :ok = :sys.suspend(shard)
      for index <- 1..500, do: send(shard, batch([index]))
      trace_replica_messages(shard)
      :ok = :sys.resume(shard)

      assert handled_replica_messages(500) == Enum.to_list(1..500)
    end

    test "keeps an oversized batch ahead of the batches behind it", %{shard: shard} do
      :ok = :sys.suspend(shard)
      send(shard, batch(Enum.to_list(1..150)))
      send(shard, batch(Enum.to_list(151..160)))
      send(shard, {:group_replica_frame, @source, {:backlog_marker, 161}})
      trace_replica_messages(shard)
      :ok = :sys.resume(shard)

      assert handled_replica_messages(161) == Enum.to_list(1..161)
    end

    test "yields to a local write queued behind it", %{name: name, shard: shard} do
      :ok = :sys.suspend(shard)
      for index <- 1..20_000, do: send(shard, batch([index]))

      parent = self()
      owner = spawn(fn -> replica_ingress_fairness_owner(parent) end)
      :erlang.trace(shard, true, [:call, {:tracer, owner}])
      :erlang.trace_pattern({Group.Replica, :handle_replica_message, 3}, true, [:local])

      key = "backlog/local/#{System.unique_integer([:positive])}"
      epoch = Group.Replica.Data.local_cluster_epoch(name, nil)
      send(owner, {:write, shard, {:register, nil, epoch, key, owner, %{}}})
      wait_until(fn -> queue_len(shard) > 20_000 end)
      :ok = :sys.resume(shard)

      assert_receive {:local_write_finished, ^owner, :ok, batches_before_local}, 5_000
      assert batches_before_local <= 64
    end

    test "does not starve behind a storm of local requests", %{name: name, shard: shard} do
      :ok = :sys.suspend(shard)
      for index <- 1..200, do: send(shard, batch([index]))

      epoch = Group.Replica.Data.local_cluster_epoch(name, nil)
      owner = spawn(fn -> Process.sleep(:infinity) end)

      for index <- 1..800 do
        request = {:join, nil, epoch, "backlog/storm/#{index}", owner, %{}}
        send(shard, {:group_local_request, :noreply, request})
      end

      :erlang.trace(shard, true, [:call, {:tracer, self()}])
      :erlang.trace_pattern({Group.Replica, :handle_replica_message, 3}, true, [:local])
      :erlang.trace_pattern({Group.Replica, :process_local_request_turn, 2}, true, [:local])
      :ok = :sys.resume(shard)

      turns = collect_turns(shard, 200, [])
      :erlang.trace(shard, false, [:call])

      # Every local turn takes at most eight requests, so 800 requests need at
      # least 100 turns. A batch must run between them; only the turn taken by
      # priority and the first one taken in arrival order may be adjacent.
      local_runs =
        turns
        |> Enum.chunk_by(& &1)
        |> Enum.filter(&(hd(&1) == :local))
        |> Enum.map(&length/1)

      assert Enum.count(turns, &(&1 == :local)) >= 100
      assert Enum.max(local_runs) <= 2
    end

    test "leaves later batches a barrier for :sys calls once drained", %{shard: shard} do
      :ok = :sys.suspend(shard)
      for index <- 1..2_000, do: send(shard, batch([index]))
      :ok = :sys.resume(shard)
      await_idle(shard)

      trace_replica_messages(shard)
      send(shard, batch([:after_backlog]))
      _state = :sys.get_state(shard)

      assert_received {:trace, ^shard, :call,
                       {Group.Replica, :handle_replica_message,
                        [_state, @source, {:backlog_marker, :after_backlog}]}}
    end
  end

  describe "code change" do
    test "gives a state from an older release the fields added since", %{shard: shard} do
      before = :sys.get_state(shard)
      :ok = :sys.suspend(shard)

      :sys.replace_state(shard, fn state ->
        Map.drop(state, [:incoming, :incoming_turn_ref])
      end)

      :ok = :sys.change_code(shard, Group.Replica, nil, [])
      :ok = :sys.resume(shard)

      assert :sys.get_state(shard) == before
      send(shard, batch([1]))
      assert is_struct(:sys.get_state(shard), Group.Replica)
    end
  end

  defp batch(markers) do
    {:group_replica_batch, @source, Enum.map(markers, &{:backlog_marker, &1})}
  end

  defp reductions_per_batch(name, count) do
    shard = Process.whereis(Group.Replica.shard_name(name, 0))
    await_idle(shard)
    :ok = :sys.suspend(shard)
    for index <- 1..count, do: send(shard, batch([index]))
    {:reductions, before} = Process.info(shard, :reductions)
    :ok = :sys.resume(shard)
    await_idle(shard)
    {:reductions, done} = Process.info(shard, :reductions)
    div(done - before, count)
  end

  # A shard that still owes a deferred batch a turn always has that turn's
  # message queued or is running it, so an empty mailbox on a waiting process
  # means every batch has been handled.
  defp await_idle(shard) do
    wait_until(
      fn ->
        Process.info(shard, [:message_queue_len, :status]) ==
          [message_queue_len: 0, status: :waiting]
      end,
      60_000
    )
  end

  defp queue_len(shard) do
    {:message_queue_len, length} = Process.info(shard, :message_queue_len)
    length
  end

  defp trace_replica_messages(shard) do
    :erlang.trace(shard, true, [:call, {:tracer, self()}])
    :erlang.trace_pattern({Group.Replica, :handle_replica_message, 3}, true, [:local])
  end

  defp handled_replica_messages(count) do
    for _ <- 1..count do
      assert_receive {:trace, _shard, :call,
                      {Group.Replica, :handle_replica_message,
                       [_state, @source, {:backlog_marker, marker}]}},
                     5_000

      marker
    end
  end

  defp collect_turns(_shard, 0, turns), do: Enum.reverse(turns)

  defp collect_turns(shard, batches_left, turns) do
    receive do
      {:trace, ^shard, :call, {Group.Replica, :handle_replica_message, _args}} ->
        collect_turns(shard, batches_left - 1, [:batch | turns])

      {:trace, ^shard, :call, {Group.Replica, :process_local_request_turn, _args}} ->
        collect_turns(shard, batches_left, [:local | turns])
    after
      5_000 -> flunk("the shard stopped handling batches with #{batches_left} left")
    end
  end
end

defmodule Group.ReplicaBacklogDistributedTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 180_000

  alias Group.{ShardBacklog, TestCluster}

  setup do
    peers = TestCluster.start_peers(2)
    on_exit(fn -> TestCluster.stop_peers(peers) end)
    [{_, source}, {_, receiver}] = peers
    {:ok, source: source, receiver: receiver}
  end

  test "a deep backlog of accepted deltas is applied completely and in order", context do
    %{source: source, receiver: receiver} = context

    name =
      start_groups(context,
        replicated_anti_entropy_interval: 60_000,
        replicated_peer_lease_timeout: 120_000
      )

    stream = TestCluster.rpc!(source, Group.Replica.Data, :local_stream_id, [name, 0, nil])
    member = TestCluster.rpc!(source, ShardBacklog, :spawn_member, [])

    # The source never wrote these records, so it cannot repair a gap: the
    # cursor reaches the last sequence only if every batch was applied in order.
    count = 20_001
    args = ["plain", name, source, stream, member, count, count, []]
    result = :erpc.call(receiver, ShardBacklog, :run, args, 120_000)

    assert result.queued_by_tag[{:group_replica_batch, :delta_batch}] == nil

    assert TestCluster.rpc!(receiver, Group.Replica.Data, :replica_cursor, [name, 0, stream]) ==
             count

    last_key = "backlog/remote/#{div(count - 1, 2)}"
    assert [{^member, _meta}] = TestCluster.rpc!(receiver, Group, :members, [name, last_key])
    assert TestCluster.rpc!(receiver, Group, :members, [name, "backlog/remote/0"]) == []
  end

  test "a shard working through a backlog keeps its peer leases", context do
    %{source: source, receiver: receiver} = context

    name =
      start_groups(context,
        replicated_anti_entropy_interval: 100,
        replicated_peer_lease_timeout: 1_000
      )

    key = "backlog/lease/#{System.unique_integer([:positive])}"
    owner = TestCluster.spawn_register(receiver, name, key, %{})
    TestCluster.assert_eventually(fn -> remote_lookup(source, name, key) == owner end)

    _forwarder = TestCluster.spawn_monitor_forwarder(source, name, key, self())
    assert_receive {:monitor_ready, _pid}, 5_000

    stream = TestCluster.rpc!(source, Group.Replica.Data, :local_stream_id, [name, 0, nil])
    member = TestCluster.rpc!(source, ShardBacklog, :spawn_member, [])

    # Enough work to keep the shard busy for several lease timeouts. The shard
    # is not suspended while the backlog is queued, so every heartbeat it
    # misses is one it was too far behind to send.
    count = 6_000
    opts = [records_per_batch: 64, suspend: false]
    args = ["plain", name, source, stream, member, count, count * 64, opts]
    result = :erpc.call(receiver, ShardBacklog, :run, args, 150_000)

    assert result.micros > 2_000_000, "the backlog drained before a lease could expire"
    refute_received {:got_event, %Group.Event{type: :unregistered}}
    assert remote_lookup(source, name, key) == owner
  end

  defp start_groups(%{source: source, receiver: receiver}, opts) do
    name = :"replica_backlog_#{System.unique_integer([:positive])}"

    for node <- [source, receiver] do
      {:ok, _pid} = TestCluster.start_group(node, [name: name, shards: 1] ++ opts)
    end

    TestCluster.assert_eventually(
      fn ->
        receiver in TestCluster.rpc!(source, Group, :nodes, [name]) and
          source in TestCluster.rpc!(receiver, Group, :nodes, [name])
      end,
      timeout: 10_000
    )

    name
  end

  defp remote_lookup(node, name, key) do
    case TestCluster.rpc!(node, Group, :lookup, [name, key]) do
      {pid, _meta} -> pid
      nil -> nil
    end
  end
end
