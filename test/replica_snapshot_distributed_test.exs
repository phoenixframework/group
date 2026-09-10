defmodule Group.ReplicaSnapshotDistributedTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias Group.TestCluster

  setup_all do
    peers = TestCluster.start_peers(3, schedulers: 4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)
    [{_, node_a}, {_, node_b}, {_, node_c}] = peers
    {:ok, node_a: node_a, node_b: node_b, node_c: node_c}
  end

  test "complete provisional chunks expose nothing until terminal commit", context do
    %{name: name, node_a: node_a, node_b: node_b} = start_pair(context)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    entries =
      for index <- 1..8 do
        key = "snapshot/terminal-commit/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("c", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    {chunks, commit} = capture_snapshot_with_commit(node_a, node_b, name, stream_id, 1)
    assert length(chunks) > 1

    deliver_frames(node_b, node_a, name, Enum.reverse(chunks))
    TestCluster.flush_shards(node_b, name)

    assert replica_cursor(node_b, name, stream_id) == 0

    assert Enum.all?(entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    deliver_frames(node_b, node_a, name, [commit])
    TestCluster.flush_shards(node_b, name)
    snapshot_seq = elem(commit, 3)

    assert replica_cursor(node_b, name, stream_id) == snapshot_seq

    assert Enum.all?(entries, fn {key, pid} ->
             match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
           end)
  end

  test "exact PG install repairs an impossible conflicting stored origin", context do
    %{name: name, node_a: node_a, node_b: node_b, node_c: node_c} = start_pair(context)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    key = "snapshot/pg-origin-collision"
    member = TestCluster.spawn_join(node_a, name, key, %{source: true})

    # Move the source stream floor past the membership so requesting sequence
    # one necessarily exercises an exact snapshot rather than a delta.
    for index <- 1..4 do
      TestCluster.spawn_register(node_a, name, "snapshot/pg-origin-filler/#{index}", %{})
    end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    {chunks, commit} = capture_snapshot_with_commit(node_a, node_b, name, stream_id, 1)

    :ok =
      TestCluster.rpc!(node_b, Group.Replica.Data, :pg_insert_many, [
        name,
        0,
        [{nil, key, member, %{corrupt: true}, 0, node_c}]
      ])

    shard = TestCluster.rpc!(node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])
    monitor = Process.monitor(shard)

    deliver_frames(node_b, node_a, name, chunks ++ [commit])
    TestCluster.flush_shards(node_b, name)

    refute_receive {:DOWN, ^monitor, :process, ^shard, _reason}, 250
    assert TestCluster.rpc!(node_b, Process, :alive?, [shard])
    assert [{^member, %{source: true}}] = TestCluster.rpc!(node_b, Group, :members, [name, key])

    assert {%{source: true}, _time, ^node_a} =
             TestCluster.rpc!(node_b, Group.Replica.Data, :pg_lookup, [
               name,
               0,
               nil,
               key,
               member
             ])

    assert :ok = TestCluster.rpc!(node_b, Group.TestCluster, :assert_replica_consistent, [name])
  end

  test "a source mutation during a single-pass scan prevents terminal commit", context do
    %{name: name, node_a: node_a, node_b: node_b} = start_pair(context)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    entries =
      for index <- 1..8 do
        key = "snapshot/concurrent-write/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("s", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :clear_captured, [name])

    :ok =
      TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [
        name,
        {:capture_drop_pause_once, [:snapshot_chunk, :snapshot_commit], self()}
      ])

    :ok =
      TestCluster.rpc!(node_a, Group.Transport, :incoming, [
        name,
        node_b,
        0,
        {:need, Group.Replica.WireProtocol.version(), stream_id, 1}
      ])

    assert_receive {:replica_transport_paused, worker, ^name, :snapshot_chunk}, 5_000

    extra_key = "snapshot/concurrent-write/after-scan-started"
    extra_pid = TestCluster.spawn_register(node_a, name, extra_key, %{after_start: true})
    TestCluster.flush_shards(node_a, name)
    send(worker, {:resume_replica_transport, name})

    source = TestCluster.rpc!(node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(node_a, :sys, :get_state, [source]).snapshot_send == nil
    end)

    captured = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :captured, [name])

    chunks =
      Enum.flat_map(captured, fn
        {^node_b, 0, {:snapshot_chunk, _, ^stream_id, _, _, _, _} = chunk} -> [chunk]
        _other -> []
      end)

    assert chunks != []

    refute Enum.any?(captured, fn
             {^node_b, 0, {:snapshot_commit, _, ^stream_id, _, _, _, _}} -> true
             _other -> false
           end)

    deliver_frames(node_b, node_a, name, chunks)
    assert replica_cursor(node_b, name, stream_id) == 0

    assert Enum.all?(entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    assert TestCluster.rpc!(node_b, Group, :lookup, [name, extra_key]) == nil

    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :pass])

    :ok =
      TestCluster.rpc!(node_a, Group.Transport, :incoming, [
        name,
        node_b,
        0,
        {:need, Group.Replica.WireProtocol.version(), stream_id, 1}
      ])

    TestCluster.assert_eventually(fn ->
      Enum.all?(entries, fn {key, pid} ->
        match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
      end) and
        match?(
          {^extra_pid, _},
          TestCluster.rpc!(node_b, Group, :lookup, [name, extra_key])
        )
    end)
  end

  test "conflicting terminal manifests discard the candidate instead of manufacturing commit",
       context do
    %{name: name, node_a: node_a, node_b: node_b} = start_pair(context)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    entries =
      for index <- 1..8 do
        key = "snapshot/conflicting-commit/#{index}"
        {key, TestCluster.spawn_register(node_a, name, key, %{index: index})}
      end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    {chunks, commit} = capture_snapshot_with_commit(node_a, node_b, name, stream_id, 1)
    conflicting_commit = put_elem(commit, 5, elem(commit, 5) + 1)

    deliver_frames(node_b, node_a, name, chunks ++ [conflicting_commit])
    assert replica_cursor(node_b, name, stream_id) == 0
    assert snapshot_transfer_count(node_b, name) == 1

    deliver_frames(node_b, node_a, name, [commit])
    assert replica_cursor(node_b, name, stream_id) == 0
    assert snapshot_transfer_count(node_b, name) == 0

    deliver_frames(node_b, node_a, name, chunks ++ [commit])
    assert replica_cursor(node_b, name, stream_id) == elem(commit, 3)

    assert Enum.all?(entries, fn {key, pid} ->
             match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
           end)
  end

  test "loss, reordering, and duplication expose nothing until exact commit", context do
    %{name: name, node_a: node_a, node_b: node_b} = start_pair(context)

    stale_reg_key = "snapshot/stale-reg"
    stale_pg_key = "snapshot/stale-pg"
    stale_reg_pid = TestCluster.spawn_register(node_a, name, stale_reg_key, %{stale: true})
    stale_pg_pid = TestCluster.spawn_join(node_a, name, stale_pg_key, %{stale: true})

    TestCluster.assert_eventually(fn ->
      match?({^stale_reg_pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, stale_reg_key])) and
        match?(
          [{^stale_pg_pid, _}],
          TestCluster.rpc!(node_b, Group, :members, [name, stale_pg_key])
        )
    end)

    forwarder = TestCluster.spawn_monitor_forwarder(node_b, name, :all, self())
    assert_receive {:monitor_ready, ^forwarder}, 5_000

    stream_id = local_stream(node_a, name, nil)
    old_cursor = replica_cursor(node_b, name, stream_id)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])
    TestCluster.rpc!(node_a, Process, :exit, [stale_reg_pid, :kill])
    TestCluster.rpc!(node_a, Process, :exit, [stale_pg_pid, :kill])

    fresh_reg =
      for index <- 1..8 do
        key = "snapshot/fresh-reg/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("r", 160)
         })}
      end

    fresh_pg_key = "snapshot/fresh-pg"

    fresh_pg =
      for _index <- 1..8 do
        TestCluster.spawn_join(node_a, name, fresh_pg_key, %{
          payload: String.duplicate("p", 160)
        })
      end

    TestCluster.flush_shards(node_a, name)
    frames = capture_snapshot(node_a, node_b, name, stream_id, old_cursor + 1)
    assert length(frames) > 2
    [missing | delivered] = frames

    deliver_frames(node_b, node_a, name, Enum.reverse(delivered))
    deliver_frames(node_b, node_a, name, [List.last(delivered)])
    TestCluster.flush_shards(node_b, name)

    assert replica_cursor(node_b, name, stream_id) == old_cursor

    assert match?(
             {^stale_reg_pid, _},
             TestCluster.rpc!(node_b, Group, :lookup, [name, stale_reg_key])
           )

    assert match?(
             [{^stale_pg_pid, _}],
             TestCluster.rpc!(node_b, Group, :members, [name, stale_pg_key])
           )

    assert Enum.all?(fresh_reg, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    assert TestCluster.rpc!(node_b, Group, :members, [name, fresh_pg_key]) == []

    deliver_frames(node_b, node_a, name, [missing])
    TestCluster.flush_shards(node_b, name)
    snapshot_seq = elem(missing, 3)

    assert replica_cursor(node_b, name, stream_id) == snapshot_seq
    assert TestCluster.rpc!(node_b, Group, :lookup, [name, stale_reg_key]) == nil
    assert TestCluster.rpc!(node_b, Group, :members, [name, stale_pg_key]) == []

    assert Enum.all?(fresh_reg, fn {key, pid} ->
             match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
           end)

    assert MapSet.new(
             Enum.map(
               TestCluster.rpc!(node_b, Group, :members, [name, fresh_pg_key]),
               &elem(&1, 0)
             )
           ) ==
             MapSet.new(fresh_pg)

    {fresh_reg_key, fresh_reg_pid} = hd(fresh_reg)
    fresh_pg_pid = hd(fresh_pg)

    assert_receive {:got_event,
                    %Group.Event{type: :registered, key: ^fresh_reg_key, pid: ^fresh_reg_pid}},
                   5_000

    assert_receive {:got_event,
                    %Group.Event{type: :joined, key: ^fresh_pg_key, pid: ^fresh_pg_pid}},
                   5_000

    assert snapshot_transfer_count(node_b, name) == 0
  end

  test "a newer exact snapshot supersedes an incomplete older one", context do
    %{name: name, node_a: node_a, node_b: node_b} = start_pair(context)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    old_entries =
      for index <- 1..8 do
        key = "snapshot/superseded/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("o", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    older = capture_snapshot(node_a, node_b, name, stream_id, 1)
    assert length(older) > 1

    Enum.each(old_entries, fn {_key, pid} ->
      TestCluster.rpc!(node_a, Process, :exit, [pid, :kill])
    end)

    fresh_entries =
      for index <- 1..8 do
        key = "snapshot/replacement/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("n", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    newer = capture_snapshot(node_a, node_b, name, stream_id, 1)
    assert elem(hd(newer), 3) > elem(hd(older), 3)

    deliver_frames(node_b, node_a, name, [hd(older)])
    assert snapshot_transfer_count(node_b, name) == 1

    deliver_frames(node_b, node_a, name, Enum.reverse(newer))
    deliver_frames(node_b, node_a, name, tl(older))
    TestCluster.flush_shards(node_b, name)

    assert replica_cursor(node_b, name, stream_id) == elem(hd(newer), 3)

    assert Enum.all?(old_entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    assert Enum.all?(fresh_entries, fn {key, pid} ->
             match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
           end)

    assert snapshot_transfer_count(node_b, name) == 0
  end

  test "conflicting retransmission chunks cannot manufacture an exact snapshot", context do
    %{name: name, node_a: node_a, node_b: node_b} = start_pair(context)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    entries =
      for index <- 1..8 do
        key = "snapshot/conflicting/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("m", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    [first, second | rest] = capture_snapshot(node_a, node_b, name, stream_id, 1)
    [first_row | first_tail] = elem(first, 5)
    [second_row | second_tail] = elem(second, 5)
    conflicting_first = put_elem(first, 5, [second_row | first_tail])
    conflicting_second = put_elem(second, 5, [first_row | second_tail])

    deliver_frames(node_b, node_a, name, [first, conflicting_first])

    assert replica_cursor(node_b, name, stream_id) == 0
    assert snapshot_transfer_count(node_b, name) == 0

    assert Enum.all?(entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    # A different chunk index cannot reuse an identity already staged by the
    # first chunk to satisfy the terminal row count while omitting another row.
    deliver_frames(node_b, node_a, name, [first, conflicting_second | rest])
    assert replica_cursor(node_b, name, stream_id) == 0

    assert Enum.all?(entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    deliver_frames(node_b, node_a, name, [first, second | rest])

    assert replica_cursor(node_b, name, stream_id) == elem(first, 3)

    assert Enum.all?(entries, fn {key, pid} ->
             match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
           end)
  end

  test "rejected first chunks clear and reuse their private staging tables", context do
    %{name: name, node_a: node_a, node_b: node_b} = start_pair(context)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    for index <- 1..4 do
      TestCluster.spawn_register(node_a, name, "snapshot/rejected/#{index}", %{
        payload: String.duplicate("x", 160)
      })
    end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    frames = capture_snapshot(node_a, node_b, name, stream_id, 1)

    rows =
      frames
      |> Enum.filter(&(elem(&1, 0) == :snapshot_chunk))
      |> Enum.flat_map(&elem(&1, 5))
      |> Enum.take(2)

    assert [first_row, second_row] = rows
    {:snapshot_chunk, version, ^stream_id, snapshot_seq, _, _, _} = hd(frames)
    assert snapshot_staging_tables(node_b, name) == []

    commit = {:snapshot_commit, version, stream_id, snapshot_seq, 1, 1, 0}
    deliver_frames(node_b, node_a, name, [commit])
    assert snapshot_transfer_count(node_b, name) == 1
    first_tables = snapshot_transfer_tables(node_b, name)

    overflow =
      {:snapshot_chunk, version, stream_id, snapshot_seq, 1, [first_row, second_row], []}

    deliver_frames(node_b, node_a, name, [overflow])
    assert snapshot_transfer_count(node_b, name) == 0
    assert snapshot_staging_tables(node_b, name) == []

    duplicate =
      {:snapshot_chunk, version, stream_id, snapshot_seq, 1, [first_row, first_row], []}

    deliver_frames(node_b, node_a, name, [duplicate])
    assert snapshot_transfer_count(node_b, name) == 0
    assert snapshot_staging_tables(node_b, name) == []

    deliver_frames(node_b, node_a, name, [hd(frames)])
    assert snapshot_transfer_count(node_b, name) == 1
    assert snapshot_transfer_tables(node_b, name) == first_tables
  end

  test "an incomplete current-authority snapshot expires without touching visible state",
       context do
    %{name: name, node_a: node_a, node_b: node_b} =
      start_pair(context,
        replicated_anti_entropy_interval: 25,
        replicated_peer_lease_timeout: 250
      )

    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    entries =
      for index <- 1..8 do
        key = "snapshot/expiry/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("x", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    frames = capture_snapshot(node_a, node_b, name, stream_id, 1)
    assert length(frames) > 1

    deliver_frames(node_b, node_a, name, [hd(frames)])
    assert snapshot_transfer_count(node_b, name) == 1
    assert replica_cursor(node_b, name, stream_id) == 0

    assert Enum.all?(entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    TestCluster.assert_eventually(
      fn -> snapshot_transfer_count(node_b, name) == 0 end,
      timeout: 2_000,
      interval: 25
    )

    assert replica_cursor(node_b, name, stream_id) == 0

    assert Enum.all?(entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)
  end

  test "nodedown immediately destroys partial staging owned by the retired source", context do
    %{name: name, node_a: node_a, node_b: node_b} = start_pair(context)
    on_exit(fn -> TestCluster.reconnect_nodes(node_a, node_b) end)
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    for index <- 1..4 do
      TestCluster.spawn_register(node_a, name, "snapshot/nodedown/#{index}", %{
        payload: String.duplicate("n", 160)
      })
    end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    frames = capture_snapshot(node_a, node_b, name, stream_id, 1)
    assert length(frames) > 1
    deliver_frames(node_b, node_a, name, [hd(frames)])
    assert snapshot_transfer_count(node_b, name) == 1

    TestCluster.disconnect_nodes(node_a, node_b)

    TestCluster.assert_eventually(fn ->
      node_a not in TestCluster.rpc!(node_b, Group, :nodes, [name])
    end)

    assert snapshot_transfer_count(node_b, name) == 0
    assert snapshot_staging_tables(node_b, name) == []
  end

  test "an authority epoch change fences partial chunks and their staging expires", context do
    cluster = "snapshot-epoch"

    %{name: name, node_a: node_a, node_b: node_b} =
      start_pair(context,
        replicated_anti_entropy_interval: 25,
        replicated_peer_lease_timeout: 250
      )

    :ok = TestCluster.rpc!(node_a, Group, :connect, [name, cluster])
    :ok = TestCluster.rpc!(node_b, Group, :connect, [name, cluster])

    TestCluster.assert_eventually(fn ->
      length(TestCluster.rpc!(node_a, Group, :nodes, [name, cluster])) == 2 and
        length(TestCluster.rpc!(node_b, Group, :nodes, [name, cluster])) == 2
    end)

    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    old_entries =
      for index <- 1..8 do
        key = "snapshot/epoch/#{index}"

        {key,
         TestCluster.spawn_register_in_cluster(
           node_a,
           name,
           key,
           %{payload: String.duplicate("e", 160)},
           cluster
         )}
      end

    TestCluster.flush_shards(node_a, name)
    old_stream = local_stream(node_a, name, cluster)
    old_epoch = Group.Replica.WireProtocol.stream_epoch(old_stream)
    frames = capture_snapshot(node_a, node_b, name, old_stream, 1)
    assert length(frames) > 1
    {partial, [last]} = Enum.split(frames, -1)
    deliver_frames(node_b, node_a, name, partial)
    assert snapshot_transfer_count(node_b, name) == 1

    :ok = TestCluster.rpc!(node_a, Group, :disconnect, [name, cluster])
    :ok = TestCluster.rpc!(node_a, Group, :connect, [name, cluster])

    TestCluster.assert_eventually(fn ->
      epoch =
        TestCluster.rpc!(node_b, Group.Replica.Data, :remote_cluster_epoch, [
          name,
          node_a,
          cluster
        ])

      not is_nil(epoch) and epoch != old_epoch
    end)

    # Replay the entire retired snapshot. A receiver that checks authority only
    # when the transfer was first created would now stage and commit it.
    deliver_frames(node_b, node_a, name, partial ++ [last])
    TestCluster.flush_shards(node_b, name)

    assert Enum.all?(old_entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key, [cluster: cluster]]) == nil
           end)

    assert replica_cursor(node_b, name, old_stream) == 0

    TestCluster.assert_eventually(
      fn -> snapshot_transfer_count(node_b, name) == 0 end,
      timeout: 2_000,
      interval: 25
    )
  end

  test "an origin restart fences a partial old generation and commits the new exact snapshot",
       context do
    %{name: name, node_a: node_a, node_b: node_b, opts: opts} =
      start_pair(context,
        replicated_anti_entropy_interval: 25,
        replicated_peer_lease_timeout: 2_000
      )

    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    old_entries =
      for index <- 1..8 do
        key = "snapshot/restart/old/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("o", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    old_stream = local_stream(node_a, name, nil)
    old_frames = capture_snapshot(node_a, node_b, name, old_stream, 1)
    assert length(old_frames) > 1

    {old_partial, _old_tail} = Enum.split(old_frames, -1)
    deliver_frames(node_b, node_a, name, old_partial)
    assert snapshot_transfer_count(node_b, name) == 1
    assert replica_cursor(node_b, name, old_stream) == 0

    supervisor = TestCluster.rpc!(node_a, Process, :whereis, [:"#{name}_group_sup"])
    :ok = TestCluster.rpc!(node_a, Supervisor, :stop, [supervisor, :normal, 5_000])
    {:ok, _pid} = TestCluster.start_group(node_a, opts)

    new_stream = local_stream(node_a, name, nil)
    refute new_stream == old_stream
    new_generation = Group.Replica.WireProtocol.stream_generation(new_stream)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(node_b, Group.Replica.Data, :remote_generation, [name, node_a]) ==
        new_generation
    end)

    assert snapshot_transfer_count(node_b, name) == 0
    assert snapshot_staging_tables(node_b, name) == []

    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    new_entries =
      for index <- 1..8 do
        key = "snapshot/restart/new/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("n", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    new_frames = capture_snapshot(node_a, node_b, name, new_stream, 1)
    assert length(new_frames) > 1

    deliver_frames(node_b, node_a, name, Enum.reverse(new_frames))
    new_snapshot_seq = new_frames |> hd() |> elem(3)

    assert replica_cursor(node_b, name, new_stream) == new_snapshot_seq

    assert Enum.all?(new_entries, fn {key, pid} ->
             match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
           end)

    assert Enum.all?(old_entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    # Replaying every old-generation chunk after the new exact commit must not
    # resurrect the old slice or advance its cursor.
    deliver_frames(node_b, node_a, name, old_frames)
    assert replica_cursor(node_b, name, old_stream) == 0
    assert replica_cursor(node_b, name, new_stream) == new_snapshot_seq

    assert Enum.all?(new_entries, fn {key, pid} ->
             match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
           end)

    assert Enum.all?(old_entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    TestCluster.assert_eventually(
      fn -> snapshot_transfer_count(node_b, name) == 0 end,
      timeout: 5_000,
      interval: 25
    )
  end

  test "a receiver shard crash destroys partial staging and anti-entropy rebuilds exactly",
       context do
    %{name: name, node_a: node_a, node_b: node_b} =
      start_pair(context,
        replicated_anti_entropy_interval: 25,
        replicated_peer_lease_timeout: 1_000
      )

    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    entries =
      for index <- 1..8 do
        key = "snapshot/crash/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("c", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)
    frames = capture_snapshot(node_a, node_b, name, stream_id, 1)
    assert length(frames) > 1
    deliver_frames(node_b, node_a, name, [hd(frames)])

    {old_shard, staging_info_after_crash} =
      TestCluster.rpc!(node_b, TestCluster, :kill_shard_with_snapshot_staging, [name, 0])

    assert staging_info_after_crash == :undefined

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)]) do
        pid when is_pid(pid) -> pid != old_shard
        nil -> false
      end
    end)

    assert snapshot_transfer_count(node_b, name) == 0

    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :pass])

    TestCluster.assert_eventually(
      fn ->
        Enum.all?(entries, fn {key, pid} ->
          match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
        end)
      end,
      timeout: 10_000,
      interval: 25
    )

    assert :ok = TestCluster.rpc!(node_b, Group.TestCluster, :assert_replica_consistent, [name])
  end

  test "authority fanout tolerates a sibling that is not registered", context do
    name = :"authority_startup_fanout_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      shards: 2,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000
    ]

    for node <- [context.node_a, context.node_b, context.node_c] do
      {:ok, _pid} = TestCluster.start_group(node, opts)
    end

    TestCluster.assert_eventually(fn ->
      context.node_b in TestCluster.rpc!(context.node_a, Group, :nodes, [name])
    end)

    replica_supervisor =
      TestCluster.rpc!(context.node_b, Process, :whereis, [:"#{name}_replica_sup"])

    :ok = TestCluster.rpc!(context.node_b, :sys, :suspend, [replica_supervisor])

    on_exit(fn ->
      TestCluster.rpc!(context.node_b, Group.TestCluster, :resume_if_alive, [replica_supervisor])
    end)

    sibling =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)])

    sibling_monitor = Process.monitor(sibling)
    TestCluster.rpc!(context.node_b, Process, :exit, [sibling, :kill])
    assert_receive {:DOWN, ^sibling_monitor, :process, ^sibling, :killed}, 5_000

    assert TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 1)]) ==
             nil

    source_control =
      TestCluster.rpc!(context.node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    target_control =
      TestCluster.rpc!(context.node_b, Process, :whereis, [Group.Replica.shard_name(name, 0)])

    :ok = TestCluster.rpc!(context.node_a, Group, :connect, [name, "late-lane-authority"])

    {generation, revision, epochs} =
      TestCluster.rpc!(context.node_a, Group.Replica.Data, :local_replica_authority, [name])

    control_monitor = Process.monitor(target_control)

    send(
      target_control,
      {:replica_hello, source_control, Group.Replica.WireProtocol.version(), generation, revision,
       epochs, Group.Transport.DistErl.id(), Group.Transport.DistErl.descriptor(name, [])}
    )

    _state = TestCluster.rpc!(context.node_b, :sys, :get_state, [target_control])
    refute_receive {:DOWN, ^control_monitor, :process, ^target_control, _reason}, 250
    assert TestCluster.rpc!(context.node_b, Process, :alive?, [target_control])

    :ok = TestCluster.rpc!(context.node_b, :sys, :resume, [replica_supervisor])

    TestCluster.assert_eventually(fn ->
      sibling =
        TestCluster.rpc!(context.node_b, Process, :whereis, [
          Group.Replica.shard_name(name, 1)
        ])

      view_generation =
        TestCluster.rpc!(context.node_b, Group.Replica.Data, :remote_view_generation, [
          name,
          1,
          context.node_a
        ])

      view_exact_revision =
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_view_cluster_epoch_revision,
          [name, 1, context.node_a]
        )

      exact_revision =
        TestCluster.rpc!(
          context.node_b,
          Group.Replica.Data,
          :remote_cluster_epoch_exact_revision,
          [name, context.node_a]
        )

      is_pid(sibling) and view_generation == generation and
        view_exact_revision == exact_revision
    end)
  end

  test "a bounded transport makes forward progress across a snapshot larger than its window",
       context do
    %{name: name, node_a: node_a, node_b: node_b} =
      start_pair(context,
        replicated_sender_buffer_size: 1,
        replicated_oplog_max_entries: 2,
        replicated_snapshot_chunk_target_bytes: 700,
        replicated_anti_entropy_interval: 60_000,
        replicated_peer_lease_timeout: 120_000
      )

    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [name, :drop])

    entries =
      for index <- 1..20 do
        key = "snapshot/window/#{index}"

        {key,
         TestCluster.spawn_register(node_a, name, key, %{
           payload: String.duplicate("w", 160)
         })}
      end

    TestCluster.flush_shards(node_a, name)
    stream_id = local_stream(node_a, name, nil)

    {chunk_count, registry_count, pg_count} =
      drive_snapshot_chunks_until_commit_pending(node_a, node_b, name, stream_id, 100)

    TestCluster.flush_shards(node_b, name)
    {received, manifest} = snapshot_transfer_progress(node_b, name)
    assert received == chunk_count
    assert manifest == nil
    assert registry_count == length(entries)
    assert pg_count == 0
    assert replica_cursor(node_b, name, stream_id) == 0

    assert Enum.all?(entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    # The sender retained only the tiny terminal manifest after backpressure.
    # Its next repair attempt sends that commit directly without rescanning or
    # retransmitting the already staged chunks.
    request_snapshot_window(node_a, node_b, name, stream_id, 1)

    TestCluster.assert_eventually(
      fn ->
        Enum.all?(entries, fn {key, pid} ->
          match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
        end)
      end,
      timeout: 5_000,
      interval: 25
    )

    assert snapshot_transfer_count(node_b, name) == 0
    assert :ok = TestCluster.rpc!(node_b, Group.TestCluster, :assert_replica_consistent, [name])
  end

  defp start_pair(context, extra_opts \\ []) do
    name = :"snapshot_chunks_#{System.unique_integer([:positive])}"

    opts =
      Keyword.merge(
        [
          name: name,
          shards: 1,
          replica_transport: Group.TestReplicaTransport,
          replicated_sender_buffer_size: 1,
          replicated_oplog_max_entries: 2,
          replicated_snapshot_chunk_target_bytes: 700,
          replicated_anti_entropy_interval: 60_000,
          replicated_peer_lease_timeout: 120_000
        ],
        extra_opts
      )

    for node <- [context.node_a, context.node_b, context.node_c] do
      {:ok, _pid} = TestCluster.start_group(node, opts)
    end

    TestCluster.assert_eventually(fn ->
      context.node_b in TestCluster.rpc!(context.node_a, Group, :nodes, [name]) and
        context.node_c in TestCluster.rpc!(context.node_a, Group, :nodes, [name]) and
        context.node_a in TestCluster.rpc!(context.node_b, Group, :nodes, [name]) and
        context.node_a in TestCluster.rpc!(context.node_c, Group, :nodes, [name])
    end)

    %{
      name: name,
      node_a: context.node_a,
      node_b: context.node_b,
      node_c: context.node_c,
      opts: opts
    }
  end

  defp capture_snapshot(node_a, node_b, name, stream_id, next_seq) do
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :clear_captured, [name])

    :ok =
      TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [
        name,
        {:capture_drop, [:snapshot_chunk, :snapshot_commit]}
      ])

    :ok =
      TestCluster.rpc!(node_a, Group.Transport, :incoming, [
        name,
        node_b,
        0,
        {:needs, Group.Replica.WireProtocol.version(), [{stream_id, next_seq}]}
      ])

    TestCluster.assert_eventually(
      fn ->
        captured = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :captured, [name])

        snapshot_send =
          TestCluster.rpc!(node_a, :sys, :get_state, [Group.Replica.shard_name(name, 0)]).snapshot_send

        has_chunk? =
          Enum.any?(captured, fn
            {^node_b, 0, {:snapshot_chunk, _, ^stream_id, _, _, _, _}} -> true
            _ -> false
          end)

        has_commit? =
          Enum.any?(captured, fn
            {^node_b, 0, {:snapshot_commit, _, ^stream_id, _, _, _, _}} -> true
            _ -> false
          end)

        has_chunk? and has_commit? and is_nil(snapshot_send)
      end,
      timeout: 5_000,
      interval: 10
    )

    messages =
      TestCluster.rpc!(node_a, Group.TestReplicaTransport, :captured, [name])
      |> Enum.flat_map(fn
        {^node_b, 0, {:snapshot_chunk, _, ^stream_id, _, _, _, _} = frame} -> [frame]
        {^node_b, 0, {:snapshot_commit, _, ^stream_id, _, _, _, _} = frame} -> [frame]
        _other -> []
      end)

    chunks =
      messages
      |> Enum.filter(&(elem(&1, 0) == :snapshot_chunk))
      |> Enum.sort_by(&elem(&1, 4))

    [commit] = Enum.filter(messages, &(elem(&1, 0) == :snapshot_commit))
    chunks ++ [commit]
  end

  defp capture_snapshot_with_commit(node_a, node_b, name, stream_id, next_seq) do
    frames = capture_snapshot(node_a, node_b, name, stream_id, next_seq)
    {Enum.drop(frames, -1), List.last(frames)}
  end

  defp request_snapshot_window(node_a, node_b, name, stream_id, limit) do
    :ok =
      TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [
        name,
        {:accept_types_up_to, [:snapshot_chunk, :snapshot_commit], limit}
      ])

    :ok =
      TestCluster.rpc!(node_a, Group.Transport, :incoming, [
        name,
        node_b,
        0,
        {:needs, Group.Replica.WireProtocol.version(), [{stream_id, 1}]}
      ])

    source = TestCluster.rpc!(node_a, Process, :whereis, [Group.Replica.shard_name(name, 0)])
    _state = TestCluster.rpc!(node_a, :sys, :get_state, [source])

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(node_a, :sys, :get_state, [source]).snapshot_send == nil
      end,
      timeout: 5_000,
      interval: 10
    )
  end

  defp snapshot_transfer_progress(node, name) do
    state =
      TestCluster.rpc!(node, :sys, :get_state, [Group.Replica.shard_name(name, 0)])

    [{_key, transfer}] = Map.to_list(state.snapshot_transfers)
    {MapSet.size(transfer.received), transfer.manifest}
  end

  defp drive_snapshot_chunks_until_commit_pending(
         node_a,
         node_b,
         name,
         stream_id,
         attempts_left
       )
       when attempts_left > 0 do
    request_snapshot_window(node_a, node_b, name, stream_id, 1)
    TestCluster.flush_shards(node_b, name)

    state =
      TestCluster.rpc!(node_a, :sys, :get_state, [Group.Replica.shard_name(name, 0)])

    case Map.values(state.snapshot_send_offsets) do
      [{:commit, manifest}] ->
        manifest

      [{:chunk, _next_index}] ->
        drive_snapshot_chunks_until_commit_pending(
          node_a,
          node_b,
          name,
          stream_id,
          attempts_left - 1
        )
    end
  end

  defp deliver_frames(node_b, node_a, name, frames) do
    Enum.each(frames, fn frame ->
      :ok =
        TestCluster.rpc!(node_b, Group.Transport, :incoming, [
          name,
          node_a,
          0,
          frame
        ])
    end)

    TestCluster.flush_shards(node_b, name)
  end

  defp local_stream(node, name, cluster) do
    TestCluster.rpc!(node, Group.Replica.Data, :local_stream_id, [name, 0, cluster])
  end

  defp replica_cursor(node, name, stream_id) do
    TestCluster.rpc!(node, Group.Replica.Data, :replica_cursor, [name, 0, stream_id])
  end

  defp snapshot_transfer_count(node, name) do
    TestCluster.rpc!(node, :erlang, :map_size, [
      TestCluster.rpc!(node, :sys, :get_state, [Group.Replica.shard_name(name, 0)]).snapshot_transfers
    ])
  end

  defp snapshot_transfer_tables(node, name) do
    state =
      TestCluster.rpc!(node, :sys, :get_state, [Group.Replica.shard_name(name, 0)])

    [{_key, transfer}] = Map.to_list(state.snapshot_transfers)
    {transfer.table, transfer.events}
  end

  defp snapshot_staging_tables(node, name) do
    TestCluster.rpc!(node, TestCluster, :snapshot_staging_tables, [name, 0])
  end
end
