defmodule Group.ReplicaSnapshotDistributedTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias Group.TestCluster

  setup_all do
    peers = TestCluster.start_peers(2, schedulers: 4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)
    [{_, node_a}, {_, node_b}] = peers
    {:ok, node_a: node_a, node_b: node_b}
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
    [first, second | rest] = frames = capture_snapshot(node_a, node_b, name, stream_id, 1)
    [first_row | _] = elem(first, 8)
    [_second_row | second_tail] = elem(second, 8)
    conflicting_second = put_elem(second, 8, [first_row | second_tail])

    deliver_frames(node_b, node_a, name, [first, conflicting_second | rest])

    assert replica_cursor(node_b, name, stream_id) == 0

    assert Enum.all?(entries, fn {key, _pid} ->
             TestCluster.rpc!(node_b, Group, :lookup, [name, key]) == nil
           end)

    deliver_frames(node_b, node_a, name, frames)

    assert replica_cursor(node_b, name, stream_id) == elem(first, 3)

    assert Enum.all?(entries, fn {key, pid} ->
             match?({^pid, _}, TestCluster.rpc!(node_b, Group, :lookup, [name, key]))
           end)
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
    old_epoch = Group.Replica.Protocol.stream_epoch(old_stream)
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

    deliver_frames(node_b, node_a, name, [last])
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
    new_generation = Group.Replica.Protocol.stream_generation(new_stream)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(node_b, Group.Replica.Data, :remote_generation, [name, node_a]) ==
        new_generation
    end)

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

    for node <- [context.node_a, context.node_b] do
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
      {:replica_hello, source_control, Group.Replica.Protocol.version(), generation, revision,
       epochs, Group.Replica.Transport.Distribution.id(),
       Group.Replica.Transport.Distribution.descriptor(name, [])}
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

    for node <- [context.node_a, context.node_b] do
      {:ok, _pid} = TestCluster.start_group(node, opts)
    end

    TestCluster.assert_eventually(fn ->
      context.node_b in TestCluster.rpc!(context.node_a, Group, :nodes, [name]) and
        context.node_a in TestCluster.rpc!(context.node_b, Group, :nodes, [name])
    end)

    %{name: name, node_a: context.node_a, node_b: context.node_b, opts: opts}
  end

  defp capture_snapshot(node_a, node_b, name, stream_id, next_seq) do
    :ok = TestCluster.rpc!(node_a, Group.TestReplicaTransport, :clear_captured, [name])

    :ok =
      TestCluster.rpc!(node_a, Group.TestReplicaTransport, :set_mode, [
        name,
        {:capture_drop, [:snapshot_chunk]}
      ])

    :ok =
      TestCluster.rpc!(node_a, Group.Replica.Transport, :deliver, [
        name,
        node_b,
        0,
        {:needs, Group.Replica.Protocol.version(), [{stream_id, next_seq}]}
      ])

    TestCluster.flush_shards(node_a, name)

    TestCluster.rpc!(node_a, Group.TestReplicaTransport, :captured, [name])
    |> Enum.flat_map(fn
      {^node_b, 0,
       {:snapshot_chunk, _version, ^stream_id, _seq, _index, _count, _reg_count, _pg_count, _reg,
        _pg} = frame} ->
        [frame]

      _other ->
        []
    end)
    |> Enum.sort_by(&elem(&1, 4))
  end

  defp deliver_frames(node_b, node_a, name, frames) do
    Enum.each(frames, fn frame ->
      :ok =
        TestCluster.rpc!(node_b, Group.Replica.Transport, :deliver, [
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
end
