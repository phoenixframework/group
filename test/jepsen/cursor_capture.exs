alias Group.{JepsenCursorCapture, TestCluster}

peers = TestCluster.start_peers(2, schedulers: 1)
nodes = [node() | Enum.map(peers, &elem(&1, 1))]
path = Path.expand("node.exs", __DIR__)
rpc = fn target, function, args -> :erpc.call(target, JepsenCursorCapture, function, args) end

await = fn fun ->
  Enum.reduce_while(1..200, nil, fn _, _ ->
    if fun.(),
      do: {:halt, :ok},
      else:
        (
          Process.sleep(25)
          {:cont, nil}
        )
  end)
  |> case do
    :ok -> :ok
    _ -> raise "capture did not converge"
  end
end

try do
  for target <- nodes, do: rpc.(target, :start, [path])

  :ok =
    await.(fn ->
      Enum.all?(nodes, &(length(:erpc.call(&1, Group, :nodes, [:jepsen_group])) == 2))
    end)

  capture = fn -> Map.new(nodes, &{Atom.to_string(&1), rpc.(&1, :snapshot, [])}) end
  pristine = capture.()

  [origin, receiver, _survivor] = nodes
  :ok = rpc.(origin, :write, [])
  shard = :erlang.phash2({nil, "jepsen/registry/0"}, 2)
  stream = rpc.(origin, :stream, [nil, shard])

  :ok =
    await.(fn ->
      Enum.all?(tl(nodes), fn target ->
        :erpc.call(target, Group.Replica.Data, :replica_cursor, [:jepsen_group, shard, stream]) ==
          1
      end)
    end)

  :ok =
    await.(fn ->
      Enum.all?(tl(nodes), fn target ->
        state = rpc.(origin, :peer_head_state, [shard, target])
        state.connected? and state.pending_heads == 0 and is_reference(state.send_token)
      end)
    end)

  healthy = capture.()
  old_send_token = rpc.(origin, :peer_head_state, [shard, receiver]).send_token
  :ok = :erpc.call(receiver, TestCluster, :expire_replica_lane, [:jepsen_group, shard, origin])

  :ok =
    await.(fn ->
      state = rpc.(origin, :peer_head_state, [shard, receiver])

      state.connected? and state.pending_heads == 0 and
        state.send_token != old_send_token and
        :erpc.call(receiver, Group.Replica.Data, :replica_cursor, [
          :jepsen_group,
          shard,
          stream
        ]) == 1
    end)

  lease_recovered = capture.()

  # Withhold the first recovery probe, then let an older exact hello restore
  # the route. The receiver must keep probing until the sender ACKs that probe
  # epoch; otherwise an already-ACKed stream can remain absent forever.
  source_shard =
    :erpc.call(origin, Process, :whereis, [Group.Replica.shard_name(:jepsen_group, shard)])

  {receiver_shard, probe_epoch, tick_ref} =
    :erpc.call(receiver, TestCluster, :expire_replica_lane_without_probe, [
      :jepsen_group,
      shard,
      origin
    ])

  {generation, revision, epochs} =
    :erpc.call(origin, Group.Replica.Data, :local_replica_authority, [:jepsen_group])

  source_state = :erpc.call(origin, :sys, :get_state, [source_shard])
  transport = source_state.replica_transport

  descriptor =
    :erpc.call(origin, transport, :descriptor, [
      :jepsen_group,
      source_state.replica_transport_opts
    ])

  send(
    receiver_shard,
    {:replica_hello, source_shard, Group.Replica.WireProtocol.version(), generation, revision,
     epochs, :erpc.call(origin, transport, :id, []), descriptor}
  )

  receiver_state = :erpc.call(receiver, :sys, :get_state, [receiver_shard])
  true = Map.get(receiver_state.remote_shards, origin) == source_shard
  ^probe_epoch = Map.fetch!(receiver_state.pending_peer_probes, origin)
  0 = :erpc.call(receiver, Group.Replica.Data, :replica_cursor, [:jepsen_group, shard, stream])

  send(receiver_shard, {:group_replica_anti_entropy, tick_ref})

  :ok =
    await.(fn ->
      state = rpc.(origin, :peer_head_state, [shard, receiver])
      receiver_state = :erpc.call(receiver, :sys, :get_state, [receiver_shard])

      state.connected? and state.pending_heads == 0 and
        not Map.has_key?(receiver_state.pending_peer_probes, origin) and
        :erpc.call(receiver, Group.Replica.Data, :replica_cursor, [
          :jepsen_group,
          shard,
          stream
        ]) == 1
    end)

  stale_hello_recovered = capture.()
  :ok = rpc.(receiver, :freeze, [])

  corruptions =
    for {selected, value} <- [
          {stream, 101},
          {stream, :missing},
          {rpc.(origin, :stream, ["red", 0]), 101}
        ] do
      old = rpc.(receiver, :set_cursor, [selected, value])
      snapshot = capture.()
      true = snapshot[Atom.to_string(receiver)].internal.healthy
      :ok = rpc.(receiver, :restore_cursor, [selected, old])
      snapshot
    end

  zero_stream = rpc.(origin, :stream, ["red", 1])
  old = rpc.(receiver, :set_cursor, [zero_stream, 0])
  zero = capture.()
  :ok = rpc.(receiver, :restore_cursor, [zero_stream, old])
  :ok = rpc.(receiver, :thaw, [])

  for target <- nodes, do: :ok = :erpc.call(target, Group, :disconnect, [:jepsen_group, ["red"]])

  :ok =
    await.(fn ->
      Enum.all?(capture.(), fn {_, snapshot} ->
        Enum.all?(snapshot.streams.cursors, &(&1.stream.cluster != "red"))
      end)
    end)

  closed = capture.()

  :ok = rpc.(origin, :restart, [])
  new_generation = rpc.(origin, :snapshot, []).streams.generation
  false = new_generation == healthy[Atom.to_string(origin)].streams.generation

  :ok =
    await.(fn ->
      Enum.all?(capture.(), fn {_, snapshot} ->
        snapshot.internal.healthy and
          Enum.all?(snapshot.streams.cursors, fn cursor ->
            cursor.stream.origin != Atom.to_string(origin) or
              cursor.stream.generation == new_generation
          end)
      end)
    end)

  restarted = capture.()
  :ok = rpc.(origin, :stop_group, [])

  for target <- tl(nodes), shard <- 0..1 do
    :ok = :erpc.call(target, TestCluster, :expire_replica_lane, [:jepsen_group, shard, origin])
  end

  retired_capture = fn ->
    Map.new(tl(nodes), &{Atom.to_string(&1), rpc.(&1, :snapshot, [[origin]])})
  end

  :ok =
    await.(fn ->
      Enum.all?(retired_capture.(), fn {_, snapshot} -> snapshot.internal.healthy end)
    end)

  retired = retired_capture.()

  File.write!(
    hd(System.argv()),
    Group.Jepsen.EDN.encode(%{
      pristine: pristine,
      healthy: healthy,
      lease_recovered: lease_recovered,
      stale_hello_recovered: stale_hello_recovered,
      zero: zero,
      closed: closed,
      restarted: restarted,
      retired: retired,
      corruptions: corruptions
    })
  )
after
  TestCluster.stop_peers(peers)
end
