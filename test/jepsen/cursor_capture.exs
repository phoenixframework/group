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

  healthy = capture.()
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
