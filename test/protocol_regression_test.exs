defmodule Group.ProtocolRegressionTest do
  use ExUnit.Case

  alias Group.Replica
  alias Group.Replica.Data
  alias Group.TestCluster

  @moduletag :capture_log

  for path <- [:replication, :snapshot], winner_location <- [:local, :remote] do
    @tag path: path, winner_location: winner_location
    test "equal timestamps choose the #{winner_location} larger pid via #{path}", context do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      [{_, node_a}, {_, node_b}] = peers
      key = "conflict/equal"
      time = 123

      # Distinct instances keep discovery and winner rebroadcasts from resolving
      # the conflict before we deliver the exact input under test.
      candidates =
        for {node, name} <- [{node_a, :tie_a}, {node_b, :tie_b}] do
          TestCluster.start_group(node, name: name, shards: 1)
          meta = %{owner: name}
          pid = TestCluster.spawn_register(node, name, key, meta)
          {pid, node, name, meta}
        end

      [loser, winner] = Enum.sort_by(candidates, &elem(&1, 0))
      {winner_pid, winner_node, _, winner_meta} = winner
      {loser_pid, _, _, _} = loser
      loser_ref = Process.monitor(loser_pid)

      {local, remote} =
        case context.winner_location do
          :local -> {winner, loser}
          :remote -> {loser, winner}
        end

      {local_pid, receiver, name, local_meta} = local
      {remote_pid, remote_node, _, remote_meta} = remote
      TestCluster.flush_shards(receiver, name)

      # Set both indexes together after registration has settled. Never rely on
      # sequential wall-clock reads happening to return the same timestamp.
      TestCluster.rpc!(receiver, Data, :registry_insert, [
        name,
        0,
        nil,
        key,
        local_pid,
        local_meta,
        time,
        receiver
      ])

      assert {^local_pid, ^local_meta, ^time, ^receiver} =
               TestCluster.rpc!(receiver, Data, :registry_lookup, [name, 0, nil, key])

      forwarder = TestCluster.spawn_monitor_forwarder(receiver, name, :all, self())
      assert_receive {:monitor_ready, ^forwarder}, 5_000

      message =
        case context.path do
          :replication ->
            {:replicate_registry_batch,
             [{:register, nil, key, remote_pid, remote_meta, time, remote_node}]}

          :snapshot ->
            {:cluster_state, nil, [{key, remote_pid, remote_meta, time}], []}
        end

      TestCluster.rpc!(receiver, :erlang, :send, [Replica.shard_name(name, 0), message])
      TestCluster.flush_shards(receiver, name)

      assert TestCluster.rpc!(receiver, Group, :lookup, [name, key]) ==
               {winner_pid, winner_meta}

      assert_receive {:DOWN, ^loser_ref, :process, ^loser_pid,
                      {:group_registry_conflict, ^key, ^winner_meta}},
                     5_000

      assert TestCluster.rpc!(winner_node, Process, :alive?, [winner_pid])
      assert [] = TestCluster.rpc!(receiver, Data, :registry_lookup_by_pid, [name, 0, loser_pid])
      assert :ok = TestCluster.rpc!(receiver, TestCluster, :assert_ets_consistent, [name])

      if context.winner_location == :remote do
        assert_receive {:got_event,
                        %Group.Event{
                          type: :unregistered,
                          key: ^key,
                          pid: ^local_pid,
                          reason: :resolve_conflict
                        }},
                       5_000
      end
    end
  end

  for cleanup <- [:nodedown, :shard_down] do
    @tag cleanup: cleanup
    test "#{cleanup} on a nonzero shard clears membership re-added after shard zero cleanup",
         context do
      peers = TestCluster.start_peers(1)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      [{_, remote_node}] = peers
      remote_pid = TestCluster.rpc!(remote_node, :erlang, :spawn, [:timer, :sleep, [:infinity]])
      name = :"late_discovery_#{context.cleanup}"
      cluster = "shared"
      start_supervised!({Group, name: name, shards: 2, log: false})
      :ok = Group.connect(name, cluster)
      shard_zero = Replica.shard_name(name, 0)
      shard_one = Replica.shard_name(name, 1)

      # Deliver protocol signals explicitly while the peer stays alive. A real
      # VM shutdown would also enqueue monitor DOWNs, which could accidentally
      # repair a broken nodedown handler (or vice versa) before the assertion.
      send(shard_zero, {:peer_connect, remote_pid, 0, 2, [nil, cluster]})
      barrier(shard_zero)
      assert remote_node in Data.cluster_nodes(name, cluster)

      send(shard_zero, {:nodedown, remote_node})
      barrier(shard_zero)
      refute remote_node in Data.cluster_nodes(name, cluster)

      # This is the exact bad ordering: shared cleanup finishes on shard zero,
      # then an already-in-flight discovery message runs on shard one.
      send(shard_one, {:peer_connect, remote_pid, 1, 2, [nil, cluster]})
      barrier(shard_one)
      assert remote_node in Data.cluster_nodes(name, nil)
      assert remote_node in Data.cluster_nodes(name, cluster)

      message =
        case context.cleanup do
          :nodedown -> {:nodedown, remote_node}
          :shard_down -> {:DOWN, make_ref(), :process, remote_pid, :noconnection}
        end

      send(shard_one, message)
      barrier(shard_one)

      for table <- [Data.cluster_nodes_table(name), Data.node_clusters_table(name)] do
        refute Enum.any?(:ets.tab2list(table), fn
                 {^remote_node, _} -> true
                 {_, ^remote_node} -> true
                 _ -> false
               end)
      end

      assert Data.cluster_nodes(name, cluster) == [node()]
      assert :ok = TestCluster.assert_ets_consistent(name)
    end
  end

  defp barrier(shard) do
    ref = make_ref()
    send(shard, {:group_dispatch, [self()], {:barrier, ref}})
    assert_receive {:barrier, ^ref}, 5_000
  end
end
