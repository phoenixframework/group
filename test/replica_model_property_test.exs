defmodule Group.ReplicaModelPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Group.{
    ControlledReplicaTransport,
    ModelConflictResolver,
    ReplicaModelScheduler,
    TestCluster
  }

  @moduletag :capture_log
  @moduletag timeout: 180_000

  @model_runs System.get_env("GROUP_MODEL_RUNS", "12") |> String.to_integer()
  @max_commands System.get_env("GROUP_MODEL_COMMANDS", "30") |> String.to_integer()

  setup_all do
    peers = TestCluster.start_peers(3, schedulers: 4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, node_a}, {_, node_b}, {_, node_c}] = peers
    {:ok, nodes: %{a: node_a, b: node_b, c: node_c}}
  end

  property "accepted owner lifecycles converge without permanent orphans or zombies", %{
    nodes: nodes
  } do
    check all(
            commands <- command_history(),
            max_runs: @model_runs,
            max_shrinking_steps: 100
          ) do
      name = :"replica_model_#{System.unique_integer([:positive])}"

      opts = [
        name: name,
        shards: 2,
        resolve_registry_conflict: {ModelConflictResolver, :resolve, []},
        replica_transport: {ControlledReplicaTransport, controller: self()},
        replicated_sender_buffer_size: 1,
        replicated_anti_entropy_interval: 60_000,
        replicated_peer_lease_timeout: 120_000,
        replicated_snapshot_chunk_target_bytes: 256,
        replicated_oplog_max_entries: 4
      ]

      Enum.each(nodes, fn {_id, node} ->
        {:ok, _pid} = TestCluster.start_group(node, opts)
      end)

      scheduler = ReplicaModelScheduler.new(name, nodes, opts)

      try do
        await_discovery(nodes, name)

        scheduler =
          commands
          |> Enum.reduce(ReplicaModelScheduler.sync(scheduler), fn command, state ->
            ReplicaModelScheduler.execute(state, command)
          end)
          |> ReplicaModelScheduler.stabilize_and_assert!()

        assert scheduler.model != nil
      after
        ReplicaModelScheduler.cleanup(scheduler)
      end
    end
  end

  property "concurrent claims converge to one live winner and permanently retire the loser", %{
    nodes: nodes
  } do
    check all(
            key_slot <- integer(0..2),
            winner <- member_of([:a, :b]),
            schedule <- list_of(network_command(), min_length: 0, max_length: 20),
            max_runs: max(div(@model_runs, 2), 1),
            max_shrinking_steps: 100
          ) do
      name = :"replica_conflict_model_#{System.unique_integer([:positive])}"

      opts = [
        name: name,
        shards: 2,
        resolve_registry_conflict: {ModelConflictResolver, :resolve, []},
        replica_transport: {ControlledReplicaTransport, controller: self()},
        replicated_sender_buffer_size: 1,
        replicated_anti_entropy_interval: 60_000,
        replicated_peer_lease_timeout: 120_000,
        replicated_snapshot_chunk_target_bytes: 256,
        replicated_oplog_max_entries: 2
      ]

      Enum.each(nodes, fn {_id, node} ->
        {:ok, _pid} = TestCluster.start_group(node, opts)
      end)

      scheduler = ReplicaModelScheduler.new(name, nodes, opts)

      try do
        await_discovery(nodes, name)

        {rank_a, rank_b} = if winner == :a, do: {2, 1}, else: {1, 2}

        scheduler =
          scheduler
          |> ReplicaModelScheduler.sync()
          |> ReplicaModelScheduler.execute({:register, 12, :c, key_slot + 10, 1})
          |> ReplicaModelScheduler.execute({:join, 12, :c, key_slot + 10, 1})
          |> ReplicaModelScheduler.execute({:claim, 10, :a, key_slot, rank_a})
          |> ReplicaModelScheduler.execute({:claim, 11, :b, key_slot, rank_b})

        scheduler =
          schedule
          |> Enum.reduce(scheduler, fn command, state ->
            ReplicaModelScheduler.execute(state, command)
          end)
          |> ReplicaModelScheduler.stabilize_and_assert!()

        assert scheduler.model != nil
      after
        ReplicaModelScheduler.cleanup(scheduler)
      end
    end
  end

  property "old frames cannot survive a real Group restart and generation change", %{nodes: nodes} do
    check all(
            before_restart <- list_of(network_command(), max_length: 8),
            after_restart <- list_of(network_command(), max_length: 12),
            max_runs: transition_runs(),
            max_shrinking_steps: 100
          ) do
      name = :"replica_restart_model_#{System.unique_integer([:positive])}"
      opts = model_opts(name, self(), shards: 2, oplog: 2)
      start_groups(nodes, opts)
      scheduler = ReplicaModelScheduler.new(name, nodes, opts)

      try do
        await_discovery(nodes, name)

        scheduler =
          scheduler
          |> ReplicaModelScheduler.sync()
          |> ReplicaModelScheduler.execute({:transport, :a, :pass})
          |> ReplicaModelScheduler.execute({:transport, :b, :pass})
          |> ReplicaModelScheduler.execute({:transport, :c, :pass})
          |> ReplicaModelScheduler.execute({:register, 30, :a, 0, 1})
          |> ReplicaModelScheduler.execute({:join, 30, :a, 0, 1})
          |> ReplicaModelScheduler.execute({:register, 32, :c, 1, 1})
          |> ReplicaModelScheduler.execute({:join, 32, :c, 1, 1})
          |> ReplicaModelScheduler.stabilize_and_assert!()
          |> ReplicaModelScheduler.execute({:transport, :a, :capture})
          |> ReplicaModelScheduler.execute({:transport, :b, :capture})
          |> ReplicaModelScheduler.execute({:transport, :c, :capture})
          |> ReplicaModelScheduler.execute({:register, 30, :a, 0, 2})
          |> ReplicaModelScheduler.execute({:join, 30, :a, 0, 2})
          |> run_schedule(before_restart)
          |> ReplicaModelScheduler.execute({:restart, :a})
          |> ReplicaModelScheduler.execute(:deliver_all)
          |> ReplicaModelScheduler.execute({:register, 31, :a, 1, 2})
          |> ReplicaModelScheduler.execute({:join, 31, :a, 1, 2})
          |> run_schedule(after_restart)
          |> ReplicaModelScheduler.stabilize_and_assert!()

        assert scheduler.model != nil
      after
        ReplicaModelScheduler.cleanup(scheduler)
      end
    end
  end

  property "a receiver below the real oplog floor converges through an exact snapshot", %{
    nodes: nodes
  } do
    check all(
            schedule <- list_of(network_command(), max_length: 10),
            max_runs: transition_runs(),
            max_shrinking_steps: 100
          ) do
      name = :"replica_pruning_model_#{System.unique_integer([:positive])}"
      opts = model_opts(name, self(), shards: 1, oplog: 2)
      start_groups(nodes, opts)
      scheduler = ReplicaModelScheduler.new(name, nodes, opts)

      try do
        await_discovery(nodes, name)

        scheduler =
          scheduler
          |> ReplicaModelScheduler.sync()
          |> ReplicaModelScheduler.execute({:transport, :a, :pass})
          |> ReplicaModelScheduler.execute({:transport, :b, :pass})
          |> ReplicaModelScheduler.execute({:transport, :c, :pass})
          |> ReplicaModelScheduler.execute({:register, 40, :a, 0, 1})
          |> ReplicaModelScheduler.execute({:join, 40, :a, 0, 1})
          |> ReplicaModelScheduler.execute({:register, 47, :c, 1, 1})
          |> ReplicaModelScheduler.execute({:join, 47, :c, 1, 1})
          |> ReplicaModelScheduler.stabilize_and_assert!()
          |> ReplicaModelScheduler.execute({:transport, :a, :drop})
          |> ReplicaModelScheduler.execute({:kill, 40})

        scheduler =
          Enum.reduce(41..46, scheduler, fn owner_id, state ->
            state =
              ReplicaModelScheduler.execute(
                state,
                {:register, owner_id, :a, 0, owner_id}
              )

            if rem(owner_id, 2) == 0 do
              ReplicaModelScheduler.execute(state, {:unregister, owner_id, 0})
            else
              state
            end
          end)

        scheduler =
          scheduler
          |> run_schedule(schedule)
          |> ReplicaModelScheduler.stabilize_and_assert!()

        assert scheduler.model != nil
      after
        ReplicaModelScheduler.cleanup(scheduler)
      end
    end
  end

  property "named-cluster close and reopen fences stale epoch frames on real shards", %{
    nodes: nodes
  } do
    check all(
            cluster_slot <- integer(0..2),
            key_slot <- integer(0..2),
            before_reopen <- list_of(network_command(), max_length: 8),
            after_reopen <- list_of(network_command(), max_length: 12),
            max_runs: transition_runs(),
            max_shrinking_steps: 100
          ) do
      name = :"replica_authority_model_#{System.unique_integer([:positive])}"
      cluster = "model_cluster_#{cluster_slot}"
      opts = model_opts(name, self(), shards: 2, oplog: 2)
      start_groups(nodes, opts)
      scheduler = ReplicaModelScheduler.new(name, nodes, opts)

      try do
        await_discovery(nodes, name)

        scheduler =
          scheduler
          |> ReplicaModelScheduler.sync()
          |> ReplicaModelScheduler.execute({:connect, :a, cluster})
          |> ReplicaModelScheduler.execute({:connect, :b, cluster})
          |> ReplicaModelScheduler.execute({:connect, :c, cluster})
          |> ReplicaModelScheduler.execute({:register_cluster, 50, :a, cluster, key_slot, 1})
          |> ReplicaModelScheduler.execute({:join_cluster, 50, :a, cluster, key_slot, 1})
          |> ReplicaModelScheduler.execute({
            :register_cluster,
            52,
            :c,
            cluster,
            key_slot + 10,
            1
          })
          |> ReplicaModelScheduler.execute({
            :join_cluster,
            52,
            :c,
            cluster,
            key_slot + 10,
            1
          })
          |> run_schedule(before_reopen)
          |> ReplicaModelScheduler.execute({:disconnect, :a, cluster})
          |> ReplicaModelScheduler.execute({:connect, :a, cluster})
          |> ReplicaModelScheduler.execute(:deliver_all)
          |> ReplicaModelScheduler.execute({:register_cluster, 51, :a, cluster, key_slot, 2})
          |> ReplicaModelScheduler.execute({:join_cluster, 51, :a, cluster, key_slot, 2})
          |> run_schedule(after_reopen)
          |> ReplicaModelScheduler.stabilize_and_assert!()

        assert scheduler.model != nil
      after
        ReplicaModelScheduler.cleanup(scheduler)
      end
    end
  end

  defp command_history do
    list_of(command(), min_length: 1, max_length: @max_commands)
  end

  defp command do
    one_of([
      owner_command(:register),
      owner_command(:join),
      owner_key_command(:unregister),
      owner_key_command(:leave),
      map(owner_id(), &{:kill, &1}),
      transport_mode_command(),
      map(non_negative_integer(), &{:deliver, &1}),
      map(non_negative_integer(), &{:duplicate, &1}),
      map(non_negative_integer(), &{:drop, &1}),
      constant(:deliver_all),
      constant(:anti_entropy),
      constant(:flush)
    ])
  end

  defp network_command do
    one_of([
      transport_mode_command(),
      map(non_negative_integer(), &{:deliver, &1}),
      map(non_negative_integer(), &{:duplicate, &1}),
      map(non_negative_integer(), &{:drop, &1}),
      constant(:anti_entropy),
      constant(:flush)
    ])
  end

  defp owner_command(operation) do
    gen all(
          owner_id <- owner_id(),
          node_id <- member_of([:a, :b, :c]),
          slot <- integer(0..1),
          revision <- integer(0..3)
        ) do
      {operation, owner_id, node_id, slot, revision}
    end
  end

  defp owner_key_command(operation) do
    gen all(
          owner_id <- owner_id(),
          slot <- integer(0..1)
        ) do
      {operation, owner_id, slot}
    end
  end

  defp transport_mode_command do
    gen all(
          node_id <- member_of([:a, :b, :c]),
          mode <- member_of([:capture, :busy, :drop])
        ) do
      {:transport, node_id, mode}
    end
  end

  defp owner_id, do: integer(0..5)

  defp transition_runs, do: max(div(@model_runs, 3), 1)

  defp run_schedule(state, commands) do
    Enum.reduce(commands, state, fn command, acc ->
      ReplicaModelScheduler.execute(acc, command)
    end)
  end

  defp model_opts(name, controller, overrides) do
    [
      name: name,
      shards: Keyword.fetch!(overrides, :shards),
      resolve_registry_conflict: {ModelConflictResolver, :resolve, []},
      replica_transport: {ControlledReplicaTransport, controller: controller},
      replicated_sender_buffer_size: 1,
      replicated_anti_entropy_interval: 60_000,
      replicated_peer_lease_timeout: 120_000,
      replicated_snapshot_chunk_target_bytes: 256,
      replicated_oplog_max_entries: Keyword.fetch!(overrides, :oplog)
    ]
  end

  defp start_groups(nodes, opts) do
    Enum.each(nodes, fn {_id, node} ->
      {:ok, _pid} = TestCluster.start_group(node, opts)
    end)
  end

  defp await_discovery(nodes, name) do
    TestCluster.assert_eventually(
      fn ->
        Enum.all?(nodes, fn {_id, node} ->
          length(TestCluster.rpc!(node, Group, :nodes, [name])) == map_size(nodes) - 1
        end)
      end,
      timeout: 10_000
    )
  end
end
