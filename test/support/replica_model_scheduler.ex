defmodule Group.ReplicaModelScheduler do
  @moduledoc false

  alias Group.{ControlledReplicaTransport, ReplicaLifecycleModel, TestCluster}

  defmodule Envelope do
    @moduledoc false
    defstruct [:id, :source, :target, :shard, :frame]
  end

  defstruct [:name, :nodes, :model, :group_opts, owners: %{}, queue: [], next_frame_id: 1]

  def new(name, nodes, group_opts \\ []) do
    %__MODULE__{
      name: name,
      nodes: Map.new(nodes),
      model: ReplicaLifecycleModel.new(),
      group_opts: group_opts
    }
  end

  def execute(%__MODULE__{} = state, {:register, owner_id, node_id, slot, revision}) do
    with_owner(state, owner_id, node_id, fn state, _pid ->
      key = registration_key(owner_id, slot)
      meta = %{owner: owner_id, rank: owner_id, revision: revision}
      result = owner_call(state, owner_id, {:register, state.name, key, meta, []})

      model =
        ReplicaLifecycleModel.record_register(state.model, owner_id, {nil, key}, meta, result)

      sync(%{state | model: model})
    end)
  end

  def execute(%__MODULE__{} = state, {:claim, owner_id, node_id, key_slot, rank}) do
    with_owner(state, owner_id, node_id, fn state, _pid ->
      key = "model/conflict/#{key_slot}"
      meta = %{owner: owner_id, rank: rank}
      result = owner_call(state, owner_id, {:register, state.name, key, meta, []})

      model =
        ReplicaLifecycleModel.record_register(state.model, owner_id, {nil, key}, meta, result)

      sync(%{state | model: model})
    end)
  end

  def execute(%__MODULE__{} = state, {:unregister, owner_id, slot}) do
    with_existing_owner(state, owner_id, fn state, _pid ->
      key = registration_key(owner_id, slot)
      result = owner_call(state, owner_id, {:unregister, state.name, key, []})
      model = ReplicaLifecycleModel.record_unregister(state.model, owner_id, {nil, key}, result)
      sync(%{state | model: model})
    end)
  end

  def execute(%__MODULE__{} = state, {:join, owner_id, node_id, slot, revision}) do
    with_owner(state, owner_id, node_id, fn state, _pid ->
      key = membership_key(owner_id, slot)
      meta = %{owner: owner_id, revision: revision}
      result = owner_call(state, owner_id, {:join, state.name, key, meta, []})
      model = ReplicaLifecycleModel.record_join(state.model, owner_id, {nil, key}, meta, result)
      sync(%{state | model: model})
    end)
  end

  def execute(%__MODULE__{} = state, {:leave, owner_id, slot}) do
    with_existing_owner(state, owner_id, fn state, _pid ->
      key = membership_key(owner_id, slot)
      result = owner_call(state, owner_id, {:leave, state.name, key, []})
      model = ReplicaLifecycleModel.record_leave(state.model, owner_id, {nil, key}, result)
      sync(%{state | model: model})
    end)
  end

  def execute(
        %__MODULE__{} = state,
        {:register_cluster, owner_id, node_id, cluster, slot, revision}
      ) do
    with_owner(state, owner_id, node_id, fn state, _pid ->
      key = cluster_registration_key(owner_id, slot)
      meta = %{owner: owner_id, rank: owner_id, revision: revision, cluster: cluster}
      opts = [cluster: cluster]
      result = owner_call(state, owner_id, {:register, state.name, key, meta, opts})
      scope = {cluster, key}
      model = ReplicaLifecycleModel.record_register(state.model, owner_id, scope, meta, result)
      sync(%{state | model: model})
    end)
  end

  def execute(%__MODULE__{} = state, {:join_cluster, owner_id, node_id, cluster, slot, revision}) do
    with_owner(state, owner_id, node_id, fn state, _pid ->
      key = cluster_membership_key(owner_id, slot)
      meta = %{owner: owner_id, revision: revision, cluster: cluster}
      opts = [cluster: cluster]
      result = owner_call(state, owner_id, {:join, state.name, key, meta, opts})
      scope = {cluster, key}
      model = ReplicaLifecycleModel.record_join(state.model, owner_id, scope, meta, result)
      sync(%{state | model: model})
    end)
  end

  def execute(%__MODULE__{} = state, {:connect, node_id, cluster}) do
    node = Map.fetch!(state.nodes, node_id)
    :ok = TestCluster.rpc!(node, Group, :connect, [state.name, cluster])
    sync(state)
  end

  def execute(%__MODULE__{} = state, {:disconnect, node_id, cluster}) do
    node = Map.fetch!(state.nodes, node_id)
    :ok = TestCluster.rpc!(node, Group, :disconnect, [state.name, cluster])
    model = ReplicaLifecycleModel.disconnect_cluster(state.model, node, cluster)
    sync(%{state | model: model})
  end

  def execute(%__MODULE__{} = state, {:kill, owner_id}) do
    case Map.get(state.owners, owner_id) do
      nil ->
        state

      %{pid: pid, node: node} ->
        if remote_alive?(node, pid) do
          true = TestCluster.rpc!(node, Process, :exit, [pid, :kill])
        end

        state
        |> Map.update!(:model, &ReplicaLifecycleModel.kill(&1, owner_id))
        |> sync()
    end
  end

  def execute(%__MODULE__{} = state, {:transport, node_id, mode}) do
    node = Map.fetch!(state.nodes, node_id)
    :ok = TestCluster.rpc!(node, ControlledReplicaTransport, :set_mode, [state.name, mode])
    state
  end

  def execute(%__MODULE__{} = state, {:deliver, selector}) do
    state
    |> sync()
    |> take_envelope(selector, fn state, envelope ->
      deliver_envelope(state, envelope, 1)
    end)
  end

  def execute(%__MODULE__{} = state, {:duplicate, selector}) do
    state
    |> sync()
    |> take_envelope(selector, fn state, envelope ->
      deliver_envelope(state, envelope, 2)
    end)
  end

  def execute(%__MODULE__{} = state, {:drop, selector}) do
    state
    |> sync()
    |> take_envelope(selector, fn state, _envelope -> state end)
  end

  def execute(%__MODULE__{} = state, :deliver_all) do
    state = sync(state)
    envelopes = state.queue

    Enum.reduce(envelopes, %{state | queue: []}, fn envelope, acc ->
      deliver_envelope(acc, envelope, 1)
    end)
  end

  def execute(%__MODULE__{} = state, {:restart, node_id}) do
    state = sync(state)
    node = Map.fetch!(state.nodes, node_id)
    :ok = stop_group_local(node, state.name)
    model = ReplicaLifecycleModel.restart_node(state.model, node)
    {:ok, _pid} = TestCluster.start_group(node, state.group_opts)
    state = %{state | model: model}

    TestCluster.assert_eventually(
      fn ->
        Enum.all?(state.nodes, fn {_id, peer} ->
          expected = map_size(state.nodes) - 1
          length(TestCluster.rpc!(peer, Group, :nodes, [state.name])) == expected
        end)
      end,
      timeout: 10_000,
      interval: 25
    )

    sync(state)
  end

  def execute(%__MODULE__{} = state, :anti_entropy) do
    Enum.each(state.nodes, fn {_id, node} ->
      TestCluster.rpc!(node, __MODULE__, :trigger_anti_entropy_local, [state.name])
    end)

    sync(state)
  end

  def execute(%__MODULE__{} = state, :flush), do: sync(state)

  def stabilize_and_assert!(%__MODULE__{} = state) do
    state = sync(state)

    Enum.each(state.nodes, fn {_id, node} ->
      :ok = TestCluster.rpc!(node, ControlledReplicaTransport, :set_mode, [state.name, :pass])
    end)

    expected = ReplicaLifecycleModel.resolve_registry_conflicts(state.model)
    state = %{state | model: expected, queue: []}

    TestCluster.assert_eventually(
      fn ->
        pump_anti_entropy(state)
        converged?(state)
      end,
      timeout: 15_000,
      interval: 25
    )

    Enum.each(state.nodes, fn {_id, node} ->
      :ok = TestCluster.rpc!(node, TestCluster, :assert_replica_consistent, [state.name])
    end)

    assert_expected_owner_lifecycle!(state)
    assert_no_dead_retained_owners!(state)
    state
  end

  def cleanup(%__MODULE__{} = state) do
    Enum.each(state.nodes, fn {_id, node} ->
      TestCluster.rpc!(node, __MODULE__, :cleanup_owners_local, [state.name])
      TestCluster.rpc!(node, ControlledReplicaTransport, :clear, [state.name])
      stop_group_local(node, state.name)
    end)

    drain_transport_messages(state.name)
    :ok
  end

  def sync(%__MODULE__{} = state) do
    Enum.each(state.nodes, fn {_id, node} ->
      TestCluster.flush_shards(node, state.name)
    end)

    drain(state, 2)
  end

  def drain(%__MODULE__{} = state, wait_ms \\ 0) do
    receive do
      {ControlledReplicaTransport, :frame, group, source, target, shard, frame}
      when group == state.name ->
        envelope = %Envelope{
          id: state.next_frame_id,
          source: source,
          target: target,
          shard: shard,
          frame: frame
        }

        drain(
          %{state | queue: state.queue ++ [envelope], next_frame_id: state.next_frame_id + 1},
          wait_ms
        )
    after
      wait_ms -> state
    end
  end

  def trigger_anti_entropy_local(name) do
    num_shards = Group.get_config(name).num_shards

    Enum.each(0..(num_shards - 1), fn shard ->
      shard_name = Group.Replica.shard_name(name, shard)
      state = :sys.get_state(shard_name)
      send(shard_name, {:group_replica_anti_entropy, state.anti_entropy_ref})
    end)

    :ok
  end

  def spawn_owner(name) do
    pid = spawn(fn -> owner_loop() end)
    key = {__MODULE__, :owners, name}
    owners = :persistent_term.get(key, [])
    :persistent_term.put(key, [pid | owners])
    pid
  end

  def cleanup_owners_local(name) do
    key = {__MODULE__, :owners, name}

    key
    |> :persistent_term.get([])
    |> Enum.each(fn pid ->
      if Process.alive?(pid), do: Process.exit(pid, :kill)
    end)

    :persistent_term.erase(key)
    :ok
  end

  def call_owner(pid, operation) do
    if Process.alive?(pid) do
      ref = make_ref()
      send(pid, {__MODULE__, :call, self(), ref, operation})

      receive do
        {__MODULE__, :reply, ^ref, result} -> result
      after
        5_000 -> raise "model owner #{inspect(pid)} did not answer #{inspect(operation)}"
      end
    else
      {:error, :owner_dead}
    end
  end

  defp owner_loop do
    receive do
      {__MODULE__, :call, caller, ref, operation} ->
        result = apply_owner_operation(operation)
        send(caller, {__MODULE__, :reply, ref, result})
        owner_loop()
    end
  end

  defp apply_owner_operation({:register, name, key, meta, opts}),
    do: Group.register(name, key, meta, opts)

  defp apply_owner_operation({:unregister, name, key, opts}),
    do: Group.unregister(name, key, opts)

  defp apply_owner_operation({:join, name, key, meta, opts}),
    do: Group.join(name, key, meta, opts)

  defp apply_owner_operation({:leave, name, key, opts}),
    do: Group.leave(name, key, opts)

  defp with_owner(state, owner_id, node_id, fun) do
    case Map.get(state.owners, owner_id) do
      nil ->
        node = Map.fetch!(state.nodes, node_id)
        pid = TestCluster.rpc!(node, __MODULE__, :spawn_owner, [state.name])
        model = ReplicaLifecycleModel.put_owner(state.model, owner_id, node)

        state = %{
          state
          | owners: Map.put(state.owners, owner_id, %{node: node, pid: pid}),
            model: model
        }

        fun.(state, pid)

      %{node: node, pid: pid} ->
        if remote_alive?(node, pid), do: fun.(state, pid), else: state
    end
  end

  defp with_existing_owner(state, owner_id, fun) do
    case Map.get(state.owners, owner_id) do
      nil ->
        state

      %{node: node, pid: pid} ->
        if remote_alive?(node, pid), do: fun.(state, pid), else: state
    end
  end

  defp owner_call(state, owner_id, operation) do
    %{node: node, pid: pid} = Map.fetch!(state.owners, owner_id)

    case TestCluster.rpc!(node, __MODULE__, :call_owner, [pid, operation]) do
      {:error, :owner_dead} ->
        raise "model owner #{owner_id} died outside an expected lifecycle transition"

      result ->
        result
    end
  end

  defp take_envelope(%{queue: []} = state, _selector, _fun), do: state

  defp take_envelope(state, selector, fun) do
    index = rem(selector, length(state.queue))
    {envelope, queue} = List.pop_at(state.queue, index)
    fun.(%{state | queue: queue}, envelope)
  end

  defp deliver_envelope(state, envelope, times) do
    Enum.each(1..times, fn _ ->
      :ok =
        TestCluster.rpc!(
          envelope.target,
          Group.Replica.Transport,
          :deliver,
          [state.name, envelope.source, envelope.shard, envelope.frame]
        )

      TestCluster.flush_shards(envelope.target, state.name)
    end)

    drain(state, 2)
  end

  defp pump_anti_entropy(state) do
    Enum.each(state.nodes, fn {_id, node} ->
      TestCluster.rpc!(node, __MODULE__, :trigger_anti_entropy_local, [state.name])
    end)

    Enum.each(1..2, fn _ ->
      Enum.each(state.nodes, fn {_id, node} ->
        TestCluster.flush_shards(node, state.name)
      end)
    end)
  end

  defp converged?(state) do
    expected_registrations = ReplicaLifecycleModel.expected_registrations(state.model)
    expected_memberships = ReplicaLifecycleModel.expected_memberships(state.model)
    pid_to_owner = Map.new(state.owners, fn {owner_id, %{pid: pid}} -> {pid, owner_id} end)

    Enum.all?(state.nodes, fn {_node_id, node} ->
      registrations_match?(
        node,
        state.name,
        state.model.seen_registration_keys,
        expected_registrations,
        pid_to_owner
      ) and
        memberships_match?(
          node,
          state.name,
          state.model.seen_membership_keys,
          expected_memberships,
          pid_to_owner
        )
    end)
  end

  defp registrations_match?(node, name, keys, expected, pid_to_owner) do
    Enum.all?(keys, fn {cluster, key} = scope ->
      actual =
        case TestCluster.rpc!(node, Group, :lookup, [name, key, cluster_opts(cluster)]) do
          nil -> nil
          {pid, meta} -> {Map.get(pid_to_owner, pid, {:unknown_pid, pid}), meta}
        end

      actual == Map.get(expected, scope)
    end)
  end

  defp memberships_match?(node, name, keys, expected, pid_to_owner) do
    Enum.all?(keys, fn {cluster, key} = scope ->
      actual =
        node
        |> TestCluster.rpc!(Group, :members, [name, key, cluster_opts(cluster)])
        |> Enum.map(fn {pid, meta} -> {Map.get(pid_to_owner, pid, {:unknown_pid, pid}), meta} end)
        |> Enum.sort()

      actual == Map.get(expected, scope, [])
    end)
  end

  defp assert_no_dead_retained_owners!(state) do
    retained =
      state.nodes
      |> Enum.flat_map(fn {_id, node} ->
        TestCluster.rpc!(node, TestCluster, :replica_owner_pids, [state.name])
      end)
      |> Enum.uniq()

    dead =
      Enum.reject(retained, fn pid -> TestCluster.rpc!(node(pid), Process, :alive?, [pid]) end)

    if dead != [], do: raise("dead owners retained after convergence: #{inspect(dead)}")
  end

  defp assert_expected_owner_lifecycle!(state) do
    mismatches =
      Enum.flat_map(state.model.owners, fn {owner_id, %{alive?: expected_alive?}} ->
        %{node: node, pid: pid} = Map.fetch!(state.owners, owner_id)
        actual_alive? = remote_alive?(node, pid)

        if expected_alive? == actual_alive? do
          []
        else
          [{owner_id, expected_alive?, actual_alive?, pid}]
        end
      end)

    if mismatches != [] do
      raise "owner lifecycle diverged from model: #{inspect(mismatches)}"
    end
  end

  defp remote_alive?(node, pid), do: TestCluster.rpc!(node, Process, :alive?, [pid])

  defp stop_group_local(node, name) do
    case TestCluster.rpc!(node, Process, :whereis, [:"#{name}_group_sup"]) do
      nil -> :ok
      pid -> TestCluster.rpc!(node, Supervisor, :stop, [pid, :normal, 5_000])
    end
  catch
    :exit, _ -> :ok
  end

  defp drain_transport_messages(name) do
    receive do
      {ControlledReplicaTransport, :frame, ^name, _source, _target, _shard, _frame} ->
        drain_transport_messages(name)
    after
      0 -> :ok
    end
  end

  defp registration_key(owner_id, slot), do: "model/reg/#{owner_id}/#{slot}"
  defp membership_key(owner_id, slot), do: "model/pg/#{owner_id}/#{slot}"
  defp cluster_registration_key(owner_id, slot), do: "model/cluster/reg/#{owner_id}/#{slot}"
  defp cluster_membership_key(owner_id, slot), do: "model/cluster/pg/#{owner_id}/#{slot}"
  defp cluster_opts(nil), do: []
  defp cluster_opts(cluster), do: [cluster: cluster]
end
