defmodule Group.LocalCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  using do
    quote do
      import Group.LocalCase
      @moduletag :local
      @moduletag :capture_log
    end
  end

  setup do
    name = :"test_group_#{System.unique_integer([:positive])}"
    start_supervised!({Group, name: name, shards: 4, log: false})
    {:ok, name: name}
  end

  def start_single_shard_group(opts \\ []) do
    name = :"test_timeout_group_#{System.unique_integer([:positive])}"
    opts = Keyword.merge([name: name, shards: 1, log: false], opts)
    start_supervised!({Group, opts})
    name
  end

  def keys_for_shard(cluster, prefix, num_shards, shard, count) do
    1
    |> Stream.iterate(&(&1 + 1))
    |> Stream.map(&"#{prefix}/#{&1}")
    |> Stream.filter(&(Group.Replica.shard_index_for(cluster, &1, num_shards) == shard))
    |> Enum.take(count)
  end

  def replica_ingress_fairness_owner(parent) do
    receive do
      {:write, shard, request} ->
        ref = make_ref()
        send(shard, {:group_local_request, self(), ref, request})
        {reply, calls} = receive_local_write_with_trace(shard, ref, 0)
        send(parent, {:local_write_finished, self(), reply, calls})
        Process.sleep(:infinity)
    end
  end

  def membership_count_owner_loop do
    receive do
      {:membership_count_call, caller, ref, {:join, name, key, meta, opts}} ->
        send(caller, {ref, Group.join(name, key, meta, opts)})
        membership_count_owner_loop()

      {:membership_count_call, caller, ref, {:leave, name, key, _meta, opts}} ->
        send(caller, {ref, Group.leave(name, key, opts)})
        membership_count_owner_loop()
    end
  end

  def membership_count_owner_call(owner, request) do
    ref = make_ref()
    send(owner, {:membership_count_call, self(), ref, request})

    receive do
      {^ref, result} -> result
    after
      1_000 -> flunk("membership count owner call timed out")
    end
  end

  defp receive_local_write_with_trace(shard, ref, calls) do
    receive do
      {:trace, ^shard, :call,
       {Group.Replica, :handle_replica_message, [_state, _source_node, _message]}} ->
        receive_local_write_with_trace(shard, ref, calls + 1)

      {:group_local_reply, ^ref, reply} ->
        {reply, calls}
    end
  end

  def suspend_only_shard(name) do
    shard = Group.Replica.shard_name(name, 0)
    :ok = :sys.suspend(shard)
    shard
  end

  def resume_shard_if_alive(shard) do
    if Process.whereis(shard) do
      :ok = :sys.resume(shard)
    end

    :ok
  end

  def assert_genserver_call_timeout(fun) do
    assert {:timeout, {GenServer, :call, _}} = catch_exit(fun.())
  end

  def replicated_pg_join(cluster, key, pid, meta, reason) do
    {:replicate_pg_batch,
     [{:join, cluster, key, pid, meta, System.system_time(), reason, node(pid)}]}
  end

  def replicated_register(cluster, key, pid, meta, _reason, time \\ System.system_time()) do
    {:replicate_registry_batch, [{:register, cluster, key, pid, meta, time, node(pid)}]}
  end

  def spawn_requester(fun, tag) do
    parent = self()

    spawn(fn ->
      result = fun.()
      send(parent, {tag, self(), result})
      Process.sleep(:infinity)
    end)
  end

  def shard_message_queue_len(shard) do
    case Process.info(Process.whereis(shard), :message_queue_len) do
      {:message_queue_len, len} -> len
      nil -> 0
    end
  end

  def flush_replicated_registry_barrier(shard) do
    send(shard, {:group_dispatch, [self()], {:replicated_registry_buffer_flushed, shard}})
  end

  def force_cluster_lease_sweep(name) do
    lease_manager = Group.ClusterLease.lease_name(name)
    send(lease_manager, :force_sweep)
    :sys.get_state(lease_manager)
    :ok
  end

  def expire_cluster_lease(name, cluster) do
    {ttl_ms, _expires_at} = Group.Replica.Data.cluster_lease(name, cluster)

    Group.Replica.Data.put_cluster_lease(
      name,
      cluster,
      ttl_ms,
      System.monotonic_time(:millisecond) - 1
    )

    ttl_ms
  end

  def spawn_forever do
    spawn(fn -> Process.sleep(:infinity) end)
  end

  def kill_if_alive(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :kill)
    end

    :ok
  end

  def wait_until(fun, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true")
      end

      Process.sleep(10)
      do_wait_until(fun, deadline)
    end
  end
end
