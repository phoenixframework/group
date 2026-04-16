defmodule GroupBench.Replica do
  @moduledoc """
  Replica role for distributed benchmarks.

  Started as a separate BEAM VM by the coordinator. Also provides helper
  functions that the coordinator calls via :erpc.call MFA to avoid anonymous
  function serialization issues across nodes.
  """

  def start do
    Application.ensure_all_started(:group)
    IO.puts("[replica] #{node()} ready")
    Process.sleep(:infinity)
  end

  @doc """
  Starts a Group instance, unlinks from caller (safe for :erpc).
  """
  def start_group(opts) do
    {:ok, pid} = Group.start_link(opts)
    Process.unlink(pid)
    {:ok, pid}
  end

  @doc """
  Stops a Group instance by supervisor name.
  """
  def stop_group(name) do
    sup_name = :"#{name}_group_sup"

    case Process.whereis(sup_name) do
      nil -> :ok
      pid -> Supervisor.stop(pid)
    end
  end

  @doc """
  Registers N keys from spawned processes. Each process calls Group.register
  on its own behalf (register uses self()). Returns list of pids.
  """
  def bulk_register(name, n, key_prefix, opts \\ []) do
    parent = self()

    pids =
      Enum.map(1..n, fn i ->
        spawn(fn ->
          :ok = Group.register(name, "#{key_prefix}#{i}", %{}, opts)
          send(parent, {:done, self()})
          Process.sleep(:infinity)
        end)
      end)

    Enum.each(pids, fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        30_000 -> raise "Timed out waiting for bulk_register"
      end
    end)

    pids
  end

  @doc """
  Counts total registry entries across all shards (all nodes, not just local).
  """
  def total_registry_count(name) do
    num_shards = Group.get_config(name).num_shards

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = Group.Replica.Data.reg_by_key_table(name, shard)
      acc + :ets.info(table, :size)
    end)
  end

  @doc """
  Registers a single key from a spawned process. Returns after registration.
  """
  def spawn_register(name, key, opts \\ []) do
    parent = self()

    spawn(fn ->
      :ok = Group.register(name, key, %{}, opts)
      send(parent, :registered)
      Process.sleep(:infinity)
    end)

    receive do
      :registered -> :ok
    after
      5_000 -> raise "spawn_register timed out"
    end
  end

  @doc """
  Spawns N processes, each joining the same group key. Returns list of pids.
  """
  def bulk_join(name, n, key, opts \\ []) do
    parent = self()

    pids =
      Enum.map(1..n, fn _i ->
        spawn(fn ->
          :ok = Group.join(name, key, %{}, opts)
          send(parent, {:done, self()})
          Process.sleep(:infinity)
        end)
      end)

    Enum.each(pids, fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        30_000 -> raise "Timed out waiting for bulk_join"
      end
    end)

    pids
  end

  @doc """
  Kills a list of processes.
  """
  def kill_processes(pids) do
    Enum.each(pids, &Process.exit(&1, :kill))
    :ok
  end

  @doc """
  Counts total pg entries across all shards.
  """
  def total_pg_count(name) do
    num_shards = Group.get_config(name).num_shards

    Enum.reduce(0..(num_shards - 1), 0, fn shard, acc ->
      table = Group.Replica.Data.pg_by_key_table(name, shard)
      acc + :ets.info(table, :size)
    end)
  end

  @doc """
  Starts `worker_count` local processes that repeatedly re-join the same key with
  changing metadata, generating a sustained stream of replicated PG updates.
  Returns the spammer pids after they complete their initial join.
  """
  def start_pg_update_spammers(name, worker_count, key_prefix, opts \\ []) do
    parent = self()

    pids =
      Enum.map(1..worker_count, fn i ->
        spawn(fn ->
          key = "#{key_prefix}#{i}"
          :ok = Group.join(name, key, %{seq: 0}, opts)
          send(parent, {:ready, self()})
          pg_update_spam_loop(name, key, 1, opts)
        end)
      end)

    Enum.each(pids, fn pid ->
      receive do
        {:ready, ^pid} -> :ok
      after
        30_000 -> raise "Timed out waiting for start_pg_update_spammers"
      end
    end)

    pids
  end

  @doc """
  Starts `worker_count` local processes that repeatedly re-register the same key
  with changing metadata, generating a sustained stream of replicated registry updates.
  Returns the spammer pids after they complete their initial register.
  """
  def start_registry_update_spammers(name, worker_count, key_prefix, opts \\ []) do
    parent = self()

    pids =
      Enum.map(1..worker_count, fn i ->
        spawn(fn ->
          key = "#{key_prefix}#{i}"
          :ok = Group.register(name, key, %{seq: 0}, opts)
          send(parent, {:ready, self()})
          registry_update_spam_loop(name, key, 1, opts)
        end)
      end)

    Enum.each(pids, fn pid ->
      receive do
        {:ready, ^pid} -> :ok
      after
        30_000 -> raise "Timed out waiting for start_registry_update_spammers"
      end
    end)

    pids
  end

  defp pg_update_spam_loop(name, key, seq, opts) do
    :ok = Group.join(name, key, %{seq: seq}, opts)
    pg_update_spam_loop(name, key, seq + 1, opts)
  end

  defp registry_update_spam_loop(name, key, seq, opts) do
    :ok = Group.register(name, key, %{seq: seq}, opts)
    registry_update_spam_loop(name, key, seq + 1, opts)
  end

  @doc """
  Returns the message queue length for the given shard.
  """
  def shard_message_queue_len(name, shard_index) do
    shard = Group.Replica.shard_name(name, shard_index)

    case Process.info(Process.whereis(shard), :message_queue_len) do
      {:message_queue_len, len} -> len
      nil -> 0
    end
  end

  @doc """
  Returns the current receiver-pressure snapshot for a shard.
  """
  def replicated_pg_pressure(name, shard_index) do
    shard = Group.Replica.shard_name(name, shard_index)
    queue_len = shard_message_queue_len(name, shard_index)
    state = :sys.get_state(shard)

    %{
      message_queue_len: queue_len,
      pending_replicated_pg_len: state.pending_replicated_pg_len
    }
  end

  @doc """
  Returns the current replicated registry receiver-pressure snapshot for a shard.
  Works against older code that does not yet expose a registry receiver buffer.
  """
  def replicated_registry_pressure(name, shard_index) do
    shard = Group.Replica.shard_name(name, shard_index)
    queue_len = shard_message_queue_len(name, shard_index)
    state = :sys.get_state(shard)

    %{
      message_queue_len: queue_len,
      pending_replicated_registry_len: Map.get(state, :pending_replicated_registry_len, 0)
    }
  end

  @doc """
  Collects local register latency samples on this node under the current load.
  Uses unique keys and leaves the owner processes alive until the caller kills
  them, so each sample measures the register call itself under concurrent local
  load.
  """
  def local_register_samples(name, sample_count, key_prefix, opts \\ []) do
    parent = self()

    pids =
      Enum.map(1..sample_count, fn i ->
        spawn(fn ->
          key = "#{key_prefix}#{i}"
          {us, result} = :timer.tc(fn -> Group.register(name, key, %{sample: i}, opts) end)
          send(parent, {:sample, self(), us, result})
          Process.sleep(:infinity)
        end)
      end)

    {samples, completed_pids} =
      Enum.reduce(1..sample_count, {[], []}, fn _, {samples, completed_pids} ->
        receive do
          {:sample, pid, us, :ok} -> {[us | samples], [pid | completed_pids]}
          {:sample, _pid, _us, other} -> raise "register sample failed: #{inspect(other)}"
        after
          30_000 -> raise "Timed out waiting for local_register_samples"
        end
      end)

    completed_pids =
      MapSet.new(completed_pids)

    if Enum.any?(pids, &(not MapSet.member?(completed_pids, &1))) do
      raise "local_register_samples lost sample pids"
    end

    samples = Enum.sort(samples)

    {samples, pids}
  end

  @doc """
  Collects local join latency samples on this node under the current load.
  Uses unique keys and leaves the member processes alive until the caller kills
  them, so each sample measures the join call itself under concurrent local
  load.
  """
  def local_join_samples(name, sample_count, key_prefix, opts \\ []) do
    parent = self()

    pids =
      Enum.map(1..sample_count, fn i ->
        spawn(fn ->
          key = "#{key_prefix}#{i}"
          {us, result} = :timer.tc(fn -> Group.join(name, key, %{sample: i}, opts) end)
          send(parent, {:sample, self(), us, result})
          Process.sleep(:infinity)
        end)
      end)

    {samples, completed_pids} =
      Enum.reduce(1..sample_count, {[], []}, fn _, {samples, completed_pids} ->
        receive do
          {:sample, pid, us, :ok} -> {[us | samples], [pid | completed_pids]}
          {:sample, _pid, _us, other} -> raise "join sample failed: #{inspect(other)}"
        after
          30_000 -> raise "Timed out waiting for local_join_samples"
        end
      end)

    completed_pids =
      MapSet.new(completed_pids)

    if Enum.any?(pids, &(not MapSet.member?(completed_pids, &1))) do
      raise "local_join_samples lost sample pids"
    end

    samples = Enum.sort(samples)

    {samples, pids}
  end

  @doc """
  Runs a fixed-inflight local register load test under the current receiver load.
  Each attempted operation uses a unique key and a fresh owner process so the
  benchmark exercises the one-shot public API path without interleaving cleanup.
  """
  def local_register_load(name, total_ops, max_inflight, key_prefix, opts \\ []) do
    run_local_load(:register, name, total_ops, max_inflight, key_prefix, opts)
  end

  @doc """
  Runs a fixed-inflight local join load test under the current receiver load.
  Each attempted operation uses a unique key and a fresh member process so the
  benchmark exercises the one-shot public API path without interleaving cleanup.
  """
  def local_join_load(name, total_ops, max_inflight, key_prefix, opts \\ []) do
    run_local_load(:join, name, total_ops, max_inflight, key_prefix, opts)
  end

  @doc """
  Collects local named-cluster connect latency samples on this node under the
  current load. Each sample connects to a fresh cluster name.
  """
  def local_connect_samples(name, sample_count, cluster_prefix, opts \\ []) do
    Enum.map(1..sample_count, fn i ->
      cluster = "#{cluster_prefix}#{i}"

      {us, result} =
        :timer.tc(fn ->
          Group.connect(name, cluster, opts)
        end)

      case result do
        :ok -> us
        other -> raise "connect sample failed: #{inspect(other)}"
      end
    end)
    |> Enum.sort()
  end

  defp run_local_load(kind, name, total_ops, max_inflight, key_prefix, opts)
       when total_ops > 0 and max_inflight > 0 do
    parent = self()
    initial_inflight = min(total_ops, max_inflight)

    next_index =
      Enum.reduce(1..initial_inflight, 1, fn index, _next_index ->
        spawn_local_load_sample(kind, parent, name, key_prefix, index, opts)
        index + 1
      end)

    started_at = System.monotonic_time(:microsecond)

    collect_local_load_results(
      kind,
      name,
      total_ops,
      max_inflight,
      key_prefix,
      opts,
      %{
        completed_ops: 0,
        inflight: initial_inflight,
        next_index: next_index,
        successful_ops: 0,
        timeout_count: 0,
        error_count: 0,
        samples: [],
        pids: []
      },
      started_at
    )
  end

  defp collect_local_load_results(
         _kind,
         _name,
         total_ops,
         _max_inflight,
         _key_prefix,
         _opts,
         %{completed_ops: total_ops, inflight: 0} = acc,
         started_at
       ) do
    wall_us = System.monotonic_time(:microsecond) - started_at

    %{
      wall_us: wall_us,
      successful_ops: acc.successful_ops,
      timeout_count: acc.timeout_count,
      error_count: acc.error_count,
      samples: Enum.sort(acc.samples),
      pids: Enum.reverse(acc.pids)
    }
  end

  defp collect_local_load_results(
         kind,
         name,
         total_ops,
         max_inflight,
         key_prefix,
         opts,
         acc,
         started_at
       ) do
    receive do
      {:local_load_sample, pid, us, outcome} ->
        acc =
          case outcome do
            :ok ->
              %{
                acc
                | completed_ops: acc.completed_ops + 1,
                  successful_ops: acc.successful_ops + 1,
                  samples: [us | acc.samples],
                  pids: [pid | acc.pids]
              }

            {:exit, {:timeout, _genserver_call}} ->
              %{
                acc
                | completed_ops: acc.completed_ops + 1,
                  timeout_count: acc.timeout_count + 1
              }

            {:error, _reason} ->
              %{
                acc
                | completed_ops: acc.completed_ops + 1,
                  error_count: acc.error_count + 1
              }

            {:exit, _reason} ->
              %{
                acc
                | completed_ops: acc.completed_ops + 1,
                  error_count: acc.error_count + 1
              }
          end

        acc =
          if acc.next_index <= total_ops do
            spawn_local_load_sample(kind, self(), name, key_prefix, acc.next_index, opts)
            %{acc | next_index: acc.next_index + 1}
          else
            %{acc | inflight: acc.inflight - 1}
          end

        collect_local_load_results(
          kind,
          name,
          total_ops,
          max_inflight,
          key_prefix,
          opts,
          acc,
          started_at
        )
    after
      60_000 ->
        raise "Timed out waiting for #{kind} load samples"
    end
  end

  defp spawn_local_load_sample(kind, parent, name, key_prefix, index, opts) do
    spawn(fn ->
      key = "#{key_prefix}#{index}"

      {us, outcome} =
        :timer.tc(fn ->
          try do
            case kind do
              :register -> Group.register(name, key, %{sample: index}, opts)
              :join -> Group.join(name, key, %{sample: index}, opts)
            end
          catch
            :exit, reason -> {:exit, reason}
          end
        end)

      outcome =
        case outcome do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
          {:exit, reason} -> {:exit, reason}
          other -> {:error, other}
        end

      send(parent, {:local_load_sample, self(), us, outcome})

      if outcome == :ok do
        Process.sleep(:infinity)
      end
    end)
  end

  @doc """
  Connects N named clusters concurrently (one per caller, like real usage).
  """
  def bulk_connect(name, n, prefix) do
    parent = self()

    Enum.map(1..n, fn i ->
      spawn(fn ->
        :ok = Group.connect(name, "#{prefix}#{i}")
        send(parent, {:done, self()})
      end)
    end)
    |> Enum.each(fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        120_000 -> raise "Timed out waiting for bulk_connect"
      end
    end)
  end

  @doc """
  Disconnects N named clusters concurrently (one per caller, like real usage).
  """
  def bulk_disconnect(name, n, prefix) do
    parent = self()

    Enum.map(1..n, fn i ->
      spawn(fn ->
        :ok = Group.disconnect(name, "#{prefix}#{i}")
        send(parent, {:done, self()})
      end)
    end)
    |> Enum.each(fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        120_000 -> raise "Timed out waiting for bulk_disconnect"
      end
    end)
  end

  @doc """
  Spawns N processes, each registering key "key" in a distinct cluster.
  Returns list of pids.
  """
  def bulk_register_per_cluster(name, n, prefix) do
    parent = self()

    pids =
      Enum.map(1..n, fn i ->
        spawn(fn ->
          :ok = Group.register(name, "key", %{}, cluster: "#{prefix}#{i}")
          send(parent, {:done, self()})
          Process.sleep(:infinity)
        end)
      end)

    Enum.each(pids, fn pid ->
      receive do
        {:done, ^pid} -> :ok
      after
        60_000 -> raise "Timed out waiting for bulk_register_per_cluster"
      end
    end)

    pids
  end

  @doc """
  Returns the number of clusters this node is a member of (via reverse index).
  """
  def my_cluster_count(name) do
    length(Group.Replica.Data.my_clusters(name))
  end

  @doc """
  Simulates a busy app worker: registers, joins groups, does lookups and
  dispatches, then some processes die and re-register. Returns final pids.

  Each worker operates in its own cluster (like an org scope), performing
  a mix of operations that mirrors real app usage patterns.
  """
  def run_busy_worker(name, cluster, worker_id, opts) do
    num_users = Keyword.get(opts, :users, 20)
    num_rooms = Keyword.get(opts, :rooms, 5)
    churn_rounds = Keyword.get(opts, :churn_rounds, 3)
    lookups_per_round = Keyword.get(opts, :lookups_per_round, 50)
    dispatches_per_round = Keyword.get(opts, :dispatches_per_round, 10)
    parent = self()

    # Phase 1: Register users
    user_pids =
      Enum.map(1..num_users, fn i ->
        spawn(fn ->
          key = "w#{worker_id}/user/#{i}"
          :ok = Group.register(name, key, %{worker: worker_id, i: i}, cluster: cluster)
          send(parent, {:reg_done, self()})
          Process.sleep(:infinity)
        end)
      end)

    Enum.each(user_pids, fn pid ->
      receive do
        {:reg_done, ^pid} -> :ok
      after
        30_000 -> raise "busy_worker register timed out"
      end
    end)

    # Phase 2: Join rooms
    room_pids =
      Enum.map(1..num_rooms, fn room_i ->
        Enum.map(1..div(num_users, num_rooms), fn member_i ->
          spawn(fn ->
            key = "w#{worker_id}/room/#{room_i}"
            :ok = Group.join(name, key, %{seat: member_i}, cluster: cluster)
            send(parent, {:join_done, self()})
            Process.sleep(:infinity)
          end)
        end)
      end)
      |> List.flatten()

    Enum.each(room_pids, fn pid ->
      receive do
        {:join_done, ^pid} -> :ok
      after
        30_000 -> raise "busy_worker join timed out"
      end
    end)

    # Phase 3: Churn — interleave lookups, dispatches, kills, re-registers
    final_user_pids =
      Enum.reduce(1..churn_rounds, user_pids, fn round, current_pids ->
        # Lookups (read pressure)
        for i <- 1..lookups_per_round do
          key = "w#{worker_id}/user/#{rem(i, num_users) + 1}"
          Group.lookup(name, key, cluster: cluster)
        end

        # Dispatches (fan-out pressure)
        for i <- 1..dispatches_per_round do
          key = "w#{worker_id}/room/#{rem(i, num_rooms) + 1}"
          Group.dispatch(name, key, {:ping, round, i}, cluster: cluster)
        end

        # Kill ~25% of user pids
        kill_count = div(length(current_pids), 4)
        {to_kill, to_keep} = Enum.split(current_pids, kill_count)
        Enum.each(to_kill, &Process.exit(&1, :kill))

        # Re-register replacements
        new_pids =
          Enum.map(1..kill_count, fn i ->
            spawn(fn ->
              key = "w#{worker_id}/user/r#{round}_#{i}"
              :ok = Group.register(name, key, %{round: round}, cluster: cluster)
              send(parent, {:rereg_done, self()})
              Process.sleep(:infinity)
            end)
          end)

        Enum.each(new_pids, fn pid ->
          receive do
            {:rereg_done, ^pid} -> :ok
          after
            30_000 -> raise "busy_worker re-register timed out"
          end
        end)

        to_keep ++ new_pids
      end)

    final_user_pids ++ room_pids
  end
end
