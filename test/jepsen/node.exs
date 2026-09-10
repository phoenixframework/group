Code.require_file("../support/test_tcp_transport.ex", __DIR__)

defmodule Group.Jepsen.Transport.Stats do
  @moduledoc false
  use GenServer

  @table :group_jepsen_transport_stats
  @gate :group_jepsen_transport_gate
  @persistent_event_log "/tmp/group-jepsen-persistent-events"

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def increment(event, amount \\ 1) do
    :ets.update_counter(@table, event, {2, amount}, {event, 0})
    :ok
  rescue
    ArgumentError -> :ok
  end

  def increment_persistent(event, amount \\ 1) do
    increment(event, amount)
    File.write!(@persistent_event_log, "#{event}\t#{amount}\n", [:append])
    :ok
  end

  def observe_max(event, value) when is_integer(value) and value >= 0 do
    current = :ets.update_counter(@table, event, {2, 0}, {event, 0})
    if value > current, do: :ets.insert(@table, {event, value})
    :ok
  rescue
    ArgumentError -> :ok
  end

  def snapshot do
    persisted = persistent_events()

    @table
    |> :ets.tab2list()
    |> Map.new()
    |> Map.merge(persisted, fn _event, current, durable -> max(current, durable) end)
  end

  def block(target_node), do: :ets.insert(@gate, {target_node})
  def unblock(target_node), do: :ets.delete(@gate, target_node)
  def heal, do: :ets.delete_all_objects(@gate)
  def blocked?(target_node), do: :ets.member(@gate, target_node)

  @impl true
  def init(:ok) do
    _table = :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    _gate = :ets.new(@gate, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  defp persistent_events do
    case File.read(@persistent_event_log) do
      {:ok, contents} ->
        contents
        |> String.split("\n", trim: true)
        |> Enum.reduce(%{}, fn line, events ->
          case String.split(line, "\t", parts: 2) do
            [event, amount] ->
              Map.update(
                events,
                String.to_existing_atom(event),
                String.to_integer(amount),
                &(&1 + String.to_integer(amount))
              )

            _invalid ->
              events
          end
        end)

      {:error, :enoent} ->
        %{}
    end
  end
end

defmodule Group.Jepsen.Transport.Common do
  @moduledoc false
  alias Group.Jepsen.Transport.Stats

  def outgoing(delegate, group, target_node, shard, message, opts) do
    record(message)

    if Stats.blocked?(target_node) do
      Stats.increment(:logical_drop)
      :ok
    else
      result = delegate.outgoing(group, target_node, shard, message, opts)
      Stats.increment(transport_result(result))
      observe_outbox(group, shard)
      result
    end
  end

  def record({:snapshot_chunk, _version, _stream, _seq, _index, _registry, _pg}) do
    Stats.increment(:snapshot_chunk)
  end

  def record({:snapshot_commit, _version, _stream, _seq, chunk_count, _, _}) do
    Stats.increment(:snapshot_commit)

    if chunk_count > 1 do
      Stats.increment(:multi_chunk_snapshot)
    end
  end

  def record({:delta_batch, _version, runs}) do
    Stats.increment(:delta_batch)

    peak =
      runs
      |> Enum.map(fn
        {_stream, _first_seq, records, _head} when is_list(records) -> length(records)
        _invalid_run -> 0
      end)
      |> Enum.max(fn -> 0 end)

    Stats.observe_max(:delta_run_records_peak, peak)
  end

  def record(_message), do: Stats.increment(:other_message)

  defp transport_result(:ok), do: :transport_ok
  defp transport_result(:busy), do: :transport_busy
  defp transport_result(:disconnected), do: :transport_disconnected

  defp observe_outbox(group, shard) do
    case Process.whereis(Group.Transport.Outbox.name(group, shard)) do
      pid when is_pid(pid) ->
        case Process.info(pid, :message_queue_len) do
          {:message_queue_len, length} -> Stats.observe_max(:outbox_mailbox_peak, length)
          _ -> :ok
        end

      nil ->
        :ok
    end
  end
end

defmodule Group.Jepsen.Transport.Distribution do
  @moduledoc false
  @behaviour Group.Transport

  alias Group.Jepsen.Transport.{Common, Stats}
  alias Group.Transport.DistErl, as: Delegate

  @impl true
  def id, do: Delegate.id()

  @impl true
  def descriptor(group, opts), do: Delegate.descriptor(group, opts)

  @impl true
  def child_spec(opts), do: {Stats, opts}

  @impl true
  def outgoing(group, target_node, shard, message, opts) do
    Common.outgoing(Delegate, group, target_node, shard, message, opts)
  end
end

defmodule Group.Jepsen.Transport.TCP do
  @moduledoc false
  @behaviour Group.Transport

  alias Group.Jepsen.Transport.Common
  alias Group.TestTCPTransport, as: Delegate

  @impl true
  def id, do: Delegate.id()

  @impl true
  def descriptor(group, opts), do: Delegate.descriptor(group, opts)

  @impl true
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :name)},
      start: {Group.Jepsen.Transport.TCP.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  @impl true
  def outgoing(group, target_node, shard, message, opts) do
    Common.outgoing(Delegate, group, target_node, shard, message, opts)
  end

  @impl true
  def peer_up(group, remote_node, shard, descriptor, opts),
    do: Delegate.peer_up(group, remote_node, shard, descriptor, opts)

  @impl true
  def peer_down(group, remote_node, shard, opts),
    do: Delegate.peer_down(group, remote_node, shard, opts)
end

defmodule Group.Jepsen.Transport.TCP.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    children = [
      {Group.Jepsen.Transport.Stats, opts},
      Group.TestTCPTransport.child_spec(opts)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Group.Jepsen.Transport.Chaos do
  @moduledoc false
  @behaviour Group.Transport

  alias Group.Jepsen.Transport.{Common, Stats}

  @impl true
  def id, do: :group_jepsen_unordered_v1

  @impl true
  def descriptor(_group, _opts), do: :group_jepsen_unordered_v1

  @impl true
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :name)},
      start: {Group.Jepsen.Transport.Chaos.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  @impl true
  def outgoing(group, target_node, shard, message, _opts) do
    Common.record(message)

    if Stats.blocked?(target_node) do
      Stats.increment(:logical_drop)
      :ok
    else
      case Process.whereis(worker_name(group, shard)) do
        pid when is_pid(pid) ->
          send(pid, {:outgoing, target_node, message})
          :ok

        nil ->
          :disconnected
      end
    end
  end

  def worker_name(group, shard), do: :"#{group}_jepsen_chaos_#{shard}"
end

defmodule Group.Jepsen.Transport.Chaos.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    group = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)

    workers =
      for shard <- 0..(num_shards - 1) do
        %{
          id: {Group.Jepsen.Transport.Chaos.Worker, shard},
          start: {Group.Jepsen.Transport.Chaos.Worker, :start_link, [group, shard]}
        }
      end

    Supervisor.init([{Group.Jepsen.Transport.Stats, opts} | workers], strategy: :one_for_one)
  end
end

defmodule Group.Jepsen.Transport.Chaos.Worker do
  @moduledoc false
  use GenServer

  alias Group.Jepsen.Transport.{Chaos, Stats}

  def start_link(group, shard) do
    GenServer.start_link(__MODULE__, {group, shard}, name: Chaos.worker_name(group, shard))
  end

  @impl true
  def init({group, shard}), do: {:ok, %{group: group, shard: shard, counter: 0}}

  @impl true
  def handle_info({:outgoing, target_node, message}, state) do
    counter = state.counter + 1
    next = %{state | counter: counter}

    if rem(counter, 5) == 0 do
      Stats.increment(:chaos_drop)
    else
      delay = rem(counter * 17, 41)
      Process.send_after(self(), {:forward, target_node, message}, delay)

      if rem(counter, 7) == 0 do
        Stats.increment(:chaos_duplicate)
        Process.send_after(self(), {:forward, target_node, message}, rem(delay + 19, 47))
      end

      if delay > 0, do: Stats.increment(:chaos_delay)
    end

    {:noreply, next}
  end

  def handle_info({:forward, target_node, replica_message}, state) do
    destination = {Group.Replica.shard_name(state.group, state.shard), target_node}
    message = {:group_replica_frame, node(), replica_message}

    case :erlang.send_nosuspend(destination, message, [:noconnect]) do
      true -> Stats.increment(:chaos_delivered)
      false -> Stats.increment(:chaos_busy)
    end

    {:noreply, state}
  end
end

defmodule Group.Jepsen.Transport.Control do
  @moduledoc false

  alias Group.Jepsen.Transport.Stats

  def profile do
    case System.get_env("GROUP_JEPSEN_TRANSPORT", "distribution") do
      "distribution" -> :distribution
      "tcp" -> :tcp
      "chaos" -> :chaos
      other -> raise "unknown GROUP_JEPSEN_TRANSPORT #{inspect(other)}"
    end
  end

  def transport(node_id) do
    case profile() do
      :distribution ->
        Group.Jepsen.Transport.Distribution

      :chaos ->
        Group.Jepsen.Transport.Chaos

      :tcp ->
        {:ok, advertised_ip} = :inet.getaddr(String.to_charlist(node_id), :inet)

        {Group.Jepsen.Transport.TCP,
         [
           ip: {0, 0, 0, 0},
           advertised_ip: advertised_ip,
           port: 10_000,
           max_queue: 32,
           connect_timeout: 100,
           send_timeout: 100,
           reconnect_interval: 25,
           outbox_batch_size: 16,
           outbox_batch_bytes: 65_536,
           outbox_flush_interval: 1,
           outbox_deadline: 100
         ]}
    end
  end

  def block(target_node) do
    Stats.block(target_node)
    maybe_disconnect(target_node)
    :ok
  end

  def unblock(target_node) do
    Stats.unblock(target_node)
    maybe_reconnect(target_node)
    :ok
  end

  def heal(peer_nodes) do
    Stats.heal()
    Enum.each(peer_nodes, &maybe_reconnect/1)
    :ok
  end

  def reset(target_node) do
    maybe_disconnect(target_node)
    Process.sleep(10)
    maybe_reconnect(target_node)
    :ok
  end

  defp maybe_disconnect(target_node) do
    if profile() == :tcp do
      Group.TestTCPTransport.disconnect_peer(:jepsen_group, target_node)
    end
  catch
    :exit, _ -> :ok
  end

  defp maybe_reconnect(target_node) do
    if profile() == :tcp do
      Group.TestTCPTransport.reconnect_peer(:jepsen_group, target_node)
    end
  catch
    :exit, _ -> :ok
  end
end

defmodule Group.Jepsen.ConflictResolver do
  @moduledoc false

  def resolve(_name, _key, {_pid, meta, _time}), do: rank(meta)

  defp rank(%{revision: revision, token: token}), do: {revision, token}
  defp rank(_meta), do: {-1, ""}
end

defmodule Group.Jepsen.Owner do
  @moduledoc false
  use GenServer

  def start(token), do: GenServer.start(__MODULE__, token)

  @impl true
  def init(token), do: {:ok, %{token: token, registrations: %{}, memberships: %{}}}

  @impl true
  def handle_call({:mutate, :register, cluster, key, revision}, _from, state) do
    meta = %{token: state.token, revision: revision}

    case safe_group_call(fn ->
           Group.register(:jepsen_group, registry_key(key), meta, cluster_opts(cluster))
         end) do
      :ok ->
        entry = %{cluster: cluster, key: key, revision: revision}
        state = put_in(state.registrations[{cluster, key}], entry)
        {:reply, {:ok, snapshot(state)}, state}

      {:error, reason} ->
        {:reply, {:error, reason, snapshot(state)}, state}
    end
  end

  def handle_call({:mutate, :unregister, cluster, key, _revision}, _from, state) do
    owner_key = {cluster, key}

    if Map.has_key?(state.registrations, owner_key) do
      case safe_group_call(fn ->
             Group.unregister(:jepsen_group, registry_key(key), cluster_opts(cluster))
           end) do
        :ok ->
          state = %{state | registrations: Map.delete(state.registrations, owner_key)}
          {:reply, {:ok, snapshot(state)}, state}

        {:error, reason} ->
          {:reply, {:error, reason, snapshot(state)}, state}
      end
    else
      {:reply, {:error, :not_owned, snapshot(state)}, state}
    end
  end

  def handle_call({:mutate, :join, cluster, key, revision}, _from, state) do
    meta = %{token: state.token, revision: revision}

    case safe_group_call(fn ->
           Group.join(:jepsen_group, pg_key(key), meta, cluster_opts(cluster))
         end) do
      :ok ->
        entry = %{cluster: cluster, key: key, revision: revision}
        state = put_in(state.memberships[{cluster, key}], entry)
        {:reply, {:ok, snapshot(state)}, state}

      {:error, reason} ->
        {:reply, {:error, reason, snapshot(state)}, state}
    end
  end

  def handle_call({:mutate, :leave, cluster, key, _revision}, _from, state) do
    owner_key = {cluster, key}

    if Map.has_key?(state.memberships, owner_key) do
      case safe_group_call(fn ->
             Group.leave(:jepsen_group, pg_key(key), cluster_opts(cluster))
           end) do
        :ok ->
          state = %{state | memberships: Map.delete(state.memberships, owner_key)}
          {:reply, {:ok, snapshot(state)}, state}

        {:error, reason} ->
          {:reply, {:error, reason, snapshot(state)}, state}
      end
    else
      {:reply, {:error, :not_owned, snapshot(state)}, state}
    end
  end

  def handle_call({:drop_cluster, cluster}, _from, state) do
    registrations = drop_cluster(state.registrations, cluster)
    memberships = drop_cluster(state.memberships, cluster)
    state = %{state | registrations: registrations, memberships: memberships}
    {:reply, :ok, state}
  end

  def handle_call(:snapshot, _from, state), do: {:reply, snapshot(state), state}

  defp drop_cluster(entries, cluster) do
    entries
    |> Enum.reject(fn {{entry_cluster, _key}, _entry} -> entry_cluster == cluster end)
    |> Map.new()
  end

  defp snapshot(state) do
    %{
      token: state.token,
      registrations: state.registrations |> Map.values() |> sort_entries(),
      memberships: state.memberships |> Map.values() |> sort_entries()
    }
  end

  defp sort_entries(entries), do: Enum.sort_by(entries, &{&1.cluster || "", &1.key})

  defp safe_group_call(fun) do
    fun.()
  rescue
    exception -> {:error, {:exception, Exception.message(exception)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp cluster_opts(nil), do: []
  defp cluster_opts(cluster), do: [cluster: cluster]
  defp registry_key(key), do: "jepsen/registry/#{key}"
  defp pg_key(key), do: "jepsen/pg/#{key}"
end

defmodule Group.Jepsen.Driver do
  @moduledoc false
  use GenServer

  alias Group.Jepsen.Transport.Stats

  @driver_count 8
  @unexpected_death_log "/tmp/group-jepsen-unexpected-deaths"

  def start_link(opts) do
    index = Keyword.fetch!(opts, :index)
    GenServer.start_link(__MODULE__, opts, name: name(index))
  end

  def child_specs(opts) do
    for index <- 0..(@driver_count - 1) do
      %{
        id: {__MODULE__, index},
        start: {__MODULE__, :start_link, [Keyword.put(opts, :index, index)]}
      }
    end
  end

  def mutate(operation, logical_owner, cluster, key, revision) do
    started_at = System.monotonic_time(:microsecond)

    response =
      GenServer.call(
        driver(logical_owner),
        {:mutate, operation, logical_owner, cluster, key, revision},
        10_000
      )

    Map.put(response, :latency_us, System.monotonic_time(:microsecond) - started_at)
  end

  def kill(logical_owner), do: GenServer.call(driver(logical_owner), {:kill, logical_owner})

  def drop_cluster(cluster) do
    Enum.each(names(), &GenServer.call(&1, {:drop_cluster, cluster}, 10_000))
    :ok
  end

  def owner_snapshots do
    names()
    |> Enum.flat_map(fn driver ->
      case GenServer.call(driver, :owner_snapshots, 30_000) do
        {:ok, snapshots} -> snapshots
        {:error, reason} -> raise "owner snapshot refresh failed: #{inspect(reason)}"
      end
    end)
    |> Enum.sort_by(& &1.token)
  end

  def unexpected_deaths do
    in_memory = Enum.flat_map(names(), &GenServer.call(&1, :unexpected_deaths, 30_000))

    (in_memory ++ persisted_unexpected_deaths())
    |> Enum.uniq()
    |> Enum.sort_by(& &1.token)
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       node_id: Keyword.fetch!(opts, :node_id),
       boot_id: Keyword.fetch!(opts, :boot_id),
       owners: %{},
       monitors: %{},
       incarnations: %{},
       unexpected_deaths: []
     }}
  end

  @impl true
  def handle_call({:mutate, operation, logical_owner, cluster, key, revision}, _from, state) do
    {pid, state} = owner(state, logical_owner)

    try do
      case GenServer.call(pid, {:mutate, operation, cluster, key, revision}, 8_000) do
        {:ok, owner_state} ->
          {:reply, %{status: :ok, owner: owner_state},
           put_owner_state(state, logical_owner, pid, owner_state)}

        {:error, reason, owner_state} ->
          {:reply, %{status: :fail, error: inspect(reason), owner: owner_state},
           put_owner_state(state, logical_owner, pid, owner_state)}
      end
    catch
      :exit, reason ->
        {:reply, %{status: :unknown, error: inspect(reason)}, state}
    end
  end

  def handle_call({:kill, logical_owner}, _from, state) do
    case Map.get(state.owners, logical_owner) do
      nil ->
        {:reply, %{status: :ok, killed: nil}, state}

      {pid, token, monitor_ref, _owner_state} ->
        if Process.alive?(pid) do
          Process.exit(pid, :kill)
          Process.demonitor(monitor_ref, [:flush])

          {:reply, %{status: :ok, killed: token},
           %{
             state
             | owners: Map.delete(state.owners, logical_owner),
               monitors: Map.delete(state.monitors, monitor_ref)
           }}
        else
          {:reply, %{status: :unknown, error: "owner already dead"}, state}
        end
    end
  end

  def handle_call({:drop_cluster, cluster}, _from, state) do
    Enum.each(state.owners, fn {_logical_owner, {pid, _token, _monitor, _owner_state}} ->
      if Process.alive?(pid), do: GenServer.call(pid, {:drop_cluster, cluster}, 10_000)
    end)

    owners =
      Map.new(state.owners, fn {logical_owner, {pid, token, monitor, _owner_state}} ->
        owner_state = if Process.alive?(pid), do: GenServer.call(pid, :snapshot), else: nil
        {logical_owner, {pid, token, monitor, owner_state}}
      end)

    {:reply, :ok, %{state | owners: owners}}
  end

  def handle_call(:owner_snapshots, _from, state) do
    result =
      Enum.reduce_while(state.owners, {[], %{}}, fn
        {logical_owner, {pid, token, monitor_ref, cached}}, {snapshots, acc} ->
          case live_owner_snapshot(pid) do
            {:ok, owner_state} ->
              {:cont,
               {
                 [owner_state | snapshots],
                 Map.put(acc, logical_owner, {pid, token, monitor_ref, owner_state})
               }}

            {:error, reason} ->
              {:halt, {:error, {logical_owner, token, reason, cached}}}
          end
      end)

    case result do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {owners, refreshed} ->
        {:reply, {:ok, Enum.reverse(owners)}, %{state | owners: refreshed}}
    end
  end

  defp live_owner_snapshot(pid) do
    if Process.alive?(pid) do
      try do
        {:ok, GenServer.call(pid, :snapshot, 10_000)}
      catch
        :exit, reason -> {:error, reason}
      end
    else
      {:error, :not_alive}
    end
  end

  def handle_call(:unexpected_deaths, _from, state) do
    {:reply, state.unexpected_deaths, state}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {{logical_owner, token}, monitors} ->
        owners =
          case Map.get(state.owners, logical_owner) do
            {_pid, ^token, ^monitor_ref, _owner_state} -> Map.delete(state.owners, logical_owner)
            _newer_incarnation -> state.owners
          end

        unexpected_deaths =
          if match?({:group_registry_conflict, _key, _winner_meta}, reason) do
            Stats.increment_persistent(:registry_conflict_death)
            state.unexpected_deaths
          else
            death = %{token: token, reason: inspect(reason)}
            :ok = persist_unexpected_death(death)
            [death | state.unexpected_deaths]
          end

        {:noreply,
         %{state | owners: owners, monitors: monitors, unexpected_deaths: unexpected_deaths}}
    end
  end

  defp owner(state, logical_owner) do
    case Map.get(state.owners, logical_owner) do
      {pid, _token, _monitor_ref, _owner_state} when is_pid(pid) ->
        if Process.alive?(pid), do: {pid, state}, else: start_owner(state, logical_owner)

      nil ->
        start_owner(state, logical_owner)
    end
  end

  defp start_owner(state, logical_owner) do
    incarnation = Map.get(state.incarnations, logical_owner, 0) + 1
    token = "#{state.node_id}/#{state.boot_id}/#{logical_owner}/#{incarnation}"
    {:ok, pid} = Group.Jepsen.Owner.start(token)
    monitor_ref = Process.monitor(pid)
    owner_state = %{token: token, registrations: [], memberships: []}

    state = %{
      state
      | owners: Map.put(state.owners, logical_owner, {pid, token, monitor_ref, owner_state}),
        monitors: Map.put(state.monitors, monitor_ref, {logical_owner, token}),
        incarnations: Map.put(state.incarnations, logical_owner, incarnation)
    }

    {pid, state}
  end

  defp put_owner_state(state, logical_owner, pid, owner_state) do
    owners =
      case Map.get(state.owners, logical_owner) do
        {^pid, token, monitor_ref, _old_owner_state} ->
          Map.put(state.owners, logical_owner, {pid, token, monitor_ref, owner_state})

        _replaced_owner ->
          state.owners
      end

    %{state | owners: owners}
  end

  defp names, do: Enum.map(0..(@driver_count - 1), &name/1)
  defp driver(logical_owner), do: name(:erlang.phash2(logical_owner, @driver_count))
  defp name(index), do: :"group_jepsen_driver_#{index}"

  defp persist_unexpected_death(%{token: token, reason: reason}) do
    File.write(@unexpected_death_log, token <> "\t" <> reason <> "\n", [:append])
  end

  defp persisted_unexpected_deaths do
    case File.read(@unexpected_death_log) do
      {:ok, contents} ->
        contents
        |> String.split("\n", trim: true)
        |> Enum.flat_map(fn line ->
          case String.split(line, "\t", parts: 2) do
            [token, reason] -> [%{token: token, reason: reason}]
            _invalid -> []
          end
        end)

      {:error, :enoent} ->
        []

      {:error, reason} ->
        [%{token: "ORACLE-READ-FAILURE", reason: inspect(reason)}]
    end
  end
end

defmodule Group.Jepsen.Driver.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts),
    do: Supervisor.init(Group.Jepsen.Driver.child_specs(opts), strategy: :one_for_one)
end

defmodule Group.Jepsen.Cluster do
  @moduledoc false
  use GenServer

  def start_link(clusters), do: GenServer.start_link(__MODULE__, clusters, name: __MODULE__)
  def connect(cluster), do: GenServer.call(__MODULE__, {:connect, cluster}, 60_000)
  def disconnect(cluster), do: GenServer.call(__MODULE__, {:disconnect, cluster}, 60_000)
  def connect_all, do: GenServer.call(__MODULE__, :connect_all, 60_000)

  @impl true
  def init(clusters) do
    :ok = Group.connect(:jepsen_group, clusters)
    {:ok, %{clusters: clusters}}
  end

  @impl true
  def handle_call({:connect, cluster}, _from, state) do
    result = Group.connect(:jepsen_group, cluster)
    {:reply, result(result), state}
  end

  def handle_call({:disconnect, cluster}, _from, state) do
    result = Group.disconnect(:jepsen_group, cluster)
    if result == :ok, do: Group.Jepsen.Driver.drop_cluster(cluster)
    {:reply, result(result), state}
  end

  def handle_call(:connect_all, _from, state) do
    result = Group.connect(:jepsen_group, state.clusters)
    {:reply, result(result), state}
  end

  defp result(:ok), do: %{status: :ok}
  defp result({:error, reason}), do: %{status: :fail, error: inspect(reason)}
end

defmodule Group.Jepsen.Invariant do
  @moduledoc false

  alias Group.Replica.{Data, WireProtocol}

  def snapshot(retired_nodes) do
    config = Group.get_config(:jepsen_group)
    shards = 0..(config.num_shards - 1)
    maybe_inject_cursor_marker_corruption(shards)

    errors =
      check("dual indexes", &assert_dual_indexes/0) ++
        check("registry claims", &assert_registry_claims/0) ++
        check("oplog", &assert_oplogs/0) ++
        check("cursor authority", &assert_cursor_authority/0) ++
        check("retired origins", fn -> assert_retired_origins(retired_nodes) end)

    staging_count =
      Enum.reduce(shards, 0, fn shard, total ->
        state = :sys.get_state(Group.Replica.shard_name(:jepsen_group, shard))
        total + map_size(state.snapshot_transfers)
      end)

    oplog_entries =
      Enum.reduce(shards, 0, fn shard, total ->
        total + :ets.info(Data.replica_oplog_order_table(:jepsen_group, shard), :size)
      end)

    %{
      healthy: errors == [] and staging_count == 0,
      errors: errors,
      snapshot_staging_count: staging_count,
      oplog_entries: oplog_entries,
      oplog_max_entries_per_shard: config.replicated_oplog_max_entries,
      shard_mailbox_max:
        mailbox_max(Enum.map(shards, &Group.Replica.shard_name(:jepsen_group, &1))),
      outbox_mailbox_max:
        mailbox_max(Enum.map(shards, &Group.Transport.Outbox.name(:jepsen_group, &1))),
      total_memory_bytes: :erlang.memory(:total)
    }
  rescue
    exception ->
      %{
        healthy: false,
        errors: ["invariant snapshot failed: #{Exception.message(exception)}"],
        snapshot_staging_count: -1
      }
  end

  defp maybe_inject_cursor_marker_corruption(shards) do
    if File.exists?("/tmp/group-jepsen-cursor-marker-corruption") do
      cursor =
        Enum.find_value(shards, fn shard ->
          Data.replica_cursor_table(:jepsen_group, shard)
          |> :ets.tab2list()
          |> case do
            [{stream, _cursor} | _] -> {shard, stream}
            [] -> nil
          end
        end)

      case cursor do
        {shard, stream} ->
          :ets.insert(
            Data.replica_cursor_table(:jepsen_group, shard),
            {stream, {:snapshot_installing, 1}}
          )

        nil ->
          raise "no remote replica cursor available for corruption"
      end
    end
  end

  defp check(label, fun) do
    fun.()
    []
  rescue
    exception -> ["#{label}: #{Exception.message(exception)}"]
  catch
    kind, reason -> ["#{label}: #{inspect({kind, reason})}"]
  end

  defp assert_dual_indexes do
    shards(fn shard ->
      reg_key =
        Data.reg_by_key_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> MapSet.new(fn {{cluster, key}, pid, meta, time, origin} ->
          {cluster, key, pid, meta, time, origin}
        end)

      reg_pid =
        Data.reg_by_pid_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> MapSet.new(fn {{pid, cluster, key}, meta, time, origin} ->
          {cluster, key, pid, meta, time, origin}
        end)

      pg_key =
        Data.pg_by_key_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> MapSet.new(fn {{cluster, key, pid}, meta, time, origin} ->
          {cluster, key, pid, meta, time, origin}
        end)

      pg_pid =
        Data.pg_by_pid_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> MapSet.new(fn {{pid, cluster, key}, meta, time, origin} ->
          {cluster, key, pid, meta, time, origin}
        end)

      assert_equal!(reg_key, reg_pid, "registry dual indexes shard #{shard}")
      assert_equal!(pg_key, pg_pid, "PG dual indexes shard #{shard}")

      expected_counts =
        Enum.reduce(pg_key, %{}, fn {cluster, key, _pid, _meta, _time, origin}, counts ->
          local_increment = if origin == node(), do: 1, else: 0

          [{:exact, key} | Enum.map(Data.prefix_patterns_for_key(key), &{:prefix, &1})]
          |> Enum.reduce(counts, fn {kind, pattern}, inner ->
            Map.update(inner, {cluster, kind, pattern}, {1, local_increment}, fn {total, local} ->
              {total + 1, local + local_increment}
            end)
          end)
        end)

      materialized_counts =
        Data.pg_count_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> Map.new(fn {count_key, total, local} -> {count_key, {total, local}} end)

      assert_equal!(expected_counts, materialized_counts, "PG counts shard #{shard}")
    end)

    cluster_nodes =
      Data.cluster_nodes_table(:jepsen_group)
      |> :ets.tab2list()
      |> MapSet.new(fn {cluster, origin} -> {cluster, origin} end)

    node_clusters =
      Data.node_clusters_table(:jepsen_group)
      |> :ets.tab2list()
      |> MapSet.new(fn {origin, cluster} -> {cluster, origin} end)

    assert_equal!(cluster_nodes, node_clusters, "cluster dual indexes")
  end

  defp assert_registry_claims do
    shards(fn shard ->
      by_key =
        Data.reg_claim_by_key_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> MapSet.new(fn
          {{cluster, key, origin, generation, epoch}, pid, meta, time, seq} ->
            {cluster, key, pid, meta, time, origin, generation, epoch, seq}
        end)

      by_pid =
        Data.reg_claim_by_pid_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> MapSet.new(fn
          {{pid, cluster, key, origin, generation, epoch}, meta, time, seq} ->
            {cluster, key, pid, meta, time, origin, generation, epoch, seq}
        end)

      assert_equal!(by_key, by_pid, "registry claim indexes shard #{shard}")

      invalid_origin =
        Enum.find(by_key, fn {_cluster, _key, pid, _meta, _time, origin, _gen, _epoch, _seq} ->
          node(pid) != origin
        end)

      if invalid_origin, do: raise("claim with invalid PID origin #{inspect(invalid_origin)}")

      stale_claim =
        Enum.find(by_key, fn
          {cluster, _key, _pid, _meta, _time, origin, generation, epoch, _seq} ->
            not current_registry_claim?(shard, cluster, origin, generation, epoch)
        end)

      if stale_claim, do: raise("claim outside current authority #{inspect(stale_claim)}")

      expected =
        by_key
        |> Enum.group_by(fn {cluster, key, _pid, _meta, _time, _origin, _gen, _epoch, _seq} ->
          {cluster, key}
        end)
        |> MapSet.new(fn {{cluster, key}, claims} ->
          {^cluster, ^key, pid, meta, time, origin, _generation, _epoch, _seq} =
            Enum.max_by(claims, fn
              {^cluster, ^key, claim_pid, claim_meta, claim_time, _origin, _generation, _epoch,
               _seq} ->
                {
                  Group.Jepsen.ConflictResolver.resolve(
                    :jepsen_group,
                    key,
                    {claim_pid, claim_meta, claim_time}
                  ),
                  claim_pid
                }
            end)

          {cluster, key, pid, meta, time, origin}
        end)

      visible =
        Data.reg_by_key_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> MapSet.new(fn {{cluster, key}, pid, meta, time, origin} ->
          {cluster, key, pid, meta, time, origin}
        end)

      assert_equal!(expected, visible, "registry projection shard #{shard}")
    end)
  end

  defp current_registry_claim?(shard, cluster, origin, generation, epoch) do
    if origin == node() do
      generation == Data.generation(:jepsen_group) and
        epoch == Data.local_cluster_epoch(:jepsen_group, cluster)
    else
      known_generation = Data.remote_generation(:jepsen_group, origin)
      observed_revision = Data.remote_cluster_epoch_observed_revision(:jepsen_group, origin)

      generation == known_generation and
        epoch == Data.remote_cluster_epoch(:jepsen_group, origin, cluster) and
        Data.remote_replica_authority_hint(:jepsen_group, origin) ==
          {known_generation, observed_revision} and
        Data.remote_cluster_epoch_revision(:jepsen_group, origin) == observed_revision and
        Data.remote_view_generation(:jepsen_group, shard, origin) == known_generation and
        Data.remote_view_cluster_epoch_revision(:jepsen_group, shard, origin) ==
          Data.remote_cluster_epoch_exact_revision(:jepsen_group, origin) and
        Data.remote_view_observed_revision(:jepsen_group, shard, origin) == observed_revision
    end
  end

  defp assert_oplogs do
    max_entries = Group.get_config(:jepsen_group).replicated_oplog_max_entries

    shards(fn shard ->
      oplog =
        Data.replica_oplog_table(:jepsen_group, shard)
        |> :ets.tab2list()
        |> MapSet.new(fn {{stream, seq}, append_id, _mutations} -> {append_id, stream, seq} end)

      order =
        Data.replica_oplog_order_table(:jepsen_group, shard) |> :ets.tab2list() |> MapSet.new()

      assert_equal!(oplog, order, "oplog/order shard #{shard}")

      if MapSet.size(order) > max_entries do
        raise "oplog bound exceeded shard #{shard}: #{MapSet.size(order)} > #{max_entries}"
      end

      Data.replica_stream_meta_table(:jepsen_group, shard)
      |> :ets.tab2list()
      |> Enum.each(fn {stream, head, floor, applied} ->
        unless floor >= 1 and floor <= head + 1 and applied >= 0 and applied <= head do
          raise "invalid stream bounds #{inspect({stream, head, floor, applied})}"
        end

        retained =
          oplog
          |> Enum.filter(fn {_append, row_stream, _seq} -> row_stream == stream end)
          |> Enum.map(&elem(&1, 2))
          |> Enum.sort()

        expected = if floor <= head, do: Enum.to_list(floor..head), else: []
        if retained != expected, do: raise("non-contiguous oplog #{inspect(stream)}")
      end)
    end)
  end

  defp assert_cursor_authority do
    shards(fn shard ->
      Data.replica_cursor_table(:jepsen_group, shard)
      |> :ets.tab2list()
      |> Enum.each(fn {stream, seq} ->
        origin = WireProtocol.stream_origin(stream)
        cluster = WireProtocol.stream_cluster(stream)

        valid? =
          WireProtocol.stream_name(stream) == :jepsen_group and
            WireProtocol.stream_shard(stream) == shard and
            origin != node() and
            WireProtocol.stream_generation(stream) ==
              Data.remote_generation(:jepsen_group, origin) and
            WireProtocol.stream_epoch(stream) ==
              Data.remote_cluster_epoch(:jepsen_group, origin, cluster) and is_integer(seq) and
            seq >= 0

        unless valid?, do: raise("cursor lacks current authority #{inspect({stream, seq})}")
      end)
    end)
  end

  defp assert_retired_origins(retired_nodes) do
    Enum.each(retired_nodes, fn origin ->
      shards(fn shard ->
        claims =
          Data.reg_claim_by_key_table(:jepsen_group, shard)
          |> :ets.tab2list()
          |> Enum.filter(fn {{_cluster, _key, row_origin, _gen, _epoch}, _, _, _, _} ->
            row_origin == origin
          end)

        registry =
          Data.reg_by_key_table(:jepsen_group, shard)
          |> :ets.tab2list()
          |> Enum.filter(fn {_key, _pid, _meta, _time, row_origin} -> row_origin == origin end)

        pg =
          Data.pg_by_key_table(:jepsen_group, shard)
          |> :ets.tab2list()
          |> Enum.filter(fn {_key, _meta, _time, row_origin} -> row_origin == origin end)

        cursors =
          Data.replica_cursor_table(:jepsen_group, shard)
          |> :ets.tab2list()
          |> Enum.filter(fn {stream, _seq} -> WireProtocol.stream_origin(stream) == origin end)

        view = Data.remote_view_generation(:jepsen_group, shard, origin)

        unless claims == [] and registry == [] and pg == [] and cursors == [] and is_nil(view) do
          raise "retired origin retained on shard #{shard}: #{inspect(origin)}"
        end
      end)

      unless is_nil(Data.remote_generation(:jepsen_group, origin)) and
               Data.clusters_for_node(:jepsen_group, origin) == [] do
        raise "retired origin retained shared authority: #{inspect(origin)}"
      end
    end)
  end

  defp shards(fun) do
    num_shards = Group.get_config(:jepsen_group).num_shards
    Enum.each(0..(num_shards - 1), fun)
  end

  defp assert_equal!(left, right, label) do
    if left != right do
      raise "#{label}: left-only=#{inspect(MapSet.difference(left, right))} " <>
              "right-only=#{inspect(MapSet.difference(right, left))}"
    end
  end

  defp mailbox_max(names) do
    names
    |> Enum.map(fn name ->
      case Process.whereis(name) do
        pid when is_pid(pid) ->
          case Process.info(pid, :message_queue_len) do
            {:message_queue_len, length} -> length
            _ -> 0
          end

        nil ->
          0
      end
    end)
    |> Enum.max(fn -> 0 end)
  end
end

defmodule Group.Jepsen.Snapshot do
  @moduledoc false

  def capture(node_id, boot_id, key_count, clusters, retired_nodes) do
    owners = Group.Jepsen.Driver.owner_snapshots()

    registry =
      Map.new([nil | clusters], fn cluster ->
        values =
          Map.new(0..(key_count - 1), fn key ->
            value =
              case Group.lookup(:jepsen_group, registry_key(key), cluster_opts(cluster)) do
                nil -> nil
                {_pid, %{token: token}} -> token
                {_pid, other} -> "INVALID:#{inspect(other)}"
              end

            {key, value}
          end)

        {cluster_name(cluster), values}
      end)

    pg =
      Map.new([nil | clusters], fn cluster ->
        values =
          Map.new(0..(key_count - 1), fn key ->
            tokens =
              :jepsen_group
              |> Group.members(pg_key(key), cluster_opts(cluster))
              |> Enum.map(fn
                {_pid, %{token: token}} -> token
                {_pid, other} -> "INVALID:#{inspect(other)}"
              end)
              |> Enum.sort()

            {key, tokens}
          end)

        {cluster_name(cluster), values}
      end)

    %{
      status: :ok,
      snapshot: %{
        node: node_id,
        boot: boot_id,
        peers: Group.nodes(:jepsen_group) |> Enum.map(&Atom.to_string/1) |> Enum.sort(),
        owners: owners,
        unexpected_deaths: Group.Jepsen.Driver.unexpected_deaths(),
        transport_events: Group.Jepsen.Transport.Stats.snapshot(),
        transport_profile: Group.Jepsen.Transport.Control.profile(),
        internal: Group.Jepsen.Invariant.snapshot(retired_nodes),
        registry: registry,
        pg: pg
      }
    }
  end

  defp cluster_name(nil), do: "root"
  defp cluster_name(cluster), do: cluster
  defp cluster_opts(nil), do: []
  defp cluster_opts(cluster), do: [cluster: cluster]
  defp registry_key(key), do: "jepsen/registry/#{key}"
  defp pg_key(key), do: "jepsen/pg/#{key}"
end

defmodule Group.Jepsen.EDN do
  @moduledoc false

  def encode(nil), do: "nil"
  def encode(true), do: "true"
  def encode(false), do: "false"
  def encode(value) when is_integer(value), do: Integer.to_string(value)
  def encode(value) when is_binary(value), do: inspect(value)

  def encode(value) when is_atom(value) do
    ":" <> (value |> Atom.to_string() |> String.replace("_", "-"))
  end

  def encode(value) when is_list(value) do
    "[" <> Enum.map_join(value, " ", &encode/1) <> "]"
  end

  def encode(%MapSet{} = value) do
    "#" <> "{" <> (value |> Enum.sort() |> Enum.map_join(" ", &encode/1)) <> "}"
  end

  def encode(value) when is_map(value) do
    encoded =
      value
      |> Enum.map(fn {key, inner} -> {encode(key), encode(inner)} end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map_join(" ", fn {key, inner} -> key <> " " <> inner end)

    "{" <> encoded <> "}"
  end
end

defmodule Group.Jepsen.Wire do
  @moduledoc false

  def serve(port, context) do
    {:ok, listener} =
      :gen_tcp.listen(port, [:binary, packet: 4, active: false, reuseaddr: true])

    accept(listener, context)
  end

  defp accept(listener, context) do
    {:ok, socket} = :gen_tcp.accept(listener)
    spawn(fn -> connection(socket, context) end)
    accept(listener, context)
  end

  defp connection(socket, context) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, payload} ->
        response = payload |> command(context) |> Group.Jepsen.EDN.encode()
        :ok = :gen_tcp.send(socket, response)
        connection(socket, context)

      {:error, _reason} ->
        :gen_tcp.close(socket)
    end
  end

  defp command(payload, context) do
    case String.split(payload, "\t") do
      ["ping"] ->
        %{status: :ok}

      ["ready", expected] ->
        expected = String.to_integer(expected)

        if length(Group.nodes(:jepsen_group)) == expected - 1 do
          %{status: :ok}
        else
          %{status: :retry, peers: length(Group.nodes(:jepsen_group))}
        end

      ["mutate", operation, logical_owner, cluster, key, revision] ->
        Group.Jepsen.Driver.mutate(
          String.to_existing_atom(operation),
          logical_owner,
          parse_cluster(cluster),
          String.to_integer(key),
          String.to_integer(revision)
        )

      ["kill", logical_owner] ->
        Group.Jepsen.Driver.kill(logical_owner)

      ["cluster", "connect", cluster] ->
        Group.Jepsen.Cluster.connect(cluster)

      ["cluster", "disconnect", cluster] ->
        Group.Jepsen.Cluster.disconnect(cluster)

      ["cluster", "connect-all"] ->
        Group.Jepsen.Cluster.connect_all()

      ["transport", "block", target] ->
        :ok = Group.Jepsen.Transport.Control.block(String.to_atom(target))
        %{status: :ok}

      ["transport", "unblock", target] ->
        :ok = Group.Jepsen.Transport.Control.unblock(String.to_atom(target))
        %{status: :ok}

      ["transport", "heal"] ->
        :ok = Group.Jepsen.Transport.Control.heal(context.peers)
        %{status: :ok}

      ["transport", "reset", target] ->
        :ok = Group.Jepsen.Transport.Control.reset(String.to_atom(target))
        %{status: :ok}

      ["snapshot", key_count, clusters, retired] ->
        Group.Jepsen.Snapshot.capture(
          context.node_id,
          context.boot_id,
          String.to_integer(key_count),
          parse_list(clusters),
          retired |> parse_list() |> Enum.map(&String.to_atom/1)
        )

      ["corrupt", mode] ->
        corrupt(mode)

      other ->
        %{status: :fail, error: "unknown command #{inspect(other)}"}
    end
  rescue
    exception -> %{status: :unknown, error: Exception.message(exception)}
  catch
    kind, reason -> %{status: :unknown, error: inspect({kind, reason})}
  end

  defp corrupt("unexpected-death") do
    File.write!(
      "/tmp/group-jepsen-unexpected-deaths",
      "oracle-self-test\t:injected\n",
      [:append]
    )

    %{status: :ok}
  end

  defp corrupt("internal-index") do
    table = Group.Replica.Data.reg_by_pid_table(:jepsen_group, 0)
    :ets.insert(table, {{self(), nil, "jepsen/registry/corrupt"}, %{}, 0, node()})
    %{status: :ok}
  end

  defp corrupt("cursor-marker") do
    File.write!("/tmp/group-jepsen-cursor-marker-corruption", "enabled\n")
    %{status: :ok}
  end

  defp corrupt("registry-projection") do
    config = Group.get_config(:jepsen_group)

    corrupted =
      Enum.find_value(0..(config.num_shards - 1), fn shard ->
        by_key = Group.Replica.Data.reg_claim_by_key_table(:jepsen_group, shard)

        Enum.find_value(:ets.tab2list(by_key), fn
          {{cluster, key, origin, generation, epoch}, pid, %{revision: revision} = meta, time,
           seq} = claim ->
            case Group.Replica.Data.registry_lookup(:jepsen_group, shard, cluster, key) do
              {^pid, ^meta, ^time, ^origin} ->
                changed_meta = %{meta | revision: revision + 1}

                :ets.insert(
                  by_key,
                  {{cluster, key, origin, generation, epoch}, pid, changed_meta, time, seq}
                )

                :ets.insert(
                  Group.Replica.Data.reg_claim_by_pid_table(:jepsen_group, shard),
                  {{pid, cluster, key, origin, generation, epoch}, changed_meta, time, seq}
                )

                claim

              _other_projection ->
                nil
            end

          _other_claim ->
            nil
        end)
      end)

    if corrupted do
      %{status: :ok}
    else
      %{status: :fail, error: "no visible registry claim available for corruption"}
    end
  end

  defp corrupt(other), do: %{status: :fail, error: "unknown corruption #{inspect(other)}"}
  defp parse_cluster("root"), do: nil
  defp parse_cluster(cluster), do: cluster
  defp parse_list(""), do: []
  defp parse_list(value), do: String.split(value, ",", trim: true)
end

defmodule Group.Jepsen.Main do
  @moduledoc false
  @clusters ["red", "blue"]

  def run(argv) do
    {opts, _rest, []} =
      OptionParser.parse(argv,
        strict: [node: :string, port: :integer, peers: :string]
      )

    node_id = Keyword.get(opts, :node) || System.fetch_env!("GROUP_JEPSEN_NODE")

    port =
      Keyword.get(opts, :port) ||
        System.get_env("GROUP_JEPSEN_PORT", "9080") |> String.to_integer()

    peers =
      (Keyword.get(opts, :peers) ||
         System.get_env("GROUP_JEPSEN_PEERS", "group@n1,group@n2,group@n3"))
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_atom/1)

    {:ok, _apps} = Application.ensure_all_started(:group)

    {:ok, group} =
      Group.start_link(
        name: :jepsen_group,
        shards: 4,
        log: false,
        resolve_registry_conflict: {Group.Jepsen.ConflictResolver, :resolve, []},
        replica_transport: Group.Jepsen.Transport.Control.transport(node_id),
        replicated_sender_buffer_size: positive_env!("GROUP_JEPSEN_SENDER_BUFFER_SIZE", 1),
        replicated_oplog_max_entries: 16,
        replicated_snapshot_chunk_target_bytes: 1_024,
        replicated_anti_entropy_interval: 50,
        replicated_peer_lease_timeout: 750
      )

    Process.unlink(group)
    boot_id = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    {:ok, _drivers} =
      Group.Jepsen.Driver.Supervisor.start_link(node_id: node_id, boot_id: boot_id)

    {:ok, _cluster} = Group.Jepsen.Cluster.start_link(@clusters)
    spawn_link(fn -> reconnect_loop(peers) end)

    Group.Jepsen.Wire.serve(port, %{
      node_id: node_id,
      boot_id: boot_id,
      peers: Enum.reject(peers, &(&1 == node()))
    })
  end

  defp reconnect_loop(peers) do
    Enum.each(peers, fn peer ->
      if peer != node(), do: Node.connect(peer)
    end)

    Process.sleep(100)
    reconnect_loop(peers)
  end

  defp positive_env!(name, default) do
    value = System.get_env(name, Integer.to_string(default)) |> String.to_integer()
    if value > 0, do: value, else: raise("#{name} must be positive")
  end
end

Group.Jepsen.Main.run(System.argv())
