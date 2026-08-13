defmodule Group.TestTCPTransport do
  @moduledoc false

  use GenServer

  @behaviour Group.Transport
  @behaviour Group.Transport.Outbox

  alias Group.Transport.Outbox

  @impl true
  def id, do: :group_test_sideband_tcp_v2

  @impl true
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {Group.TestTCPTransport.Supervisor, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: server_name(name))
  end

  @impl true
  def descriptor(group, _opts) do
    :persistent_term.get({__MODULE__, group, :descriptor})
  end

  @impl true
  def outgoing(group, target_node, shard, message, opts),
    do: Outbox.push(group, target_node, shard, message, opts)

  @impl Group.Transport.Outbox
  def init_outbox(group, shard, _opts), do: {:ok, %{group: group, shard: shard}}

  @impl Group.Transport.Outbox
  def send_batch(target_node, messages, deadline, %{group: group, shard: shard} = state) do
    result =
      try do
        case :ets.lookup(route_table(group), target_node) do
          [{^target_node, writer, queued, max_queue}] ->
            if :atomics.add_get(queued, 1, 1) <= max_queue do
              send(writer, {:replica_batch, deadline, shard, messages})
              :ok
            else
              :atomics.sub(queued, 1, 1)
              :busy
            end

          [] ->
            :disconnected
        end
      rescue
        ArgumentError -> :disconnected
      end

    {result, state}
  end

  @impl true
  def peer_up(group, remote_node, shard, descriptor, _opts) do
    send_manager(group, {:peer_up, remote_node, shard, descriptor})
  end

  @impl true
  def peer_down(group, remote_node, shard, _opts) do
    send_manager(group, {:peer_down, remote_node, shard})
  end

  @doc false
  def disconnect_peer(group, remote_node) do
    GenServer.call(server_name(group), {:disable_peer, remote_node})
  end

  @doc false
  def reconnect_peer(group, remote_node) do
    GenServer.call(server_name(group), {:enable_peer, remote_node})
  end

  @doc false
  def connected?(group, remote_node) do
    :ets.member(route_table(group), remote_node)
  end

  @doc false
  def status(group) do
    GenServer.call(server_name(group), :status)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    group = Keyword.fetch!(opts, :name)
    ip = Keyword.get(opts, :ip, {127, 0, 0, 1})
    advertised_ip = Keyword.get(opts, :advertised_ip, ip)
    port = Keyword.get(opts, :port, 0)

    {:ok, listener} =
      :gen_tcp.listen(port, [
        :binary,
        packet: 4,
        active: false,
        reuseaddr: true,
        ip: ip
      ])

    {:ok, {_listen_ip, listen_port}} = :inet.sockname(listener)

    capability = :erlang.term_to_binary({node(), make_ref(), System.unique_integer()})
    descriptor = {:group_test_sideband_tcp_v2, advertised_ip, listen_port, capability}
    :persistent_term.put({__MODULE__, group, :descriptor}, descriptor)

    :ets.new(route_table(group), [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    manager = self()
    acceptor = spawn_link(fn -> accept_loop(listener, manager, group, capability) end)

    {:ok,
     %{
       group: group,
       listener: listener,
       acceptor: acceptor,
       peers: %{},
       writers: %{},
       inbound: %{},
       disabled: MapSet.new(),
       max_queue: Keyword.get(opts, :max_queue, 1_024),
       connect_timeout: Keyword.get(opts, :connect_timeout, 1_000),
       send_timeout: Keyword.get(opts, :send_timeout, 1_000),
       reconnect_interval: Keyword.get(opts, :reconnect_interval, 50)
     }}
  end

  @impl true
  def handle_info({:peer_up, remote_node, shard, descriptor}, state) do
    peer =
      state.peers
      |> Map.get(remote_node, %{descriptor: descriptor, lanes: MapSet.new()})
      |> Map.put(:descriptor, descriptor)
      |> Map.update!(:lanes, &MapSet.put(&1, shard))

    state = %{state | peers: Map.put(state.peers, remote_node, peer)}

    state =
      if MapSet.member?(state.disabled, remote_node) do
        state
      else
        ensure_writer(state, remote_node)
      end

    {:noreply, state}
  end

  def handle_info({:peer_down, remote_node, shard}, state) do
    case Map.get(state.peers, remote_node) do
      nil ->
        {:noreply, state}

      peer ->
        lanes = MapSet.delete(peer.lanes, shard)

        if MapSet.size(lanes) == 0 do
          {:noreply, drop_peer(state, remote_node, true)}
        else
          peer = %{peer | lanes: lanes}
          {:noreply, %{state | peers: Map.put(state.peers, remote_node, peer)}}
        end
    end
  end

  def handle_info({:writer_ready, remote_node, writer, queued}, state) do
    if Map.get(state.writers, remote_node) == writer and
         not MapSet.member?(state.disabled, remote_node) do
      :ets.insert(
        route_table(state.group),
        {remote_node, writer, queued, state.max_queue}
      )
    end

    {:noreply, state}
  end

  def handle_info({:writer_failed, remote_node, writer}, state) do
    {:noreply, writer_failed(state, remote_node, writer)}
  end

  def handle_info({:reader_ready, source_node, reader}, state) do
    {:noreply, %{state | inbound: Map.put(state.inbound, source_node, reader)}}
  end

  def handle_info({:reconnect, remote_node}, state) do
    {:noreply, ensure_writer(state, remote_node)}
  end

  def handle_info({:EXIT, pid, _reason}, %{acceptor: pid} = state) do
    {:stop, :acceptor_stopped, state}
  end

  def handle_info({:EXIT, writer, _reason}, state) do
    case Enum.find(state.writers, fn {_node, pid} -> pid == writer end) do
      {remote_node, ^writer} ->
        {:noreply, writer_failed(state, remote_node, writer)}

      nil ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:disable_peer, remote_node}, _from, state) do
    state = %{state | disabled: MapSet.put(state.disabled, remote_node)}
    {:reply, :ok, drop_writer(state, remote_node)}
  end

  def handle_call({:enable_peer, remote_node}, _from, state) do
    state = %{state | disabled: MapSet.delete(state.disabled, remote_node)}
    {:reply, :ok, ensure_writer(state, remote_node)}
  end

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       peers: Map.keys(state.peers),
       writers: Map.keys(state.writers),
       connected: :ets.tab2list(route_table(state.group)) |> Enum.map(&elem(&1, 0)),
       inbound: state.inbound,
       disabled: MapSet.to_list(state.disabled)
     }, state}
  end

  @impl true
  def terminate(_reason, state) do
    :persistent_term.erase({__MODULE__, state.group, :descriptor})
    :gen_tcp.close(state.listener)
    :ok
  end

  defp send_manager(group, message) do
    case Process.whereis(server_name(group)) do
      nil ->
        :ok

      pid ->
        send(pid, message)
        :ok
    end
  end

  defp ensure_writer(state, remote_node) do
    cond do
      MapSet.member?(state.disabled, remote_node) ->
        state

      Map.has_key?(state.writers, remote_node) ->
        state

      peer = Map.get(state.peers, remote_node) ->
        manager = self()

        writer =
          spawn_link(fn ->
            writer_connect(
              manager,
              state.group,
              remote_node,
              peer.descriptor,
              state.connect_timeout,
              state.send_timeout
            )
          end)

        %{state | writers: Map.put(state.writers, remote_node, writer)}

      true ->
        state
    end
  end

  defp writer_failed(state, remote_node, writer) do
    if Map.get(state.writers, remote_node) == writer do
      :ets.delete(route_table(state.group), remote_node)
      state = %{state | writers: Map.delete(state.writers, remote_node)}

      if Map.has_key?(state.peers, remote_node) and
           not MapSet.member?(state.disabled, remote_node) do
        Process.send_after(self(), {:reconnect, remote_node}, state.reconnect_interval)
      end

      state
    else
      state
    end
  end

  defp drop_peer(state, remote_node, remove_descriptor?) do
    state = drop_writer(state, remote_node)

    if remove_descriptor? do
      %{state | peers: Map.delete(state.peers, remote_node)}
    else
      state
    end
  end

  defp drop_writer(state, remote_node) do
    :ets.delete(route_table(state.group), remote_node)

    case Map.pop(state.writers, remote_node) do
      {nil, writers} ->
        %{state | writers: writers}

      {writer, writers} ->
        Process.exit(writer, :shutdown)
        %{state | writers: writers}
    end
  end

  defp writer_connect(
         manager,
         group,
         remote_node,
         {:group_test_sideband_tcp_v2, host, port, capability},
         connect_timeout,
         send_timeout
       ) do
    opts = [
      :binary,
      packet: 4,
      active: false,
      send_timeout: send_timeout,
      send_timeout_close: true
    ]

    case :gen_tcp.connect(host, port, opts, connect_timeout) do
      {:ok, socket} ->
        case :gen_tcp.send(socket, :erlang.term_to_binary({:hello, group, node(), capability})) do
          :ok ->
            queued = :atomics.new(1, signed: false)
            send(manager, {:writer_ready, remote_node, self(), queued})
            writer_loop(socket, manager, remote_node, queued)

          {:error, _reason} ->
            :gen_tcp.close(socket)
            send(manager, {:writer_failed, remote_node, self()})
        end

      {:error, _reason} ->
        send(manager, {:writer_failed, remote_node, self()})
    end
  end

  defp writer_connect(manager, _group, remote_node, _descriptor, _connect_timeout, _send_timeout) do
    send(manager, {:writer_failed, remote_node, self()})
  end

  defp writer_loop(socket, manager, remote_node, queued) do
    receive do
      {:replica_batch, deadline, shard, messages} ->
        result =
          if deadline <= Outbox.monotonic_ms() do
            :expired
          else
            :gen_tcp.send(socket, :erlang.term_to_binary({:batch, shard, messages}))
          end

        :atomics.sub(queued, 1, 1)

        case result do
          result when result in [:ok, :expired] ->
            writer_loop(socket, manager, remote_node, queued)

          {:error, _reason} ->
            :gen_tcp.close(socket)
            send(manager, {:writer_failed, remote_node, self()})
        end
    end
  end

  defp accept_loop(listener, manager, group, capability) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        reader =
          spawn(fn ->
            receive do
              {:accepted_socket, accepted} ->
                reader_handshake(accepted, manager, group, capability)
            end
          end)

        :ok = :gen_tcp.controlling_process(socket, reader)
        send(reader, {:accepted_socket, socket})
        accept_loop(listener, manager, group, capability)

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        send(manager, {:EXIT, self(), :accept_failed})
    end
  end

  defp reader_handshake(socket, manager, group, capability) do
    with {:ok, payload} <- :gen_tcp.recv(socket, 0),
         {:ok, {:hello, ^group, source_node, ^capability}} <- decode(payload),
         true <- is_atom(source_node) do
      send(manager, {:reader_ready, source_node, self()})
      reader_loop(socket, group, source_node)
    else
      _ -> :gen_tcp.close(socket)
    end
  end

  defp reader_loop(socket, group, source_node) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, payload} ->
        case decode_authenticated_frame(payload) do
          {:ok, {:batch, shard, messages}}
          when is_integer(shard) and shard >= 0 and is_list(messages) ->
            case Group.Transport.incoming_batch(group, source_node, shard, messages) do
              result when result in [:ok, :disconnected] ->
                reader_loop(socket, group, source_node)
            end

          _ ->
            :gen_tcp.close(socket)
        end

      {:error, _reason} ->
        :ok
    end
  end

  defp decode(payload) do
    {:ok, :erlang.binary_to_term(payload, [:safe])}
  rescue
    ArgumentError -> :error
  end

  # The capability handshake above establishes the same trusted-cluster
  # boundary as Erlang distribution. Replica metadata is an arbitrary BEAM
  # term and may legitimately contain atoms not yet loaded on this node.
  defp decode_authenticated_frame(payload) do
    {:ok, :erlang.binary_to_term(payload)}
  rescue
    ArgumentError -> :error
  end

  defp server_name(group), do: :"#{group}_replica_tcp_transport"
  defp route_table(group), do: :"#{group}_replica_tcp_routes"
end

defmodule Group.TestTCPTransport.Supervisor do
  @moduledoc false
  use Supervisor

  alias Group.TestTCPTransport, as: TCP
  alias Group.Transport.Outbox

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    group = Keyword.fetch!(opts, :name)

    manager = %{
      id: {TCP, group, :manager},
      start: {TCP, :start_link, [opts]},
      type: :worker,
      restart: :transient,
      shutdown: 5_000,
      significant: true
    }

    outboxes =
      opts
      |> Keyword.put(:backend, TCP)
      |> Outbox.child_spec()

    Supervisor.init([manager, outboxes],
      strategy: :rest_for_one,
      auto_shutdown: :any_significant
    )
  end
end
