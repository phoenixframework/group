defmodule Group.Transport.Outbox do
  @moduledoc """
  Lossy per-shard outboxes for sideband replica transports.

  This module is an implementation helper, not a replacement for
  `Group.Transport`. Distribution can continue sending directly with
  `:erlang.send_nosuspend/3`. A sideband adapter delegates `outgoing/5` to
  `push/5`, which performs only a local `send/2` to the matching shard
  outbox.

  Each outbox batches messages by target node outside the Group shard. Expired
  messages and batches rejected by the backend are deliberately dropped;
  anti-entropy repairs them. Backends may perform bounded blocking work in
  `send_batch/4` because they run in the outbox rather than a Group process.

  A backend using this helper implements:

      @behaviour Group.Transport.Outbox

      def init_outbox(group, shard, opts), do: {:ok, backend_state}

      def send_batch(target_node, messages, deadline, backend_state) do
        # Return promptly once `deadline` has passed. It is safe to drop.
        {:ok, backend_state}
      end

  The backend must pass only complete logical messages to
  `Group.Transport.incoming_batch/4`.

  ## Options

    * `:outbox_batch_size` - maximum logical messages collected per flush,
      default `64`
    * `:outbox_batch_bytes` - approximate external-term bytes collected per
      flush, default `1_048_576`
    * `:outbox_flush_interval` - maximum batching delay in milliseconds,
      default `1`
    * `:outbox_deadline` - maximum useful residence time for an outgoing message
      in milliseconds, default `100`

  The deadline bounds stale work, not mailbox memory. A backend must also put a
  finite bound on every socket enqueue or write it performs. Exact snapshot
  messages are independently bounded by `:replicated_snapshot_chunk_target_bytes`.
  Other logical messages or a whole batch may still exceed `:outbox_batch_bytes`;
  a transport with a smaller finite frame size must segment and completely
  reassemble those batches before local delivery.
  """

  @type message :: Group.Transport.message()
  @type outgoing_result :: Group.Transport.outgoing_result()
  @type backend_state :: term()

  @callback init_outbox(group :: atom(), shard :: non_neg_integer(), opts :: keyword()) ::
              {:ok, backend_state()}

  @callback send_batch(
              target_node :: node(),
              messages :: [message()],
              deadline :: integer(),
              backend_state()
            ) :: {outgoing_result(), backend_state()}

  @default_deadline 100

  @doc """
  Returns a supervisor child specification for one outbox per Group shard.

  `:name`, `:num_shards`, and `:backend` are required. All options are passed
  unchanged to `backend.init_outbox/3`.
  """
  def child_spec(opts) do
    group = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, group},
      start: {Group.Transport.Outbox.Supervisor, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end

  @doc """
  Pushes a replica message into its shard's local outbox.

  This operation is local and nonblocking and performs no backend or socket
  work. `:ok` only means the outbox was available; the message may later expire
  or be dropped under transport pressure. A concurrently terminating outbox
  can also lose an accepted message, which anti-entropy repairs.
  """
  def push(group, target_node, shard, message, opts)
      when is_atom(group) and is_atom(target_node) and is_integer(shard) and shard >= 0 and
             is_list(opts) do
    case Process.whereis(name(group, shard)) do
      pid when is_pid(pid) ->
        deadline = monotonic_ms() + deadline(opts)
        send(pid, {:group_replica_outbox_push, target_node, deadline, message})
        :ok

      nil ->
        :disconnected
    end
  end

  @doc false
  def name(group, shard), do: :"#{group}_replica_transport_outbox_#{shard}"

  @doc false
  def monotonic_ms, do: System.monotonic_time(:millisecond)

  defp deadline(opts) do
    case Keyword.get(opts, :outbox_deadline, @default_deadline) do
      value when is_integer(value) and value > 0 ->
        value

      other ->
        raise ArgumentError, "expected :outbox_deadline to be positive, got: #{inspect(other)}"
    end
  end
end

defmodule Group.Transport.Outbox.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    group = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)
    backend = Keyword.fetch!(opts, :backend)

    Code.ensure_loaded!(backend)

    for {function, arity} <- [init_outbox: 3, send_batch: 4] do
      unless function_exported?(backend, function, arity) do
        raise ArgumentError,
              "outbox backend #{inspect(backend)} must implement #{function}/#{arity}"
      end
    end

    children =
      for shard <- 0..(num_shards - 1) do
        %{
          id: {Group.Transport.Outbox.Worker, group, shard},
          start: {Group.Transport.Outbox.Worker, :start_link, [opts, shard]},
          restart: :permanent,
          shutdown: 5_000
        }
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Group.Transport.Outbox.Worker do
  @moduledoc false
  use GenServer

  alias Group.Transport.Outbox

  @default_batch_size 64
  @default_batch_bytes 1_048_576
  @default_flush_interval 1

  def start_link(opts, shard) do
    group = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, {opts, shard}, name: Outbox.name(group, shard))
  end

  @impl true
  def init({opts, shard}) do
    group = Keyword.fetch!(opts, :name)
    backend = Keyword.fetch!(opts, :backend)
    {:ok, backend_state} = backend.init_outbox(group, shard, opts)

    {:ok,
     %{
       group: group,
       shard: shard,
       backend: backend,
       backend_state: backend_state,
       batch_size: positive_opt(opts, :outbox_batch_size, @default_batch_size),
       batch_bytes: positive_opt(opts, :outbox_batch_bytes, @default_batch_bytes),
       flush_interval: non_negative_opt(opts, :outbox_flush_interval, @default_flush_interval),
       pending: [],
       pending_count: 0,
       pending_bytes: 0,
       flush_ref: nil
     }}
  end

  @impl true
  def handle_info({:group_replica_outbox_push, target_node, deadline, message}, state)
      when is_atom(target_node) and is_integer(deadline) do
    if deadline <= Outbox.monotonic_ms() do
      {:noreply, state}
    else
      bytes = :erlang.external_size({target_node, message})

      state =
        if state.pending_count > 0 and
             (state.pending_count + 1 > state.batch_size or
                state.pending_bytes + bytes > state.batch_bytes) do
          flush(state)
        else
          state
        end

      state = put_pending(state, target_node, deadline, message, bytes)

      if state.pending_count >= state.batch_size or state.pending_bytes >= state.batch_bytes do
        {:noreply, flush(state)}
      else
        {:noreply, schedule_flush(state)}
      end
    end
  end

  def handle_info({:group_replica_outbox_flush, ref}, %{flush_ref: ref} = state) do
    {:noreply, flush(%{state | flush_ref: nil})}
  end

  def handle_info({:group_replica_outbox_flush, _stale_ref}, state), do: {:noreply, state}
  def handle_info(_message, state), do: {:noreply, state}

  defp put_pending(state, target_node, deadline, message, bytes) do
    entry = {target_node, deadline, message}

    %{
      state
      | pending: [entry | state.pending],
        pending_count: state.pending_count + 1,
        pending_bytes: state.pending_bytes + bytes
    }
  end

  defp schedule_flush(%{pending_count: 0} = state), do: state
  defp schedule_flush(%{flush_ref: ref} = state) when is_reference(ref), do: state

  defp schedule_flush(state) do
    ref = make_ref()
    Process.send_after(self(), {:group_replica_outbox_flush, ref}, state.flush_interval)
    %{state | flush_ref: ref}
  end

  defp flush(%{pending_count: 0} = state), do: cancel_flush(state)

  defp flush(state) do
    state = cancel_flush(state)
    now = Outbox.monotonic_ms()

    batches =
      state.pending
      |> Enum.reverse()
      |> Enum.reject(fn {_target_node, deadline, _message} -> deadline <= now end)
      |> Enum.group_by(fn {target_node, _deadline, _message} -> target_node end)

    backend_state =
      Enum.reduce(batches, state.backend_state, fn {target_node, entries}, backend_state ->
        messages = Enum.map(entries, fn {_target_node, _deadline, message} -> message end)

        deadline =
          entries
          |> Enum.map(fn {_target_node, deadline, _message} -> deadline end)
          |> Enum.min()

        if deadline <= Outbox.monotonic_ms() do
          backend_state
        else
          case state.backend.send_batch(target_node, messages, deadline, backend_state) do
            {result, next_backend_state} when result in [:ok, :busy, :disconnected] ->
              next_backend_state

            other ->
              raise "invalid #{inspect(state.backend)}.send_batch/4 return: #{inspect(other)}"
          end
        end
      end)

    %{
      state
      | backend_state: backend_state,
        pending: [],
        pending_count: 0,
        pending_bytes: 0
    }
  end

  defp cancel_flush(%{flush_ref: nil} = state), do: state

  defp cancel_flush(state) do
    Process.cancel_timer(state.flush_ref)
    %{state | flush_ref: nil}
  end

  defp positive_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 ->
        value

      other ->
        raise ArgumentError, "expected #{inspect(key)} to be positive, got: #{inspect(other)}"
    end
  end

  defp non_negative_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 ->
        value

      other ->
        raise ArgumentError,
              "expected #{inspect(key)} to be non-negative, got: #{inspect(other)}"
    end
  end
end
