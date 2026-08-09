defmodule Group.Replica.Transport do
  @moduledoc """
  Transport contract for Group replica data.

  Implementations must return promptly and must never wait for socket or remote
  mailbox backpressure. This applies to `try_send/5` and the optional lifecycle
  callbacks. Returning `:busy` or `:disconnected` is safe: replica anti-entropy
  will retransmit the missing state.

  Erlang distribution remains Group's control plane and supplies the stable
  node identity used here. A sideband adapter can use its `descriptor/2` in the
  control hello to exchange endpoints, authenticate the connection as that
  node, and pass inbound frames to `deliver/4`.

  Adapters do not need to preserve ordering. Group serializes writes per shard
  and sequences each origin/generation/shard/cluster/epoch stream; receivers
  discard duplicates and request gaps. Per-shard ordered delivery avoids repair
  traffic and is therefore the preferred fast path.

  A sideband implementation can delegate `try_send/5` to
  `Group.Replica.Transport.Outbox.try_send/5`. That adds one local send only for
  the configured sideband transport; the default distribution adapter retains
  its direct remote `:erlang.send_nosuspend/3` path.
  """

  @type frame :: term()
  @type send_result :: :ok | :busy | :disconnected

  @callback id() :: term()
  @callback descriptor(group :: atom(), opts :: keyword()) :: term()
  @callback try_send(
              group :: atom(),
              target_node :: node(),
              shard :: non_neg_integer(),
              frame(),
              opts :: keyword()
            ) :: send_result()

  @callback child_spec(keyword()) :: Supervisor.child_spec() | :ignore
  @callback peer_up(group :: atom(), node(), descriptor :: term(), opts :: keyword()) :: :ok
  @callback peer_down(group :: atom(), node(), opts :: keyword()) :: :ok

  @optional_callbacks child_spec: 1, peer_up: 4, peer_down: 3

  @doc """
  Delivers a frame received by a transport adapter to the local replica shard.

  `source_node` must come from the adapter's authenticated peer identity, never
  from untrusted frame contents. Delivery is a local mailbox operation; stream
  generation, epoch, group, shard, and origin are validated by the replica.
  """
  def deliver(group, source_node, shard, frame)
      when is_atom(group) and is_atom(source_node) and is_integer(shard) and shard >= 0 do
    send(Group.Replica.shard_name(group, shard), {:group_replica_frame, source_node, frame})
    :ok
  end

  @doc """
  Delivers a complete batch received from one authenticated peer.

  A finite-frame transport may segment the encoded batch on the wire, but it
  must authenticate the peer and reassemble every segment before calling this
  function. Group never observes or applies a partial batch.
  """
  def deliver_batch(group, source_node, shard, frames)
      when is_atom(group) and is_atom(source_node) and is_integer(shard) and shard >= 0 and
             is_list(frames) do
    send(
      Group.Replica.shard_name(group, shard),
      {:group_replica_batch, source_node, frames}
    )

    :ok
  end

  def normalize(module) when is_atom(module), do: {module, []}
  def normalize({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}

  def normalize(other) do
    raise ArgumentError,
          "expected :replica_transport to be a module or {module, opts}, got: #{inspect(other)}"
  end

  def validate!({module, _opts} = transport) do
    Code.ensure_loaded!(module)

    for {function, arity} <- [id: 0, descriptor: 2, try_send: 5] do
      unless function_exported?(module, function, arity) do
        raise ArgumentError,
              "replica transport #{inspect(module)} must implement #{function}/#{arity}"
      end
    end

    transport
  end
end

defmodule Group.Replica.Transport.Distribution do
  @moduledoc false
  @behaviour Group.Replica.Transport

  alias Group.Replica

  @impl true
  def id, do: :erlang_distribution

  @impl true
  def descriptor(_group, _opts), do: :erlang_distribution

  @impl true
  def try_send(group, target_node, shard, frame, _opts) do
    destination = {Replica.shard_name(group, shard), target_node}
    message = {:group_replica_frame, node(), frame}

    case :erlang.send_nosuspend(destination, message, [:noconnect]) do
      true -> :ok
      false -> :busy
    end
  end
end
