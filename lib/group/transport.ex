defmodule Group.Transport do
  @moduledoc """
  Transport contract for Group replica data.

  Implementations must return promptly and must never wait for socket or remote
  mailbox backpressure. This applies to `outgoing/5` and the optional lifecycle
  callbacks. Returning `:busy` or `:disconnected` is safe: replica anti-entropy
  will retransmit the missing state.

  Erlang distribution remains Group's control plane and supplies the stable
  node identity used here. A sideband adapter can use its `descriptor/2` in the
  control hello to exchange endpoints and pass incoming messages to
  `incoming/4` or `incoming_batch/4`. `source_node` is routing metadata supplied
  by the adapter; Group uses it to validate stream origins and member pids.

  Adapters do not need to preserve ordering. Group serializes writes per shard
  and sequences each origin/generation/shard/cluster/epoch stream; receivers
  discard duplicates and request gaps. Per-shard ordered delivery avoids repair
  traffic and is therefore the preferred fast path.

  A sideband implementation can delegate `outgoing/5` to
  `Group.Transport.Outbox.push/5`. That adds one local send only for
  the configured sideband transport; the default distribution adapter retains
  its direct remote `:erlang.send_nosuspend/3` path.
  """

  @type message :: term()
  @type outgoing_result :: :ok | :busy | :disconnected

  @callback id() :: term()
  @callback descriptor(group :: atom(), opts :: keyword()) :: term()
  @doc """
  Called when Group has an outgoing replica message for another node.

  This callback must return promptly and must never wait for transport or
  remote backpressure. `:ok` means the transport took responsibility for the
  message, not that the remote shard received it.
  """
  @callback outgoing(
              group :: atom(),
              target_node :: node(),
              shard :: non_neg_integer(),
              message(),
              opts :: keyword()
            ) :: outgoing_result()

  @callback child_spec(keyword()) :: Supervisor.child_spec() | :ignore

  @doc """
  Reports that one shard lane can address a peer through this transport.

  A transport sharing one node-level connection across lanes should retain the
  route until `peer_down/4` has retired every shard previously reported up.
  """
  @callback peer_up(
              group :: atom(),
              node(),
              shard :: non_neg_integer(),
              descriptor :: term(),
              opts :: keyword()
            ) :: :ok

  @doc "Reports that one shard lane no longer has live peer authority."
  @callback peer_down(
              group :: atom(),
              node(),
              shard :: non_neg_integer(),
              opts :: keyword()
            ) :: :ok

  @optional_callbacks child_spec: 1, peer_up: 5, peer_down: 4

  @doc """
  Passes an incoming replica message to the corresponding local shard.

  This is a local mailbox operation. `source_node` identifies the peer whose
  replica lane supplied the message. Stream generation, epoch, group, shard,
  origin, and member-pid ownership are validated by the replica.

  Returns `:disconnected` and drops the message if that shard is not currently
  registered, for example while its supervisor is restarting.
  """
  def incoming(group, source_node, shard, message)
      when is_atom(group) and is_atom(source_node) and is_integer(shard) and shard >= 0 do
    case Process.whereis(Group.Replica.shard_name(group, shard)) do
      pid when is_pid(pid) ->
        send(pid, {:group_replica_frame, source_node, message})
        :ok

      nil ->
        :disconnected
    end
  end

  @doc """
  Passes a complete incoming batch to the corresponding local shard.

  A finite-message transport may segment the encoded batch on the wire, but it
  must reassemble every segment before calling this function. Group never
  observes or applies a partial batch. `source_node` has the same routing
  meaning as in `incoming/4`.

  Like `incoming/4`, this returns `:disconnected` if the destination shard is
  unavailable.
  """
  def incoming_batch(group, source_node, shard, messages)
      when is_atom(group) and is_atom(source_node) and is_integer(shard) and shard >= 0 and
             is_list(messages) do
    case Process.whereis(Group.Replica.shard_name(group, shard)) do
      pid when is_pid(pid) ->
        send(pid, {:group_replica_batch, source_node, messages})
        :ok

      nil ->
        :disconnected
    end
  end

  def normalize(module) when is_atom(module), do: {module, []}
  def normalize({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}

  def normalize(other) do
    raise ArgumentError,
          "expected :replica_transport to be a module or {module, opts}, got: #{inspect(other)}"
  end

  def validate!({module, _opts} = transport) do
    Code.ensure_loaded!(module)

    for {function, arity} <- [id: 0, descriptor: 2, outgoing: 5] do
      unless function_exported?(module, function, arity) do
        raise ArgumentError,
              "replica transport #{inspect(module)} must implement #{function}/#{arity}"
      end
    end

    transport
  end
end
