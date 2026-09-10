defmodule Group.Transport.DistErl do
  @moduledoc """
  Default nonblocking replica transport over Erlang distribution.

  Messages are sent directly to the matching remote shard with
  `:erlang.send_nosuspend/3` and `:noconnect`, so the caller never waits for a
  busy distribution socket and never initiates a connection. A busy or absent
  link returns `:busy`; Group drops that message and repairs it through periodic
  anti-entropy.
  """

  @behaviour Group.Transport

  alias Group.Replica

  @impl true
  def id, do: :erlang_distribution

  @impl true
  def descriptor(_group, _opts), do: :erlang_distribution

  @impl true
  def outgoing(group, target_node, shard, replica_message, _opts) do
    destination = {Replica.shard_name(group, shard), target_node}
    message = {:group_replica_frame, node(), replica_message}

    case :erlang.send_nosuspend(destination, message, [:noconnect]) do
      true -> :ok
      false -> :busy
    end
  end
end
