defmodule Group.ControlledReplicaTransport do
  @moduledoc false
  @behaviour Group.Transport

  @impl true
  def id, do: :group_controlled_replica_transport

  @impl true
  def descriptor(_group, _opts), do: :group_controlled_replica_transport

  def set_mode(group, mode) when mode in [:capture, :pass, :busy, :drop] do
    :persistent_term.put({__MODULE__, group, :mode}, mode)
    :ok
  end

  def clear(group) do
    :persistent_term.erase({__MODULE__, group, :mode})
    :ok
  end

  @impl true
  def outgoing(group, target_node, shard, message, opts) do
    case :persistent_term.get({__MODULE__, group, :mode}, :capture) do
      :capture ->
        controller = Keyword.fetch!(opts, :controller)
        send(controller, {__MODULE__, :message, group, node(), target_node, shard, message})
        :ok

      :pass ->
        forward(group, target_node, shard, message)

      :busy ->
        :busy

      :drop ->
        :ok
    end
  end

  defp forward(group, target_node, shard, replica_message) do
    destination = {Group.Replica.shard_name(group, shard), target_node}
    message = {:group_replica_frame, node(), replica_message}

    case :erlang.send_nosuspend(destination, message, [:noconnect]) do
      true -> :ok
      false -> :busy
    end
  end
end
