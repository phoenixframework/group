defmodule Group.ControlledReplicaTransport do
  @moduledoc false
  @behaviour Group.Replica.Transport

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
  def try_send(group, target_node, shard, frame, opts) do
    case :persistent_term.get({__MODULE__, group, :mode}, :capture) do
      :capture ->
        controller = Keyword.fetch!(opts, :controller)
        send(controller, {__MODULE__, :frame, group, node(), target_node, shard, frame})
        :ok

      :pass ->
        deliver(group, target_node, shard, frame)

      :busy ->
        :busy

      :drop ->
        :ok
    end
  end

  defp deliver(group, target_node, shard, frame) do
    destination = {Group.Replica.shard_name(group, shard), target_node}
    message = {:group_replica_frame, node(), frame}

    case :erlang.send_nosuspend(destination, message, [:noconnect]) do
      true -> :ok
      false -> :busy
    end
  end
end
