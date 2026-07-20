defmodule Group.Dispatcher do
  @moduledoc false

  use GenServer

  alias Group.Replica.Data

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)

    GenServer.start_link(__MODULE__, opts, name: dispatcher_name(name, shard_index))
  end

  def dispatcher_name(name, shard_index), do: :"#{name}_dispatcher_#{shard_index}"

  @impl true
  def init(opts) do
    state = %{
      name: Keyword.fetch!(opts, :name),
      shard_index: Keyword.fetch!(opts, :shard_index)
    }

    {:ok, state}
  end

  @impl true
  def handle_info({:group_dispatch, cluster, key, message}, state) when is_binary(key) do
    dispatch_local_members(state.name, state.shard_index, cluster, key, message)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp dispatch_local_members(name, shard, cluster, key, message) do
    local = node()

    case Data.registry_lookup(name, shard, cluster, key) do
      {pid, _meta, _time, ^local} -> send(pid, message)
      _ -> :ok
    end

    for pid <- Data.pg_members_local(name, shard, cluster, key) do
      send(pid, message)
    end

    :ok
  end
end
