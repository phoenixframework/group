defmodule Group.TestReplicaTransport do
  @moduledoc false
  @behaviour Group.Transport

  @impl true
  def id, do: :group_test_transport

  @impl true
  def descriptor(_group, _opts), do: :group_test_transport

  @simple_modes [:pass, :drop, :busy, :duplicate]

  def set_mode(group, mode)
      when mode in @simple_modes or
             (is_tuple(mode) and tuple_size(mode) == 2 and
                elem(mode, 0) in [:drop_types, :duplicate_types, :capture_drop, :capture_pass]) or
             (is_tuple(mode) and tuple_size(mode) == 3 and elem(mode, 0) == :delay_types) or
             (is_tuple(mode) and tuple_size(mode) == 2 and elem(mode, 0) == :chaos) do
    :persistent_term.put({__MODULE__, group}, mode)
    :ok
  end

  def captured(group) do
    :persistent_term.get({__MODULE__, group, :captured}, []) |> Enum.reverse()
  end

  def clear_captured(group) do
    :persistent_term.erase({__MODULE__, group, :captured})
    :ok
  end

  def clear(group) do
    :persistent_term.erase({__MODULE__, group})
    :persistent_term.erase({__MODULE__, group, :captured})
    :ok
  end

  @impl true
  def outgoing(group, target_node, shard, message, _opts) do
    case :persistent_term.get({__MODULE__, group}, :pass) do
      :drop ->
        :ok

      :busy ->
        :busy

      :duplicate ->
        forward(group, target_node, shard, message)
        forward(group, target_node, shard, message)

      {:drop_types, types} ->
        if message_type(message) in types,
          do: :ok,
          else: forward(group, target_node, shard, message)

      {:duplicate_types, types} ->
        if message_type(message) in types do
          forward(group, target_node, shard, message)
          forward(group, target_node, shard, message)
        else
          forward(group, target_node, shard, message)
        end

      {:delay_types, delays, default_delay} ->
        delay = Map.get(delays, message_type(message), default_delay)
        delayed_forward(group, target_node, shard, message, delay)

      {:capture_drop, types} ->
        if message_type(message) in types, do: capture(group, target_node, shard, message)
        :ok

      {:capture_pass, types} ->
        if message_type(message) in types, do: capture(group, target_node, shard, message)
        forward(group, target_node, shard, message)

      {:chaos, opts} ->
        chaos_forward(group, target_node, shard, message, opts)

      :pass ->
        forward(group, target_node, shard, message)
    end
  end

  defp chaos_forward(group, target_node, shard, message, opts) do
    hash = :erlang.phash2({target_node, shard, message}, 1_000_003)
    drop_every = Keyword.get(opts, :drop_every, 0)
    duplicate_every = Keyword.get(opts, :duplicate_every, 0)
    max_delay = Keyword.get(opts, :max_delay, 0)

    cond do
      drop_every > 0 and rem(hash, drop_every) == 0 ->
        :ok

      duplicate_every > 0 and rem(hash, duplicate_every) == 0 ->
        delay = if max_delay > 0, do: rem(hash, max_delay + 1), else: 0
        delayed_forward(group, target_node, shard, message, delay)
        delayed_forward(group, target_node, shard, message, max(max_delay - delay, 0))

      true ->
        delay = if max_delay > 0, do: rem(hash, max_delay + 1), else: 0
        delayed_forward(group, target_node, shard, message, delay)
    end
  end

  defp delayed_forward(group, target_node, shard, message, delay) when delay <= 0,
    do: forward(group, target_node, shard, message)

  defp delayed_forward(group, target_node, shard, message, delay) do
    source_node = node()

    spawn(fn ->
      receive do
      after
        delay -> forward(group, target_node, shard, message, source_node)
      end
    end)

    :ok
  end

  defp capture(group, target_node, shard, message) do
    key = {__MODULE__, group, :captured}
    captured = :persistent_term.get(key, [])
    :persistent_term.put(key, [{target_node, shard, message} | captured])
  end

  defp forward(group, target_node, shard, message),
    do: forward(group, target_node, shard, message, node())

  defp forward(group, target_node, shard, replica_message, source_node) do
    destination = {Group.Replica.shard_name(group, shard), target_node}
    message = {:group_replica_frame, source_node, replica_message}

    case :erlang.send_nosuspend(destination, message, [:noconnect]) do
      true -> :ok
      false -> :busy
    end
  end

  defp message_type(message) when is_tuple(message), do: elem(message, 0)
  defp message_type(_message), do: :unknown
end
