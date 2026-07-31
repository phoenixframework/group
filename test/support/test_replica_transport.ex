defmodule Group.TestReplicaTransport do
  @moduledoc false
  @behaviour Group.Replica.Transport

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
  def try_send(group, target_node, shard, frame, _opts) do
    case :persistent_term.get({__MODULE__, group}, :pass) do
      :drop ->
        :ok

      :busy ->
        :busy

      :duplicate ->
        deliver(group, target_node, shard, frame)
        deliver(group, target_node, shard, frame)

      {:drop_types, types} ->
        if frame_type(frame) in types, do: :ok, else: deliver(group, target_node, shard, frame)

      {:duplicate_types, types} ->
        if frame_type(frame) in types do
          deliver(group, target_node, shard, frame)
          deliver(group, target_node, shard, frame)
        else
          deliver(group, target_node, shard, frame)
        end

      {:delay_types, delays, default_delay} ->
        delay = Map.get(delays, frame_type(frame), default_delay)
        delayed_deliver(group, target_node, shard, frame, delay)

      {:capture_drop, types} ->
        if frame_type(frame) in types, do: capture(group, target_node, shard, frame)
        :ok

      {:capture_pass, types} ->
        if frame_type(frame) in types, do: capture(group, target_node, shard, frame)
        deliver(group, target_node, shard, frame)

      {:chaos, opts} ->
        chaos_deliver(group, target_node, shard, frame, opts)

      :pass ->
        deliver(group, target_node, shard, frame)
    end
  end

  defp chaos_deliver(group, target_node, shard, frame, opts) do
    hash = :erlang.phash2({target_node, shard, frame}, 1_000_003)
    drop_every = Keyword.get(opts, :drop_every, 0)
    duplicate_every = Keyword.get(opts, :duplicate_every, 0)
    max_delay = Keyword.get(opts, :max_delay, 0)

    cond do
      drop_every > 0 and rem(hash, drop_every) == 0 ->
        :ok

      duplicate_every > 0 and rem(hash, duplicate_every) == 0 ->
        delay = if max_delay > 0, do: rem(hash, max_delay + 1), else: 0
        delayed_deliver(group, target_node, shard, frame, delay)
        delayed_deliver(group, target_node, shard, frame, max(max_delay - delay, 0))

      true ->
        delay = if max_delay > 0, do: rem(hash, max_delay + 1), else: 0
        delayed_deliver(group, target_node, shard, frame, delay)
    end
  end

  defp delayed_deliver(group, target_node, shard, frame, delay) when delay <= 0,
    do: deliver(group, target_node, shard, frame)

  defp delayed_deliver(group, target_node, shard, frame, delay) do
    source_node = node()

    spawn(fn ->
      receive do
      after
        delay -> deliver(group, target_node, shard, frame, source_node)
      end
    end)

    :ok
  end

  defp capture(group, target_node, shard, frame) do
    key = {__MODULE__, group, :captured}
    captured = :persistent_term.get(key, [])
    :persistent_term.put(key, [{target_node, shard, frame} | captured])
  end

  defp deliver(group, target_node, shard, frame),
    do: deliver(group, target_node, shard, frame, node())

  defp deliver(group, target_node, shard, frame, source_node) do
    destination = {Group.Replica.shard_name(group, shard), target_node}
    message = {:group_replica_frame, source_node, frame}

    case :erlang.send_nosuspend(destination, message, [:noconnect]) do
      true -> :ok
      false -> :busy
    end
  end

  defp frame_type(frame) when is_tuple(frame), do: elem(frame, 0)
  defp frame_type(_frame), do: :unknown
end
