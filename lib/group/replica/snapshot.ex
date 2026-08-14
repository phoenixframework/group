defmodule Group.Replica.Snapshot do
  @moduledoc false

  @max_frame_counter 18_446_744_073_709_551_615

  def stream_envelope_bytes(stream_id, snapshot_seq) do
    :erlang.external_size(
      {:snapshot_chunk, Group.Replica.WireProtocol.version(), stream_id, snapshot_seq,
       @max_frame_counter, [], []}
    ) + 10
  end

  def new_stream(target_bytes, envelope_bytes, start_index, emit)
      when is_integer(target_bytes) and target_bytes > 0 and is_integer(envelope_bytes) and
             envelope_bytes >= 0 and is_integer(start_index) and start_index > 0 and
             is_function(emit, 3) do
    %{
      payload_target: max(target_bytes - envelope_bytes, 1),
      start_index: start_index,
      emit: emit,
      registry: [],
      pg: [],
      bytes: 0,
      current_count: 0,
      chunk_count: 0,
      registry_count: 0,
      pg_count: 0
    }
  end

  def stream_registry_many(rows, stream) when is_list(rows) do
    Enum.reduce(rows, stream, &stream_row(&2, :registry, &1))
  end

  def stream_pg_many(rows, stream) when is_list(rows) do
    Enum.reduce(rows, stream, &stream_row(&2, :pg, &1))
  end

  def finish_stream(%{current_count: 0, chunk_count: 0} = stream),
    do: flush_stream_chunk(stream)

  def finish_stream(%{current_count: 0} = stream), do: stream
  def finish_stream(stream), do: flush_stream_chunk(stream)

  def new_staging_table do
    :ets.new(__MODULE__, [:set, :private])
  end

  def new_event_table do
    :ets.new(__MODULE__, [:ordered_set, :private])
  end

  def delete_staging_table(table) do
    try do
      :ets.delete(table)
    rescue
      ArgumentError -> true
    end

    :ok
  end

  def clear_staging_table(table) do
    try do
      :ets.delete_all_objects(table)
    rescue
      ArgumentError -> true
    end

    :ok
  end

  def stage_rows(table, chunk_index, registry_rows, pg_rows) do
    row_objects =
      Enum.map(registry_rows, &staging_object(:registry, chunk_index, &1)) ++
        Enum.map(pg_rows, &staging_object(:pg, chunk_index, &1))

    objects = [{{:chunk, chunk_index}, length(registry_rows), length(pg_rows)} | row_objects]
    size_before = :ets.info(table, :size)

    if :ets.insert_new(table, objects) and
         :ets.info(table, :size) - size_before == length(objects) do
      :ok
    else
      {:error, :duplicate_row}
    end
  end

  def chunk_matches?(table, chunk_index, registry_rows, pg_rows) do
    case :ets.lookup(table, {:chunk, chunk_index}) do
      [{{:chunk, ^chunk_index}, registry_count, pg_count}] ->
        registry_count == length(registry_rows) and pg_count == length(pg_rows) and
          Enum.all?(registry_rows, &staged_registry_row?(table, chunk_index, &1)) and
          Enum.all?(pg_rows, &staged_pg_row?(table, chunk_index, &1))

      [] ->
        false
    end
  end

  def fold_registry(table, _chunk_count, acc, fun) when is_function(fun, 2) do
    fold_staged_rows(
      table,
      [
        {{{:registry, :"$1"}, :"$2", :"$3", :"$4", :_}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ],
      acc,
      fun
    )
  end

  def fold_pg(table, _chunk_count, acc, fun) when is_function(fun, 2) do
    fold_staged_rows(
      table,
      [
        {{{:pg, :"$1", :"$2"}, :"$3", :"$4", :_}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
      ],
      acc,
      fun
    )
  end

  def new_event_buffer(table) do
    %{table: table, chunk_index: 0, events: [], count: 0}
  end

  def buffer_events(events, buffer) when is_list(events) do
    Enum.reduce(events, buffer, &buffer_event/2)
  end

  def buffer_event(event, buffer) do
    buffer =
      if buffer.count == 512 do
        flush_event_buffer(buffer)
      else
        buffer
      end

    %{buffer | events: [event | buffer.events], count: buffer.count + 1}
  end

  def finish_event_buffer(%{count: 0} = buffer), do: buffer
  def finish_event_buffer(buffer), do: flush_event_buffer(buffer)

  def fold_events(table, acc, fun) when is_function(fun, 2) do
    :ets.foldl(
      fn {{:events, _chunk_index}, events}, inner ->
        Enum.reduce(events, inner, fun)
      end,
      acc,
      table
    )
  end

  def member_pg?(table, key, pid), do: :ets.member(table, {:pg, key, pid})
  def member_registry?(table, key), do: :ets.member(table, {:registry, key})

  defp staging_object(:registry, chunk_index, {key, pid, meta, time}),
    do: {{:registry, key}, pid, meta, time, chunk_index}

  defp staging_object(:pg, chunk_index, {key, pid, meta, time}),
    do: {{:pg, key, pid}, meta, time, chunk_index}

  defp staged_registry_row?(table, chunk_index, {key, pid, meta, time}) do
    :ets.lookup(table, {:registry, key}) ==
      [{{:registry, key}, pid, meta, time, chunk_index}]
  end

  defp staged_pg_row?(table, chunk_index, {key, pid, meta, time}) do
    :ets.lookup(table, {:pg, key, pid}) == [{{:pg, key, pid}, meta, time, chunk_index}]
  end

  defp fold_staged_rows(table, match_spec, acc, fun) do
    case :ets.select(table, match_spec, 4_096) do
      :"$end_of_table" -> acc
      {rows, continuation} -> fold_staged_rows(continuation, Enum.reduce(rows, acc, fun), fun)
    end
  end

  defp fold_staged_rows(continuation, acc, fun) do
    case :ets.select(continuation) do
      :"$end_of_table" -> acc
      {rows, next} -> fold_staged_rows(next, Enum.reduce(rows, acc, fun), fun)
    end
  end

  defp stream_row(stream, domain, row) do
    row_bytes = :erlang.external_size(row)

    stream =
      if stream.current_count > 0 and stream.bytes + row_bytes > stream.payload_target do
        flush_stream_chunk(stream)
      else
        stream
      end

    case domain do
      :registry ->
        %{
          stream
          | registry: [row | stream.registry],
            bytes: stream.bytes + row_bytes,
            current_count: stream.current_count + 1,
            registry_count: stream.registry_count + 1
        }

      :pg ->
        %{
          stream
          | pg: [row | stream.pg],
            bytes: stream.bytes + row_bytes,
            current_count: stream.current_count + 1,
            pg_count: stream.pg_count + 1
        }
    end
  end

  defp flush_stream_chunk(stream) do
    chunk_index = stream.chunk_count + 1

    if chunk_index >= stream.start_index do
      :ok =
        stream.emit.(
          Enum.reverse(stream.registry),
          Enum.reverse(stream.pg),
          chunk_index
        )
    end

    %{
      stream
      | registry: [],
        pg: [],
        bytes: 0,
        current_count: 0,
        chunk_count: chunk_index
    }
  end

  defp flush_event_buffer(buffer) do
    chunk_index = buffer.chunk_index + 1
    true = :ets.insert_new(buffer.table, {{:events, chunk_index}, Enum.reverse(buffer.events)})
    %{buffer | chunk_index: chunk_index, events: [], count: 0}
  end
end
