defmodule Group.Replica.Snapshot do
  @moduledoc false

  # The target is for the complete snapshot frame, not just its rows. Per-row
  # external sizes conservatively include an extra ETF version byte, and this
  # reserve covers the frame tuple, stream identity, both list headers, and
  # integer fields. A single entry larger than the target remains one chunk.
  @default_envelope_reserve 512
  @max_compact_chunk_count 2_147_483_647

  def chunk_rows(registry_rows, pg_rows, target_bytes)
      when is_list(registry_rows) and is_list(pg_rows) and is_integer(target_bytes) and
             target_bytes > 0 do
    chunk_rows(registry_rows, pg_rows, target_bytes, @default_envelope_reserve)
  end

  def chunk_rows(registry_rows, pg_rows, target_bytes, envelope_bytes)
      when is_list(registry_rows) and is_list(pg_rows) and is_integer(target_bytes) and
             target_bytes > 0 and is_integer(envelope_bytes) and envelope_bytes >= 0 do
    registry_rows = Enum.sort_by(registry_rows, fn {key, _pid, _meta, _time} -> key end)
    pg_rows = Enum.sort_by(pg_rows, fn {key, pid, _meta, _time} -> {key, pid} end)
    payload_target = max(target_bytes - envelope_bytes, 1)

    acc = %{chunks: [], registry: [], pg: [], bytes: 0, count: 0}
    acc = Enum.reduce(registry_rows, acc, &add_row(&2, :registry, &1, payload_target))
    acc = Enum.reduce(pg_rows, acc, &add_row(&2, :pg, &1, payload_target))

    chunks =
      acc
      |> flush_chunk()
      |> Map.fetch!(:chunks)
      |> Enum.reverse()
      |> case do
        [] -> [{[], []}]
        chunks -> chunks
      end

    %{
      registry_count: length(registry_rows),
      pg_count: length(pg_rows),
      chunks: chunks
    }
  end

  def frame_envelope_bytes(stream_id, snapshot_seq, registry_count, pg_count) do
    # A non-empty ETF list adds a five-byte LIST_EXT header relative to an
    # empty list. Reserve that once for each domain. The large chunk integers
    # ensure every practical index/count uses no more space than this envelope.
    :erlang.external_size(
      {:snapshot_chunk, Group.Replica.Protocol.version(), stream_id, snapshot_seq,
       @max_compact_chunk_count, @max_compact_chunk_count, registry_count, pg_count, [], []}
    ) + 10
  end

  def new_staging_table do
    :ets.new(__MODULE__, [:set, :private])
  end

  def delete_staging_table(table) do
    try do
      :ets.delete(table)
    rescue
      ArgumentError -> true
    end

    :ok
  end

  def stage_rows(table, chunk_index, registry_rows, pg_rows) do
    objects =
      Enum.map(registry_rows, &staging_object(:registry, &1)) ++
        Enum.map(pg_rows, &staging_object(:pg, &1))

    size_before = :ets.info(table, :size)

    if :ets.insert_new(table, objects) and
         :ets.info(table, :size) - size_before == length(objects) do
      true = :ets.insert_new(table, {{:chunk, chunk_index}, registry_rows, pg_rows})
      :ok
    else
      {:error, :duplicate_row}
    end
  end

  def fold_registry(table, chunk_count, acc, fun) when is_function(fun, 2) do
    Enum.reduce(1..chunk_count, acc, fn chunk_index, inner ->
      {registry_rows, _pg_rows} = fetch_chunk(table, chunk_index)
      Enum.reduce(registry_rows, inner, fun)
    end)
  end

  def fold_pg(table, chunk_count, acc, fun) when is_function(fun, 2) do
    Enum.reduce(1..chunk_count, acc, fn chunk_index, inner ->
      {_registry_rows, pg_rows} = fetch_chunk(table, chunk_index)
      Enum.reduce(pg_rows, inner, fun)
    end)
  end

  def member_pg?(table, key, pid), do: :ets.member(table, {:pg, key, pid})

  defp add_row(%{count: count, bytes: bytes} = acc, domain, row, target) do
    row_bytes = :erlang.external_size(row)

    acc =
      if count > 0 and bytes + row_bytes > target do
        flush_chunk(acc)
      else
        acc
      end

    case domain do
      :registry ->
        %{
          acc
          | registry: [row | acc.registry],
            bytes: acc.bytes + row_bytes,
            count: acc.count + 1
        }

      :pg ->
        %{acc | pg: [row | acc.pg], bytes: acc.bytes + row_bytes, count: acc.count + 1}
    end
  end

  defp flush_chunk(%{count: 0} = acc), do: acc

  defp flush_chunk(acc) do
    chunk = {Enum.reverse(acc.registry), Enum.reverse(acc.pg)}

    %{acc | chunks: [chunk | acc.chunks], registry: [], pg: [], bytes: 0, count: 0}
  end

  defp staging_object(:registry, {key, _pid, _meta, _time}),
    do: {{:registry, key}}

  defp staging_object(:pg, {key, pid, _meta, _time}),
    do: {{:pg, key, pid}}

  defp fetch_chunk(table, chunk_index) do
    case :ets.lookup(table, {:chunk, chunk_index}) do
      [{{:chunk, ^chunk_index}, registry_rows, pg_rows}] -> {registry_rows, pg_rows}
    end
  end
end
