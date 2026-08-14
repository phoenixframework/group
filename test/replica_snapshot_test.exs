defmodule Group.ReplicaSnapshotTest do
  use ExUnit.Case, async: true

  alias Group.Replica.Snapshot

  test "partitions a complete exact slice into byte-bounded deterministic chunks" do
    pid = self()
    metadata = %{payload: String.duplicate("x", 96)}

    registry_rows =
      for index <- 1..80 do
        {"registry/#{index}", pid, metadata, index}
      end

    pg_rows =
      for index <- 1..80 do
        {"pg/#{index}", pid, metadata, index}
      end

    target = 2_048
    stream_id = {:group, node(), make_ref(), 0, nil, make_ref()}
    envelope = Snapshot.frame_envelope_bytes(stream_id, 123, 80, 80)

    snapshot =
      Snapshot.chunk_rows(Enum.reverse(registry_rows), Enum.reverse(pg_rows), target, envelope)

    assert snapshot.registry_count == 80
    assert snapshot.pg_count == 80
    assert length(snapshot.chunks) > 1

    assert snapshot.chunks ==
             Snapshot.chunk_rows(registry_rows, pg_rows, target, envelope).chunks

    assert snapshot.chunks |> Enum.flat_map(&elem(&1, 0)) |> MapSet.new() ==
             MapSet.new(registry_rows)

    assert snapshot.chunks |> Enum.flat_map(&elem(&1, 1)) |> MapSet.new() ==
             MapSet.new(pg_rows)

    chunk_count = length(snapshot.chunks)

    Enum.with_index(snapshot.chunks, 1)
    |> Enum.each(fn {{registry, pg}, index} ->
      frame =
        {:snapshot_chunk, Group.Replica.WireProtocol.version(), stream_id, 123, index,
         chunk_count, snapshot.registry_count, snapshot.pg_count, registry, pg}

      assert :erlang.external_size(frame) <= target
    end)
  end

  test "represents an empty exact slice and permits one intrinsically oversized row" do
    assert Snapshot.chunk_rows([], [], 1_024).chunks == [{[], []}]

    row = {"large", self(), String.duplicate("x", 4_096), 1}
    snapshot = Snapshot.chunk_rows([row], [], 1_024)
    assert snapshot.chunks == [{[row], []}]
  end

  test "staging is set-valued across chunks and remains private to its owner" do
    table = Snapshot.new_staging_table()
    registry = {"registry", self(), %{v: 1}, 1}
    pg = {"pg", self(), %{v: 2}, 2}

    assert :ok = Snapshot.stage_rows(table, 1, [registry], [pg])
    assert :ets.info(table, :size) == 3
    assert {:error, :duplicate_row} = Snapshot.stage_rows(table, 2, [registry], [])

    assert Snapshot.fold_registry(table, 1, [], fn row, acc -> [row | acc] end) == [registry]
    assert Snapshot.fold_pg(table, 1, [], fn row, acc -> [row | acc] end) == [pg]
    assert Snapshot.member_pg?(table, "pg", self())

    assert :ok = Snapshot.delete_staging_table(table)
    assert :ets.info(table) == :undefined
  end

  test "capture tables emit deterministic chunks with only one bounded chunk on the heap" do
    table = Snapshot.new_capture_table()
    pid = self()
    metadata = %{payload: String.duplicate("x", 96)}

    registry_rows =
      for index <- 80..1//-1 do
        {"registry/#{index}", pid, metadata, index}
      end

    pg_rows =
      for index <- 80..1//-1 do
        {"pg/#{index}", pid, metadata, index}
      end

    target = 2_048
    stream_id = {:group, node(), make_ref(), 0, nil, make_ref()}
    envelope = Snapshot.capture_envelope_bytes(stream_id, 123)

    capture = Snapshot.new_capture(table, target, envelope)
    capture = Snapshot.capture_registry_many(registry_rows, capture)
    capture = Snapshot.capture_pg_many(pg_rows, capture)
    capture = Snapshot.finish_capture(capture)
    chunk_count = capture.chunk_count

    assert chunk_count > 1
    assert :ets.info(table, :size) == chunk_count
    assert capture.registry == []
    assert capture.pg == []

    assert {:ok, chunks} =
             Snapshot.reduce_capture_chunks(table, chunk_count, [], fn registry, pg, acc ->
               {:cont, [{registry, pg} | acc]}
             end)

    chunks = Enum.reverse(chunks)
    assert length(chunks) == chunk_count

    chunks
    |> Enum.with_index(1)
    |> Enum.each(fn {{registry, pg}, index} ->
      frame =
        {:snapshot_chunk, Group.Replica.WireProtocol.version(), stream_id, 123, index,
         chunk_count, 80, 80, registry, pg}

      assert :erlang.external_size(frame) <= target
    end)

    assert chunks |> Enum.flat_map(&elem(&1, 0)) |> MapSet.new() == MapSet.new(registry_rows)
    assert chunks |> Enum.flat_map(&elem(&1, 1)) |> MapSet.new() == MapSet.new(pg_rows)
    assert :ets.info(table, :size) == chunk_count

    assert :ok = Snapshot.delete_staging_table(table)
  end

  test "event buffering preserves every event across bounded ETS chunks" do
    table = Snapshot.new_event_table()
    events = Enum.map(1..1_025, &{:event, &1})

    buffer = Snapshot.new_event_buffer(table)
    buffer = Snapshot.buffer_events(events, buffer)
    buffer = Snapshot.finish_event_buffer(buffer)

    assert buffer.events == []
    assert buffer.count == 0
    assert buffer.chunk_index == 3
    assert :ets.info(table, :size) == 3

    assert Snapshot.fold_events(table, [], fn event, acc -> [event | acc] end)
           |> Enum.reverse() == events

    assert :ok = Snapshot.delete_staging_table(table)
  end
end
