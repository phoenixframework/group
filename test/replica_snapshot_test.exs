defmodule Group.ReplicaSnapshotTest do
  use ExUnit.Case, async: true

  alias Group.Replica.Snapshot

  test "streams a complete exact slice in byte-bounded chunks without retaining prior chunks" do
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
    envelope = Snapshot.stream_envelope_bytes(stream_id, 123)
    owner = self()

    emit = fn registry, pg, index ->
      send(owner, {:chunk, index, registry, pg})
      :ok
    end

    stream = Snapshot.new_stream(target, envelope, 1, emit)
    stream = Snapshot.stream_registry_many(registry_rows, stream)
    stream = Snapshot.stream_pg_many(pg_rows, stream)
    stream = Snapshot.finish_stream(stream)

    assert stream.registry_count == 80
    assert stream.pg_count == 80
    assert stream.chunk_count > 1
    assert stream.registry == []
    assert stream.pg == []

    chunks =
      for index <- 1..stream.chunk_count do
        assert_receive {:chunk, ^index, registry, pg}

        {registry, pg}
      end

    assert chunks |> Enum.flat_map(&elem(&1, 0)) == registry_rows
    assert chunks |> Enum.flat_map(&elem(&1, 1)) == pg_rows

    Enum.with_index(chunks, 1)
    |> Enum.each(fn {{registry, pg}, index} ->
      frame =
        {:snapshot_chunk, Group.Replica.WireProtocol.version(), stream_id, 123, index, registry,
         pg}

      assert :erlang.external_size(frame) <= target
    end)
  end

  test "streams an empty exact slice and permits one intrinsically oversized row" do
    owner = self()

    emit = fn registry, pg, index ->
      send(owner, {:chunk, index, registry, pg})
      :ok
    end

    empty = Snapshot.new_stream(1_024, 512, 1, emit) |> Snapshot.finish_stream()
    assert empty.chunk_count == 1
    assert_receive {:chunk, 1, [], []}

    row = {"large", self(), String.duplicate("x", 4_096), 1}
    large = Snapshot.new_stream(1_024, 512, 1, emit)
    large = Snapshot.stream_registry_many([row], large) |> Snapshot.finish_stream()
    assert large.chunk_count == 1
    assert_receive {:chunk, 1, [^row], []}
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

  test "a resumed stream recounts the exact snapshot but emits only the requested suffix" do
    pid = self()
    metadata = %{payload: String.duplicate("x", 96)}

    registry_rows =
      for index <- 1..80 do
        {"registry/#{index}", pid, metadata, index}
      end

    target = 2_048
    stream_id = {:group, node(), make_ref(), 0, nil, make_ref()}
    envelope = Snapshot.stream_envelope_bytes(stream_id, 123)

    emit_all = fn _registry, _pg, index ->
      send(self(), {:first_pass, index})
      :ok
    end

    first = Snapshot.new_stream(target, envelope, 1, emit_all)
    first = Snapshot.stream_registry_many(registry_rows, first) |> Snapshot.finish_stream()
    assert first.chunk_count > 3

    for index <- 1..first.chunk_count do
      assert_receive {:first_pass, ^index}
    end

    emit_suffix = fn registry, pg, index ->
      send(self(), {:suffix, index, registry, pg})
      :ok
    end

    resumed = Snapshot.new_stream(target, envelope, 3, emit_suffix)
    resumed = Snapshot.stream_registry_many(registry_rows, resumed) |> Snapshot.finish_stream()

    assert resumed.chunk_count == first.chunk_count
    assert resumed.registry_count == 80
    refute_receive {:suffix, 1, _, _}
    refute_receive {:suffix, 2, _, _}

    for index <- 3..resumed.chunk_count do
      assert_receive {:suffix, ^index, _, _}
    end
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
