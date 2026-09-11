defmodule Group.Jepsen.RepairCoverage do
  @moduledoc """
  Test-only instrumentation of receiver commit boundaries. No sender admission,
  socket completion, or provisional snapshot chunk is evidence of application.

  Compile before starting Group. Wrapping the two private functions keeps the
  cursor observations in the receiving shard's serialized turn; an unrelated
  run cannot advance the cursor between them. Production BEAMs are unchanged.
  """
  use GenServer

  def install! do
    path = Path.expand("../../lib/group/replica.ex", __DIR__)
    ast = path |> File.read!() |> Code.string_to_quoted!(file: path)

    {ast, wrapped} =
      Macro.postwalk(ast, [], fn
        {:defp, meta, [{name, _, args} = signature, [do: body]]}, found
        when name in [:apply_replica_delta_run, :commit_snapshot_transfer] ->
          expected =
            case name do
              :apply_replica_delta_run ->
                [:state, :source_node, :stream_id, :records, :advertised_head]

              :commit_snapshot_transfer ->
                [:state, :key, :source_node, :stream_id, :transfer]
            end

          unless Enum.map(args, &elem(&1, 0)) == expected do
            raise "replica repair signature changed: #{Macro.to_string(signature)}"
          end

          [state, _, stream | _] =
            if name == :commit_snapshot_transfer do
              [Enum.at(args, 0), Enum.at(args, 2), Enum.at(args, 3)]
            else
              args
            end

          kind =
            if name == :commit_snapshot_transfer,
              do: quote(do: {:snapshot, elem(unquote(List.last(args)).manifest, 0)}),
              else: :delta

          wrapped =
            quote do
              before_cursor =
                Group.Replica.Data.replica_cursor(
                  unquote(state).name,
                  unquote(state).shard_index,
                  unquote(stream)
                )

              result = unquote(body)

              after_cursor =
                Group.Replica.Data.replica_cursor(
                  unquote(state).name,
                  unquote(state).shard_index,
                  unquote(stream)
                )

              Group.Jepsen.RepairCoverage.observe(
                unquote(kind),
                before_cursor,
                after_cursor
              )

              result
            end

          {{:defp, meta, [signature, [do: wrapped]]}, [name | found]}

        other, found ->
          {other, found}
      end)

    unless Enum.sort(wrapped) == [:apply_replica_delta_run, :commit_snapshot_transfer] do
      raise "replica repair boundaries changed: #{inspect(wrapped)}"
    end

    Code.compile_quoted(ast, path)
    :ok
  end

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  # Only a local message on the shard. Disk I/O happens in this test-only owner.
  def observe(kind, before_cursor, after_cursor) when after_cursor > before_cursor do
    GenServer.cast(__MODULE__, {:committed, kind, after_cursor - before_cursor})
  end

  def observe(_, _, _), do: :ok

  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    {:ok, %{path: path, events: read_events(path)}}
  end

  @impl true
  def handle_cast({:committed, kind, records}, state) do
    evidence =
      case kind do
        :delta -> %{applied_delta_run_records_peak: records}
        {:snapshot, chunks} when chunks > 1 -> %{multi_chunk_snapshot_committed: 1}
        {:snapshot, _} -> %{}
      end

    previous = if File.exists?(state.path), do: state.events, else: %{}
    events = Map.merge(previous, evidence, fn _, old, new -> max(old, new) end)

    if events != previous do
      # Append only this observation, never cached evidence from a previous
      # history. reset-oracle! deletes the file while the VM may already be up.
      for {event, value} <- evidence do
        File.write!(state.path, "#{event}\t#{value}\n", [:append])
      end
    end

    {:noreply, %{state | events: events}}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    events = read_events(state.path)
    {:reply, events, %{state | events: events}}
  end

  defp read_events(path) do
    case File.read(path) do
      {:ok, contents} ->
        {committed, [suffix]} = contents |> String.split("\n") |> Enum.split(-1)

        events =
          Enum.reduce(committed, %{}, fn line, events ->
            [event, value] = String.split(line, "\t")
            event = String.to_existing_atom(event)
            value = String.to_integer(value)
            Map.update(events, event, value, &max(&1, value))
          end)

        # Validate committed records before touching the file. An interrupted
        # append is not evidence, and must not become the next append's prefix.
        # Truncate only the suffix in place, preserving every committed byte.
        if suffix != "" do
          offset = byte_size(contents) - byte_size(suffix)

          File.open!(path, [:read, :write, :binary], fn file ->
            {:ok, ^offset} = :file.position(file, offset)
            :ok = :file.truncate(file)
          end)
        end

        events

      {:error, :enoent} ->
        %{}
    end
  end
end
