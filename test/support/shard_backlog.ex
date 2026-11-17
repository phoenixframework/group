defmodule Group.ShardBacklog do
  @moduledoc false

  # Preloads one replica shard's mailbox with replicated batches and measures
  # what handling them costs. Every function runs on the node that owns the
  # shard, so the backlog is built with local sends and measured without a
  # distribution hop in the path.

  alias Group.Replica
  alias Group.Replica.{Data, WireProtocol}

  def spawn_member, do: spawn(fn -> Process.sleep(:infinity) end)

  def stop_group(pid), do: Process.exit(pid, :kill)

  @doc """
  Preloads `size` batches and measures the first `measured` of them in `mode`.
  """
  def run(mode, name, source, stream, member, size, measured, opts \\ []) do
    shard = preload(name, source, stream, member, size, opts)

    case mode do
      "live" -> drain_with_live_writer(name, shard, stream, measured)
      "profile" -> profile(name, shard, stream, measured)
      _ -> drain(name, shard, stream, measured, 600_000, Keyword.get(opts, :suspend, true))
    end
  end

  @doc """
  Queues `count` replicated batches behind shard 0, suspending it first unless
  `suspend: false` is given.

  Each batch carries `:records_per_batch` (default one) `delta_batch` messages,
  each one contiguous run of one record. Records alternate a join and the
  matching leave so the applied state stays small while every record is
  accepted and advances the stream cursor.

  `:local_every` interleaves one asynchronous local join per that many batches.
  """
  def preload(name, source, stream, member, count, opts \\ []) do
    shard = Process.whereis(Replica.shard_name(name, 0))
    local_every = Keyword.get(opts, :local_every)
    first_seq = Keyword.get(opts, :first_seq, 1)
    epoch = Data.local_cluster_epoch(name, nil)
    local_owner = spawn(fn -> Process.sleep(:infinity) end)
    records = Keyword.get(opts, :records_per_batch, 1)

    # Build every message first so the shard is suspended only while they are
    # sent. A suspended shard sends no heartbeats; pass `suspend: false` when a
    # peer lease is short enough to notice.
    messages =
      Enum.flat_map(0..(count - 1)//1, fn index ->
        batch = batch(source, stream, member, first_seq + index * records, records)

        if local_every && rem(index + 1, local_every) == 0 do
          request = {:join, nil, epoch, "backlog/local/#{index}", local_owner, %{}}
          [batch, {:group_local_request, :noreply, request}]
        else
          [batch]
        end
      end)

    if Keyword.get(opts, :suspend, true), do: :ok = :sys.suspend(shard)
    Enum.each(messages, &send(shard, &1))
    shard
  end

  def batch(source, stream, member, first_seq, records \\ 1) do
    version = WireProtocol.version()

    deltas =
      for seq <- first_seq..(first_seq + records - 1) do
        {:delta_batch, version, [{stream, seq, [{seq, [mutation(source, member, seq)]}], seq}]}
      end

    {:group_replica_batch, source, deltas}
  end

  defp mutation(source, member, seq) do
    key = "backlog/remote/#{div(seq - 1, 2)}"
    meta = %{region: "iad", weight: seq, tags: ["replicated", "backlog"]}

    if rem(seq, 2) == 1 do
      {:join, nil, key, member, meta, seq, :normal, source}
    else
      {:leave, nil, key, member, meta, :normal}
    end
  end

  @doc """
  Resumes the shard, unless `resume?` is false because it was never suspended,
  and waits until the stream cursor reaches `target`.

  Returns the elapsed microseconds, the reductions the shard spent, and the
  number of messages still queued.
  """
  def drain(name, shard, stream, target, timeout \\ 600_000, resume? \\ true) do
    {:reductions, reductions_before} = Process.info(shard, :reductions)
    started = System.monotonic_time(:microsecond)
    if resume?, do: :ok = :sys.resume(shard)
    :ok = await_cursor(name, stream, target, started + timeout * 1_000)
    elapsed = System.monotonic_time(:microsecond) - started
    {:reductions, reductions_after} = Process.info(shard, :reductions)
    {:message_queue_len, queued} = Process.info(shard, :message_queue_len)

    %{
      micros: elapsed,
      reductions: reductions_after - reductions_before,
      queued: queued,
      queued_by_tag: queued_by_tag(shard)
    }
  end

  defp queued_by_tag(shard) do
    {:messages, messages} = Process.info(shard, :messages)

    messages
    |> Enum.frequencies_by(fn
      {:group_replica_batch, _node, [message | _]} -> {:group_replica_batch, elem(message, 0)}
      {:group_replica_frame, _node, message} -> {:group_replica_frame, elem(message, 0)}
      message when is_tuple(message) -> elem(message, 0)
      message -> message
    end)
  end

  @doc """
  Like `drain/5`, with a caller issuing synchronous local joins and leaves for
  the whole drain. Adds the number of local requests served and their latency.
  """
  def drain_with_live_writer(name, shard, stream, target, timeout \\ 600_000) do
    parent = self()

    writer =
      spawn(fn ->
        receive do
          :go -> live_writer_loop(parent, name, 0, [])
        end
      end)

    send(writer, :go)
    result = drain(name, shard, stream, target, timeout)
    send(writer, {:stop, self()})

    receive do
      {:writer_done, count, latencies} ->
        sorted = Enum.sort(latencies)

        Map.merge(result, %{
          local_requests: count,
          local_p50_micros: percentile(sorted, 0.5),
          local_max_micros: List.last(sorted)
        })
    after
      30_000 -> Map.put(result, :local_requests, :writer_timeout)
    end
  end

  defp live_writer_loop(parent, name, count, latencies) do
    receive do
      {:stop, reply_to} -> send(reply_to, {:writer_done, count, latencies})
    after
      0 ->
        key = "backlog/live/#{rem(count, 64)}"
        started = System.monotonic_time(:microsecond)
        :ok = Group.join(name, key, %{}, timeout: 60_000)
        :ok = Group.leave(name, key, timeout: 60_000)
        elapsed = System.monotonic_time(:microsecond) - started
        live_writer_loop(parent, name, count + 2, [div(elapsed, 2) | latencies])
    end
  end

  defp percentile([], _fraction), do: nil
  defp percentile(sorted, fraction), do: Enum.at(sorted, trunc(length(sorted) * fraction))

  @doc """
  Drains to `target` under call-time tracing and returns the microseconds each
  `Group.Replica` function spent in its own body, largest first.
  """
  def profile(name, shard, stream, target, timeout \\ 600_000) do
    modules = [Replica, Data]

    for module <- modules do
      :erlang.trace_pattern({module, :_, :_}, true, [:local, :call_time, :call_count])
    end

    :erlang.trace(shard, true, [:call, :silent])
    result = drain(name, shard, stream, target, timeout)
    :erlang.trace(shard, false, [:call, :silent])

    rows =
      for module <- modules,
          {function, arity} <- module.module_info(:functions),
          {:call_time, [_ | _] = per_pid} <-
            [:erlang.trace_info({module, function, arity}, :call_time)],
          {_pid, count, seconds, micros} <- per_pid,
          count > 0 do
        {seconds * 1_000_000 + micros, count, {module, function, arity}}
      end

    for module <- modules do
      :erlang.trace_pattern({module, :_, :_}, false, [:local, :call_time, :call_count])
    end

    rows = rows |> Enum.uniq() |> Enum.sort(:desc)
    Map.put(result, :call_time, Enum.take(rows, 12))
  end

  defp await_cursor(name, stream, target, deadline) do
    cond do
      Data.replica_cursor(name, 0, stream) >= target ->
        :ok

      System.monotonic_time(:microsecond) > deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(1)
        await_cursor(name, stream, target, deadline)
    end
  end
end
