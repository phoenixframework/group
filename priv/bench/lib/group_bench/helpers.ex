defmodule GroupBench.Helpers do
  @moduledoc """
  Timing, formatting, percentile math, and setup utilities for benchmarks.
  """

  @doc """
  Times a function call in microseconds. Returns {microseconds, result}.
  """
  def time_us(fun) do
    :timer.tc(fun)
  end

  @doc """
  Collects N timing samples by calling `fun` repeatedly.
  Validates each result outside its measured interval.
  Returns a sorted list of microsecond timings.
  """
  def collect_samples(n, fun, validate \\ fn _ -> :ok end) do
    Enum.map(1..n, fn _ ->
      {us, result} = :timer.tc(fun)
      :ok = validate.(result)
      us
    end)
    |> Enum.sort()
  end

  @doc """
  Returns the value at the given percentile (0-100) from a sorted list.
  """
  def percentile(sorted, p) when is_list(sorted) and p >= 0 and p <= 100 do
    len = length(sorted)

    if len == 0 do
      0
    else
      index = max(0, min(round(p / 100 * len) - 1, len - 1))
      Enum.at(sorted, index)
    end
  end

  @doc """
  Formats a number with comma separators.
  """
  def format_number(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.reverse()
    |> String.to_charlist()
    |> Enum.chunk_every(3)
    |> Enum.join(",")
    |> String.reverse()
  end

  def format_number(n) when is_float(n) do
    :erlang.float_to_binary(n, decimals: 1)
  end

  @doc """
  Prints a section header.
  """
  def header(text) do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("  #{text}")
    IO.puts(String.duplicate("=", 60))
  end

  @doc """
  Prints a sub-header.
  """
  def subheader(text) do
    IO.puts("\n  --- #{text} ---")
  end

  @doc """
  Reports throughput and latency stats from a sorted list of microsecond timings.
  """
  def report_latency(label, sorted_us) do
    count = length(sorted_us)
    total_us = Enum.sum(sorted_us)
    ops_sec = if total_us > 0, do: round(count * 1_000_000 / total_us), else: 0

    IO.puts("  #{label}")
    IO.puts("    ops/sec:  #{format_number(ops_sec)}")
    IO.puts("    p50:      #{percentile(sorted_us, 50)} µs")
    IO.puts("    p99:      #{percentile(sorted_us, 99)} µs")
    IO.puts("    max:      #{percentile(sorted_us, 100)} µs")
  end

  @doc """
  Reports wall-clock throughput, successful-call latency stats, and timeout/error counts.
  """
  def report_load_profile(label, successful_ops, wall_us, sorted_us, timeout_count, error_count) do
    attempted_ops = successful_ops + timeout_count + error_count
    ops_sec = if wall_us > 0, do: round(successful_ops * 1_000_000 / wall_us), else: 0

    IO.puts("  #{label}")

    IO.puts(
      "    total:     #{format_number(successful_ops)} successful / #{format_number(attempted_ops)} attempted in #{format_number(div(wall_us, 1000))} ms"
    )

    IO.puts("    ops/sec:   #{format_number(ops_sec)}")

    case sorted_us do
      [] ->
        IO.puts("    p50:       n/a")
        IO.puts("    p99:       n/a")
        IO.puts("    max:       n/a")

      _ ->
        IO.puts("    p50:       #{percentile(sorted_us, 50)} µs")
        IO.puts("    p99:       #{percentile(sorted_us, 99)} µs")
        IO.puts("    max:       #{percentile(sorted_us, 100)} µs")
    end

    IO.puts("    timeouts:  #{format_number(timeout_count)}")
    IO.puts("    errors:    #{format_number(error_count)}")
  end

  @doc """
  Reports throughput from total wall-clock time and operation count.
  """
  def report_throughput(label, count, wall_us) do
    ops_sec = if wall_us > 0, do: round(count * 1_000_000 / wall_us), else: 0

    IO.puts("  #{label}")

    IO.puts(
      "    total:    #{format_number(count)} ops in #{format_number(div(wall_us, 1000))} ms"
    )

    IO.puts("    ops/sec:  #{format_number(ops_sec)}")
  end

  @doc """
  Runs `fun` with a scenario-owned worker supervisor and a fresh Group instance.
  Both supervisors are stopped synchronously, even when the scenario raises.
  """
  def with_group(opts, fun) do
    opts = Keyword.put_new(opts, :name, :bench)
    opts = Keyword.put_new(opts, :log, false)
    {:ok, workers} = Task.Supervisor.start_link()

    try do
      {:ok, sup} = Group.start_link(opts)

      try do
        fun.(workers)
      after
        # Registry links subscribers to its partition. Remove this scenario's
        # subscriptions before shutdown instead of trapping and discarding exits.
        registry = Group.registry_name(Keyword.fetch!(opts, :name))
        Enum.each(Registry.keys(registry, self()), &Registry.unregister(registry, &1))
        Supervisor.stop(sup)
      end
    after
      Supervisor.stop(workers)
    end
  end

  @doc """
  Provisions N workers, then times releasing them and waiting for successful ops.

  Workers stay alive until the scenario supervisor stops. Provisioning is outside
  the timed interval, so the worker supervisor is not a throughput bottleneck.
  `after_ready` optionally waits for event delivery within the same interval.
  Returns `{microseconds, pids}`.
  """
  def run_workers(supervisor, n, operation, after_ready \\ fn _ -> :ok end, timeout \\ 10_000)
      when n > 0 do
    parent = self()
    ref = make_ref()

    workers =
      for i <- 1..n do
        {:ok, pid} =
          Task.Supervisor.start_child(supervisor, fn ->
            receive do
              {:run, ^ref} ->
                result = operation.(i)
                send(parent, {ref, self(), result})
                Process.sleep(:infinity)
            end
          end)

        {pid, Process.monitor(pid)}
      end

    pids = Enum.map(workers, &elem(&1, 0))
    monitors = Map.new(workers)

    try do
      {us, _} =
        time_us(fn ->
          deadline = System.monotonic_time(:millisecond) + timeout
          Enum.each(pids, &send(&1, {:run, ref}))
          await_workers(monitors, monitors, ref, deadline)
          :ok = after_ready.(pids)
        end)

      {us, pids}
    after
      Enum.each(workers, fn {_pid, monitor} -> Process.demonitor(monitor, [:flush]) end)
    end
  end

  defp await_workers(pending, _monitors, _ref, _deadline) when map_size(pending) == 0, do: :ok

  defp await_workers(pending, monitors, ref, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^ref, pid, :ok} when is_map_key(pending, pid) ->
        await_workers(Map.delete(pending, pid), monitors, ref, deadline)

      {^ref, pid, result} when is_map_key(pending, pid) ->
        raise "benchmark worker #{inspect(pid)} returned #{inspect(result)}, expected :ok"

      {:DOWN, monitor, :process, pid, reason}
      when is_map_key(monitors, pid) and :erlang.map_get(pid, monitors) == monitor ->
        raise "benchmark worker #{inspect(pid)} exited: #{inspect(reason)}"
    after
      remaining -> raise "timed out waiting for #{map_size(pending)} benchmark workers"
    end
  end

  @doc "Checks the exact registry dataset before reporting a benchmark result."
  def verify_registry(name, entries, opts \\ []) do
    count = Group.local_registry_count(name, opts)

    if count != length(entries) do
      raise "unexpected registry count: expected #{length(entries)}, got #{count}"
    end

    for {key, pid, meta} <- entries do
      unless Group.lookup(name, key, opts) == {pid, meta} do
        raise "unexpected registration for #{inspect(key)}"
      end
    end

    :ok
  end

  @doc "Checks exact group membership, including metadata and duplicate entries."
  def verify_members(name, entries, opts \\ []) do
    for {key, expected} <-
          Enum.group_by(entries, &elem(&1, 0), fn {_, pid, meta} -> {pid, meta} end) do
      unless Enum.sort(Group.members(name, key, opts)) == Enum.sort(expected) do
        raise "unexpected memberships for #{inspect(key)}"
      end
    end

    :ok
  end

  @doc "Requires exactly the expected registration events, with a bounded total wait."
  def await_registered_events(name, cluster, entries, timeout \\ 5_000) do
    pending = MapSet.new(entries)

    if MapSet.size(pending) != length(entries) do
      raise "duplicate expected registration events"
    end

    deadline = System.monotonic_time(:millisecond) + timeout
    receive_registered_events(name, cluster, pending, deadline)
  end

  defp receive_registered_events(name, cluster, pending, deadline) do
    timeout =
      if MapSet.size(pending) == 0,
        do: 0,
        else: max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:group, events, %{name: ^name}} ->
        pending =
          Enum.reduce(events, pending, fn
            %Group.Event{
              supervisor: ^name,
              cluster: ^cluster,
              type: :registered,
              previous_meta: nil,
              reason: nil,
              key: key,
              pid: pid,
              meta: meta
            } = event,
            pending ->
              entry = {key, pid, meta}

              unless MapSet.member?(pending, entry) do
                raise "unexpected or duplicate registration event: #{inspect(event)}"
              end

              MapSet.delete(pending, entry)

            event, _pending ->
              raise "unexpected registration event: #{inspect(event)}"
          end)

        receive_registered_events(name, cluster, pending, deadline)
    after
      timeout ->
        if MapSet.size(pending) != 0 do
          raise "timed out waiting for #{MapSet.size(pending)} registration events"
        end

        :ok
    end
  end

  @doc """
  Runs `fun` as warmup for `n` iterations (discards results).
  """
  def warmup(n, fun) do
    Enum.each(1..n, fn _ -> fun.() end)
  end
end
