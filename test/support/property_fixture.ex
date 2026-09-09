defmodule Group.PropertyFixture do
  @moduledoc false

  import ExUnit.Callbacks

  # Invoke inside check all, not setup: failed examples and shrink candidates
  # must not share owners, subscriptions, tables, buffers, or lease timers.
  def with_group(name, opts, fun) do
    children =
      [{Group, Keyword.put(opts, :name, name)}] ++
        for id <- [:observer, 0, 1] do
          Supervisor.child_spec({Task, &actor_loop/0}, id: id)
        end

    id = {__MODULE__, name}

    supervisor =
      start_supervised!(%{
        id: id,
        start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
        type: :supervisor
      })

    try do
      actors = Map.new(Supervisor.which_children(supervisor), fn {id, pid, _, _} -> {id, pid} end)
      fun.(actors)
    after
      # Reverse child shutdown stops the subscriber before Registry. The ExUnit
      # process never subscribes, so Registry shutdown cannot abort shrinking.
      stop_supervised!(id)
      :persistent_term.erase({Group, name})
    end
  end

  def in_process(pid, fun) do
    ref = Process.monitor(pid)
    send(pid, {:run, self(), ref, fun})

    try do
      receive do
        {^ref, {:ok, result}} -> result
        {^ref, {:error, error, stacktrace}} -> reraise error, stacktrace
        {:DOWN, ^ref, :process, ^pid, reason} -> raise "property actor exited: #{inspect(reason)}"
      after
        5_000 -> raise "property actor timed out"
      end
    after
      Process.demonitor(ref, [:flush])
    end
  end

  defp actor_loop do
    # Selective receive preserves event messages until the observer drains them.
    receive do
      {:run, caller, ref, fun} ->
        result =
          try do
            {:ok, fun.()}
          rescue
            error -> {:error, error, __STACKTRACE__}
          end

        send(caller, {ref, result})
        actor_loop()
    end
  end

  def events_after_barrier(name, observer) do
    in_process(observer, fn ->
      for shard <- 0..(Group.get_config(name).num_shards - 1) do
        ref = make_ref()
        send(Group.Replica.shard_name(name, shard), {:group_dispatch, [self()], {:settled, ref}})

        receive do
          {:settled, ^ref} -> :ok
        after
          1_000 -> raise "property barrier timed out"
        end
      end

      drain_events(name)
    end)
  end

  defp drain_events(name) do
    receive do
      {:group, events, %{name: ^name}} -> events ++ drain_events(name)
    after
      0 -> []
    end
  end
end
