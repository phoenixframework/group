defmodule Group.PausingConflictResolver do
  @moduledoc false

  # Test-only deterministic scheduling point. The resolver still returns the
  # normal total rank; a marked claim merely waits until the test releases the
  # replica shard that is evaluating it.
  def resolve(_name, key, {pid, %{rank: rank} = meta, _time}, controller) do
    if Map.get(meta, :pause, false) do
      ref = make_ref()
      send(controller, {:conflict_resolver_waiting, self(), ref, key, pid})

      receive do
        {:continue_conflict_resolution, ^ref} -> :ok
      end
    end

    rank
  end
end
