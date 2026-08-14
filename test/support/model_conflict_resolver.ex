defmodule Group.ModelConflictResolver do
  @moduledoc false

  def resolve(_name, _key, {_pid, meta, _time}), do: Map.fetch!(meta, :rank)
end
