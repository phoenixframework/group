defmodule Group.TestConflictResolver do
  @moduledoc false

  # A compiled module for conflict resolution that records calls to an ETS table.
  # Must be in test/support/ so it's compiled to beam and available on peer nodes.

  def resolve(_name, _key, {_pid, _meta, time}), do: time
end
